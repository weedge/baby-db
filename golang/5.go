package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"unsafe"
)

const (
	COLUMN_USERNAME_SIZE = 32
	COLUMN_EMAIL_SIZE    = 255
	ID_SIZE              = 4
	USERNAME_SIZE        = COLUMN_USERNAME_SIZE + 1
	EMAIL_SIZE           = COLUMN_EMAIL_SIZE + 1
	ID_OFFSET            = 0
	USERNAME_OFFSET      = ID_OFFSET + ID_SIZE
	EMAIL_OFFSET         = USERNAME_OFFSET + USERNAME_SIZE
	ROW_SIZE             = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE
	PAGE_SIZE            = 4096
	TABLE_MAX_PAGES      = 100
	ROWS_PER_PAGE        = PAGE_SIZE / ROW_SIZE
	TABLE_MAX_ROWS       = ROWS_PER_PAGE * TABLE_MAX_PAGES
)

type InputBuffer struct {
	buffer       string
	bufferLength int
	inputLength  int
}

type MetaCommandResult int

const (
	META_COMMAND_SUCCESS MetaCommandResult = iota
	META_COMMAND_UNRECOGNIZED_COMMAND
)

type PrepareResult int

const (
	PREPARE_SUCCESS PrepareResult = iota
	PREPARE_NEGATIVE_ID
	PREPARE_STRING_TOO_LONG
	PREPARE_SYNTAX_ERROR
	PREPARE_UNRECOGNIZED_STATEMENT
)

type StatementType int

const (
	STATEMENT_INSERT StatementType = iota
	STATEMENT_SELECT
)

type Row struct {
	id       uint32
	username [COLUMN_USERNAME_SIZE + 1]byte
	email    [COLUMN_EMAIL_SIZE + 1]byte
}

type Statement struct {
	typ         StatementType
	rowToInsert Row
}

type Pager struct {
	fileDescriptor *os.File
	fileLength     uint32
	pages          [TABLE_MAX_PAGES][]byte
}

type Table struct {
	numRows uint32
	pager   *Pager
}

type ExecuteResult int

const (
	EXECUTE_SUCCESS ExecuteResult = iota
	EXECUTE_TABLE_FULL
)

func newInputBuffer() *InputBuffer {
	buffer := ""
	return &InputBuffer{
		buffer:       buffer,
		bufferLength: 0,
		inputLength:  0,
	}
}

func printRow(row *Row) {
	//fmt.Printf("(%d, %s, %s)\n", row.id, row.username, row.email)
	fmt.Printf("(%d, %s, %s)\n", row.id, strings.TrimRight(string(row.username[:]), "\x00"), strings.TrimRight(string(row.email[:]), "\x00"))
}

func serializeRow(source *Row, destination []byte) {
	copy(destination[ID_OFFSET:], (*(*[ID_SIZE]byte)(unsafe.Pointer(&source.id)))[:])
	copy(destination[USERNAME_OFFSET:], source.username[:])
	copy(destination[EMAIL_OFFSET:], source.email[:])
}

func deserializeRow(source []byte, destination *Row) {
	destination.id = *(*uint32)(unsafe.Pointer(&source[ID_OFFSET]))
	copy(destination.username[:], source[USERNAME_OFFSET:USERNAME_OFFSET+USERNAME_SIZE])
	copy(destination.email[:], source[EMAIL_OFFSET:EMAIL_OFFSET+EMAIL_SIZE])
}

func getPage(pager *Pager, pageNum uint32) []byte {
	if pageNum > TABLE_MAX_PAGES {
		fmt.Printf("Tried to fetch page number out of bounds. %d > %d\n", pageNum, TABLE_MAX_PAGES)
		os.Exit(1)
	}

	if pager.pages[pageNum] == nil {
		page := make([]byte, PAGE_SIZE)
		numPages := pager.fileLength / PAGE_SIZE

		if pager.fileLength%PAGE_SIZE != 0 {
			numPages++
		}

		if pageNum <= numPages {
			_, err := pager.fileDescriptor.Seek(int64(pageNum*PAGE_SIZE), os.SEEK_SET)
			if err != nil {
				fmt.Printf("Error seeking: %v\n", err)
				os.Exit(1)
			}

			_, err = pager.fileDescriptor.Read(page)
			if err != nil && err != io.EOF {
				fmt.Printf("Error reading file: %v\n", err)
				os.Exit(1)
			}
		}

		pager.pages[pageNum] = page
	}

	return pager.pages[pageNum]
}

func rowSlot(table *Table, rowNum uint32) []byte {
	pageNum := rowNum / ROWS_PER_PAGE
	page := getPage(table.pager, pageNum)
	rowOffset := rowNum % ROWS_PER_PAGE
	byteOffset := rowOffset * ROW_SIZE
	return page[byteOffset : byteOffset+ROW_SIZE]
}

func pagerOpen(filename string) *Pager {
	fileDescriptor, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		fmt.Printf("Unable to open file: %v\n", err)
		os.Exit(1)
	}

	fileLength, err := fileDescriptor.Seek(0, os.SEEK_END)
	if err != nil {
		fmt.Printf("Error seeking: %v\n", err)
		os.Exit(1)
	}

	pager := &Pager{
		fileDescriptor: fileDescriptor,
		fileLength:     uint32(fileLength),
	}

	for i := 0; i < TABLE_MAX_PAGES; i++ {
		pager.pages[i] = nil
	}

	return pager
}

func dbOpen(filename string) *Table {
	pager := pagerOpen(filename)
	numRows := pager.fileLength / ROW_SIZE

	table := &Table{
		numRows: numRows,
		pager:   pager,
	}

	return table
}

func pagerFlush(pager *Pager, pageNum uint32, size uint32) {
	if pager.pages[pageNum] == nil {
		fmt.Printf("Tried to flush null page\n")
		os.Exit(1)
	}

	offset, err := pager.fileDescriptor.Seek(int64(pageNum*PAGE_SIZE), os.SEEK_SET)
	if err != nil {
		fmt.Printf("Error seeking: %v\n", err)
		os.Exit(1)
	}

	if offset != int64(pageNum*PAGE_SIZE) {
		fmt.Printf("Seek offset does not match page start\n")
		os.Exit(1)
	}

	_, err = pager.fileDescriptor.Write(pager.pages[pageNum][:size])
	if err != nil {
		fmt.Printf("Error writing: %v\n", err)
		os.Exit(1)
	}
}

func dbClose(table *Table) {
	pager := table.pager
	numFullPages := table.numRows / ROWS_PER_PAGE

	for i := uint32(0); i < numFullPages; i++ {
		if pager.pages[i] == nil {
			continue
		}
		pagerFlush(pager, i, PAGE_SIZE)
		pager.pages[i] = nil
	}

	numAdditionalRows := table.numRows % ROWS_PER_PAGE
	if numAdditionalRows > 0 {
		pageNum := numFullPages
		if pager.pages[pageNum] != nil {
			pagerFlush(pager, pageNum, numAdditionalRows*ROW_SIZE)
			pager.pages[pageNum] = nil
		}
	}

	err := pager.fileDescriptor.Close()
	if err != nil {
		fmt.Printf("Error closing db file: %v\n", err)
		os.Exit(1)
	}

	for i := 0; i < TABLE_MAX_PAGES; i++ {
		page := pager.pages[i]
		if page != nil {
			pager.pages[i] = nil
		}
	}

	os.Exit(0)
}

func printPrompt() {
	fmt.Print("db > ")
}

func readInput(reader *bufio.Reader, inputBuffer *InputBuffer) {
	// chatGPT init error, need to debug
	//reader := bufio.NewReader(os.Stdin)
	buffer, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Error reading input: ", err.Error())
		os.Exit(1)
	}

	// Ignore newline character
	buffer = buffer[:len(buffer)-1]
	inputBuffer.inputLength = len(buffer)
	inputBuffer.buffer = buffer
}

func closeInputBuffer(inputBuffer *InputBuffer) {
	inputBuffer.buffer = ""
}

func doMetaCommand(inputBuffer *InputBuffer, table *Table) MetaCommandResult {
	if inputBuffer.buffer == ".exit" {
		closeInputBuffer(inputBuffer)
		dbClose(table)
		return META_COMMAND_SUCCESS
	} else {
		return META_COMMAND_UNRECOGNIZED_COMMAND
	}
}

func prepareInsert(inputBuffer *InputBuffer, statement *Statement) PrepareResult {
	statement.typ = STATEMENT_INSERT

	tokens := strings.Fields(inputBuffer.buffer)
	if len(tokens) != 4 {
		return PREPARE_SYNTAX_ERROR
	}

	id, err := strconv.Atoi(tokens[1])
	if err != nil {
		return PREPARE_NEGATIVE_ID
	}

	if id < 0 {
		return PREPARE_NEGATIVE_ID
	}

	if len(tokens[2]) > COLUMN_USERNAME_SIZE || len(tokens[3]) > COLUMN_EMAIL_SIZE {
		return PREPARE_STRING_TOO_LONG
	}

	statement.rowToInsert.id = uint32(id)
	copy(statement.rowToInsert.username[:], tokens[2])
	copy(statement.rowToInsert.email[:], tokens[3])

	return PREPARE_SUCCESS
}

func prepareStatement(inputBuffer *InputBuffer, statement *Statement) PrepareResult {
	tokens := strings.Fields(inputBuffer.buffer)

	if len(tokens) == 0 {
		return PREPARE_UNRECOGNIZED_STATEMENT
	}

	switch tokens[0] {
	case "insert":
		return prepareInsert(inputBuffer, statement)
	case "select":
		statement.typ = STATEMENT_SELECT
		return PREPARE_SUCCESS
	default:
		return PREPARE_UNRECOGNIZED_STATEMENT
	}
}

func executeInsert(statement *Statement, table *Table) ExecuteResult {
	if table.numRows >= TABLE_MAX_ROWS {
		return EXECUTE_TABLE_FULL
	}

	rowToInsert := &statement.rowToInsert

	serializeRow(rowToInsert, rowSlot(table, table.numRows))
	table.numRows++

	return EXECUTE_SUCCESS
}

func executeSelect(statement *Statement, table *Table) ExecuteResult {
	var row Row
	for i := uint32(0); i < table.numRows; i++ {
		deserializeRow(rowSlot(table, i), &row)
		printRow(&row)
	}
	return EXECUTE_SUCCESS
}

func executeStatement(statement *Statement, table *Table) ExecuteResult {
	switch statement.typ {
	case STATEMENT_INSERT:
		return executeInsert(statement, table)
	case STATEMENT_SELECT:
		return executeSelect(statement, table)
	default:
		return EXECUTE_SUCCESS
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Must supply a database filename.")
		os.Exit(1)
	}

	filename := os.Args[1]
	table := dbOpen(filename)

	inputBuffer := newInputBuffer()
	reader := bufio.NewReader(os.Stdin)
	for {
		printPrompt()
		readInput(reader, inputBuffer)

		if inputBuffer.buffer[0] == '.' {
			switch doMetaCommand(inputBuffer, table) {
			case META_COMMAND_SUCCESS:
				continue
			case META_COMMAND_UNRECOGNIZED_COMMAND:
				fmt.Printf("Unrecognized command '%s'\n", inputBuffer.buffer)
				continue
			}
		}

		var statement Statement
		switch prepareStatement(inputBuffer, &statement) {
		case PREPARE_SUCCESS:
			break
		case PREPARE_NEGATIVE_ID:
			fmt.Println("ID must be positive.")
			continue
		case PREPARE_STRING_TOO_LONG:
			fmt.Println("String is too long.")
			continue
		case PREPARE_SYNTAX_ERROR:
			fmt.Println("Syntax error. Could not parse statement.")
			continue
		case PREPARE_UNRECOGNIZED_STATEMENT:
			fmt.Printf("Unrecognized keyword at start of '%s'.\n", inputBuffer.buffer)
			continue
		}

		switch executeStatement(&statement, table) {
		case EXECUTE_SUCCESS:
			fmt.Println("Executed.")
		case EXECUTE_TABLE_FULL:
			fmt.Println("Error: Table full.")
		}
	}
}
