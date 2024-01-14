package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"unsafe"
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
	PREPARE_SYNTAX_ERROR
	PREPARE_UNRECOGNIZED_STATEMENT
)

type StatementType int

const (
	STATEMENT_INSERT StatementType = iota
	STATEMENT_SELECT
)

const (
	COLUMN_USERNAME_SIZE = 32
	COLUMN_EMAIL_SIZE    = 255
)

type Row struct {
	id       uint32
	username [COLUMN_USERNAME_SIZE]byte
	email    [COLUMN_EMAIL_SIZE]byte
}

type Statement struct {
	Type        StatementType
	rowToInsert Row //only used by insert statement
}

const (
	ID_SIZE         = int(unsafe.Sizeof(uint32(0)))
	USERNAME_SIZE   = int(unsafe.Sizeof([COLUMN_USERNAME_SIZE]byte{}))
	EMAIL_SIZE      = int(unsafe.Sizeof([COLUMN_EMAIL_SIZE]byte{}))
	ID_OFFSET       = 0
	USERNAME_OFFSET = ID_OFFSET + ID_SIZE
	EMAIL_OFFSET    = USERNAME_OFFSET + USERNAME_SIZE
	ROW_SIZE        = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE
)

const (
	PAGE_SIZE       = 4096
	TABLE_MAX_PAGES = 100
	ROWS_PER_PAGE   = PAGE_SIZE / ROW_SIZE
	TABLE_MAX_ROWS  = ROWS_PER_PAGE * TABLE_MAX_PAGES
)

type Table struct {
	numRows uint32
	pages   [TABLE_MAX_PAGES][]byte
}

func printRow(row *Row) {
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

func rowSlot(table *Table, rowNum uint32) []byte {
	pageNum := rowNum / uint32(ROWS_PER_PAGE)
	page := table.pages[pageNum]
	if page == nil {
		page = make([]byte, PAGE_SIZE)
		table.pages[pageNum] = page
	}
	rowOffset := rowNum % uint32(ROWS_PER_PAGE)
	byteOffset := rowOffset * uint32(ROW_SIZE)
	return page[byteOffset : byteOffset+uint32(ROW_SIZE)]
}

func newTable() *Table {
	table := new(Table)
	table.numRows = 0
	return table
}

func freeTable(table *Table) {
	for i := 0; i < TABLE_MAX_PAGES; i++ {
		if table.pages[i] != nil {
			table.pages[i] = nil
		}
	}
}

func doMetaCommand(inputBuffer *InputBuffer, table *Table) MetaCommandResult {
	if inputBuffer.buffer == ".exit" {
		closeInputBuffer(inputBuffer)
		freeTable(table)
		os.Exit(0)
	}
	return META_COMMAND_UNRECOGNIZED_COMMAND
}
func BytesToString(b []byte) string {
	p := unsafe.SliceData(b)
	return unsafe.String(p, len(b))
}
func StringToBytes(s string) []byte {
	p := unsafe.StringData(s)
	b := unsafe.Slice(p, len(s))
	return b
}

func prepareStatement(inputBuffer *InputBuffer, statement *Statement) PrepareResult {
	if strings.HasPrefix(inputBuffer.buffer, "insert") {
		statement.Type = STATEMENT_INSERT
		var username string
		var email string
		// chatGPT generate code, need debug
		//argsAssigned, _ := fmt.Sscanf(inputBuffer.buffer, "insert %d %s %s", &statement.rowToInsert.id, &statement.rowToInsert.username, &statement.rowToInsert.email)
		argsAssigned, _ := fmt.Sscanf(inputBuffer.buffer, "insert %d %s %s", &statement.rowToInsert.id, &username, &email)
		if argsAssigned < 3 {
			return PREPARE_SYNTAX_ERROR
		}
		copy(statement.rowToInsert.username[:], StringToBytes(username))
		copy(statement.rowToInsert.email[:], StringToBytes(email))
		return PREPARE_SUCCESS
	}
	if inputBuffer.buffer == "select" {
		statement.Type = STATEMENT_SELECT
		return PREPARE_SUCCESS
	}

	return PREPARE_UNRECOGNIZED_STATEMENT
}

func executeInsert(statement *Statement, table *Table) error {
	if table.numRows >= uint32(TABLE_MAX_ROWS) {
		err := fmt.Errorf("Error: Table full.")
		return err
	}

	rowToInsert := &statement.rowToInsert

	serializeRow(rowToInsert, rowSlot(table, table.numRows))
	table.numRows++
	return nil
}

func executeSelect(table *Table) {
	var row Row
	for i := uint32(0); i < table.numRows; i++ {
		deserializeRow(rowSlot(table, i), &row)
		printRow(&row)
	}
}

func executeStatement(statement *Statement, table *Table) error {
	switch statement.Type {
	case STATEMENT_INSERT:
		return executeInsert(statement, table)
	case STATEMENT_SELECT:
		executeSelect(table)
	}
	return nil
}

func closeInputBuffer(inputBuffer *InputBuffer) {
	inputBuffer = nil
}

func printPrompt() {
	fmt.Print("db > ")
}

func readInput(reader *bufio.Reader, inputBuffer *InputBuffer) {
	// chatGPT init error, need to debug
	//reader := bufio.NewReader(os.Stdin)
	input, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Error reading input: ", err.Error())
		os.Exit(1)
	}

	input = strings.TrimSuffix(input, "\n")
	inputBuffer.inputLength = len(input)
	inputBuffer.buffer = input
}

func main() {
	table := newTable()
	inputBuffer := new(InputBuffer)
	reader := bufio.NewReader(os.Stdin)
	for {
		printPrompt()
		readInput(reader, inputBuffer)
		if len(inputBuffer.buffer) == 0 {
			continue
		}

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
		case PREPARE_SYNTAX_ERROR:
			fmt.Println("Syntax error. Could not parse statement.")
			continue
		case PREPARE_UNRECOGNIZED_STATEMENT:
			fmt.Printf("Unrecognized keyword at start of '%s'\n", inputBuffer.buffer)
			continue
		}

		if err := executeStatement(&statement, table); err != nil {
			fmt.Println(err.Error())
			continue
		}
		fmt.Println("Executed.")
	}
}
