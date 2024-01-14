package main

import (
	"bufio"
	"fmt"
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

type Row struct {
	id       uint32
	username [COLUMN_USERNAME_SIZE + 1]byte
	email    [COLUMN_EMAIL_SIZE + 1]byte
}

type Table struct {
	numRows uint32
	pages   [TABLE_MAX_PAGES][]byte
}

type InputBuffer struct {
	buffer       []byte
	bufferLength int
	inputLength  int
}

type StatementType int

const (
	STATEMENT_INSERT StatementType = iota
	STATEMENT_SELECT
)

type Statement struct {
	stmtType    StatementType
	rowToInsert Row
}

type ExecuteResult int

const (
	EXECUTE_SUCCESS ExecuteResult = iota
	EXECUTE_TABLE_FULL
)

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

func newInputBuffer() *InputBuffer {
	return &InputBuffer{
		buffer: make([]byte, 0),
	}
}

func newTable() *Table {
	table := &Table{
		numRows: 0,
	}
	for i := 0; i < TABLE_MAX_PAGES; i++ {
		table.pages[i] = nil
	}
	return table
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
	pageNum := rowNum / ROWS_PER_PAGE
	page := table.pages[pageNum]
	if page == nil {
		page = make([]byte, PAGE_SIZE)
		table.pages[pageNum] = page
	}
	rowOffset := rowNum % ROWS_PER_PAGE
	byteOffset := rowOffset * ROW_SIZE
	return page[byteOffset : byteOffset+ROW_SIZE]
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
	// Remove newline character
	input = strings.TrimSpace(input)

	inputBuffer.buffer = []byte(input)
	inputBuffer.inputLength = len(inputBuffer.buffer)
}

func closeInputBuffer(inputBuffer *InputBuffer) {
	// Go has automatic garbage collection, so no explicit freeing is needed
}

func doMetaCommand(inputBuffer *InputBuffer, table *Table) MetaCommandResult {
	switch string(inputBuffer.buffer) {
	case ".exit":
		closeInputBuffer(inputBuffer)
		os.Exit(0)
	default:
		return META_COMMAND_UNRECOGNIZED_COMMAND
	}
	return META_COMMAND_SUCCESS
}

func prepareInsert(inputBuffer *InputBuffer, statement *Statement) PrepareResult {
	statement.stmtType = STATEMENT_INSERT

	tokens := strings.Fields(string(inputBuffer.buffer))

	if len(tokens) != 4 {
		return PREPARE_SYNTAX_ERROR
	}

	id, err := strconv.Atoi(tokens[1])
	if err != nil || id < 0 {
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
	tokens := strings.Fields(string(inputBuffer.buffer))

	if len(tokens) == 0 {
		return PREPARE_SUCCESS
	}

	switch tokens[0] {
	case "insert":
		return prepareInsert(inputBuffer, statement)
	case "select":
		statement.stmtType = STATEMENT_SELECT
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
	switch statement.stmtType {
	case STATEMENT_INSERT:
		return executeInsert(statement, table)
	case STATEMENT_SELECT:
		return executeSelect(statement, table)
	default:
		return EXECUTE_SUCCESS
	}
}

func main() {
	table := newTable()
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
			break
		case EXECUTE_TABLE_FULL:
			fmt.Println("Error: Table full.")
			break
		}
	}
}
