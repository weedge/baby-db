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
)

type NodeType uint8

const (
	NODE_INTERNAL NodeType = iota
	NODE_LEAF
)

// Common Node Header Layout
const (
	NODE_TYPE_SIZE          = 1
	NODE_TYPE_OFFSET        = 0
	IS_ROOT_SIZE            = 1
	IS_ROOT_OFFSET          = NODE_TYPE_OFFSET + NODE_TYPE_SIZE
	PARENT_POINTER_SIZE     = 4
	PARENT_POINTER_OFFSET   = IS_ROOT_OFFSET + IS_ROOT_SIZE
	COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE
)

// Leaf Node Header Layout
const (
	LEAF_NODE_NUM_CELLS_SIZE   = 4
	LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE
	LEAF_NODE_NEXT_LEAF_SIZE   = 4
	LEAF_NODE_NEXT_LEAF_OFFSET = LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE
	LEAF_NODE_HEADER_SIZE      = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE + LEAF_NODE_NEXT_LEAF_SIZE
)

// Leaf Node Body Layout
const (
	LEAF_NODE_KEY_SIZE        = 4
	LEAF_NODE_KEY_OFFSET      = 0
	LEAF_NODE_VALUE_SIZE      = ROW_SIZE
	LEAF_NODE_VALUE_OFFSET    = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE
	LEAF_NODE_CELL_SIZE       = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE
	LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE
	LEAF_NODE_MAX_CELLS       = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE
)

/*
 * Leaf Node Split
 */
const LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) / 2
const LEAF_NODE_LEFT_SPLIT_COUNT = LEAF_NODE_MAX_CELLS + 1 - LEAF_NODE_RIGHT_SPLIT_COUNT

/*
 * Internal Node Header Layout
 */
const INTERNAL_NODE_NUM_KEYS_SIZE = 4
const INTERNAL_NODE_NUM_KEYS_OFFSET = COMMON_NODE_HEADER_SIZE
const INTERNAL_NODE_RIGHT_CHILD_SIZE = 4
const INTERNAL_NODE_RIGHT_CHILD_OFFSET = INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE
const INTERNAL_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE

/*
 * Internal Node Body Layout
 */
const INTERNAL_NODE_KEY_SIZE = 4
const INTERNAL_NODE_CHILD_SIZE = 4
const INTERNAL_NODE_CELL_SIZE = INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE

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
	numPages       uint32
	pages          [TABLE_MAX_PAGES][]byte
}

type Table struct {
	rootPageNum uint32
	pager       *Pager
}

type Cursor struct {
	table      *Table
	pageNum    uint32
	cellNum    uint32
	endOfTable bool // 表示最后一个元素之后的位置
}

type ExecuteResult int

const (
	EXECUTE_SUCCESS ExecuteResult = iota
	EXECUTE_TABLE_FULL
	EXECUTE_DUPLICATE_KEY
)

func newInputBuffer() *InputBuffer {
	buffer := ""
	return &InputBuffer{
		buffer:       buffer,
		bufferLength: 0,
		inputLength:  0,
	}
}

func leafNodeNumCells(node []byte) *uint32 {
	return (*uint32)(unsafe.Pointer(&node[LEAF_NODE_NUM_CELLS_OFFSET]))
}

func leafNodeCell(node []byte, cellNum uint32) []byte {
	offset := LEAF_NODE_HEADER_SIZE + cellNum*LEAF_NODE_CELL_SIZE
	return node[offset : offset+LEAF_NODE_CELL_SIZE]
}

func leafNodeKey(node []byte, cellNum uint32) *uint32 {
	offset := LEAF_NODE_HEADER_SIZE + cellNum*LEAF_NODE_CELL_SIZE
	return (*uint32)(unsafe.Pointer(&node[offset]))
}

func leafNodeValue(node []byte, cellNum uint32) []byte {
	offset := LEAF_NODE_HEADER_SIZE + cellNum*LEAF_NODE_CELL_SIZE + LEAF_NODE_KEY_SIZE
	return node[offset : offset+LEAF_NODE_VALUE_SIZE]
}

func leafNodeNextLeaf(node []byte) *uint32 {
	return (*uint32)(unsafe.Pointer(&node[LEAF_NODE_NEXT_LEAF_OFFSET]))
}

func printConstants() {
	fmt.Printf("ROW_SIZE: %d\n", ROW_SIZE)
	fmt.Printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE)
	fmt.Printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE)
	fmt.Printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE)
	fmt.Printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LEAF_NODE_SPACE_FOR_CELLS)
	fmt.Printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS)
}

func indent(level uint32) {
	for i := uint32(0); i < level; i++ {
		fmt.Print("  ")
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

func getNodeType(node []byte) NodeType {
	return NodeType(node[NODE_TYPE_OFFSET])
}

func setNodeType(node []byte, nodeType NodeType) {
	node[NODE_TYPE_OFFSET] = byte(nodeType)
}

func isNodeRoot(node []byte) bool {
	value := node[IS_ROOT_OFFSET]
	return value != 0
}

func setNodeRoot(node []byte, isRoot bool) {
	if isRoot {
		node[IS_ROOT_OFFSET] = 1
	} else {
		node[IS_ROOT_OFFSET] = 0
	}
}

func internalNodeNumKeys(node []byte) *uint32 {
	return (*uint32)(unsafe.Pointer(&node[INTERNAL_NODE_NUM_KEYS_OFFSET]))
}

func internalNodeRightChild(node []byte) *uint32 {
	return (*uint32)(unsafe.Pointer(&node[INTERNAL_NODE_RIGHT_CHILD_OFFSET]))
}

func internalNodeCell(node []byte, cellNum uint32) *uint32 {
	offset := INTERNAL_NODE_HEADER_SIZE + cellNum*INTERNAL_NODE_CELL_SIZE
	return (*uint32)(unsafe.Pointer(&node[offset]))
}

func initializeLeafNode(node []byte) {
	setNodeType(node, NODE_LEAF)
	setNodeRoot(node, false)
	*leafNodeNumCells(node) = 0
	*leafNodeNextLeaf(node) = 0 // 0 表示无兄弟节点
}

func initializeInternalNode(node []byte) {
	setNodeType(node, NODE_INTERNAL)
	setNodeRoot(node, false)
	*internalNodeNumKeys(node) = 0
}

func getPage(pager *Pager, pageNum uint32) []byte {
	if pageNum > TABLE_MAX_PAGES {
		fmt.Printf("Tried to fetch page number out of bounds. %d > %d\n", pageNum, TABLE_MAX_PAGES)
		os.Exit(1)
	}

	if pager.pages[pageNum] == nil {
		// Cache miss. Allocate memory and load from file.
		page := make([]byte, PAGE_SIZE)
		numPages := pager.fileLength / PAGE_SIZE

		// We might save a partial page at the end of the file
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
		if pageNum >= pager.numPages {
			pager.numPages = pageNum + 1
		}
	}

	return pager.pages[pageNum]
}

func internalNodeChild(node []byte, childNum uint32) *uint32 {
	numKeys := *internalNodeNumKeys(node)
	if childNum > numKeys {
		fmt.Printf("Tried to access childNum %d > numKeys %d\n", childNum, numKeys)
		os.Exit(1)
	}
	if childNum == numKeys {
		return internalNodeRightChild(node)
	}
	return internalNodeCell(node, childNum)
}

func internalNodeKey(node []byte, keyNum uint32) *uint32 {
	offset := INTERNAL_NODE_HEADER_SIZE + keyNum*INTERNAL_NODE_CELL_SIZE + INTERNAL_NODE_CHILD_SIZE
	return (*uint32)(unsafe.Pointer(&node[offset]))
}

func printTree(pager *Pager, pageNum, indentationLevel uint32) {
	node := getPage(pager, pageNum)
	numKeys, child := uint32(0), uint32(0)

	switch getNodeType(node) {
	case NODE_LEAF:
		numKeys = *leafNodeNumCells(node)
		indent(indentationLevel)
		fmt.Printf("- leaf (size %d)\n", numKeys)
		for i := uint32(0); i < numKeys; i++ {
			indent(indentationLevel + 1)
			fmt.Printf("- %d\n", *leafNodeKey(node, i))
		}
	case NODE_INTERNAL:
		numKeys = *internalNodeNumKeys(node)
		indent(indentationLevel)
		fmt.Printf("- internal (size %d)\n", numKeys)
		for i := uint32(0); i < numKeys; i++ {
			child = *internalNodeChild(node, i)
			printTree(pager, child, indentationLevel+1)

			indent(indentationLevel + 1)
			fmt.Printf("- key %d\n", *internalNodeKey(node, i))
		}
		child = *internalNodeRightChild(node)
		printTree(pager, child, indentationLevel+1)
	}
}

func leafNodeFind(table *Table, pageNum, key uint32) *Cursor {
	node := getPage(table.pager, pageNum)
	numCells := *leafNodeNumCells(node)
	cursor := &Cursor{table: table, pageNum: pageNum}

	// Binary search
	minIndex := uint32(0)
	onePastMaxIndex := numCells
	for onePastMaxIndex != minIndex {
		index := (minIndex + onePastMaxIndex) / 2
		keyAtIndex := *leafNodeKey(node, index)
		if key == keyAtIndex {
			cursor.cellNum = index
			return cursor
		}
		if key < keyAtIndex {
			onePastMaxIndex = index
		} else {
			minIndex = index + 1
		}
	}

	cursor.cellNum = minIndex
	return cursor
}

func internalNodeFind(table *Table, pageNum, key uint32) *Cursor {
	node := getPage(table.pager, pageNum)
	numKeys := *internalNodeNumKeys(node)

	// Binary search to find index of child to search
	minIndex := uint32(0)
	maxIndex := numKeys // there is one more child than key

	for minIndex != maxIndex {
		index := (minIndex + maxIndex) / 2
		keyToRight := *internalNodeKey(node, index)
		if keyToRight >= key {
			maxIndex = index
		} else {
			minIndex = index + 1
		}
	}

	childNum := *internalNodeChild(node, minIndex)
	child := getPage(table.pager, childNum)

	switch getNodeType(child) {
	case NODE_LEAF:
		return leafNodeFind(table, childNum, key)
	case NODE_INTERNAL:
		return internalNodeFind(table, childNum, key)
	default:
		// Handle other node types if needed
		return nil
	}
}

func tableFind(table *Table, key uint32) *Cursor {
	rootPageNum := table.rootPageNum
	rootNode := getPage(table.pager, rootPageNum)
	nodeType := getNodeType(rootNode)

	if nodeType == NODE_LEAF {
		return leafNodeFind(table, rootPageNum, key)
	} else {
		return internalNodeFind(table, rootPageNum, key)
	}
	return nil
}

func cursorValue(cursor *Cursor) []byte {
	pageNum := cursor.pageNum
	page := getPage(cursor.table.pager, pageNum)
	return leafNodeValue(page, cursor.cellNum)
}

func cursorAdvance(cursor *Cursor) {
	pageNum := cursor.pageNum
	node := getPage(cursor.table.pager, pageNum)
	cursor.cellNum += 1
	if cursor.cellNum >= *leafNodeNumCells(node) {
		/* 前进到下一个叶子节点 */
		nextPageNum := *leafNodeNextLeaf(node)
		if nextPageNum == 0 {
			/* 这是最右边的叶子节点 */
			cursor.endOfTable = true
		} else {
			cursor.pageNum = nextPageNum
			cursor.cellNum = 0
		}
	}
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
		numPages:       uint32(fileLength / PAGE_SIZE),
	}

	if fileLength%PAGE_SIZE != 0 {
		fmt.Printf("Db file is not a whole number of pages. Corrupt file.\n")
		os.Exit(1)
	}

	for i := 0; i < TABLE_MAX_PAGES; i++ {
		pager.pages[i] = nil
	}

	return pager
}

func dbOpen(filename string) *Table {
	pager := pagerOpen(filename)

	table := &Table{
		rootPageNum: 0,
		pager:       pager,
	}

	if pager.numPages == 0 {
		// New database file. Initialize page 0 as leaf node.
		rootNode := getPage(pager, 0)
		initializeLeafNode(rootNode)
		setNodeRoot(rootNode, true)
	}

	return table
}

func pagerFlush(pager *Pager, pageNum uint32) {
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

	_, err = pager.fileDescriptor.Write(pager.pages[pageNum][:PAGE_SIZE])
	if err != nil {
		fmt.Printf("Error writing: %v\n", err)
		os.Exit(1)
	}
}

func dbClose(table *Table) {
	pager := table.pager

	for i := uint32(0); i < pager.numPages; i++ {
		if pager.pages[i] == nil {
			continue
		}
		pagerFlush(pager, i)
		pager.pages[i] = nil
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
	} else if inputBuffer.buffer == ".btree" {
		fmt.Printf(("Tree:\n"))
		printTree(table.pager, 0, 0)
		return META_COMMAND_SUCCESS
	} else if inputBuffer.buffer == ".constants" {
		fmt.Printf(("Constants:\n"))
		printConstants()
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

func getNodeMaxKey(node []byte) uint32 {
	switch getNodeType(node) {
	case NODE_INTERNAL:
		numKeys := *internalNodeNumKeys(node)
		return *internalNodeKey(node, numKeys-1)
	case NODE_LEAF:
		numCells := *leafNodeNumCells(node)
		return *leafNodeKey(node, numCells-1)
	default:
		// Handle other node types if needed
		return 0 // or appropriate default value
	}
}

func getUnusedPageNum(pager *Pager) uint32 {
	return pager.numPages
}

func createNewRoot(table *Table, rightChildPageNum uint32) {
	root := getPage(table.pager, table.rootPageNum)
	//rightChild := getPage(table.pager, rightChildPageNum)
	leftChildPageNum := getUnusedPageNum(table.pager)
	leftChild := getPage(table.pager, leftChildPageNum)

	// Left child gets data copied from the old root
	copy(leftChild, root[:])
	setNodeRoot(leftChild, false)

	// Root becomes a new internal node with one key and two children
	initializeInternalNode(root)
	setNodeRoot(root, true)
	*internalNodeNumKeys(root) = 1
	*internalNodeChild(root, 0) = leftChildPageNum

	leftChildMaxKey := getNodeMaxKey(leftChild)
	*internalNodeKey(root, 0) = leftChildMaxKey
	*internalNodeRightChild(root) = rightChildPageNum
}

// 创建一个新节点并将一半单元格移动过去。
// 在两个节点中的一个中插入新值。
// 更新父节点或创建一个新的父节点。
func leafNodeSplitAndInsert(cursor *Cursor, key uint32, value *Row) {
	oldNode := getPage(cursor.table.pager, cursor.pageNum)
	newPageNum := getUnusedPageNum(cursor.table.pager)
	newNode := getPage(cursor.table.pager, newPageNum)
	initializeLeafNode(newNode)
	*leafNodeNextLeaf(newNode) = *leafNodeNextLeaf(oldNode)
	*leafNodeNextLeaf(oldNode) = newPageNum

	/*
	  所有现有键以及新键应该均匀分布
	  在旧（左）和新（右）节点之间。
	  从右侧开始，将每个键移动到正确的位置。
	*/
	for i := LEAF_NODE_MAX_CELLS; i >= 0; i-- {
		var destinationNode []byte
		if i >= LEAF_NODE_LEFT_SPLIT_COUNT {
			destinationNode = newNode
		} else {
			destinationNode = oldNode
		}
		indexWithinNode := i % LEAF_NODE_LEFT_SPLIT_COUNT
		destination := leafNodeCell(destinationNode, uint32(indexWithinNode))

		if i == int(cursor.cellNum) {
			serializeRow(value, leafNodeValue(destinationNode, uint32(indexWithinNode)))
			*leafNodeKey(destinationNode, uint32(indexWithinNode)) = key
		} else if i > int(cursor.cellNum) {
			copy(destination, leafNodeCell(oldNode, uint32(i-1))[:LEAF_NODE_CELL_SIZE])
		} else {
			copy(destination, leafNodeCell(oldNode, uint32(i))[:LEAF_NODE_CELL_SIZE])
		}
	}

	/* 在两个叶子节点上更新单元格计数 */
	*leafNodeNumCells(oldNode) = LEAF_NODE_LEFT_SPLIT_COUNT
	*leafNodeNumCells(newNode) = LEAF_NODE_RIGHT_SPLIT_COUNT
	if isNodeRoot(oldNode) {
		createNewRoot(cursor.table, newPageNum)
	} else {
		fmt.Println("Need to implement updating parent after split")
		os.Exit(1)
	}
}

func leafNodeInsert(cursor *Cursor, key uint32, value *Row) {
	node := getPage(cursor.table.pager, cursor.pageNum)

	numCells := *leafNodeNumCells(node)
	if numCells >= LEAF_NODE_MAX_CELLS {
		leafNodeSplitAndInsert(cursor, key, value)
		return
	}

	if cursor.cellNum < numCells {
		// Make room for new cell
		for i := numCells; i > cursor.cellNum; i-- {
			copy(leafNodeCell(node, i), leafNodeCell(node, i-1))
		}
	}

	*leafNodeNumCells(node) += 1
	*leafNodeKey(node, cursor.cellNum) = key
	serializeRow(value, leafNodeValue(node, cursor.cellNum))
}

func executeInsert(statement *Statement, table *Table) ExecuteResult {
	node := getPage(table.pager, table.rootPageNum)
	numCells := *leafNodeNumCells(node)

	rowToInsert := &statement.rowToInsert
	keyToInsert := rowToInsert.id
	cursor := tableFind(table, keyToInsert)
	if cursor.cellNum < numCells {
		keyAtIndex := *leafNodeKey(node, cursor.cellNum)
		if keyAtIndex == keyToInsert {
			return EXECUTE_DUPLICATE_KEY
		}
	}

	leafNodeInsert(cursor, rowToInsert.id, rowToInsert)

	return EXECUTE_SUCCESS
}

func tableStart(table *Table) *Cursor {
	cursor := tableFind(table, 0)
	node := getPage(table.pager, cursor.pageNum)
	numCells := *leafNodeNumCells(node)
	cursor.endOfTable = numCells == 0

	return cursor
}

func executeSelect(statement *Statement, table *Table) ExecuteResult {
	cursor := tableStart(table)
	var row Row
	for cursor.endOfTable == false {
		deserializeRow(cursorValue(cursor), &row)
		printRow(&row)
		cursorAdvance(cursor)
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
		case EXECUTE_DUPLICATE_KEY:
			fmt.Println("Error: Duplicate key.")
		}
	}
}
