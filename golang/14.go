package main

import (
	"bufio"
	"fmt"
	"io"
	"math"
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

/* 为了测试，保持较小 */
const INTERNAL_NODE_MAX_CELLS = 3

const INVALID_PAGE_NUM = math.MaxUint32

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

func nodeParent(node []byte) *uint32 {
	return (*uint32)(unsafe.Pointer(&node[PARENT_POINTER_OFFSET]))
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
	/*
	  由于根页码是0，因此在初始化内部节点时，如果不将其右子节点初始化为无效的页码，可能会导致右子节点为0，这将使该节点成为根节点的父节点。
	*/
	*internalNodeRightChild(node) = INVALID_PAGE_NUM
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
		rightChild := internalNodeRightChild(node)
		if *rightChild == INVALID_PAGE_NUM {
			fmt.Printf("Tried to access right child of node, but was invalid page\n")
			os.Exit(1)
		}
		return rightChild
	}
	child := internalNodeCell(node, childNum)
	if *child == INVALID_PAGE_NUM {
		fmt.Printf("Tried to access child %d of node, but was invalid page\n", childNum)
		os.Exit(1)
	}
	return child
}

func internalNodeKey(node []byte, keyNum uint32) *uint32 {
	offset := INTERNAL_NODE_HEADER_SIZE + keyNum*INTERNAL_NODE_CELL_SIZE + INTERNAL_NODE_CHILD_SIZE
	return (*uint32)(unsafe.Pointer(&node[offset]))
}

// 返回应包含给定键的子节点的索引。
func internalNodeFindChild(node []byte, key uint32) uint32 {
	numKeys := *internalNodeNumKeys(node)

	// Binary search
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

	return minIndex
}

func updateInternalNodeKey(node []byte, oldKey, newKey uint32) {
	oldChildIndex := internalNodeFindChild(node, oldKey)
	*internalNodeKey(node, oldChildIndex) = newKey
}

func getNodeMaxKey(pager *Pager, node []byte) uint32 {
	if getNodeType(node) == NODE_LEAF {
		return *leafNodeKey(node, *leafNodeNumCells(node)-1)
	}
	rightChild := getPage(pager, *internalNodeRightChild(node))
	return getNodeMaxKey(pager, rightChild)
}

// 处理根节点的拆分。
// 将旧根复制到新页，成为左子节点。
// 重新初始化根页以包含新根节点。
// 新根节点指向两个子节点。
func createNewRoot(table *Table, rightChildPageNum uint32) {
	root := getPage(table.pager, table.rootPageNum)
	rightChild := getPage(table.pager, rightChildPageNum)
	leftChildPageNum := getUnusedPageNum(table.pager)
	leftChild := getPage(table.pager, leftChildPageNum)

	if getNodeType(root) == NODE_INTERNAL {
		initializeInternalNode(rightChild)
		initializeInternalNode(leftChild)
	}

	// Left child has data copied from the old root
	copy(leftChild, root)
	setNodeRoot(leftChild, false)

	if getNodeType(leftChild) == NODE_INTERNAL {
		var child []byte
		for i := uint32(0); i < *internalNodeNumKeys(leftChild); i++ {
			child = getPage(table.pager, *internalNodeChild(leftChild, i))
			*nodeParent(child) = leftChildPageNum
		}
		child = getPage(table.pager, *internalNodeRightChild(leftChild))
		*nodeParent(child) = leftChildPageNum
	}

	// Root becomes a new internal node with one key and two children
	initializeInternalNode(root)
	setNodeRoot(root, true)
	*internalNodeNumKeys(root) = 1
	*internalNodeChild(root, 0) = leftChildPageNum
	leftChildMaxKey := getNodeMaxKey(table.pager, leftChild)

	*internalNodeKey(root, 0) = leftChildMaxKey
	*internalNodeRightChild(root) = rightChildPageNum
	*nodeParent(leftChild) = table.rootPageNum
	*nodeParent(rightChild) = table.rootPageNum
}

// 向父节点添加一个新的子节点/键对，对应于子节点
func internalNodeInsert(table *Table, parentPageNum, childPageNum uint32) {
	parent := getPage(table.pager, parentPageNum)
	child := getPage(table.pager, childPageNum)
	childMaxKey := getNodeMaxKey(table.pager, child)
	index := internalNodeFindChild(parent, childMaxKey)

	originalNumKeys := *internalNodeNumKeys(parent)
	if originalNumKeys >= INTERNAL_NODE_MAX_CELLS {
		internalNodeSplitAndInsert(table, parentPageNum, childPageNum)
		return
	}

	rightChildPageNum := *internalNodeRightChild(parent)
	// 具有右子节点为INVALID_PAGE_NUM的内部节点为空
	if rightChildPageNum == INVALID_PAGE_NUM {
		*internalNodeRightChild(parent) = childPageNum
		return
	}

	rightChild := getPage(table.pager, rightChildPageNum)
	/*
	  如果我们已经达到节点的最大单元格数，就不能在分裂之前递增。
	  在没有插入新的键/子节点对的情况下递增，并立即调用
	  `internal_node_split_and_insert` 会导致在 `(max_cells + 1)`
	  处创建一个新的键，其值未初始化。
	*/
	*internalNodeNumKeys(parent) = originalNumKeys + 1

	if childMaxKey > getNodeMaxKey(table.pager, rightChild) {
		// Replace right child
		*internalNodeChild(parent, originalNumKeys) = rightChildPageNum
		*internalNodeKey(parent, originalNumKeys) = getNodeMaxKey(table.pager, rightChild)
		*internalNodeRightChild(parent) = childPageNum
	} else {
		// Make space for the new cell
		for i := originalNumKeys; i > index; i-- {
			destination := internalNodeCell(parent, i)
			source := internalNodeCell(parent, i-1)
			// c: memcpy(destination, source, INTERNAL_NODE_CELL_SIZE);
			copy((*(*[INTERNAL_NODE_CELL_SIZE]byte)(unsafe.Pointer(destination)))[:], (*(*[INTERNAL_NODE_CELL_SIZE]byte)(unsafe.Pointer(source)))[:])
			//*internalNodeCell(parent, i) = *internalNodeCell(parent, i-1)
		}
		*internalNodeChild(parent, index) = childPageNum
		*internalNodeKey(parent, index) = childMaxKey
	}
}
func internalNodeSplitAndInsert(table *Table, parentPageNum, childPageNum uint32) {
	oldPageNum := parentPageNum
	oldNode := getPage(table.pager, parentPageNum)
	oldMax := getNodeMaxKey(table.pager, oldNode)

	child := getPage(table.pager, childPageNum)
	childMax := getNodeMaxKey(table.pager, child)

	newPageNum := getUnusedPageNum(table.pager)

	// Flag to indicate if we are splitting the root node
	// 这个简短的注释是chatGPT总结后加上的...
	splittingRoot := isNodeRoot(oldNode)

	var parent, newNode []byte
	if splittingRoot {
		createNewRoot(table, newPageNum)
		parent = getPage(table.pager, table.rootPageNum)
		// If splitting root, update oldNode to point to the left child of the new root
		oldPageNum = *internalNodeChild(parent, 0)
		oldNode = getPage(table.pager, oldPageNum)
	} else {
		parent = getPage(table.pager, *nodeParent(oldNode))
		newNode = getPage(table.pager, newPageNum)
		initializeInternalNode(newNode)
	}

	oldNumKeys := internalNodeNumKeys(oldNode)

	curPageNum := *internalNodeRightChild(oldNode)
	cur := getPage(table.pager, curPageNum)

	// Move the right child into the new node and set the right child of old node to INVALID_PAGE_NUM
	internalNodeInsert(table, newPageNum, curPageNum)
	*nodeParent(cur) = newPageNum
	*internalNodeRightChild(oldNode) = INVALID_PAGE_NUM

	// Move keys and child nodes to the new node until the middle key
	for i := INTERNAL_NODE_MAX_CELLS - 1; i > INTERNAL_NODE_MAX_CELLS/2; i-- {
		curPageNum = *internalNodeChild(oldNode, uint32(i))
		cur = getPage(table.pager, curPageNum)

		internalNodeInsert(table, newPageNum, curPageNum)
		*nodeParent(cur) = newPageNum

		(*oldNumKeys)--
	}

	// Set the right child of old node to the highest key before the middle key and decrement the number of keys
	*internalNodeRightChild(oldNode) = *internalNodeChild(oldNode, *oldNumKeys-1)
	(*oldNumKeys)--

	// Determine which of the split nodes should contain the child to be inserted
	maxAfterSplit := getNodeMaxKey(table.pager, oldNode)
	destinationPageNum := newPageNum

	if childMax < maxAfterSplit {
		destinationPageNum = oldPageNum
	}

	// Insert the child node into the appropriate split node
	internalNodeInsert(table, destinationPageNum, childPageNum)
	*nodeParent(child) = destinationPageNum

	// Update the parent node's key to reflect the new highest key in the old node
	updateInternalNodeKey(parent, oldMax, getNodeMaxKey(table.pager, oldNode))

	// If not splitting the root, insert the new node into its parent
	if !splittingRoot {
		internalNodeInsert(table, *nodeParent(oldNode), newPageNum)
		*nodeParent(newNode) = *nodeParent(oldNode)
	}
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
		if numKeys > 0 {
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

func readInput(reader *bufio.Reader, table *Table, inputBuffer *InputBuffer) {
	// chatGPT init error, need to debug
	//reader := bufio.NewReader(os.Stdin)
	buffer, err := reader.ReadString('\n')
	if err != nil {
		dbClose(table)
		if err == io.EOF {
			os.Exit(0)
		}
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

func getUnusedPageNum(pager *Pager) uint32 {
	return pager.numPages
}

// 创建一个新节点并将一半单元格移动过去。
// 在两个节点中的一个中插入新值。
// 更新父节点或创建一个新的父节点。
func leafNodeSplitAndInsert(cursor *Cursor, key uint32, value *Row) {
	oldNode := getPage(cursor.table.pager, cursor.pageNum)
	oldMax := getNodeMaxKey(cursor.table.pager, oldNode)
	newPageNum := getUnusedPageNum(cursor.table.pager)
	newNode := getPage(cursor.table.pager, newPageNum)
	initializeLeafNode(newNode)
	*nodeParent(newNode) = *nodeParent(oldNode)
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
		parentPageNum := *nodeParent(oldNode)
		newMax := getNodeMaxKey(cursor.table.pager, oldNode)
		parent := getPage(cursor.table.pager, parentPageNum)

		updateInternalNodeKey(parent, oldMax, newMax)
		internalNodeInsert(cursor.table, parentPageNum, newPageNum)
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
	i := 0
	for cursor.endOfTable == false {
		deserializeRow(cursorValue(cursor), &row)
		printRow(&row)
		cursorAdvance(cursor)
		i++
	}
	fmt.Printf("total_rows: %d\n", i)
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
		readInput(reader, table, inputBuffer)
		if inputBuffer.inputLength == 0 {
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
