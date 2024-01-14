package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

type InputBuffer struct {
	buffer       string
	bufferLength int
	inputLength  int
}

func newInputBuffer() *InputBuffer {
	return &InputBuffer{
		buffer:       "",
		bufferLength: 0,
		inputLength:  0,
	}
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

func closeInputBuffer(inputBuffer *InputBuffer) {
	inputBuffer = nil
}

func main() {
	inputBuffer := newInputBuffer()
	reader := bufio.NewReader(os.Stdin)
	for {
		printPrompt()
		readInput(reader, inputBuffer)
		if len(inputBuffer.buffer) == 0 {
			continue
		}

		if inputBuffer.buffer == ".exit" {
			closeInputBuffer(inputBuffer)
			os.Exit(0)
		} else {
			fmt.Printf("Unrecognized command '%s'.\n", inputBuffer.buffer)
		}
	}
}
