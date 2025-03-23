package mr

import (
	"bufio"
	"bytes"
	"log"
	"math"
	"os"
	"strings"
	"syscall"
)

type Chunk struct {
	Filename string
	Offset   int64 // in bytes
	Size     int64 // in bytes
}

func readChunk(task Chunk) []string {
	lines := []string{}
	f, err := os.Open(task.Filename)
	if err != nil {
		log.Fatal("opening file:", err)
	}
	fi, err := f.Stat()
	if err != nil {
		log.Fatal("reading file stat:", err)
	}

	// when reading the last chunk, the null values are also read
	// to void considreing null values as a line, we slice the chunk
	// to only include valid lines
	toSlice := false
	if task.Offset+task.Size > fi.Size() {
		pageSize := os.Getpagesize()
		x := int64(math.Ceil(float64(fi.Size()-task.Offset) / float64(pageSize)))
		task.Size = int64(pageSize) * x
		toSlice = true
	}
	chunk, err := syscall.Mmap(int(f.Fd()), task.Offset,
		int(task.Size), syscall.PROT_READ, syscall.MAP_SHARED)
	defer syscall.Munmap(chunk)

	if err != nil {
		log.Fatal("mapping filechunk:", err)
	}
	if toSlice {
		chunk = chunk[:fi.Size()-task.Offset]
	}

	// check the previous chunks ending bytes
	// to check if it perfectly aligns or not
	// if it does read from start, or skipStart the first line
	skipStart := false
	if task.Offset != 0 {
		prevChunkLast := make([]byte, 1)
		_, err = f.ReadAt(prevChunkLast, task.Offset-1)
		if err != nil {
			log.Fatalln("reading previous file chunk:", err)
		}
		if prevChunkLast[0] != '\n' {
			skipStart = true
		}
	}

	checkNext := false
	if task.Offset+task.Size < fi.Size() {
		if chunk[len(chunk)-1] != '\n' {
			checkNext = true
		}
	}

	scanner := bufio.NewScanner(bytes.NewBuffer(chunk))
	i := 0
	for scanner.Scan() {
		if i == 0 && skipStart {
			i += 1
			continue
		}
		lines = append(lines, scanner.Text())
	}

	if checkNext {
		lastLine := lines[len(lines)-1]
		lines = lines[:len(lines)-1]
		var sb strings.Builder
		sb.WriteString(lastLine)
		nextChunk := make([]byte, os.Getpagesize())
		n, err := f.ReadAt(nextChunk, task.Offset+task.Size)
		isEOF := task.Offset+task.Size+int64(n) == fi.Size()
		if err != nil && !isEOF {
			log.Fatalln("reading next chunk:", err)
		}
		i := bytes.Index(nextChunk, []byte{'\n'})
		if i > 0 {
			sb.WriteString(string(nextChunk[:i]))
		}
		lines = append(lines, sb.String())
	}
	return lines
}

func createChunks(filename string) []Chunk {
	fi, err := os.Stat(filename)
	if err != nil {
		log.Fatal("reading file stat:", err)
	}
	size := fi.Size()

	count := math.Ceil(float64(size) / float64(CHUNK_SIZE))
	tasks := []Chunk{}

	for i := range int(count) {
		task := Chunk{
			Filename: filename,
			Offset:   int64(CHUNK_SIZE * i),
			Size:     int64(CHUNK_SIZE),
		}
		tasks = append(tasks, task)
	}
	return tasks
}
