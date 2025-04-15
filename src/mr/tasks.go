package mr

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"syscall"

	"6.5840/utils"
	"6.5840/utils/logger"
	"github.com/emirpasic/gods/queues/priorityqueue"
)

const (
	ErrProcessingMapTask    = "map tasks are being processed"
	ErrProcessingReduceTask = "reduce tasks are being processed"
	MsgCompletedProcessing  = "all tasks are processed"
)

type Task interface {
	Work()
}

type PleaseExit struct{}

func (p *PleaseExit) Work() {}

type MapTask struct {
	Id      string     // task ID
	NReduce int        // number of reducers
	Out     MapTaskOut // result
	MapF    func(string, string) []KeyValue
	Chunk
}

type MapTaskOut struct {
	Id    string
	Files []string
}

func (m *MapTask) Work() {
	logger := logger.GetLogger("worker").Sugar()
	logger.Infof("Starting MapTask Work: %s", m.Id)

	content, err := os.ReadFile(m.Filename)
	if err != nil {
		logger.Fatalf("Error reading file: %s: %s", m.Filename, err)
	}
	logger.Infof("File read successfully: %s", m.Filename)

	m.Out = MapTaskOut{}
	m.Out.Id = m.Id
	sortedOutputLines := make([]utils.StringHeap, m.NReduce)

	kvs := m.MapF(m.Filename, string(content))
	logger.Infof("Map function executed successfully for task: %s", m.Id)

	for _, kv := range kvs {
		i := ihash(kv.Key) % m.NReduce
		line := fmt.Sprintf("%v %v\n", kv.Key, kv.Value)
		sortedOutputLines[i].PushString(line)
	}
	logger.Infof("Key-value pairs sorted into %d reducers for task: %s", m.NReduce, m.Id)

	// create NReduce files to store output of map func
	taskId := m.Id
	outFiles := []*os.File{}

	for i := range m.NReduce {
		filename := "mr-out-" + strconv.Itoa(i)
		f, err := os.Create(taskId + "-" + filename)
		if err != nil {
			logger.Fatalf("Error creating file: %s", err)
		}
		m.Out.Files = append(m.Out.Files, f.Name())
		defer f.Close()
		outFiles = append(outFiles, f)
		logger.Infof("Output file created: %s", f.Name())
	}

	for i, lines := range sortedOutputLines {
		n := lines.Len()
		for j := 0; j < n; j++ {
			fmt.Fprint(outFiles[i], lines.PopString())
		}
		logger.Infof("Written sorted lines to file: %s", outFiles[i].Name())
	}

	logger.Infof("MapTask Work completed: %s", m.Id)
}

type ReduceTask struct {
	Id         string
	InputFiles []string
	Out        string
	RedF       func(string, []string) string
}

func (r *ReduceTask) Work() {
	scanners := []*bufio.Scanner{}
	output, err := os.Create(OUTPUT_FILE_PREFIX + r.Id)
	r.Out = output.Name()
	if err != nil {
		log.Fatalln("creating reduce output file: ", err)
	}
	defer output.Close()

	// create a priorty queue
	type Pair struct {
		Str string
		Num int
	}
	queue := priorityqueue.NewWith(func(a, b interface{}) int {
		return strings.Compare(a.(Pair).Str, b.(Pair).Str)
	})
	currKey := ""
	currVal := []string{}
	completed := 0
	for i, file := range r.InputFiles {
		fp, err := os.OpenFile(file, os.O_RDONLY, 0655)
		if err != nil {
			log.Fatalln("reading map file: ", err)
		}
		scanner := bufio.NewScanner(fp)
		if !scanner.Scan() {
			completed += 1
		}
		if completed == len(r.InputFiles) {
			break
		}
		queue.Enqueue(Pair{scanner.Text(), i})
		scanners = append(scanners, scanner)
		defer fp.Close()
	}

	for !queue.Empty() {
		item, _ := queue.Dequeue()
		line := item.(Pair).Str
		idx := item.(Pair).Num

		scanners[idx].Scan()
		if scanners[idx].Text() != "" {
			queue.Enqueue(Pair{scanners[idx].Text(), idx})
		}

		kv := KvFromLine(line)
		if kv == nil {
			continue
		}
		if currKey == "" {
			currKey = kv.Key
			currVal = []string{kv.Value}
		} else if kv.Key == currKey {
			currVal = append(currVal, kv.Value)
		} else {
			res := r.RedF(currKey, currVal)
			fmt.Fprintf(output, "%v %v\n", currKey, res)
			currKey = kv.Key
			currVal = []string{kv.Value}
		}
	}

	if currKey != "" {
		res := r.RedF(currKey, currVal)
		fmt.Fprintf(output, "%v %v\n", currKey, res)
	}
}

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

func KvFromLine(line string) *KeyValue {
	kv := KeyValue{}
	splitLine := strings.Split(line, " ")
	if len(splitLine) < 2 {
		return nil
	}
	kv.Key = splitLine[0]
	kv.Value = splitLine[1]
	return &kv
}
