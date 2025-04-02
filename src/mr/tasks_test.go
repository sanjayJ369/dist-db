package mr

import (
	"bufio"
	"bytes"
	"fmt"
	"math/rand/v2"
	"os"
	"runtime/pprof"
	"sort"
	"strings"
	"testing"
	"time"
	"unicode"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestCreateReadChunks(t *testing.T) {
	// create fileCount files with random size
	// and check if createChunks and readChunks
	// will correctly read the file
	f, err := os.Create("./testing/cpu.prof")
	if err != nil {
		t.Fatal(err)
	}

	if err := pprof.StartCPUProfile(f); err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		timer := time.NewTimer(29 * time.Second)
		select {
		case <-timer.C:
			pprof.StopCPUProfile()
			f.Close()
		case <-done:
			timer.Stop()
		}
	}()

	fileCount := 10
	files := []string{}
	for _ = range fileCount {
		size := rand.Int32N(100)
		name := createFile(t, "/tmp/", int(size), 10, 100)
		files = append(files, name)
	}

	for _, file := range files {
		chunks := createChunks(file)
		got := []byte{}
		for _, chunk := range chunks {
			fmt.Println(chunk)
			lines := readChunk(chunk)
			for _, line := range lines {
				got = append(got, []byte(line)...)
				got = append(got, '\n')
			}
		}
		want, err := os.ReadFile(file)

		if err != nil {
			t.Errorf("\nreading file %s : %s", file, err)
		}
		if !assert.Equal(t, got, want) {
			os.WriteFile("./debug/got.txt", got, 0644)
			os.WriteFile("./debug/want.txt", want, 0644)
		}
		fmt.Println("completed file: ", file)
	}
}

func TestMapTaskWork(t *testing.T) {
	// create map tasks
	filename := "./testdata/pg-metamorphosis.txt"
	tasks := []MapTask{}
	for _, chunk := range createChunks(filename) {
		task := MapTask{
			Id:      uuid.NewString(),
			Chunk:   chunk,
			nReduce: 1,
			out:     MapTaskOut{},
		}
		tasks = append(tasks, task)
	}

	mapf := func(filename string, contents string) []KeyValue {
		// function to detect word separators.
		ff := func(r rune) bool { return !unicode.IsLetter(r) }

		// split contents into an array of words.
		words := strings.FieldsFunc(contents, ff)

		kva := []KeyValue{}
		for _, w := range words {
			kv := KeyValue{w, "1"}
			kva = append(kva, kv)
		}
		return kva
	}

	mapRes := []KeyValue{}
	for _, task := range tasks {
		task.Work(mapf)
		assert.Equal(t, len(task.out.Files), 1)
		data, err := os.ReadFile(task.out.Files[0])
		if err != nil {
			t.Errorf("\nreading map task output: %s", err)
		}
		scanner := bufio.NewScanner(bytes.NewBuffer(data))
		for scanner.Scan() {
			kv := KeyValue{}
			line := scanner.Text()
			splitLine := strings.Split(line, " ")
			if len(splitLine) < 2 {
				continue
			}
			kv.Key = splitLine[0]
			kv.Value = splitLine[1]
			mapRes = append(mapRes, kv)
		}
	}

	want, err := os.ReadFile("./testdata/intermediate-0")
	if err != nil {
		t.Error(err)
	}
	want = append(want, '\n')
	wantRes := []KeyValue{}
	scanner := bufio.NewScanner(bytes.NewBuffer(want))
	for scanner.Scan() {
		kv := KeyValue{}
		line := scanner.Text()
		splitLine := strings.Split(line, " ")
		if len(splitLine) < 2 {
			continue
		}
		kv.Key = splitLine[0]
		kv.Value = splitLine[1]
		wantRes = append(wantRes, kv)
	}

	if !assert.ElementsMatch(t, wantRes, mapRes) {
		sort.Slice(wantRes, func(i, j int) bool {
			cmp := strings.Compare(wantRes[i].Key, wantRes[j].Key)
			return cmp == -1
		})
		sort.Slice(mapRes, func(i, j int) bool {
			cmp := strings.Compare(mapRes[i].Key, mapRes[j].Key)
			return cmp == -1
		})
		writeKVPairsToFile(t, "./testdata/wantRes.txt", wantRes)
		writeKVPairsToFile(t, "./testdata/mapRes.txt", mapRes)
	}
}

// createFile creates a file with random strings in a line of given size
// filesize (in KiB (1024 bytes) ), maxLineSize (maximum size of the line)
func createFile(t testing.TB, filedir string, filesize int, minLineSize int, maxLineSize int) string {
	t.Helper()
	name := filedir + uuid.NewString()
	f, err := os.Create(name)
	if err != nil {
		t.Errorf("creating file: %s", err)
		return ""
	}
	defer f.Close()

	filesize *= 1024 // convert kilobytes into bytes
	size := 0
	for size < filesize {
		var newLine string
		if filesize-size < maxLineSize {
			// last line
			newLine = createRandomString(int32(filesize-size-1)) + "\n"
		} else {
			lineSize := rand.Int32N(int32(maxLineSize)) + int32(minLineSize)
			newLine = createRandomString(lineSize) + "\n"
		}
		size += len(newLine)
		f.WriteString(newLine)
	}

	return name
}

// createRandomString creates a random string
func createRandomString(size int32) string {
	var sb strings.Builder
	letters := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"
	for _ = range size {
		idx := rand.Int32N(int32(len(letters)))
		sb.WriteByte(letters[idx])
	}
	return sb.String()
}

func writeKVPairsToFile(t testing.TB, file string, res []KeyValue) {
	t.Helper()
	File, err := os.Create(file)
	if err != nil {
		t.Errorf("failed to create file: %s", err)
	} else {
		defer File.Close()
		for _, kv := range res {
			File.WriteString(fmt.Sprintf("%s %s\n", kv.Key, kv.Value))
		}
	}
}
