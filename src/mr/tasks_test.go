package mr

import (
	"fmt"
	"math/rand/v2"
	"os"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestCreateReadChunks(t *testing.T) {
	// create fileCount files with random size
	// and check if createChunks and readChunks
	// will correctly read the file
	f, err := os.Create("./debug/cpu.prof")
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
