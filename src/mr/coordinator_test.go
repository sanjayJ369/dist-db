package mr

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

type StubWorker struct {
	task Task
	id   string
}

func (s *StubWorker) GetTask() bool {
	ok := call("Coordinator.GetTask", "", &s.task)
	if !ok {
		return false
	}
	switch v := s.task.(type) {
	case *MapTask:
		s.id = v.Id
	case *ReduceTask:
		s.id = v.Id
	default:
		fmt.Printf("\ninvalid type: %T", v)
		return false
	}
	return ok
}

func (s *StubWorker) MarkDone() bool {
	switch v := s.task.(type) {
	case *MapTask:
		out := MapTaskOut{
			Id:    v.Id,
			Files: getUUIDS(v.NReduce),
		}
		var res bool
		return call("Coordinator.MapTaskDone", out, &res)
	case *ReduceTask:
		var res bool
		return call("Coordinator.ReduceTaskDone", v, &res)
	default:
		fmt.Printf("\ninvalid type: %T", v)
		return false
	}
}

func TestGetTask(t *testing.T) {
	nReduce := 2
	t.Run("GetTask first returns map tasks", func(t *testing.T) {
		count := 10
		c := createStubCoordinator(nReduce, count)
		c.server()
		// worker should receive 10 map tasks and nReduce reduce tasks
		worker := StubWorker{}
		for i := 0; i < count; i++ {
			worker.GetTask()
			switch v := worker.task.(type) {
			case *MapTask:
				worker.MarkDone()
			default:
				t.Fatalf("\nexpected map task, but received: %T", v)
			}
		}
		for i := 0; i < nReduce; i++ {
			worker.GetTask()
			switch v := worker.task.(type) {
			case *ReduceTask:
				continue
			default:
				t.Fatalf("\nexpected reduce task, but received: %T", v)
			}
			worker.MarkDone()
		}
	})

	t.Run("unfinised map tasks will be added again to tasks queue", func(t *testing.T) {
		c := createStubCoordinator(nReduce, 10)
		c.server()
		worker := StubWorker{}
		worker.GetTask()
		taskId := worker.id
		time.Sleep(time.Duration(RETRY_TIME+1) * time.Second)
		currTasks := []string{}
		for _, task := range c.mapTasks {
			currTasks = append(currTasks, task.Id)
		}
		assert.NotContains(t, c.mapProcessing, taskId)
		assert.Contains(t, currTasks, taskId)
	})

	t.Run("unfinished reduce tasks will be added again to tasks queue", func(t *testing.T) {
		c := createStubCoordinator(nReduce, 1)
		c.server()
		worker := StubWorker{}
		// completing map task
		worker.GetTask()
		worker.MarkDone()

		// getting reduce task
		worker.GetTask()
		taskId := worker.id
		time.Sleep(time.Duration(RETRY_TIME+1) * time.Second)
		currTasks := []string{}
		for _, task := range c.reduceTasks {
			currTasks = append(currTasks, task.Id)
		}
		assert.NotContains(t, c.reduceProcessing, taskId)
		assert.Contains(t, currTasks, taskId)
	})
}

func TestCleanUp(t *testing.T) {
	t.Run("CleanUp removes map task output directories and renames reduce task output files", func(t *testing.T) {
		// Setup
		c := createStubCoordinator(2, 0)
		tempDir := t.TempDir()
		uuidDir := filepath.Join(tempDir, uuid.NewString())

		// Create mock map task output directories
		mapOutputDirs := []string{}
		for i := 0; i < 3; i++ {
			dir := filepath.Join(uuidDir, fmt.Sprintf("map-output-%d", i))
			err := os.MkdirAll(dir, 0755)
			assert.NoError(t, err)
			c.mapTasksOutput = append(c.mapTasksOutput, []string{filepath.Join(dir, fmt.Sprintf("mr-out-%d", i))})
			mapOutputDirs = append(mapOutputDirs, dir)
		}

		// Create mock reduce task output files
		reduceOutputFiles := []string{}
		for i := 0; i < 2; i++ {
			file := filepath.Join(uuidDir, fmt.Sprintf("reduce-output-%d", i))
			_, err := os.Create(file)
			assert.NoError(t, err)
			c.redueTasksOutput = append(c.redueTasksOutput, file)
			reduceOutputFiles = append(reduceOutputFiles, file)
		}

		// Call CleanUp
		err := c.CleanUp()
		assert.NoError(t, err)

		// Verify map task output directories are removed
		for _, dir := range mapOutputDirs {
			_, err := os.Stat(dir)
			assert.True(t, os.IsNotExist(err), "Expected directory to be removed: %s", dir)
		}

		// Verify reduce task output files are renamed
		for i := range reduceOutputFiles {
			newFile := filepath.Join(uuidDir, OUTPUT_FILE_PREFIX+strconv.Itoa(i))
			_, err := os.Stat(newFile)
			assert.NoError(t, err, "Expected file to be renamed: %s", newFile)
		}
	})
}

func createStubCoordinator(nReduce int, mapTaskCount int) *Coordinator {
	return &Coordinator{
		mapTasks:         createStubMapTasks(nReduce, mapTaskCount),
		reduceTasks:      createStubReduceTasks(nReduce),
		mapProcessing:    make(map[string]MapTask),
		reduceProcessing: make(map[string]ReduceTask),
		mapTasksDone:     false,
		reduceTasksDone:  false,
		nReducers:        nReduce,
		mapTasksOutput:   make([][]string, 0),
		redueTasksOutput: make([]string, 0),
	}
}

// returns a slice of mapTasks containing stub values
// chunk is nil,
func createStubMapTasks(nReduce int, count int) []MapTask {
	tasks := []MapTask{}
	for i := 0; i < count; i++ {
		task := MapTask{
			Id:      uuid.NewString(),
			NReduce: nReduce,
		}
		task.Out = MapTaskOut{
			Id:    task.Id,
			Files: make([]string, nReduce),
		}
		tasks = append(tasks, task)
	}
	return tasks
}

func createStubReduceTasks(nReduce int) []ReduceTask {
	tasks := []ReduceTask{}
	for i := 0; i < nReduce; i++ {
		task := ReduceTask{
			Id:  uuid.NewString(),
			Out: "",
		}
		tasks = append(tasks, task)
	}
	return tasks
}

func getUUIDS(count int) []string {
	res := []string{}
	for i := 0; i < count; i++ {
		res = append(res, uuid.NewString())
	}
	return res
}
