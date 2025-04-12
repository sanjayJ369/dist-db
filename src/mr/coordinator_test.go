package mr

import (
	"fmt"
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
