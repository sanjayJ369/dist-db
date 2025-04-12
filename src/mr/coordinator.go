package mr

import (
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
)

var (
	CHUNK_SIZE         = 1024 * 64 // bytes
	RETRY_TIME         = 10        // in seconds
	OUTPUT_FILE_PREFIX = "mr-out-" // file name prefix
)

type Coordinator struct {
	mapTasks         []MapTask
	reduceTasks      []ReduceTask
	mapProcessing    map[string]MapTask
	reduceProcessing map[string]ReduceTask
	mapTasksDone     bool
	reduceTasksDone  bool
	nReducers        int
	mapTasksOutput   [][]string // location of map task ouput file
	redueTasksOutput []string   // reduce task output
	sync.Mutex
}

func (c *Coordinator) CleanUp() error {
	c.Lock()
	defer c.Unlock()
	outfiles := c.redueTasksOutput
	newfiles := []string{}
	for i, file := range outfiles {
		dir := path.Dir(file)
		newName := OUTPUT_FILE_PREFIX + strconv.Itoa(i)
		err := os.Rename(file, dir+newName)
		newfiles = append(newfiles, dir+newName)
		if err != nil {
			return fmt.Errorf("renameing files: %s", err)
		}
	}
	c.redueTasksOutput = newfiles
	return nil
}

func (c *Coordinator) MapTaskDone(task MapTaskOut, replay *bool) error {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.mapProcessing[task.Id]; ok {
		err := updateReduceTasks(c, task)
		if err != nil {
			return fmt.Errorf("updating reduce tasks: %w", err)
		}
		c.mapTasksOutput = append(c.mapTasksOutput, task.Files)
		delete(c.mapProcessing, task.Id)
		if len(c.mapProcessing) == 0 && len(c.mapTasks) == 0 {
			c.mapTasksDone = true
		}
		*replay = true
	}
	return nil
}

// updateREduceTasks updated coordinator reduce tasks with
// the new map task results
func updateReduceTasks(c *Coordinator, task MapTaskOut) error {
	if len(task.Files) != c.nReducers {
		return errors.New("mismatch between map files generated and number of reducers")
	}
	for i, file := range task.Files {
		c.reduceTasks[i].InputFiles = append(c.reduceTasks[i].InputFiles, file)
	}
	return nil
}

func (c *Coordinator) ReduceTaskDone(task ReduceTask, replay *bool) error {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.reduceProcessing[task.Id]; ok {
		c.redueTasksOutput = append(c.redueTasksOutput, task.Out)
		delete(c.reduceProcessing, task.Id)
		if len(c.reduceProcessing) == 0 && len(c.reduceTasks) == 0 {
			c.CleanUp()
		}
	}
	return nil
}

func (c *Coordinator) handleTask(task Task) {
	time.Sleep(time.Second * time.Duration(RETRY_TIME))
	c.Lock()
	defer c.Unlock()

	switch v := task.(type) {
	case *MapTask:
		if t, ok := c.mapProcessing[v.Id]; ok {
			delete(c.mapProcessing, v.Id)
			c.mapTasks = append(c.mapTasks, t)
		}
	case *ReduceTask:
		if t, ok := c.reduceProcessing[v.Id]; ok {
			delete(c.reduceProcessing, v.Id)
			c.reduceTasks = append(c.reduceTasks, t)
		}
	}
}

func (c *Coordinator) GetTask(workerId string, task *Task) error {
	c.Lock()
	defer c.Unlock()

	if !c.mapTasksDone {
		if len(c.mapTasks) != 0 {
			t := c.mapTasks[0]
			c.mapProcessing[t.Id] = t
			go c.handleTask(&t)
			c.mapTasks = c.mapTasks[1:]
			*task = &t
			return nil
		}
		return errors.New(ErrProcessingMapTask)
	}

	if !c.reduceTasksDone {
		if len(c.reduceTasks) != 0 {
			t := c.reduceTasks[0]
			c.reduceProcessing[t.Id] = t
			go c.handleTask(&t)
			c.reduceTasks = c.reduceTasks[1:]
			*task = &t
			return nil
		}
		return errors.New(ErrProcessingReduceTask)
	}

	return errors.New(MsgCompletedProcessing)
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	gob.Register(&MapTask{})
	gob.Register(&ReduceTask{})
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.reduceTasksDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// register types for encoding and decoding
	gob.Register(&MapTask{})
	gob.Register(&MapTaskOut{})
	gob.Register(&ReduceTask{})

	c := Coordinator{
		mapTasks:         createMapTasks(files, nReduce),
		reduceTasks:      createReduceTasks(nReduce),
		mapProcessing:    make(map[string]MapTask),
		reduceProcessing: make(map[string]ReduceTask),
		mapTasksDone:     false,
		reduceTasksDone:  false,
		nReducers:        nReduce,
		mapTasksOutput:   make([][]string, 0),
		redueTasksOutput: make([]string, 0),
	}

	c.server()
	return &c
}

func createReduceTasks(nReduce int) []ReduceTask {
	tasks := []ReduceTask{}
	for i := 0; i < nReduce; i++ {
		task := ReduceTask{
			Id:         uuid.NewString(),
			InputFiles: []string{},
		}
		tasks = append(tasks, task)
	}
	return tasks
}

func createMapTasks(files []string, nReduce int) []MapTask {
	tasks := []MapTask{}
	for _, file := range files {
		chunks := createChunks(file)
		for _, chunk := range chunks {
			task := MapTask{
				Id:      uuid.NewString(),
				Chunk:   chunk,
				NReduce: nReduce,
			}
			tasks = append(tasks, task)
		}
	}
	return tasks
}
