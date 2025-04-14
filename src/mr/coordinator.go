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
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"6.5840/utils/logger"
	"go.uber.org/zap"

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
	mapTasksOutput   [][]string // location of map task output file
	redueTasksOutput []string   // reduce task output
	sync.Mutex
}

var slogger *zap.SugaredLogger

func init() {
	if err := logger.Initialize(); err != nil {
		log.Fatalf("\ninitalzing logger: %s", err)
	}
	slogger = logger.GetLogger("coordinator").Sugar()
}

func (c *Coordinator) CleanUp() error {
	slogger.Info("Starting cleanup of output files")
	for _, group := range c.mapTasksOutput {
		for _, file := range group {
			err := os.Remove(file)
			slogger.Warnf("unable to delete file: %s, err: %s", file, err)
		}
	}
	outfiles := c.redueTasksOutput
	newfiles := []string{}
	for i, file := range outfiles {
		dir := path.Dir(file)
		newName := OUTPUT_FILE_PREFIX + strconv.Itoa(i)
		err := os.Rename(file, filepath.Join(dir, newName))
		if err != nil {
			slogger.Error("Error renaming file", "file", file, "error", err)
			return fmt.Errorf("renaming files: %s", err)
		}
		newfiles = append(newfiles, dir+newName)
		slogger.Info("Renamed file", "oldName", file, "newName", dir+newName)
	}
	c.redueTasksOutput = newfiles
	slogger.Info("Cleanup completed successfully")
	return nil
}

func (c *Coordinator) MapTaskDone(task MapTaskOut, replay *bool) error {
	c.Lock()
	defer c.Unlock()

	slogger.Info("Map task completed", "taskId", task.Id)
	if _, ok := c.mapProcessing[task.Id]; ok {
		err := updateReduceTasks(c, task)
		if err != nil {
			slogger.Error("Error updating reduce tasks", "taskId", task.Id, "error", err)
			return fmt.Errorf("updating reduce tasks: %w", err)
		}
		c.mapTasksOutput = append(c.mapTasksOutput, task.Files)
		delete(c.mapProcessing, task.Id)
		if len(c.mapProcessing) == 0 && len(c.mapTasks) == 0 {
			c.mapTasksDone = true
			slogger.Info("All map tasks completed")
		}
		*replay = true
	}
	return nil
}

func updateReduceTasks(c *Coordinator, task MapTaskOut) error {
	slogger.Info("Updating reduce tasks with map task output", "taskId", task.Id)
	if len(task.Files) != c.nReducers {
		slogger.Error("Mismatch between map files generated and number of reducers", "taskId", task.Id)
		return errors.New("mismatch between map files generated and number of reducers")
	}
	for i, file := range task.Files {
		c.reduceTasks[i].InputFiles = append(c.reduceTasks[i].InputFiles, file)
		slogger.Info("Added map output to reduce task", "reduceTaskId", c.reduceTasks[i].Id, "file", file)
	}
	return nil
}

func (c *Coordinator) ReduceTaskDone(task ReduceTask, replay *bool) error {
	c.Lock()
	defer c.Unlock()

	slogger.Info("Reduce task completed", "taskId", task.Id)
	if _, ok := c.reduceProcessing[task.Id]; ok {
		c.redueTasksOutput = append(c.redueTasksOutput, task.Out)
		delete(c.reduceProcessing, task.Id)
		if len(c.reduceProcessing) == 0 && len(c.reduceTasks) == 0 {
			slogger.Info("All reduce tasks completed, starting cleanup")
			c.CleanUp()
			c.reduceTasksDone = true
		}
	}
	*replay = true
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
			slogger.Warn("Map task retrying", "taskId", v.Id)
		}
	case *ReduceTask:
		if t, ok := c.reduceProcessing[v.Id]; ok {
			delete(c.reduceProcessing, v.Id)
			c.reduceTasks = append(c.reduceTasks, t)
			slogger.Warn("Reduce task retrying", "taskId", v.Id)
		}
	}
}

func (c *Coordinator) GetTask(workerId string, task *Task) error {
	c.Lock()
	defer c.Unlock()

	slogger.Info("Worker requested task", "workerId:", workerId)
	if !c.mapTasksDone {
		if len(c.mapTasks) != 0 {
			t := c.mapTasks[0]
			c.mapProcessing[t.Id] = t
			go c.handleTask(&t)
			c.mapTasks = c.mapTasks[1:]
			*task = &t
			slogger.Info("Assigned map task to worker", "workerId", workerId, "taskId", t.Id)
			return nil
		}
		slogger.Info("map tasks in process", "map tasks ", c.mapProcessing)
		slogger.Warn("No map tasks available for worker", "workerId", workerId)
		return errors.New(ErrProcessingMapTask)
	}

	if !c.reduceTasksDone {
		if len(c.reduceTasks) != 0 {
			t := c.reduceTasks[0]
			c.reduceProcessing[t.Id] = t
			go c.handleTask(&t)
			c.reduceTasks = c.reduceTasks[1:]
			*task = &t
			slogger.Info("Assigned reduce task to worker", "workerId", workerId, "taskId", t.Id)
			return nil
		}
		slogger.Warn("No reduce tasks available for worker", "workerId", workerId)
		return errors.New(ErrProcessingReduceTask)
	}

	slogger.Info("All tasks completed, no tasks available for worker", "workerId", workerId)
	return errors.New(MsgCompletedProcessing)
}

func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	slogger.Info("Example RPC called", "args", args)
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) server() {
	slogger.Info("Starting RPC server")
	gob.Register(&MapTask{})
	gob.Register(&ReduceTask{})
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		slogger.Fatal("Listen error", "error", e)
	}
	go http.Serve(l, nil)
	slogger.Info("RPC server started successfully")
}

func (c *Coordinator) Done() bool {
	return c.reduceTasksDone
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	slogger.Info("Creating coordinator", "nReduce", nReduce)
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
	slogger.Info("Coordinator created successfully")
	return &c
}

func createReduceTasks(nReduce int) []ReduceTask {
	slogger.Info("Creating reduce tasks:", "nReduce", nReduce)
	tasks := []ReduceTask{}
	for i := 0; i < nReduce; i++ {
		task := ReduceTask{
			Id:         uuid.NewString(),
			InputFiles: []string{},
		}
		tasks = append(tasks, task)
		slogger.Info("Created reduce task", "taskId", task.Id)
	}
	return tasks
}

func createMapTasks(files []string, nReduce int) []MapTask {
	slogger.Info("Creating map tasks", "nReduce", nReduce, "files", files)
	tasks := []MapTask{}
	for _, file := range files {
		fi, err := os.Stat(file)
		if err != nil {
			log.Fatal("reading file stat:", err)
		}
		size := fi.Size()
		task := MapTask{
			Id: uuid.NewString(),
			Chunk: Chunk{
				Filename: file,
				Offset:   0,
				Size:     size,
			},
			NReduce: nReduce,
		}
		tasks = append(tasks, task)
		slogger.Info("Created map task", "taskId:", task.Id, "file:", file)
	}
	return tasks
}
