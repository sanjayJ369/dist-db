package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

var (
	CHUNK_SIZE = 1024 * 64 // bytes
)

type Coordinator struct {
	mapTasks         []MapTask
	reduceTasks      []ReduceTask
	mapProcessing    map[string]MapTask
	reduceProcessing map[string]ReduceTask
	mapTasksDone     bool
	reduceTasksDone  bool
	reducers         int
	mapTasksOutput   [][]string // location of map task ouput file
	redueTasksOutput []string   // reduce task output
	sync.Mutex
}

func (c *Coordinator) MapTaskDone(task MapTaskOut, replay *bool) error {
	// TODO: mark map task done
	return nil
}

func (c *Coordinator) ReduceTaskDone(task ReduceTaskOut, replay *bool) error {
	// TODO: mark map task done
	return nil
}

func (c *Coordinator) GetTask(workerId string, task *Task) error {
	// TODO: return task to the worker
	return nil
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
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	// TODO: what should coordinator hold

	c.server()
	return &c
}
