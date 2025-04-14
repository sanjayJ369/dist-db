package mr

import (
	"encoding/gob"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"

	"6.5840/utils/logger"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	gob.Register(&MapTask{})
	gob.Register(&ReduceTask{})
	slogger = logger.GetLogger("worker").Sugar()
	slogger.Info("Worker started")
	for {
		var task Task
		var ok bool
		slogger.Info("Requesting task from coordinator")
		if !call("Coordinator.GetTask", "", &task) {
			slogger.Fatal("Error getting task from coordinator")
		}
		switch v := task.(type) {
		case *MapTask:
			slogger.Infof("Received map task: %d", v.Id)
			v.MapF = mapf
			v.Work()
			slogger.Infof("Completed map task: %d", v.Id)
			if !call("Coordinator.MapTaskDone", v.Out, &ok) {
				slogger.Warnf("Failed to notify map task completion for task %d", v.Id)
			} else {
				slogger.Infof("Successfully notified map task completion for task %d", v.Id)
			}
		case *ReduceTask:
			slogger.Infof("Received reduce task: %d", v.Id)
			v.RedF = reducef
			v.Work()
			slogger.Infof("Completed reduce task: %d", v.Id)
			if !call("Coordinator.ReduceTaskDone", v, &ok) {
				slogger.Warnf("Failed to notify reduce task completion for task %d", v.Id)
			} else {
				slogger.Infof("Successfully notified reduce task completion for task %d", v.Id)
			}
		default:
			slogger.Warnf("Received invalid task type: %v", task)
		}
		if !ok {
			slogger.Warn("Unable to mark task as done")
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
