package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mapTasks    []*MapTask
	reduceTasks []*ReduceTask
}

type MapTask struct {
	filename   string
	isStarted  bool
	isFinished bool
	mu         sync.Mutex
}

type ReduceTask struct {
	isStarted  bool
	isFinished bool
	mu         sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetIsCoordinatorStillAlive(args *EmptyArgsOrReply, reply *EmptyArgsOrReply) error {
	return nil
}

func (c *Coordinator) GetAreAllMapTasksDone(args *EmptyArgsOrReply, reply *AreMapTasksDoneReply) error {
	reply.AreMapTasksDone = c.areAllMapTasksDone()
	return nil
}

func (c *Coordinator) areAllMapTasksDone() bool {
	for _, task := range c.mapTasks {
		task.mu.Lock()
		finished := task.isFinished
		task.mu.Unlock()
		if !finished {
			// fmt.Println("Map task is not finished: ", i)
			return false
		}
	}
	return true
}

func (c *Coordinator) areAllReduceTasksDone() bool {
	for _, task := range c.reduceTasks {
		task.mu.Lock()
		finished := task.isFinished
		task.mu.Unlock()
		if !finished {
			// fmt.Println("Reduce task is not finished: ", i)
			return false
		}
	}
	return true
}

func (c *Coordinator) waitAndResetMapTask(taskNumber int) {
	time.Sleep(10 * time.Second)
	task := c.mapTasks[taskNumber]
	task.mu.Lock()
	if !task.isFinished {
		task.isStarted = false
	}
	task.mu.Unlock()
}

func (c *Coordinator) waitAndResetReduceTask(taskNumber int) {
	time.Sleep(10 * time.Second)
	task := c.reduceTasks[taskNumber]
	task.mu.Lock()
	if !task.isFinished {
		task.isStarted = false
	}
	task.mu.Unlock()
}

func (c *Coordinator) GetMapTask(args *EmptyArgsOrReply, reply *MapTaskReply) error {
	for i, task := range c.mapTasks {
		task.mu.Lock()
		if !task.isStarted {
			reply.FileName = task.filename
			reply.NReduce = len(c.reduceTasks)
			reply.TaskNumber = i
			task.isStarted = true
			task.mu.Unlock()
			go c.waitAndResetMapTask(reply.TaskNumber)
			return nil
		}
		task.mu.Unlock()
	}
	reply.TaskNumber = NO_TASK_NUMBER
	return nil
}

func (c *Coordinator) GetReduceTask(args *EmptyArgsOrReply, reply *ReduceTaskReply) error {
	for i, task := range c.reduceTasks {
		task.mu.Lock()
		if !task.isStarted {
			reply.NMap = len(c.mapTasks)
			reply.TaskNumber = i
			task.isStarted = true
			task.mu.Unlock()
			go c.waitAndResetReduceTask(reply.TaskNumber)
			return nil
		}
		task.mu.Unlock()
	}
	reply.TaskNumber = NO_TASK_NUMBER
	return nil
}

func (c *Coordinator) PutDoneMapTask(args *DoneMapTaskArgs, reply *EmptyArgsOrReply) error {
	taskNumber := args.TaskNumber
	task := c.mapTasks[taskNumber]

	task.mu.Lock()
	defer task.mu.Unlock()
	task.isFinished = true
	// fmt.Printf("PutDoneMapTask %+v\n", task)
	return nil
}

func (c *Coordinator) PutDoneReduceTask(args *DoneReduceTaskArgs, reply *EmptyArgsOrReply) error {
	taskNumber := args.TaskNumber
	task := c.reduceTasks[taskNumber]

	task.mu.Lock()
	defer task.mu.Unlock()
	task.isFinished = true
	// fmt.Printf("PutDoneReduceTask %+v\n", task)
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	// Determine if all the map and reduce tasks are done
	if c.areAllMapTasksDone() && c.areAllReduceTasksDone() {
		// c.cleanUpIntermediateFiles()
		ret = true
	}

	return ret
}

// func (c *Coordinator) cleanUpIntermediateFiles() {
// 	for mapTaskNumber := range c.mapTasks {
// 		for reduceTaskNumber := range c.reduceTasks {
// 			fileName := fmt.Sprintf("mr-%d-%d", mapTaskNumber, reduceTaskNumber)
// 			if err := os.Remove(fileName); err != nil {
// 				log.Println("Could not remove the file: ", fileName)
// 			}
// 		}
// 	}
// }

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	for _, file := range files {
		c.mapTasks = append(c.mapTasks, &MapTask{filename: file})
	}

	for range nReduce {
		c.reduceTasks = append(c.reduceTasks, &ReduceTask{})
	}

	// Your code here.
	// fmt.Println("nReduce: ", nReduce)
	// fmt.Println("sockname: ", sockname)
	// for i, x := range files {
	// 	fmt.Printf("file %d:%s\n", i, x)
	// }
	// fmt.Println("-----------------------------------------")

	c.server(sockname)
	return &c
}
