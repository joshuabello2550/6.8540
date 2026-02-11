package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator
var NO_TASK_NUMBER = -1

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	log.Println("Starting a worker")
	coordSockName = sockname
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for CallIsCoordinatorStillAlive() {
		areMapTasksDone := CallAreAllMapTasksDone()
		log.Println("areMapTasksDone: ", areMapTasksDone)
		if !areMapTasksDone {
			CallRequestMapTask(mapf)
		} else {
			CallRequestReduceTask(reducef)
		}
		time.Sleep(5 * time.Second)
	}
}

func CallIsCoordinatorStillAlive() bool {
	args := EmptyArgsOrReply{}
	reply := EmptyArgsOrReply{}
	ok := call("Coordinator.GetIsCoordinatorStillAlive", &args, &reply)
	if ok {
		fmt.Println("Coordinator is alive")
		return true
	} else {
		fmt.Println("Coordinator NOT alive")
		return false
	}
}

func CallAreAllMapTasksDone() bool {
	args := EmptyArgsOrReply{}
	reply := AreMapTasksDoneReply{}
	ok := call("Coordinator.GetAreAllMapTasksDone", &args, &reply)
	if ok {
		return reply.AreMapTasksDone
	} else {
		fmt.Println("CallAreAllMapTasksDone failed!")
		return false
	}
}

func CallRequestMapTask(mapf func(string, string) []KeyValue) {
	log.Println("CallRequestMapTask")
	args := EmptyArgsOrReply{}
	reply := MapTaskReply{}
	ok := call("Coordinator.GetMapTask", &args, &reply)
	if ok {
		if reply.TaskNumber != NO_TASK_NUMBER {
			handleMapTask(reply, mapf)
		}
	} else {
		fmt.Println("CallRequestMapTask failed!")
	}
}

func CallRequestReduceTask(reducef func(string, []string) string) {
	log.Println("CallRequestReduceTask")
	args := EmptyArgsOrReply{}
	reply := ReduceTaskReply{}
	ok := call("Coordinator.GetReduceTask", &args, &reply)
	if ok {
		if reply.TaskNumber != NO_TASK_NUMBER {
			handleReduceTask(reply, reducef)
		}
	} else {
		fmt.Println("CallRequestReduceTask failed!")
	}
}

func CallDoneMapTask(mapTaskNumber int) {
	fmt.Println("CallDoneMapTask: ", mapTaskNumber)
	args := DoneMapTaskArgs{TaskNumber: mapTaskNumber}
	reply := EmptyArgsOrReply{}
	ok := call("Coordinator.PutDoneMapTask", &args, &reply)
	if !ok {
		fmt.Println("CallDoneMapTask failed!")
	}
}

func CallDoneReduceTask(reduceTaskNumber int) {
	fmt.Println("CallDoneReduceTask: ", reduceTaskNumber)
	args := DoneReduceTaskArgs{TaskNumber: reduceTaskNumber}
	reply := EmptyArgsOrReply{}
	ok := call("Coordinator.PutDoneReduceTask", &args, &reply)
	if !ok {
		fmt.Println("CallDoneReduceTask failed!")
	}
}

func handleMapTask(mapTask MapTaskReply, mapf func(string, string) []KeyValue) {
	fmt.Println("handleMapTask: ", mapTask)
	// Run the map function on the given file
	filename := mapTask.FileName
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	kva := mapf(filename, string(content))

	mapTaskNumber := mapTask.TaskNumber

	// Create temporary files
	tempFilesMap := make(map[int]*os.File)
	for reduceNum := range mapTask.NReduce {
		tempFileName := fmt.Sprintf("mr-%d-%d", mapTaskNumber, reduceNum)
		file, err := os.CreateTemp(".", tempFileName)
		defer os.Remove(file.Name()) // clean up
		if err != nil {
			log.Print("Could not create temp file: ", tempFileName, err)
		}
		tempFilesMap[reduceNum] = file
	}

	// Write each key value pair to the temporary file
	for _, kv := range kva {
		reduceTaskNumber := ihash(kv.Key) % mapTask.NReduce
		tempFile := tempFilesMap[reduceTaskNumber]
		enc := json.NewEncoder(tempFile)
		if err := enc.Encode(&kv); err != nil {
			log.Print("Could not write to the temp file: ", tempFile.Name(), err)
		}
	}

	// Atomically rename the files
	for reduceNum, tempFile := range tempFilesMap {
		finalFinalName := fmt.Sprintf("mr-%d-%d", mapTaskNumber, reduceNum)
		if err = os.Rename(tempFile.Name(), finalFinalName); err != nil {
			log.Print("Could not atomically rename the file: ", tempFile.Name(), err)
		}
	}

	// Tell the coordinator that mapTask is done
	CallDoneMapTask(mapTaskNumber)
}

func handleReduceTask(reduceTask ReduceTaskReply, reducef func(string, []string) string) {
	fmt.Println("handleReduceTask: ", reduceTask)
	reduceTaskNumber := reduceTask.TaskNumber
	NMap := reduceTask.NMap
	intermediate := []KeyValue{}

	// For a given reduce task, read the map results from all map tasks and save them
	for mapTaskNumber := range NMap {
		fileName := fmt.Sprintf("mr-%d-%d", mapTaskNumber, reduceTaskNumber)
		file, err := os.Open(fileName)
		if err != nil {
			log.Print("Could not open the file: ", fileName, err)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", reduceTaskNumber)
	ofile, err := os.Create(oname)
	if err != nil {
		log.Print("Could not create output for the reduce task: ", oname, err)
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	if err = ofile.Close(); err != nil {
		log.Print("Could not close file: ", oname, err)
	}

	CallDoneReduceTask(reduceTaskNumber)
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
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
