package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type EmptyArgsOrReply struct {
}

type AreMapTasksDoneReply struct {
	AreMapTasksDone bool
}

type MapTaskReply struct {
	FileName   string
	TaskNumber int
	NReduce    int
}

type ReduceTaskReply struct {
	TaskNumber int
	NMap       int
}

type DoneMapTaskArgs struct {
	TaskNumber int
}

type DoneReduceTaskArgs struct {
	TaskNumber int
}
