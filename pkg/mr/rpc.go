package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// example to show how to declare the arguments
// and reply for an RPC.
type TaskType int

const (
	NoFileAvailableTask TaskType = iota
	MapTask
	ReduceTask
	DoneTask
)

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type TaskRequestArgs struct {
	Name string
}

type TaskResponse struct {
	FName string
	TaskType
	NumMapTask    int
	NumReduceTask int
}

type TaskDoneNotif struct {
	FName string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
