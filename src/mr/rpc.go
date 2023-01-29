package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
	"strconv"
)

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
type Arg struct {
	WorkerId int
}

type WorkerTaskType byte

const (
	WorkerTaskType_Map WorkerTaskType = iota
	WorkerTaskType_Reduce
	WorkerTaskType_Exit
)

// type WorkerTaskStatus byte
// const (
// 	WorkerTaskStatus_Success
// )

type FinishResponse struct {
	WorkerId int
	Filename string
	TaskType WorkerTaskType
	Error    error
	Seq      int
}

type Reply struct {
	Filename string
	TaskType WorkerTaskType
	NReduce  int
	NMap     int
	Seq      int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

func generateMapResultFileName(mapNumber, reduceNumber int) string {
	return fmt.Sprintf("mr-%d-%d", mapNumber, reduceNumber)
}

func generateReduceResultFileName(reduceNumber int) string {
	return fmt.Sprintf("mr-out-%d", reduceNumber)
}
