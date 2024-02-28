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

type TaskAskRequest struct {
	WorkID int
}

// 派发任务
type TaskAskResponse struct {
	MapNum    int    //map任务的编号
	Filename  string //map 任务的filename
	TaskType  int    //0 表示 map  1 表示 reduce
	AllDone   bool   //所有任务都完成了
	NReduce   int
	ReduceNum int
	Wait      bool
}

type MapFinishRequest struct {
	MapNum int
}

type MapFinishResponse struct {
}

type ReduceFinishRequest struct {
	ReducepNum int
}

type ReduceFinishResponse struct {
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
