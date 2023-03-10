package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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

// RegisterArgs is the argument passed when a worker registers with the master.
type RegisterArgs struct {
	Worker string // the worker's UNIX-domain socket name, i.e. its RPC address
}

type TaskReply struct {
	Id   int
	Type TaskType

	// Filename of Map type Task is the input file name, in
	// Reduce type is the output file name.
	Filename string

	TasksNum int

	// otherNum is the total number of tasks in other phase; mappers
	// need this to compute the number of output bins, and reducers
	// needs this to know how many input files to collect.
	OtherNum int
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
