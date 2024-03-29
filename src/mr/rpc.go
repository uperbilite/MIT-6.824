package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
import "strconv"

type TaskState int

const (
	Idle TaskState = iota
	InProgress
	Completed
)

type TaskType int

const (
	Map TaskType = iota
	Reduce
	Exit
	Wait
)

type Task struct {
	// Id is the number indicates worker to create and read
	// intermediate files. mappers create mr-{TasksNum}-Y files,
	// reducers read mr-X-{TasksNum} files. While both X and Y is
	// from 0 to OtherNum.
	Id int

	// State is the state of task.
	State TaskState

	// Type is the type of task.
	Type TaskType

	// Filename of Map type Task is the input file name, of
	// Reduce type is the output file name.
	Filename string

	// OtherNum is the total number of tasks in other phase; mappers
	// need this to compute the number of output bins, and reducers
	// needs this to know how many input files to collect.
	OtherNum int

	// StartTime is to prevent worker crash.
	StartTime time.Time
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
