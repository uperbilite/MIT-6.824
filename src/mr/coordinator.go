package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

// State for Coordinator and Task
type State int

const (
	Map State = iota
	Reduce
	Exit
	Wait
)

type Coordinator struct {
	sync.Mutex

	state State

	nMap         int
	nReduce      int
	processedNum int
	files        []string

	// protected by the mutex
	newCond *sync.Cond // signals when Register() adds to workers[]
	workers []string   // each worker's UNIX-domain socket name -- its RPC address

	done chan bool
}

type Task struct {
	id       int
	filename string
	state    State
	tasksNum int
	// otherNum is the total number of tasks in other phase; mappers
	// need this to compute the number of output bins, and reducers
	// needs this to know how many input files to collect.
	otherNum int
}

func (c *Coordinator) Register(args *RegisterArgs, _ *struct{}) error {
	c.Lock()
	defer c.Unlock()
	c.workers = append(c.workers, args.Worker)

	// tell forwardRegistrations() that there's a new workers[] entry.
	c.newCond.Broadcast()

	return nil
}

// forwardRegistrations sends information about all existing
// and newly registered workers to channel ch. schedule()
// reads ch to learn about workers.
func (c *Coordinator) forwardRegistrations(ch chan string) {
	i := 0
	for {
		c.Lock()
		if len(c.workers) > i {
			// there's a worker that we haven't told schedule() about.
			w := c.workers[i]
			go func() { ch <- w }() // send without holding the lock.
			i = i + 1
		} else {
			// wait for Register() to add an entry to workers[]
			// in response to an RPC from a new worker.
			c.newCond.Wait()
		}
		c.Unlock()
	}
}

func (c *Coordinator) GetMapTask(_ *struct{}, reply *GetTaskReply) error {
	if c.processedNum == c.nMap {
		return nil
	}
	reply.Filename = c.files[c.processedNum]
	reply.TasksNum = c.processedNum
	reply.OtherNum = c.nReduce
	c.processedNum += 1
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = files
	c.nMap = len(files)
	c.nReduce = nReduce
	c.processedNum = 0

	c.newCond = sync.NewCond(&c)
	c.done = make(chan bool)

	c.server()

	return &c
}
