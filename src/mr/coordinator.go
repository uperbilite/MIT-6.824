package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
)

// TaskType for Coordinator and Task
type TaskType int

const (
	Map TaskType = iota
	Reduce
	Exit
	Wait
)

type Coordinator struct {
	sync.Mutex

	phase TaskType

	nMap    int
	nReduce int
	files   []string

	tasks []Task

	// protected by the mutex
	newCond *sync.Cond // signals when Register() adds to workers[]
	workers []string   // each worker's UNIX-domain socket name -- its RPC address

	done chan bool
}

type TaskState int

const (
	Idle TaskState = iota
	InProgress
	Completed
)

type Task struct {
	id       int
	state    TaskState
	filename string
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

func (c *Coordinator) GetTask(_ *struct{}, reply *TaskReply) error {
	c.Lock()
	defer c.Unlock()
	switch c.phase {
	case Map:
		for i, t := range c.tasks {
			if t.state == Idle {
				reply.Id = t.id
				reply.Type = Map
				reply.Filename = t.filename
				reply.OtherNum = c.nReduce
				c.tasks[i].state = InProgress
				break
			}
		}
	case Reduce:
		for i, t := range c.tasks {
			if t.state == Idle {
				reply.Id = t.id
				reply.Type = Reduce
				reply.Filename = t.filename
				reply.OtherNum = c.nMap
				c.tasks[i].state = InProgress
				break
			}
		}
	case Wait:
		reply = &TaskReply{Type: Wait}
	case Exit:
		reply = &TaskReply{Type: Exit}
	}
	return nil
}

func (c *Coordinator) setMapTasks() {
	c.tasks = c.tasks[0:0]
	// each input files has a map task.
	for i, f := range c.files {
		t := Task{
			id:       i,
			state:    Idle,
			filename: f,
		}
		c.tasks = append(c.tasks, t)
	}
}

func (c *Coordinator) setReduceTasks() {
	c.tasks = c.tasks[0:0]
	// nReduce tasks in reduce phase.
	for i := 0; i < c.nReduce; i++ {
		t := Task{
			id:       i,
			state:    Idle,
			filename: reduceOutputName(i),
		}
		c.tasks = append(c.tasks, t)
	}
}

func reduceOutputName(id int) string {
	return "mr-out-" + strconv.Itoa(id)
}

func (c *Coordinator) TaskComplete(task *TaskReply, _ *struct{}) error {
	c.Lock()
	defer c.Unlock()
	c.tasks[task.Id].state = Completed
	return nil
}

func (c *Coordinator) Phase() {
	// TODO: monitor task state
	// TODO: if all task is complete, step into next phase
	// TODO: Map -> Reduce, Reduce -> Exit
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

	c.newCond = sync.NewCond(&c)
	c.done = make(chan bool)

	c.setMapTasks()

	c.server()

	return &c
}
