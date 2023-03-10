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

type Phase int

const (
	MapPhase Phase = iota
	ReducePhase
	ExitPhase
)

type Coordinator struct {
	sync.Mutex

	phase Phase

	nMap    int
	nReduce int
	files   []string

	tasks []Task
}

func (c *Coordinator) getIdleTask(phase Phase, task *Task) {
	for i, t := range c.tasks {
		if t.State == Idle {
			task.Id = t.Id

			// workers don't need task state, while coordinator need.
			c.tasks[i].State = InProgress

			if phase == MapPhase {
				task.Type = Map
			} else if phase == ReducePhase {
				task.Type = Reduce
			}

			task.Filename = t.Filename
			task.OtherNum = t.OtherNum

			return
		}
	}
	// no reduce task is idle
	task.Type = Wait
}

func (c *Coordinator) GetTask(_ *struct{}, task *Task) error {
	c.Lock()
	defer c.Unlock()

	switch c.phase {
	case MapPhase:
		c.getIdleTask(MapPhase, task)
	case ReducePhase:
		c.getIdleTask(ReducePhase, task)
	case ExitPhase:
		task.Type = Exit
	}

	return nil
}

func (c *Coordinator) setMapTasks() {
	c.tasks = c.tasks[0:0]
	// each input files has a map task.
	for i, f := range c.files {
		t := Task{
			Id:       i,
			State:    Idle,
			Type:     Map,
			Filename: f,
			OtherNum: c.nReduce,
		}
		c.tasks = append(c.tasks, t)
	}
}

func (c *Coordinator) setReduceTasks() {
	c.tasks = c.tasks[0:0]
	// nReduce tasks in reduce phase.
	for i := 0; i < c.nReduce; i++ {
		t := Task{
			Id:       i,
			State:    Idle,
			Type:     Reduce,
			Filename: reduceOutputName(i),
			OtherNum: c.nMap,
		}
		c.tasks = append(c.tasks, t)
	}
}

func reduceOutputName(id int) string {
	return "mr-out-" + strconv.Itoa(id)
}

func (c *Coordinator) TaskCompleted(task *Task, _ *struct{}) error {
	c.Lock()
	defer c.Unlock()
	c.tasks[task.Id].State = Completed
	return nil
}

func isAllComplete(tasks []Task) bool {
	for _, t := range tasks {
		if t.State != Completed {
			return false
		}
	}
	return true
}

func (c *Coordinator) Phase() {
	for {
		c.Lock()
		switch c.phase {
		case MapPhase:
			if isAllComplete(c.tasks) {
				c.phase = ReducePhase
				c.setReduceTasks()
			}
		case ReducePhase:
			if isAllComplete(c.tasks) {
				c.phase = ExitPhase
			}
		case ExitPhase:
			c.Unlock()
			return
		}
		c.Unlock()
	}
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
	if c.phase == ExitPhase {
		return true
	}
	return false
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		phase:   MapPhase,
		nMap:    len(files),
		nReduce: nReduce,
		files:   files,
	}

	c.setMapTasks()

	c.server()
	go c.Phase()

	return &c
}
