package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Phase int

const (
	MapPhase Phase = iota
	ReducePhase
	ExitPhase
)

type Coordinator struct {
	sync.RWMutex
	phase   Phase
	nMap    int
	nReduce int
	files   []string
	tasks   []Task
}

func (c *Coordinator) getIdleTask(task *Task) {
	c.RLock()
	defer c.RUnlock()

	for _, t := range c.tasks {
		if t.State == Idle {
			task.Id = t.Id
			task.Type = t.Type
			task.Filename = t.Filename
			task.OtherNum = t.OtherNum
			return
		}
	}
	// no idle task
	task.Type = Wait
}

func (c *Coordinator) GetTask(_ *struct{}, task *Task) error {
	c.RLock()
	phase := c.phase
	c.RUnlock()

	switch phase {
	case MapPhase:
		c.getIdleTask(task)
	case ReducePhase:
		c.getIdleTask(task)
	case ExitPhase:
		task.Type = Exit
	}

	// workers don't need task state, while coordinator need.
	if task.Type != Wait {
		c.Lock()
		c.tasks[task.Id].State = InProgress
		c.Unlock()
	}

	return nil
}

// setMapTasks before map phase.
func (c *Coordinator) setMapTasks() {
	c.Lock()
	defer c.Unlock()

	c.phase = MapPhase
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

// setReduceTasks before reduce phase
func (c *Coordinator) setReduceTasks() {
	c.Lock()
	defer c.Unlock()

	c.phase = ReducePhase
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

func (c *Coordinator) setExitTasks() {
	c.Lock()
	defer c.Unlock()
	c.phase = ExitPhase
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

func (c *Coordinator) isAllComplete() bool {
	c.RLock()
	defer c.RUnlock()

	for _, t := range c.tasks {
		if t.State != Completed {
			return false
		}
	}
	return true
}

func (c *Coordinator) SchedulePhase() {
	c.setMapTasks()

	for {
		c.RLock()
		phase := c.phase
		c.RUnlock()

		switch phase {
		case MapPhase:
			if c.isAllComplete() {
				c.setReduceTasks()
			}
		case ReducePhase:
			if c.isAllComplete() {
				c.setExitTasks()
			}
		case ExitPhase:
			return
		}

		time.Sleep(1 * time.Second)
	}
}

func (c *Coordinator) CatchCrash() {
	for {
		time.Sleep(20 * time.Second)

		c.RLock()
		phase := c.phase
		c.RUnlock()

		if phase == ExitPhase {
			return
		}

		for _, t := range c.tasks {
			c.RLock()
			isCrash := t.State == InProgress && time.Now().Sub(t.StartTime) > 20*time.Second
			c.RUnlock()
			if isCrash {
				c.Lock()
				c.tasks[t.Id].State = Idle
				c.Unlock()
			}
		}
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
	c.RLock()
	defer c.RUnlock()
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
		nMap:    len(files),
		nReduce: nReduce,
		files:   files,
	}

	c.server()

	go c.SchedulePhase()
	go c.CatchCrash()

	return &c
}
