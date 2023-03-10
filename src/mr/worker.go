package mr

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapF func(string, string) []KeyValue, reduceF func(string, []string) string) {
	for {
		task := getTask()
		// log.Printf("Worker: receive coordinator's task %v \n", task)
		switch task.Type {
		case Map:
			doMapTask(task, mapF)
		case Reduce:
			doReduceTask(task, reduceF)
		case Wait:
			time.Sleep(1 * time.Second)
		case Exit:
			return
		}
	}
}

func getTask() Task {
	task := Task{}
	call("Coordinator.GetTask", new(struct{}), &task)
	return task
}

func taskCompleted(task Task) {
	call("Coordinator.TaskCompleted", &task, new(struct{}))
}

func doMapTask(task Task, mapF func(string, string) []KeyValue) {
	mapTask := task.Id
	inFileName := task.Filename
	nReduce := task.OtherNum

	content, err := os.ReadFile(inFileName)
	if err != nil {
		log.Fatalf("cannot read %v", inFileName)
	}

	intermediates := mapF(inFileName, string(content))

	fs := make([]*os.File, nReduce, nReduce)
	encs := make([]*json.Encoder, nReduce, nReduce)
	for i := 0; i < nReduce; i++ {
		fs[i], _ = os.Create("mr-" + strconv.Itoa(mapTask) + "-" + strconv.Itoa(i))
		encs[i] = json.NewEncoder(fs[i])
	}

	for _, kv := range intermediates {
		i := ihash(kv.Key) % nReduce
		encs[i].Encode(&kv)
	}

	taskCompleted(task)
}

func doReduceTask(task Task, reduceF func(key string, values []string) string) {
	reduceTask := task.Id
	outFileName := task.Filename
	nMap := task.OtherNum

	var kva []KeyValue

	for i := 0; i < nMap; i++ {
		f, err := os.Open("mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reduceTask))
		if err != nil {
			log.Fatalf("cannot open %v", f)
		}

		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}

		f.Close()
	}

	sort.Sort(ByKey(kva))
	outFile, _ := os.Create(outFileName)

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}

		fmt.Fprintf(outFile, "%v %v\n", kva[i].Key, reduceF(kva[i].Key, values))

		i = j
	}

	taskCompleted(task)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
