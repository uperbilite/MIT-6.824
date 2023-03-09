package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func doMapTask(jobName string, mapTask int, inFileName string, nReduce int, mapF func(string, string) []KeyValue) {
	f, err := os.Open(inFileName)
	if err != nil {
		log.Fatalf("cannot open %v", inFileName)
	}
	content, err := ioutil.ReadAll(f)
	if err != nil {
		log.Fatalf("cannot read %v", inFileName)
	}
	f.Close()

	kva := mapF(inFileName, string(content))
	var intermediate []KeyValue
	intermediate = append(intermediate, kva...)

	fs := make([]*os.File, nReduce, nReduce)
	encs := make([]*json.Encoder, nReduce, nReduce)
	for i := 0; i < nReduce; i++ {
		fs[i], _ = os.Create("mr-" + strconv.Itoa(mapTask) + "-" + strconv.Itoa(i))
		encs[i] = json.NewEncoder(fs[i])
	}

	for _, kv := range intermediate {
		i := ihash(kv.Key) % nReduce
		encs[i].Encode(&kv)
	}
}

func doReduceTask(jobName string, reduceTask int, outFileName string, nMap int, reduceF func(key string, values []string) string) {
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
		output := reduceF(kva[i].Key, values)

		fmt.Fprintf(outFile, "%v %v\n", kva[i].Key, output)

		i = j
	}

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
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
