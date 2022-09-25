package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

var workerID int = -1
var mapfun func(string, string) []KeyValue
var reducefun func(string, []string) string
var fileNum int = 0

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func handleMap(reply *RequestTaskReply) (ret *FinishTaskArgs) {
	ret = new(FinishTaskArgs)
	ret.IsMap = true
	ret.WorkerID = workerID
	// create nReduce encoders
	encoders := make([]*json.Encoder, reply.NReduce)
	for i := 0; i < reply.NReduce; i++ {
		mapFileName := fmt.Sprintf("mr-%d-%d-%d", workerID, i, fileNum)
		fileNum++
		ret.Files = append(ret.Files, mapFileName)
		mapFile, err := os.Create(mapFileName)
		if err != nil {
			log.Fatalf("cannot create the temporaty file %v", mapFileName)
		}
		encoders[i] = json.NewEncoder(mapFile)
	}

	for _, filename := range reply.FileName {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapfun(filename, string(content))
		// write the k/v to the corresponding file
		for _, v := range kva {
			index := ihash(v.Key) % reply.NReduce
			encoders[index].Encode(&v)
		}
	}
	return ret
}

func handleReduce(reply *RequestTaskReply) (ret *FinishTaskArgs) {
	deleteFiles := make([]string, 0)
	ret = new(FinishTaskArgs)
	ret.IsMap = false
	ret.WorkerID = workerID
	// create nReduce encoders
	intermediate := []KeyValue{}
	for _, filename := range reply.FileName {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
		deleteFiles = append(deleteFiles, filename)
	}

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", reply.NReduce)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducefun(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
	for _, v := range deleteFiles {
		os.Remove(v)
	}
	return ret
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	mapfun = mapf
	reducefun = reducef
	for {
		reply := CallRequest()
		if reply.Finished {
			break
		}
		if len(reply.FileName) == 0 {
			time.Sleep(time.Second)
			continue
		}
		var finishArgs *FinishTaskArgs
		switch {
		case reply.Postpone:
			time.Sleep(time.Second)
			continue
		case reply.IsMap:
			finishArgs = handleMap(reply)
		case !reply.IsMap:
			finishArgs = handleReduce(reply)
		}
		CallFinish(finishArgs)
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

func CallRequest() *RequestTaskReply {

	// declare an argument structure.
	args := RequestTaskArgs{WorkerID: workerID}

	// declare a reply structure.
	reply := RequestTaskReply{}
	reply.FileName = make([]string, 0)

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.RequestTask", &args, &reply)
	workerID = reply.WorkerID
	if ok {
		return &reply
	} else {
		fmt.Printf("call failed!\n")
		return nil
	}
}

func CallFinish(args *FinishTaskArgs) {
	reply := FinishTaskReply{}

	ok := call("Coordinator.FinishTask", args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
		return
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
