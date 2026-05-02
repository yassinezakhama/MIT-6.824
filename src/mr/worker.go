package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

type KeyValue struct {
	Key   string
	Value string
}

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	args, reply := GetTaskArgs{}, GetTaskReply{}
	for {
		ok := call("Coordinator.GetTask", &args, &reply)
		if !ok {
			return
		}
		switch reply.Type {
		case Exit:
			return
		case Map:
		case Reduce:
		case Wait:
			time.Sleep(time.Second)
		default:
			panic(fmt.Sprintf("unexpected mr.TaskType: %#v", reply.Type))
		}
	}
}

func handleMap(mapf func(string, string) []KeyValue, _ *GetTaskArgs, reply *GetTaskReply) {
	filename := reply.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		// TODO: notify coordinator
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		// TODO: notify coordinator
	}
	_ = file.Close()

	kva := mapf(filename, string(content))

	groupedkva := make([][]KeyValue, reply.NReduce)
	for _, kv := range kva {
		reduceID := ihash(kv.Key) % reply.NReduce
		groupedkva[reduceID] = append(groupedkva[reduceID], kv)
	}

	for reduceID, grouped := range groupedkva {
		interfilename := fmt.Sprintf("mr-%v-%v", reply.ID, reduceID)
		interfile, err := os.Create(interfilename)
		if err != nil {
			log.Fatalf("cannot create intermediate file %v", interfilename)
			// TODO: notify coordinator
		}
		enc := json.NewEncoder(interfile)
		for _, kv := range grouped {
			err = enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot encode key/value pair %v", kv)
				// TODO: notify coordinator
			}
		}
		_ = interfile.Close()
	}
}

func handleReduce(args *GetTaskArgs, reply *GetTaskReply) {}

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

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
