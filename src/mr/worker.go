package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
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
			handleMap(mapf, &args, &reply)
			return
		case Reduce:
			handleReduce(reducef, &args, &reply)
			return
		case Wait:
			time.Sleep(time.Second)
		default:
			panic(fmt.Sprintf("unexpected mr.TaskType: %#v", reply.Type))
		}
	}
}

func handleMap(mapf func(string, string) []KeyValue, _ *GetTaskArgs, reply *GetTaskReply) {
	filename := reply.Filename
	fmt.Println(reply)
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

	intermediate := mapf(filename, string(content))

	grouped := make([][]KeyValue, reply.NReduce)
	for _, kv := range intermediate {
		reduceID := ihash(kv.Key) % reply.NReduce
		grouped[reduceID] = append(grouped[reduceID], kv)
	}

	for reduceID, grouped := range grouped {
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

// for sorting by key.
type byKey []KeyValue

func (a byKey) Len() int           { return len(a) }
func (a byKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func handleReduce(reducef func(string, []string) string, args *GetTaskArgs, reply *GetTaskReply) {
	reduceID := reply.ID

	intermediate := []KeyValue{}

	for m := 0; m < reply.NMap; m++ {
		interfname := fmt.Sprintf("mr-%d-%d", m, reduceID)
		interf, err := os.Open(interfname)
		if err != nil {
			log.Fatalf("cannot open %v", interfname)
			// TODO: notify coordinator
		}
		dec := json.NewDecoder(interf)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		interf.Close()
	}

	sort.Sort(byKey(intermediate))

	ofilename := fmt.Sprintf("mr-out-%d", reduceID)
	ofile, err := ioutil.TempFile(".", "temp-"+ofilename)
	if err != nil {
		log.Fatalf("cannot create an output file")
		// TODO: notify coordinator
	}
	tmpname := ofile.Name()

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
		output := reducef(intermediate[i].Key, values)

		if _, err := fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output); err != nil {
			log.Fatalf("cannot write output")
			// TODO: notify coordinator
		}

		i = j
	}

	ofile.Close()

	err = os.Rename(tmpname, ofilename)
	if err != nil {
		log.Fatalf("cannot create an output file")
		// TODO: notify coordinator
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

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
