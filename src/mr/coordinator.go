package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type taskState int

const (
	idle taskState = iota
	inProgress
	done
)

type mapTask struct {
	fileName string
	state    taskState
}

type reduceTask struct {
	id    int
	state taskState
}

type Coordinator struct {
	mu sync.Mutex

	mapTasks []mapTask
	nRemMap  int

	reduceTasks []reduceTask
	nReduce     int
	nRemReduce  int
}

// MakeCoordinator creates a Coordinator.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.nRemMap = len(files)
	c.mapTasks = make([]mapTask, len(files))
	for idx, fname := range files {
		c.mapTasks[idx] = mapTask{
			fileName: fname,
			state:    idle,
		}
	}

	c.nRemReduce = nReduce
	c.nReduce = nReduce
	c.reduceTasks = make([]reduceTask, nReduce)
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = reduceTask{
			id:    i,
			state: idle,
		}
	}

	c.server()
	return &c
}

// Done is called by main/mrcoordinator.go periodically to find out if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.nRemReduce != 0 {
		return false
	}
	if c.nRemMap != 0 {
		log.Fatal("invariant violated: reduce done but map not")
	}
	return true
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.nRemMap > 0 {
		c.makeMapTask(args, reply)
		return nil
	}

	if c.nRemReduce > 0 {
		c.makeReduceTask(args, reply)
		return nil
	}

	for _, t := range c.reduceTasks {
		if t.state != done {
			reply.Type = Wait
			return nil
		}
	}

	reply.Type = Exit
	return nil
}

func (c *Coordinator) makeMapTask(args *GetTaskArgs, reply *GetTaskReply) {

}

func (c *Coordinator) makeReduceTask(args *GetTaskArgs, reply *GetTaskReply) {
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	_ = rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	_ = os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}
