package mr

import (
	"container/list"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mapDone           bool
	allDone           bool
	nWorkers          int
	nReduce           int
	mapWork           *list.List
	reduceWork        *list.List
	notFinished       *list.List
	workerTaskMap     map[int]*list.Element
	intermediateFiles *list.List
	mu                sync.Mutex
}

type Task struct {
	filenames []string
	time      time.Time
	workerID  int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
func (c *Coordinator) checkTimeout() {
	for tmp := c.notFinished.Front(); tmp != nil; {
		next := tmp.Next()
		current := time.Now()
		task := tmp.Value.(Task)
		previous := task.time
		diff := current.Sub(previous).Seconds()
		if diff < 10 {
			break
		}
		workerID := task.workerID
		for _, v := range task.filenames {
			if c.mapDone {
				c.reduceWork.PushBack(v)
			} else {
				c.mapWork.PushBack(v)
			}
		}
		c.notFinished.Remove(tmp)
		delete(c.workerTaskMap, workerID)
		tmp = next
	}

}

// the method through which the worker request task
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// distribute the workerID
	workerID := args.WorkerID
	if workerID == -1 {
		reply.WorkerID = c.nWorkers
		workerID = c.nWorkers
		c.nWorkers++
	}
	reply.WorkerID = workerID
	c.checkTimeout()
	switch {
	// there is remain map to do
	case c.mapWork.Len() > 0:
		// get a task from the maptasks
		reply.FileName = append(reply.FileName, c.mapWork.Front().Value.(string))
		c.notFinished.PushBack(Task{[]string{c.mapWork.Remove(c.mapWork.Front()).(string)}, time.Now(), workerID})
		c.workerTaskMap[workerID] = c.notFinished.Back()
		// set task type
		reply.IsMap = true
		reply.NReduce = c.nReduce
		// there is no map task but have map task be doing
	case !c.mapDone:
		reply.Finished = false
		// there are only reduce task to do
	case c.reduceWork.Len() > 0:
		reply.IsMap = false
		reply.NReduce = c.nReduce
		X, Y := 0, 0
		fmt.Sscanf(c.reduceWork.Front().Value.(string), "mr-%d-%d", &X, &Y)
		for tmp := c.reduceWork.Front(); tmp != nil; {
			next := tmp.Next()
			tmp_x, tmp_y := 0, 0
			fmt.Sscanf(tmp.Value.(string), "mr-%d-%d", &tmp_x, &tmp_y)
			if tmp_y == Y {
				reply.FileName = append(reply.FileName, tmp.Value.(string))
				c.reduceWork.Remove(tmp)
			}
			tmp = next
		}
		c.notFinished.PushBack(Task{reply.FileName, time.Now(), workerID})
		c.workerTaskMap[workerID] = c.notFinished.Back()
		reply.NReduce = Y
	case c.notFinished.Len() > 0:
		reply.Finished = false
	default:
		reply.Finished = true
	}

	return nil
}

func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case args.IsMap:
		v, ok := c.workerTaskMap[args.WorkerID]
		// if the task is timeout
		if !ok {
			break
		}
		// put the tmp files into reduce task
		taskFinished := args.Files
		for _, work := range taskFinished {
			c.reduceWork.PushBack(work)
		}
		// delete from the notFinished queue
		c.notFinished.Remove(v)
		delete(c.workerTaskMap, args.WorkerID)
		// judge if all map tasks have been done
		if c.mapWork.Len() == 0 && c.notFinished.Len() == 0 {
			c.mapDone = true
		}
	case !args.IsMap:
		v, ok := c.workerTaskMap[args.WorkerID]
		// if the task is timeout
		if !ok {
			break
		}
		// delete from the notFinished queue
		c.notFinished.Remove(v)
		delete(c.workerTaskMap, args.WorkerID)
		// judge if all map tasks have been done
		if c.reduceWork.Len() == 0 && c.notFinished.Len() == 0 {
			c.allDone = true
		}
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := c.allDone
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.mapWork = list.New()
	for _, v := range files {
		c.mapWork.PushBack(v)
	}
	c.intermediateFiles = list.New()
	c.reduceWork = list.New()
	c.notFinished = list.New()
	c.nWorkers = 0
	c.nReduce = nReduce
	c.mapDone = false
	c.workerTaskMap = make(map[int]*list.Element)
	c.server()
	return &c
}
