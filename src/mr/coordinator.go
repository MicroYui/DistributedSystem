package mr

import (
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type CoordinatorStatus int
type State int

// Coordinator状态
const (
	Idle CoordinatorStatus = iota
	InProgress
	Completed
)

// 整个事件的状态
// 此外，每个worker分配到的任务的状态也同样是这几个状态
const (
	Map State = iota
	Reduce
	Exit
	Wait
)

type Task struct {
	// 读入文件
	Input string
	// 输出文件
	Output string
	// 任务阶段
	TaskPhase State
	// Reduce数量
	NReduce int
	// Map任务产生的R个中间文件的信息
	Intermediates [][]string
}

type Coordinator struct {
	// Your definitions here.
	// 记录当前所有的Task
	TaskQueue chan *Task
	// 记录当前Coordinator所处的状态
	CoordinatorPhase State
	// Reduce数量
	NReduce int
	// 互斥锁
	mu sync.Mutex
}

func max(x int, y int) int {
	if x > y {
		return y
	}
	return x
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		TaskQueue:        make(chan *Task, max(nReduce, len(files))),
		CoordinatorPhase: Map,
		NReduce:          nReduce,
		mu:               sync.Mutex{},
	}

	// Your code here.

	c.server()
	return &c
}

// AssignTask 分配任务
func (c *Coordinator) AssignTask(args *ExampleArgs, reply *Task) error {
	// 分配任务为原子操作
	c.mu.Lock()
	defer c.mu.Unlock()

	// 查找当前任务管道，若非空则安排任务
	if len(c.TaskQueue) > 0 {
		// 注意：这里不能更改引用，而应该是更改指针指向的内容
		*reply = *<-c.TaskQueue
	} else if c.CoordinatorPhase == Exit {
		// 如果完成则返回Exit
		*reply = Task{TaskPhase: Exit}
	} else {
		// 否则全部视为等待
		*reply = Task{TaskPhase: Wait}
	}

	return nil
}
