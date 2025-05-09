package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type TaskStatus int

const (
	TaskReady TaskStatus = iota
	TaskInProcessing
	TaskDone
)

type Task struct {
	TaskNum  int
	TaskName string
	TaskStatus
	TaskType
}

type Coordinator struct {
	// Your definitions here.
	TaskMap           map[string]Task
	mapWG             sync.WaitGroup
	reduceWG          sync.WaitGroup
	TaskMutex         sync.Mutex
	TimeoutDisableMap map[string](chan struct{})
	numReduce         int
	numMap            int
	done              atomic.Bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) TaskRequest(args *struct{}, reply *TaskResponse) error {
	var task *Task = c.fetchNewTask()
	reply.FName = task.TaskName
	reply.TaskType = task.TaskType
	switch task.TaskType {
	case MapTask:
		{
			fmt.Printf("Dispatching Map Task %s for processing...\n", task.TaskName)
			reply.NumMapTask = task.TaskNum
			reply.NumReduceTask = c.numReduce
		}
	case ReduceTask:
		{
			fmt.Printf("Dispatching Reduce Task %s for processing...\n", task.TaskName)
			reply.NumMapTask = c.numMap
			reply.NumReduceTask = task.TaskNum
		}
	case NoFileAvailableTask:
		{
			fmt.Printf("No tasks available.\n")
		}
	default:
		{
			fmt.Printf("Unknown case\n")
		}
	}
	go c.beginTaskTimeout(task.TaskName)
	return nil
}

func (c *Coordinator) NotifTaskDone(args *TaskDoneNotif, reply *struct{}) error {
	taskName := args.FName
	c.markTaskDone(taskName)
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 2
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
	return c.done.Load()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		TaskMap:           make(map[string]Task),
		mapWG:             sync.WaitGroup{},
		reduceWG:          sync.WaitGroup{},
		TimeoutDisableMap: make(map[string](chan struct{})),
		numReduce:         nReduce,
		done:              atomic.Bool{},
	}
	c.done.Store(false)
	fmt.Println(files)
	// Your code here.
	c.setup(files)
	go c.mapReduce()
	c.server()

	return &c
}

func (c *Coordinator) fetchNewTask() *Task {
	c.TaskMutex.Lock()
	defer c.TaskMutex.Unlock()
	for taskName, task := range c.TaskMap {
		if task.TaskStatus == TaskReady {
			task.TaskStatus = TaskInProcessing
			c.TaskMap[taskName] = task // Doing this to avoid the UnaddressableFieldAssign error
			return &task
		}
	}

	return &Task{
		TaskName:   "No files available",
		TaskType:   NoFileAvailableTask,
		TaskNum:    -1,
		TaskStatus: TaskDone,
	}
}

func (c *Coordinator) markTaskDone(taskName string) {
	c.TaskMutex.Lock()
	defer c.TaskMutex.Unlock()
	if c.TaskMap[taskName].TaskStatus == TaskInProcessing {
		c.TimeoutDisableMap[taskName] <- struct{}{}
		task := c.TaskMap[taskName] // Doing this to avoid the UnaddressableFieldAssign error
		task.TaskStatus = TaskDone
		c.TaskMap[taskName] = task
		switch task.TaskType {
		case MapTask:
			c.mapWG.Done()
		case ReduceTask:
			c.reduceWG.Done()
		}
	}
}

func (c *Coordinator) resetTask(taskName string) {
	c.TaskMutex.Lock()
	defer c.TaskMutex.Unlock()
	taskDetails := c.TaskMap[taskName]
	if taskDetails.TaskStatus == TaskInProcessing {
		taskDetails.TaskStatus = TaskReady
		c.TaskMap[taskName] = taskDetails // Doing this to avoid the UnaddressableFieldAssign error
		fmt.Printf("Resetting task: %s\n", taskName)
	}
}

func (c *Coordinator) beginTaskTimeout(taskName string) {
	if taskName == "No files available" {
		return
	}
	select {
	case <-c.TimeoutDisableMap[taskName]:
		c.TaskMutex.Lock()
		switch c.TaskMap[taskName].TaskType {
		case MapTask:
			fmt.Printf("Recieved notification that Map task %s is done\n", taskName)
		case ReduceTask:
			fmt.Printf("Recieved notification that Reduce task %s is done\n", taskName)
		}
		c.TaskMutex.Unlock()
	case <-time.After(10 * time.Second):
		c.resetTask(taskName)
	}
}

func (c *Coordinator) collectFiles(files []string) []os.DirEntry {
	filesCollection := make([]os.DirEntry, 0, 10)
	for _, dir := range files {
		files, err := os.ReadDir(dir)
		if err != nil {
			log.Fatalf("cannot read the directory %s; skipping...\n", dir)
		}
		filesCollection = append(filesCollection, files...)
	}
	c.numMap = len(filesCollection)
	return filesCollection
}

func (c *Coordinator) setup(files []string) {
	for ctr, file := range files {
		c.TaskMap[file] = Task{
			TaskNum:    ctr,
			TaskName:   file,
			TaskType:   MapTask,
			TaskStatus: TaskReady,
		}
		c.numMap += 1
		c.TimeoutDisableMap[file] = make(chan struct{})
		c.mapWG.Add(1)
	}
}

func (c *Coordinator) runReducePhase() {
	c.TaskMutex.Lock()
	defer c.TaskMutex.Unlock()
	for reduceTaskNum := range c.numReduce {
		taskName := fmt.Sprintf("ReduceTask_%d", reduceTaskNum)
		task := Task{
			TaskNum:    reduceTaskNum,
			TaskName:   taskName,
			TaskStatus: TaskReady,
			TaskType:   ReduceTask,
		}
		c.TaskMap[taskName] = task
		c.TimeoutDisableMap[taskName] = make(chan struct{})
		c.reduceWG.Add(1)
	}
}

func (c *Coordinator) mapReduce() error {
	c.mapWG.Wait()
	c.runReducePhase()
	c.reduceWG.Wait()
	c.done.Store(true)
	return nil
}
