package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"sync"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

type KeyValueSlice []KeyValue

func (a KeyValueSlice) Len() int           { return len(a) }
func (a KeyValueSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a KeyValueSlice) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

workerLoop:
	for {
		task := requestTask()
		switch task.TaskType {
		case MapTask:
			{
				taskName := task.FName
				nReduce := task.NumReduceTask
				taskNum := task.NumMapTask
				fmt.Printf("Performing Map task on %s with %d reduce files\n", taskName, nReduce)
				err := performMapTask(mapf, taskName, nReduce, taskNum)
				if err != nil {
					log.Fatalf("performMapTask failed.")
				}
				notifTaskDone(taskName)
			}
		case ReduceTask:
			{
				taskName := task.FName
				taskNum := task.NumReduceTask
				nMap := task.NumMapTask
				fmt.Printf("Performing Reduce task on %s.\n", taskName)
				err := performReduceTask(reducef, nMap, taskNum)
				if err != nil {
					log.Fatalf("performReduceTask failed")
				}
				notifTaskDone(taskName)

			}
		case NoFileAvailableTask:
			{
				fmt.Printf("No files available now. Sleeping for 3 seconds...\n")
				time.Sleep(3 * time.Second)
			}
		case DoneTask:
			break workerLoop
		}
	}
}

func performMapTask(mapf func(string, string) []KeyValue,
	taskName string, nReduce, mapTaskNum int) error {
	filePath := taskName
	content, err := getFileContent(filePath)
	if err != nil {
		return err
	}
	currentWd, err := os.Getwd()
	if err != nil {
		log.Fatalf("error getting current working directory")
		return err
	}

	intermediateFiles := make([]*os.File, nReduce)
	for reduceTaskNum := range nReduce {
		file, err := os.CreateTemp("", "*")
		if err != nil {
			log.Fatalf("error creating temp file for %s", taskName)
			return err
		}
		intermediateFiles[reduceTaskNum] = file
		newPath := currentWd + fmt.Sprintf("/mr-%d-%d", mapTaskNum, reduceTaskNum)
		defer os.Rename(file.Name(), newPath)
		defer file.Close()
	}

	encoders := make([]*json.Encoder, nReduce)
	writers := make([]*bufio.Writer, nReduce)
	for reduceTaskNum := range nReduce {
		intermediateFile := intermediateFiles[reduceTaskNum]
		writer := bufio.NewWriter(intermediateFile)
		encoder := json.NewEncoder(writer)

		writers[reduceTaskNum] = writer
		encoders[reduceTaskNum] = encoder
		defer writer.Flush()
	}

	kva := mapf(filePath, string(content))

	err = writeKeyValues(kva, encoders)
	if err != nil {
		return err
	}
	return nil
}

func performReduceTask(reducef func(string, []string) string, nMap, reduceTaskNum int) (err error) {
	// Write a goroutine to open a file, read its contents and add them to the kv
	kva := make([]KeyValue, 0, 100000)

	kvaMutex := sync.Mutex{}
	kvaWriteWG := sync.WaitGroup{}
	for i := range nMap {
		filePath := fmt.Sprintf("mr-%d-%d", i, reduceTaskNum)
		kvaWriteWG.Add(1)
		go reduceFileUtil(filePath, &kva, &kvaMutex, &kvaWriteWG)
	}
	kvaWriteWG.Wait()

	sort.Sort(KeyValueSlice(kva))

	oname := fmt.Sprintf("mr-out-%d", reduceTaskNum)
	outputFile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("error creating output file %s", oname)
		return err
	}
	defer outputFile.Close()

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}

		output := reducef(kva[i].Key, values)
		fmt.Fprintf(outputFile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	return nil
}

func reduceFileUtil(filePath string, kva *[]KeyValue, kvaMutex *sync.Mutex, kvaWriteWG *sync.WaitGroup) (err error) {
	defer kvaWriteWG.Done()
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("error opening file %s", filePath)
		return err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := decoder.Decode(&kv); err != nil {
			break
		}
		kvaMutex.Lock()
		*kva = append(*kva, kv)
		kvaMutex.Unlock()
	}
	return nil
}

func getFileContent(filePath string) ([]byte, error) {
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("error opening file %s", filePath)
		return nil, err
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("error reading file %s", filePath)
		return nil, err
	}
	return content, nil
}

func writeKeyValues(kva []KeyValue, encoders []*json.Encoder) error {
	numReduceTasks := len(encoders)
	for _, kv := range kva {
		key := kv.Key
		reduceFileIdx := ihash(key) % numReduceTasks
		encoders[reduceFileIdx].Encode(kv)
	}
	return nil
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func requestTask() TaskResponse {
	args := struct{}{}
	reply := TaskResponse{}
	call("Coordinator.TaskRequest", &args, &reply)
	return reply
}

func notifTaskDone(fname string) {
	args := TaskDoneNotif{fname}
	reply := struct{}{}
	call("Coordinator.NotifTaskDone", &args, &reply)
	fmt.Printf("Notifying task as done: %s\n", fname)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args any, reply any) bool {
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
