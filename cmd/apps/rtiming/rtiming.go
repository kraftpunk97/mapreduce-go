package main

//
// a MapReduce pseudo-application to test that workers
// execute reduce tasks in parallel.
//
// go build -buildmode=plugin rtiming.go
//

import (
	"fmt"
	"io/ioutil"
	"os"
	"syscall"
	"time"

	"github.com/kraftpunk97/mapreduce-go/pkg/mr"
)

func nparallel(phase string) int {
	// create a file so that other workers will see that
	// we're running at the same time as them.
	pid := os.Getpid()
	myfilename := fmt.Sprintf("mr-worker-%s-%d", phase, pid)
	err := ioutil.WriteFile(myfilename, []byte("x"), 0666)
	if err != nil {
		panic(err)
	}

	// are any other workers running?
	// find their PIDs by scanning directory for mr-worker-XXX files.
	dd, err := os.Open(".")
	if err != nil {
		panic(err)
	}
	names, err := dd.Readdirnames(1000000)
	if err != nil {
		panic(err)
	}
	ret := 0
	for _, name := range names {
		var xpid int
		pat := fmt.Sprintf("mr-worker-%s-%%d", phase)
		n, err := fmt.Sscanf(name, pat, &xpid)
		if n == 1 && err == nil {
			err := syscall.Kill(xpid, 0)
			if err == nil {
				// if err == nil, xpid is alive.
				ret += 1
			}
		}
	}
	dd.Close()

	time.Sleep(1 * time.Second)

	err = os.Remove(myfilename)
	if err != nil {
		panic(err)
	}

	return ret
}

func Map(filename string, contents string) []mr.KeyValue {

	kva := []mr.KeyValue{}
	kva = append(kva, mr.KeyValue{Key: "a", Value: "1"})
	kva = append(kva, mr.KeyValue{Key: "b", Value: "1"})
	kva = append(kva, mr.KeyValue{Key: "c", Value: "1"})
	kva = append(kva, mr.KeyValue{Key: "d", Value: "1"})
	kva = append(kva, mr.KeyValue{Key: "e", Value: "1"})
	kva = append(kva, mr.KeyValue{Key: "f", Value: "1"})
	kva = append(kva, mr.KeyValue{Key: "g", Value: "1"})
	kva = append(kva, mr.KeyValue{Key: "h", Value: "1"})
	kva = append(kva, mr.KeyValue{Key: "i", Value: "1"})
	kva = append(kva, mr.KeyValue{Key: "j", Value: "1"})
	return kva
}

func Reduce(key string, values []string) string {
	n := nparallel("reduce")

	val := fmt.Sprintf("%d", n)

	return val
}
