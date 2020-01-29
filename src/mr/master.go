package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Master struct {
	// Your definitions here.
	sync.Mutex

	address     string    // master rpc address
	doneChannel chan bool // job finish channel

	// protected by the mutex
	newCond *sync.Cond // signals when Register() adds to workers[]
	workers []string   // each worker's UNIX-domain socket name -- its RPC address

	// Per-task information
	jobName string   // Name of currently executing job
	files   []string // Input files
	nReduce int      // Number of reduce partitions

	shutdown chan struct{} // master rpc shutdown chan
	l        net.Listener  // master rpc server listener
	stats    []int
}

// Your code here -- RPC handlers for the worker to call.

// Register is an RPC method that is called by workers after they have started
// up to report that they are ready to receive tasks.
func (m *Master) Register(args *RegisterArgs, _ *struct{}) error {
	m.Lock()
	defer m.Unlock()
	debug("Register: worker %s\n", args.Worker)
	m.workers = append(m.workers, args.Worker)

	// tell forwardRegistrations() that there's a new workers[] entry.
	m.newCond.Broadcast()

	return nil
}

// helper function that sends information about all existing
// and newly registered workers to channel ch. schedule()
// reads ch to learn about workers.
func (m *Master) forwardRegistrations(ch chan string) {
	i := 0
	for {
		m.Lock()
		if len(m.workers) > i {
			// there's a worker that we haven't told schedule() about.
			w := m.workers[i]
			go func() { ch <- w }() // send without holding the lock.
			i = i + 1
		} else {
			// wait for Register() to add an entry to workers[]
			// in response to an RPC from a new worker.
			m.newCond.Wait()
		}
		m.Unlock()
	}
}

// Your code here -- RPC handlers for the control to call.

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	os.Remove("mr-socket")
	l, e := net.Listen("unix", "mr-socket")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// run executes a mapreduce job on the given number of mappers and reducers.
//
// First, it divides up the input file among the given number of mappers, and
// schedules each task on workers as they become available. Each map task bins
// its output in a number of bins equal to the given number of reduce tasks.
// Once all the mappers have finished, workers are assigned reduce tasks.
//
// When all tasks have been completed, the reducer outputs are merged,
// statistics are collected, and the master is shut down.
//
// Note that this implementation assumes a shared file system.
func (m *Master) run(jobName string, files []string, nReduce int,
	schedule func(phase jobPhase),
	finish func(),
) {
	m.jobName = jobName
	m.files = files
	m.nReduce = nReduce

	fmt.Printf("%s: Starting Map/Reduce task %s\n", m.address, m.jobName)

	schedule(mapPhase)
	schedule(reducePhase)
	finish()
	// 2018 diff
	//m.merge()

	fmt.Printf("%s: Map/Reduce task completed\n", m.address)

	m.doneChannel <- true
}

// Wait blocks until the currently scheduled work has completed.
// This happens when all tasks have scheduled and completed, the final output
// have been computed, and all workers have been shut down.
func (m *Master) Wait() {
	<-m.doneChannel
}

// killWorkers cleans up all workers by sending each one a Shutdown RPC.
// It also collects and returns the number of tasks each worker has performed.
func (m *Master) killWorkers() []int {
	m.Lock()
	defer m.Unlock()
	nTasks := make([]int, 0, len(m.workers))
	for _, w := range m.workers {
		debug("Master: shutdown worker %s\n", w)
		var reply ShutdownReply
		ok := call(w, "Worker.Shutdown", new(struct{}), &reply)
		if ok == false {
			fmt.Printf("Master: RPC %s shutdown error\n", w)
		} else {
			nTasks = append(nTasks, reply.Ntasks)
		}
	}
	return nTasks
}

// newMaster initializes a new Map/Reduce Master
func newMaster(master string) (m *Master) {
	m = new(Master)
	m.address = master
	m.shutdown = make(chan struct{})
	m.newCond = sync.NewCond(m)
	m.doneChannel = make(chan bool)
	return
}

// Sequential runs map and reduce tasks sequentially, waiting for each task to
// complete before running the next.
func Sequential(jobName string, files []string, nreduce int,
	mapF func(string, string) []KeyValue,
	reduceF func(string, []string) string,
) (m *Master) {
	m = newMaster("master")
	go m.run(jobName, files, nreduce, func(phase jobPhase) {
		switch phase {
		case mapPhase:
			for i, f := range m.files {
				doMap(m.jobName, i, f, m.nReduce, mapF)
			}
		case reducePhase:
			for i := 0; i < m.nReduce; i++ {
				doReduce(m.jobName, i, mergeName(m.jobName, i), len(m.files), reduceF)
			}
		}
	}, func() {
		m.stats = []int{len(files) + nreduce}
	})
	return
}

// Distributed schedules map and reduce tasks on workers that register with the
// master over RPC.
func Distributed(jobName string, files []string, nreduce int, master string) (m *Master) {
	m = newMaster(master)
	m.startRPCServer()
	go m.run(jobName, files, nreduce,
		func(phase jobPhase) {
			ch := make(chan string)
			go m.forwardRegistrations(ch)
			schedule(m.jobName, m.files, m.nReduce, phase, ch)
		},
		func() {
			m.stats = m.killWorkers()
			m.stopRPCServer()
		})
	return
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	m.Wait()
	ret = true
	return ret
}

//
// create a Master and run job.
//
func MakeMaster(files []string, nReduce int) (m *Master) {
	// Your code here.
	jobName := "default"
	address := "mr-socket"

	m = new(Master)
	m.address = address
	m.shutdown = make(chan struct{})
	m.newCond = sync.NewCond(m)
	m.doneChannel = make(chan bool)

	m.startRPCServer()

	// Distributed schedules map and reduce tasks on workers that register with the
	// master over RPC.
	go m.run(jobName, files, nReduce,
		func(phase jobPhase) {
			ch := make(chan string)
			go m.forwardRegistrations(ch)
			schedule(m.jobName, m.files, m.nReduce, phase, ch)
		},
		func() {
			m.stats = m.killWorkers()
			m.stopRPCServer()
		})

	return
}
