package mr

import (
	"fmt"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)

	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	// NOTE: wait until all tasks have completed, call wg.Done() when any task completes
	constructTaskArgs := func(phase jobPhase, task int) DoTaskArgs {
		debug("task: %d\n", task)
		var taskArgs DoTaskArgs
		taskArgs.Phase = phase
		taskArgs.JobName = jobName
		taskArgs.NumOtherPhase = n_other
		taskArgs.TaskNumber = task
		if phase == mapPhase {
			taskArgs.File = mapFiles[task]
		}
		return taskArgs
	}

	/**
	We don't need to use `sync.WaitGroup`.
	Instead, worker sends back a response when it successfully execute a task.
	The master keep polling the number of finished tasks.
	When all tasks have finished, master can break the loop and finish schedule
	*/
	tasks := make(chan int, ntasks) // act as task queue
	//go func() {
	for i := 0; i < ntasks; i++ {
		tasks <- i
	}
	//}()
	successTasks := 0
	success := make(chan int)
	/*
		bug that can't stop from for select
		var wg sync.WaitGroup
		wg.Add(ntasks)
	*/
loop:
	for {
		select {
		// TODO ? task 是否会复制一份到go func，task变量是否有并发访问问题？？
		// go func 的闭包特性，是否可以显式指定哪些变量共享，哪些变量copy一份？
		case task := <-tasks:
			go func() {
				worker := <-registerChan
				fmt.Printf("Assigning %v #%d task to %s\n", phase, task, worker)
				status := call(worker, "Worker.DoTask", constructTaskArgs(phase, task), nil)
				if status {
					fmt.Printf("Assigning %v #%d task to %s is done \n", phase, task, worker)
					success <- 1
					go func() { registerChan <- worker }()
					//wg.Done()
				} else {
					tasks <- task
				}
			}()
		case <-success:
			successTasks += 1
		default:
			if successTasks == ntasks {
				break loop
			}
		}
	}

	//wg.Wait()
	fmt.Printf("Schedule: %v done\n", phase)
}
