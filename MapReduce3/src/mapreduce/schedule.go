package mapreduce

import (
	"sync"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).

var wg sync.WaitGroup

func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce

	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	wg.Add(ntasks)
	for i := 0; i < ntasks; i++ {
		// wg.Add(1)
		var DoTask DoTaskArgs
		DoTask.JobName = mr.jobName
		if phase == mapPhase {
			DoTask.File = mr.files[i]
		}
		DoTask.Phase = phase
		DoTask.TaskNumber = i
		DoTask.NumOtherPhase = nios
		debug("Dotask created with tasknumber %d ans %s", DoTask.TaskNumber, phase)
		go func() {
			defer wg.Done()
			for {
				worker := <-mr.registerChannel
				checkworkerOut := call(worker, "Worker.DoTask", &DoTask, nil)
				go func() {
					mr.registerChannel <- worker
				}()
				if checkworkerOut {
					debug("Task %s %d completed", phase, DoTask.TaskNumber)
					break
				}
				// else {
				// 	mr.registerChannel <- worker
				// 	debug("Error in completing the job %d", i)
				// 	fmt.Println()
				// }
			}
		}()
	}
	wg.Wait()

	debug("Schedule: %v phase done\n", phase)
}
