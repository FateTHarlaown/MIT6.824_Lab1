package mapreduce

import "fmt"
import "sync"

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
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

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)
	var wg sync.WaitGroup
	for i := 0; i < ntasks; i++ {
		wg.Add(1)

		workerName := <-mr.registerChannel
		go func() {
			defer wg.Done()
			var args DoTaskArgs
			args.JobName = mr.jobName
			args.File = mr.files[i]
			args.Phase = phase
			args.TaskNumber = i
			args.NumOtherPhase = nios
			for {
				ok := call(workerName, "Worker.DoTask", &args, new(struct{}))
				if ok == true {
					mr.registerChannel <- workerName
					break
				}
			}
		}()
	}
	wg.Wait()
	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	fmt.Printf("Schedule: %v phase done\n", phase)
}
