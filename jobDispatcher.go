/*
(c)2018 - Pietro Mascolo

*/
package workers

import (
	"fmt"
	"log"
)

var workerID = 0

// JobDispatcher manages a pool of workers
type JobDispatcher struct {
	maxWorkers int
	// pool of worker channels registered
	WorkerPool chan chan ConcurrentJob
	ErrorChan  chan error
	Verbose    bool
}

// NewJobDispatcher generates a neww job dispatcher
func NewJobDispatcher(maxWorkers int) *JobDispatcher {
	workersPool := make(chan chan ConcurrentJob)
	errCh := make(chan error)
	return &JobDispatcher{maxWorkers, workersPool, errCh, false}
}

// Run runs a job dispatcher.
func (jobDis *JobDispatcher) Run(queue chan ConcurrentJob) {
	for i := 0; i < jobDis.maxWorkers; i++ {
		worker := NewWorker(jobDis.WorkerPool)
		worker.Start(jobDis.ErrorChan)
	}
	go jobDis.dispatch(queue)
}

// dispatch contains the logic to route the job to the first idle worker.
// This first idel worker is returned by the worker pool of the
// dispatcher, which is a channel of channels linked to separate workers.
//
// This setp is blocking (per channel) so that if no worker is available,
// the whole dispatcher will be blocked until a new worker is available.
func (jobDis *JobDispatcher) dispatch(queue chan ConcurrentJob) {
	for {
		select {
		case job := <-queue:
			go func(job ConcurrentJob) {
				// get first idle worker
				jobChannel := <-jobDis.WorkerPool
				// send the job to the worker
				jobChannel <- job
			}(job)
		}
	}
}

///////////////////////////

// Worker is the base worker struct
type Worker struct {
	ID      int
	Pool    chan chan ConcurrentJob
	JobChan chan ConcurrentJob
	quit    chan bool
}

// NewWorker generates a new worker object
func NewWorker(pool chan chan ConcurrentJob) Worker {
	w := Worker{
		ID:      workerID,
		Pool:    pool,
		JobChan: make(chan ConcurrentJob),
		quit:    make(chan bool)}
	workerID++
	return w
}

// Start initiates a worker job execution
func (w Worker) Start(errCh chan error) {
	go func() {
		for {
			// register worker
			w.Pool <- w.JobChan
			select {
			case job := <-w.JobChan:
				verbose := job.Verbose()
				if verbose {
					fmt.Printf("%d got job: %v\n", w.ID, job)
				}
				// do job
				if err := job.Execute(); err != nil {
					if verbose {
						log.Printf("Worker %d met an error executing job: %s", w.ID, err.Error())
					}
					errCh <- err
				}
			case <-w.quit:
				return
			}
		}
	}()
}

// Stop removes the worker from the worker pool
func (w Worker) Stop() {
	go func() {
		w.quit <- true
	}()
}

///////////////////
type ConcurrentJob interface {
	Execute() error
	Verbose() bool
}
