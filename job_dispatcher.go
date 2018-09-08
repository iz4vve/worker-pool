// Package workers implements a worker pool
//
// A task function has to be passed to the pool
// in order to be executed when a signal is received
// by the job channel.
//
// (c)2018 - Pietro Mascolo
package workers

var workerID = 0

// JobDispatcher manages a pool of workers
type JobDispatcher struct {
	maxWorkers int
	Workers    []Worker
	WorkerPool chan chan ConcurrentJob
	ErrorChan  chan error
}

// NewJobDispatcher generates a neww job dispatcher
func NewJobDispatcher(maxWorkers int) *JobDispatcher {
	workersPool := make(chan chan ConcurrentJob)
	errCh := make(chan error)
	var workers []Worker
	return &JobDispatcher{maxWorkers, workers, workersPool, errCh}
}

// Run runs a job dispatcher.
func (jobDis *JobDispatcher) Run(queue chan ConcurrentJob, quit chan bool) {
	for i := 0; i < jobDis.maxWorkers; i++ {
		worker := NewWorker(jobDis.WorkerPool)
		jobDis.Workers = append(jobDis.Workers, worker)
		worker.Start(jobDis.ErrorChan)
	}
	go jobDis.dispatch(queue, quit)
}

// dispatch contains the logic to route the job to the first idle worker.
// This first idel worker is returned by the worker pool of the
// dispatcher, which is a channel of channels linked to separate workers.
//
// This setp is blocking (per channel) so that if no worker is available,
// the whole dispatcher will be blocked until a new worker is available.
func (jobDis *JobDispatcher) dispatch(queue chan ConcurrentJob, quit chan bool) {
	for {
		select {
		case job := <-queue:
			go func(job ConcurrentJob) {
				// get first idle worker
				jobChannel := <-jobDis.WorkerPool
				// send the job to the worker
				jobChannel <- job
			}(job)
		case <-quit:
			for _, worker := range jobDis.Workers {
				worker.Stop()
			}
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
				// do job
				if err := job.Execute(); err != nil {
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

// ConcurrentJob is the interface that jobs must implement
type ConcurrentJob interface {
	Execute() error
}
