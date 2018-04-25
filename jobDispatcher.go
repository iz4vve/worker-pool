package workers

import (
	"fmt"
	"log"

	"github.com/google/uuid"
)

// JobDispatcher manages a pool of workers
type JobDispatcher struct {
	maxWorkers int
	// pool of worker channels registered
	WorkerPool chan chan ConcurrentJob
	ErrorChan  chan error
}

func NewJobDispatcher(maxWorkers int) *JobDispatcher {
	workersPool := make(chan chan ConcurrentJob)
	errCh := make(chan error)
	return &JobDispatcher{maxWorkers, workersPool, errCh}
}

func (jobDis *JobDispatcher) Run(queue chan ConcurrentJob) {
	for i := 0; i < jobDis.maxWorkers; i++ {
		worker := NewWorker(jobDis.WorkerPool)
		worker.Start(jobDis.ErrorChan)
	}
	go jobDis.dispatch(queue)
}

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
type Worker struct {
	ID      uuid.UUID
	Pool    chan chan ConcurrentJob
	JobChan chan ConcurrentJob
	quit    chan bool
}

func NewWorker(pool chan chan ConcurrentJob) Worker {
	return Worker{
		ID:      uuid.New(),
		Pool:    pool,
		JobChan: make(chan ConcurrentJob),
		quit:    make(chan bool)}
}

func (w Worker) Start(errCh chan error) {
	go func() {
		for {
			// register worker
			w.Pool <- w.JobChan
			select {
			case job := <-w.JobChan:
				fmt.Printf("%s got job: %v\n", w.ID, job)
				// do job
				if err := job.Execute(); err != nil {
					log.Printf("Worker %s met an error executing job: %s", w.ID.String(), err.Error())
					errCh <- err
				}
			case <-w.quit:
				return
			}
		}
	}()
}

func (w Worker) Stop() {
	go func() {
		w.quit <- true
	}()
}

///////////////////
type ConcurrentJob interface {
	Execute() error
}
