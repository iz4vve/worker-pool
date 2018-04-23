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
}

func NewJobDispatcher(maxWorkers int) *JobDispatcher {
	workersPool := make(chan chan ConcurrentJob)
	return &JobDispatcher{maxWorkers, workersPool}
}

func (jobDis *JobDispatcher) Run(queue chan ConcurrentJob) {
	for i := 0; i < jobDis.maxWorkers; i++ {
		worker := NewWorker(jobDis.WorkerPool)
		worker.Start()
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

func (w Worker) Start() {
	go func() {
		for {

			// register worker
			w.Pool <- w.JobChan

			select {
			case job := <-w.JobChan:
				fmt.Println(job)
				// do job
				if err := job.Execute(); err != nil {
					log.Printf("Worker %s met an error executing job: %s", w.ID.String(), err.Error())
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
