# Worker pool

#### Author: Pietro Mascolo
#### Email: iz4vve (at) gmail.com

Implementation of the worker pool pattern using generic interfaces.  
In order to use the library with you Jobs, you need to implement a struct that implements the interface `workers.ConcurrentJob`, i.e. a struct that implements the method `Execute() error`.


Example:
```go
import (
    "github.com/google/uuid"
    "github.com/iz4vve/worker-pool"
)

// UUID is not really necessary, but you should consider having unique IDs for your jobs
type Job struct {
    ID uuid.UUID
}

// implements workers.ConcurrentJob
func(job *Job) Execute() error {
    // execute your job
    return nil
}


// monolithic function that listens for jobs communications
func listenJobs() {
    // number of workers goroutines
    var workersProc = 4
    // runs 4 workers listening to separate channels for job requests
    var JobDispatcher = workers.NewJobDispatcher(workersProc)
    // channel for moving jobs
    var ch = make(chan workers.ConcurrentJob)
    // channel for errors
    errCh := dispatcher.ErrorChan

    // run the dispatcher
    JobDispatcher.Run(ch)

    // send job to worker pool
    ch <- Job{uuid.New()}

    // listen for errors in jobs
    for {
        select {
        case err, ok := <-errCh:
            if ok {
                log.Printf("Got error: %s", err.Error())
            }
        default:
        }
    }
}
```