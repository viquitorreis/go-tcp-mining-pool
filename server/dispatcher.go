package server

import (
	"fmt"
	"log"
	"sync"
	"time"
)

type JobID uint64

type IDispatcher interface {
	Bootstrap()
	GetNonce(jobID JobID) (string, bool)
}

type Dispatcher struct {
	Job      *ServerJob
	History  map[JobID]string
	interval time.Duration
	jobsCh   chan<- ServerJob

	mu sync.RWMutex
}

type ServerJob struct {
	JobID       JobID  `json:"job_id"`
	ServerNonce string `json:"server_nonce"`
}

func NewDispatcher(interval time.Duration, jc chan<- ServerJob) IDispatcher {
	return &Dispatcher{
		Job:      &ServerJob{},
		History:  make(map[JobID]string),
		interval: interval,
		jobsCh:   jc,
	}
}

func (d *Dispatcher) Bootstrap() {
	go d.dispatch()
}

func (d *Dispatcher) dispatch() {
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()

	for range ticker.C {
		d.mu.Lock()

		prev := d.Job

		job := &ServerJob{
			JobID:       prev.JobID + 1,
			ServerNonce: fmt.Sprintf("random:%s", time.Now().String()),
		}

		d.History[job.JobID] = job.ServerNonce
		d.Job = job

		d.mu.Unlock()

		d.jobsCh <- *job
	}
}

func (d *Dispatcher) GetNonce(jobID JobID) (string, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	val, ok := d.History[jobID]
	if !ok {
		return "", false
	}

	log.Println("returning job: ", jobID, val)

	return val, true
}
