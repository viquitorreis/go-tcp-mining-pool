package server

import (
	"fmt"
	"sync"
	"time"
)

type JobID uint64

type Dispatcher struct {
	Job      *ServerJob
	History  map[JobID][]string
	interval time.Duration
	jobsCh   chan<- ServerJob

	mu sync.RWMutex
}

type ServerJob struct {
	JobID       JobID  `json:"job_id"`
	ServerNonce string `json:"server_nonce"`
}

func BoostrapDispatcher(jc chan ServerJob) *Dispatcher {
	d := &Dispatcher{
		Job:      &ServerJob{},
		History:  make(map[JobID][]string),
		interval: time.Second * 10,
		jobsCh:   jc,
	}

	go d.Dispatch()

	return d
}

func (d *Dispatcher) Dispatch() {
	d.mu.RLock()
	ticker := time.NewTicker(d.interval)
	d.mu.RUnlock()

	defer ticker.Stop()

	for range ticker.C {
		d.mu.Lock()

		prev := d.Job

		job := &ServerJob{
			JobID:       prev.JobID + 1,
			ServerNonce: fmt.Sprintf("random:%s", time.Now().String()),
		}

		d.History[job.JobID] = append(d.History[job.JobID], job.ServerNonce)
		d.mu.Unlock()

		d.jobsCh <- *job
	}
}
