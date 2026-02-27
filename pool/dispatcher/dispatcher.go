package dispatcher

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

type JobID uint64

type IDispatcher interface {
	Bootstrap()
	GetNonce(jobID JobID) (string, bool)
	GetCurrentJob() *ServerJob
}

// Dispatcher generates a new server nonce on a fixed interval and
// broadcasts jobs to the server through jobs channel. It keeps history of jobs ids
// and nonces so the server can validate submissions against past jobs
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

// Boobstrap starts the background ticker that generates and broadcasts jobs.
// It is called once after creation, before the server starts accepting clients
func (d *Dispatcher) Bootstrap() {
	go d.dispatch()
}

func (d *Dispatcher) generateNonce() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		// almost impossible error -> only on system without entropy
		panic(fmt.Sprintf("failed to generate nonce: :%s", err.Error()))
	}

	return hex.EncodeToString(b)
}

func (d *Dispatcher) dispatch() {
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()

	for range ticker.C {
		d.mu.Lock()

		prev := d.Job

		job := &ServerJob{
			JobID:       prev.JobID + 1,
			ServerNonce: d.generateNonce(),
		}

		d.History[job.JobID] = job.ServerNonce
		d.Job = job

		d.mu.Unlock()

		d.jobsCh <- *job
	}
}

// GetNonce returns the sever nonce for the given job ID and whether it exists
// returning false mean the job ID was never issued by the Dispatcher
func (d *Dispatcher) GetNonce(jobID JobID) (string, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	val, ok := d.History[jobID]
	if !ok {
		return "", false
	}

	return val, true
}

func (d *Dispatcher) GetCurrentJob() *ServerJob {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.Job
}
