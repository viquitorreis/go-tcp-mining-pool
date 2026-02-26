package dispatcher

import (
	"math"
	"testing"
	"time"
)

func newTestDispatcher(interval time.Duration) *Dispatcher {
	jobsCh := make(chan ServerJob, 10)
	return &Dispatcher{
		Job:      &ServerJob{},
		History:  make(map[JobID]string),
		interval: interval,
		jobsCh:   jobsCh,
	}
}

func TestGetNonce_UnknownID(t *testing.T) {
	d := newTestDispatcher(time.Hour)

	_, ok := d.GetNonce(math.MaxUint64)
	if ok {
		t.Error("expected false for unknown job ID, got true")
	}
}

func TestGetNonce_KnownID(t *testing.T) {
	d := newTestDispatcher(time.Hour)

	nonce := "testnonce"

	d.mu.Lock()
	jobID := JobID(1)
	d.History[jobID] = nonce
	d.Job = &ServerJob{JobID: jobID, ServerNonce: nonce}
	d.mu.Unlock()

	got, ok := d.GetNonce(jobID)
	if !ok {
		t.Fatal("expected true for known job ID, got false")
	}
	if got != nonce {
		t.Errorf("expected nonce %q, got %q", nonce, got)
	}
}

func TestDispatch_HistoryAccumulates(t *testing.T) {
	d := newTestDispatcher(time.Hour)

	for i := range 3 {
		d.mu.Lock()
		prev := d.Job
		job := &ServerJob{
			JobID:       prev.JobID + 1,
			ServerNonce: d.generateNonce(),
		}
		d.History[job.JobID] = job.ServerNonce
		d.Job = job
		d.mu.Unlock()
		_ = i
	}

	if len(d.History) != 3 {
		t.Errorf("expected 3 entries in history, got %d", len(d.History))
	}

	for id := JobID(1); id <= 3; id++ {
		if _, ok := d.GetNonce(id); !ok {
			t.Errorf("expected job %d to be in history", id)
		}
	}
}

func TestDispatch_NoncesAreUnique(t *testing.T) {
	d := newTestDispatcher(time.Hour)

	const numJobs = 50
	seen := make(map[string]bool)

	for i := range numJobs {
		d.mu.Lock()
		prev := d.Job
		job := &ServerJob{
			JobID:       prev.JobID + 1,
			ServerNonce: d.generateNonce(),
		}
		d.History[job.JobID] = job.ServerNonce
		d.Job = job
		d.mu.Unlock()

		nonce, _ := d.GetNonce(JobID(i + 1))
		if seen[nonce] {
			t.Fatalf("duplicate nonce detected at job %d: %q", i+1, nonce)
		}
		seen[nonce] = true
	}
}

func TestDispatch_JobIDIncrementsMonotonically(t *testing.T) {
	d := newTestDispatcher(time.Hour)

	var lastID JobID
	for range 10 {
		d.mu.Lock()
		prev := d.Job
		job := &ServerJob{
			JobID:       prev.JobID + 1,
			ServerNonce: d.generateNonce(),
		}
		d.History[job.JobID] = job.ServerNonce
		d.Job = job
		d.mu.Unlock()

		current := d.GetCurrentJob()
		if current.JobID <= lastID {
			t.Errorf("job ID did not increment: got %d after %d", current.JobID, lastID)
		}
		lastID = current.JobID
	}
}

func TestBootstrap_SendsJobToChannel(t *testing.T) {
	jobsCh := make(chan ServerJob, 1)
	d := &Dispatcher{
		Job:      &ServerJob{},
		History:  make(map[JobID]string),
		interval: 50 * time.Millisecond,
		jobsCh:   jobsCh,
	}

	d.Bootstrap()

	select {
	case job := <-jobsCh:
		if job.JobID != 1 {
			t.Errorf("expected first job ID to be 1, got %d", job.JobID)
		}
		if job.ServerNonce == "" {
			t.Error("expected non-empty server nonce")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for first job from dispatcher")
	}
}
