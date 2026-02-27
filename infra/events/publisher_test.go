package events

import (
	"testing"
	"time"
)

func TestPublisher_DropWhenBufferFull(t *testing.T) {
	p := &Publisher{
		events: make(chan SubmissionEvent, publisherBuffer),
	}

	for range publisherBuffer {
		p.Publish("testuser")
	}

	done := make(chan struct{})
	go func() {
		p.Publish("testuser")
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Publish blocked when buffer was full — should have dropped the event")
	}
}
