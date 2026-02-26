package miner

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net"
	"tcp_luxor/protocol"
	"testing"
	"time"
)

var errRejected = &rejectedError{}

type rejectedError struct{}

func (e *rejectedError) Error() string {
	return "rejected"
}

func newTestMiner(t *testing.T) (*Miner, net.Conn) {
	t.Helper()

	serverConn, clientConn := net.Pipe()

	m := &Miner{
		username: "testminer",
		conn:     clientConn,
		reader:   bufio.NewReader(clientConn),
		jobChan:  make(chan protocol.JobParams, 1),
	}

	t.Cleanup(func() {
		clientConn.Close()
		serverConn.Close()
	})

	return m, serverConn
}

func readMessage(t *testing.T, conn net.Conn) *protocol.Message {
	t.Helper()

	conn.SetDeadline(time.Now().Add(2 * time.Second))

	line, err := bufio.NewReader(conn).ReadBytes('\n')
	if err != nil {
		t.Fatalf("readMessage: failed to read: %v", err)
	}

	msg, err := protocol.Parse(line)
	if err != nil {
		t.Fatalf("readMessage: failed to parse: %v", err)
	}

	return msg
}

func writeResponse(t *testing.T, conn net.Conn, id uint64, success bool) {
	t.Helper()

	var res *protocol.Response
	if success {
		res = protocol.BuildResponse(id, nil)
	} else {
		res = protocol.BuildResponse(id, errRejected)
	}

	data, err := json.Marshal(res)
	if err != nil {
		t.Fatalf("writeResponse: marshal failed: %v", err)
	}

	conn.Write(append(data, '\n'))
}

func TestAuthenticate_Success(t *testing.T) {
	m, serverConn := newTestMiner(t)

	go func() {
		msg := readMessage(t, serverConn)

		if msg.Method != protocol.MethodAuthorize {
			t.Errorf("expected method authorize, got %s", msg.Method)
		}

		if msg.AuthParams.Username != "testminer" {
			t.Errorf("expected username testminer, got %s", msg.AuthParams.Username)
		}

		writeResponse(t, serverConn, msg.ID, true)
	}()

	if err := m.authenticate(); err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
}

func TestAuthenticate_Rejected(t *testing.T) {
	m, serverConn := newTestMiner(t)

	go func() {
		msg := readMessage(t, serverConn)
		writeResponse(t, serverConn, msg.ID, false)
	}()

	if err := m.authenticate(); err == nil {
		t.Fatal("expected error on rejected auth, got nil")
	}
}

func TestReceiveJobs_DeliversJobToChannel(t *testing.T) {
	m, serverConn := newTestMiner(t)
	ctx := t.Context()

	go m.receiveJobs(ctx)

	jobData, err := protocol.BuildJobMessage(7, "servernonce-abc")
	if err != nil {
		t.Fatalf("failed to build job message: %v", err)
	}

	serverConn.Write(jobData)

	select {
	case job := <-m.jobChan:
		if job.JobID != 7 {
			t.Errorf("expected job_id 7, got %d", job.JobID)
		}

		if job.ServerNonce != "servernonce-abc" {
			t.Errorf("expected server_nonce servernonce-abc, got %s", job.ServerNonce)
		}

	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for job to arrive in jobChan")
	}
}

func TestReceiveJobs_FullChannel(t *testing.T) {
	m, serverConn := newTestMiner(t)
	ctx := t.Context()

	m.jobChan <- protocol.JobParams{
		JobID:       1,
		ServerNonce: "old-nonce",
	}

	go m.receiveJobs(ctx)

	jobData, err := protocol.BuildJobMessage(2, "new-nonce")
	if err != nil {
		t.Fatalf("failed to build job message: %v", err)
	}
	serverConn.Write(jobData)

	time.Sleep(100 * time.Millisecond)

	select {
	case job := <-m.jobChan:
		if job.JobID != 2 {
			t.Errorf("expected new job_id 2, got %d (old job was not discarded)", job.JobID)
		}
	default:
		t.Fatal("expected a job in the channel, got none")
	}
}

func TestReceiveJobs_IgnoresNonJobMessages(t *testing.T) {
	m, serverConn := newTestMiner(t)
	ctx := t.Context()

	go m.receiveJobs(ctx)

	res := protocol.BuildResponse(42, nil)
	data, _ := json.Marshal(res)
	serverConn.Write(append(data, '\n'))

	jobData, err := protocol.BuildJobMessage(5, "real-nonce")
	if err != nil {
		t.Fatalf("failed to build job message: %v", err)
	}
	serverConn.Write(jobData)

	select {
	case job := <-m.jobChan:
		if job.JobID != 5 {
			t.Errorf("expected job_id 5, got %d", job.JobID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out — miner may have blocked on non-job message")
	}
}

func TestSubmit_CorrectSHA256(t *testing.T) {
	m, serverConn := newTestMiner(t)
	ctx := t.Context()

	job := &protocol.JobParams{JobID: 1, ServerNonce: "test-server-nonce"}

	go func() {
		if err := m.submit(ctx, job); err != nil {
			t.Errorf("submit returned unexpected error: %v", err)
		}
	}()

	msg := readMessage(t, serverConn)

	if msg.Method != protocol.MethodSubmit {
		t.Fatalf("expected method submit, got %s", msg.Method)
	}

	if msg.SubmitParams.JobID != 1 {
		t.Errorf("expected job_id 1, got %d", msg.SubmitParams.JobID)
	}

	clientNonce := msg.SubmitParams.ClientNonce
	expected := sha256.Sum256([]byte("test-server-nonce" + clientNonce))
	if msg.SubmitParams.Result != hex.EncodeToString(expected[:]) {
		t.Errorf("incorrect SHA256: got %s", msg.SubmitParams.Result)
	}
}

func TestSubmit_RespectsContextCancellation(t *testing.T) {
	m, _ := newTestMiner(t)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	job := &protocol.JobParams{JobID: 1, ServerNonce: "nonce"}
	if err := m.submit(ctx, job); err == nil {
		t.Fatal("expected error on cancelled context, got nil")
	}
}
