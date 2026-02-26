package server

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"math"
	"net"
	"tcp_luxor/pool/dispatcher"
	"tcp_luxor/pool/session"
	"tcp_luxor/protocol"
	"testing"
	"time"
)

func newTestSession(t *testing.T) (*session.Session, net.Conn) {
	t.Helper()

	serverConn, clientConn := net.Pipe()
	s := session.NewSession(0, serverConn)
	s.Authenticate("testuser")

	return s, clientConn
}

func newTestServer(t *testing.T) *Server {
	t.Helper()
	return NewTestServer()
}

type fakeDispatcher struct {
	jobs map[dispatcher.JobID]string
}

func newFakeDispatcher() *fakeDispatcher {
	return &fakeDispatcher{jobs: make(map[dispatcher.JobID]string)}
}

func (f *fakeDispatcher) Bootstrap() {}

func (f *fakeDispatcher) GetCurrentJob() *dispatcher.ServerJob {
	return nil
}

func (f *fakeDispatcher) GetNonce(id dispatcher.JobID) (string, bool) {
	nonce, ok := f.jobs[id]
	return nonce, ok
}

func (f *fakeDispatcher) addJob(id dispatcher.JobID, nonce string) {
	f.jobs[id] = nonce
}

func TestHandleSubmit_RateLimit(t *testing.T) {
	s := newTestServer(t)
	sess, clientConn := newTestSession(t)
	defer clientConn.Close()

	sess.UpdateTimeSubmit()

	msg := &protocol.Message{
		ID:     1,
		Method: protocol.MethodSubmit,
		SubmitParams: &protocol.SubmitParams{
			JobID:       1,
			ClientNonce: "anynonce",
			Result:      "anyhash",
		},
	}

	err := s.HandleSubmit(sess, msg)
	if !errors.Is(err, ErrRateLimit) {
		t.Errorf("expected ErrRateLimit, got: %v", err)
	}
}

func TestIntegration_AuthAndSubmit(t *testing.T) {
	ctx := t.Context()
	srv := NewTestServer()
	go srv.Start(ctx)

	addr := srv.WaitForAddr(2 * time.Second)
	if addr == nil {
		t.Fatal("server did not start in time")
	}

	conn, err := net.Dial("tcp", addr.String())
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	authenticate(t, conn, reader, "testuser")
}

func TestHandleSubmit_JobNotFound(t *testing.T) {
	s := newTestServer(t)
	sess, clientConn := newTestSession(t)
	defer clientConn.Close()

	go io.Copy(io.Discard, clientConn)

	msg := &protocol.Message{
		ID:     1,
		Method: protocol.MethodSubmit,
		SubmitParams: &protocol.SubmitParams{
			JobID:       math.MaxUint64,
			ClientNonce: "anynonce",
			Result:      "anyhash",
		},
	}

	err := s.HandleSubmit(sess, msg)
	if !errors.Is(err, ErrTaskNotFound) {
		t.Errorf("expected ErrInexistentServerNonce, got: %v", err)
	}
}

func TestHandleSubmit_DuplicateNonce(t *testing.T) {
	fake := newFakeDispatcher()
	fake.addJob(1, "testnonce-1")

	s := NewTestServerWithDispatcher(fake)
	sess, clientConn := newTestSession(t)
	defer clientConn.Close()

	go io.Copy(io.Discard, clientConn)

	sess.SetNonce("already-used-nonce")

	msg := &protocol.Message{
		ID:     1,
		Method: protocol.MethodSubmit,
		SubmitParams: &protocol.SubmitParams{
			JobID:       1,
			ClientNonce: "already-used-nonce",
			Result:      "anyhash",
		},
	}

	err := s.HandleSubmit(sess, msg)
	if !errors.Is(err, ErrDuplicateNonce) {
		t.Errorf("expected ErrDuplicateNonce, got: %v", err)
	}
}

func TestHandleSubmit_InvalidResult(t *testing.T) {
	fake := newFakeDispatcher()
	fake.addJob(1, "testnonce-invalid-result")

	s := NewTestServerWithDispatcher(fake)
	sess, clientConn := newTestSession(t)
	defer clientConn.Close()

	go io.Copy(io.Discard, clientConn)

	msg := &protocol.Message{
		ID:     1,
		Method: protocol.MethodSubmit,
		SubmitParams: &protocol.SubmitParams{
			JobID:       1,
			ClientNonce: "clientnonce",
			Result:      "thisiswronghash",
		},
	}

	err := s.HandleSubmit(sess, msg)
	if !errors.Is(err, ErrInvalidResult) {
		t.Errorf("expected ErrInvalidResult, got: %v", err)
	}
}

func TestHandleSubmit_ValidSubmission(t *testing.T) {
	fake := newFakeDispatcher()
	clientNonce := "myclientnonce"
	serverNonce := "serverNonce"
	fake.addJob(1, serverNonce)

	s := NewTestServerWithDispatcher(fake)
	sess, clientConn := newTestSession(t)
	defer clientConn.Close()

	go io.Copy(io.Discard, clientConn)

	h := sha256.Sum256([]byte(serverNonce + clientNonce))
	validResult := hex.EncodeToString(h[:])

	msg := &protocol.Message{
		ID:     1,
		Method: protocol.MethodSubmit,
		SubmitParams: &protocol.SubmitParams{
			JobID:       1,
			ClientNonce: clientNonce,
			Result:      validResult,
		},
	}

	err := s.HandleSubmit(sess, msg)
	if err != nil {
		t.Errorf("expected nil error for valid submission, got: %v", err)
	}
}
