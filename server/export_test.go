package server

import (
	"bufio"
	"encoding/json"
	"net"
	"tcp_luxor/pool/dispatcher"
	"tcp_luxor/pool/session"
	"tcp_luxor/protocol"
	"testing"
	"time"
)

var HandleSubmit = (*Server).handleSubmit
var HandleAuth = (*Server).handleAuth

func NewTestServer() *Server {
	jobsCh := make(chan dispatcher.ServerJob)

	return &Server{
		clients:    make(map[session.SessionID]*session.Session),
		router:     NewRouter(),
		stats:      make(map[string]int),
		dispatcher: dispatcher.NewDispatcher(time.Second*30, jobsCh),
		jobsChan:   jobsCh,
	}
}

func NewTestServerWithDispatcher(d dispatcher.IDispatcher) *Server {
	return &Server{
		clients:    make(map[session.SessionID]*session.Session),
		router:     NewRouter(),
		stats:      make(map[string]int),
		dispatcher: d,
		jobsChan:   make(chan dispatcher.ServerJob),
	}
}

func (s *Server) WaitForAddr(timeout time.Duration) net.Addr {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {

		s.mu.RLock()
		l := s.listener
		s.mu.RUnlock()

		if l != nil {
			return l.Addr()
		}

		time.Sleep(5 * time.Millisecond)
	}

	return nil
}

func sendMsg(t *testing.T, conn net.Conn, id uint64, method protocol.Method, params any) {
	t.Helper()

	data, err := protocol.BuildMessage(id, method, params)
	if err != nil {
		t.Fatalf("sendMsg: failed to build message: %v", err)
	}

	if _, err := conn.Write(data); err != nil {
		t.Fatalf("sendMsg: failed to write to conn: %v", err)
	}
}

func readResponse(t *testing.T, reader *bufio.Reader) protocol.Response {
	t.Helper()

	line, err := reader.ReadBytes('\n')
	if err != nil {
		t.Fatalf("readResponse: failed to read: %v", err)
	}

	var res protocol.Response
	if err := json.Unmarshal(line, &res); err != nil {
		t.Fatalf("readResponse: failed to unmarshal: %v", err)
	}

	return res
}

func authenticate(t *testing.T, conn net.Conn, reader *bufio.Reader, username string) {
	t.Helper()

	sendMsg(t, conn, 1,
		protocol.MethodAuthorize,
		protocol.AuthParams{
			Username: username,
		},
	)

	res := readResponse(t, reader)
	if !res.Result {
		t.Fatalf("authenticate: expected success for %q, got error: %s", username, res.Error)
	}
}
