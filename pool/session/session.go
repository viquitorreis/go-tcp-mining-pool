package session

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"
)

type SessionID string

// Session represents a single connected client, it holds authentication state,
// submission history and the underlyint TCP connection
// it uses 2 mutexes for a purpose: mu protects session states (username, nonces, timestamps)
// while writeMu serializes TCP writes independently so broadcasts
// from the dispatcher will never interleave with handler responses
type Session struct {
	id            SessionID
	conn          net.Conn
	username      string
	authenticated bool
	usedNonces    map[string]bool

	startedAt    time.Time
	lastSubmitAt time.Time

	mu      sync.RWMutex
	writeMu sync.Mutex // tcp writes only
}

type ISession interface {
	Read(buf []byte) (int, error)
}

// NewSession creates a new unauthenticated session for the given connection
// the id comes from the server's atomic counter, which guarantees uniquenes even
// when clients connect and disconnect rapidly
func NewSession(id uint64, conn net.Conn) *Session {
	host, _ := os.Hostname()
	sessionID := SessionID(fmt.Sprintf("%d_%s", id, host))
	return &Session{
		id:         sessionID,
		conn:       conn,
		startedAt:  time.Now(),
		usedNonces: make(map[string]bool),
	}
}

func (s *Session) Read(buf []byte) (int, error) {
	return s.conn.Read(buf)
}

func (s *Session) Write(data []byte) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if _, err := s.conn.Write(data); err != nil {
		slog.Error("error writing to client", "client_id", s.id, "error", err)
	}
}

func (s *Session) CloseConn() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.conn.Close()
}

func (s *Session) GetSessionID() SessionID {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.id
}

func (s *Session) GetUsername() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.username
}

func (s *Session) Authenticate(username string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.username = username
	s.authenticated = true
}

func (s *Session) IsAuthenticated() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.authenticated
}

func (s *Session) GetLastSubmitAt() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastSubmitAt
}

func (s *Session) UpdateTimeSubmit() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastSubmitAt = time.Now()
}

func (s *Session) SetNonce(nonce string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.usedNonces[nonce] = true
}

func (s *Session) HasNonce(nonce string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, exists := s.usedNonces[nonce]
	if exists {
		return true
	}

	return false
}
