package server

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"tcp_luxor/pool/dispatcher"
	"tcp_luxor/pool/session"
	"tcp_luxor/protocol"
	"time"
)

type Handler func(se *session.Session, m *protocol.Message) error

func (s *Server) sessionHandler(se *session.Session, m *protocol.Message) error {
	return s.router.dispatch(se, m)
}

func (s *Server) handleAuth(se *session.Session, m *protocol.Message) error {
	if m.AuthParams == nil {
		return fmt.Errorf("missing auth params")
	}

	if strings.TrimSpace(m.AuthParams.Username) == "" {
		return fmt.Errorf("username cannot be empty")
	}

	if m.ID <= 0 {
		return fmt.Errorf("auth messages must include a proper id: %d\n", m.ID)
	}

	s.mu.RLock()
	for _, client := range s.clients {
		if strings.EqualFold(client.GetUsername(), m.AuthParams.Username) {
			s.mu.RUnlock()
			return ErrMinerAlreadyExists
		}
	}
	s.mu.RUnlock()

	se.Authenticate(m.AuthParams.Username)

	s.write(se, protocol.BuildResponse(m.ID, nil))

	return nil
}

func (s *Server) handleSubmit(se *session.Session, m *protocol.Message) error {
	if m.SubmitParams.JobID <= 0 {
		return ErrInvalidJob
	}

	if time.Since(se.GetLastSubmitAt()) < time.Second {
		return protocol.ErrRateLimit
	}

	serverNonce, exists := s.dispatcher.GetNonce(dispatcher.JobID(m.SubmitParams.JobID))
	if !exists {
		return protocol.ErrTaskNotFound
	}

	if se.HasNonce(m.SubmitParams.ClientNonce) {
		return protocol.ErrDuplicateNonce
	}

	hasher := sha256.New()
	hasher.Write([]byte(serverNonce + m.SubmitParams.ClientNonce))
	hashBytes := hasher.Sum(nil)

	hashStr := hex.EncodeToString(hashBytes)

	if !strings.EqualFold(hashStr, m.SubmitParams.Result) {
		return protocol.ErrInvalidResult
	}

	se.UpdateTimeSubmit()
	se.SetNonce(m.SubmitParams.ClientNonce)

	s.mu.Lock()
	s.stats[se.GetUsername()]++
	s.mu.Unlock()

	s.write(se, protocol.BuildResponse(m.ID, nil))

	return nil
}
