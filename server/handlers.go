package server

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"strings"
	"tcp_luxor/client"
	"tcp_luxor/protocol"
	"time"
)

type Handler func(c *client.Client, m *protocol.Message) error

func (s *Server) SessionHandler(c *client.Client, m *protocol.Message) error {
	return s.router.Dispatch(c, m)
}

func (s *Server) HandleAuth(c *client.Client, m *protocol.Message) error {
	if m.AuthParams == nil {
		return fmt.Errorf("missing auth params")
	}

	if strings.TrimSpace(m.AuthParams.Username) == "" {
		return fmt.Errorf("username cannot be empty")
	}

	if m.ID <= 0 {
		return fmt.Errorf("auth messages must include a proper id: %d\n", m.ID)
	}

	c.Authenticate(m.AuthParams.Username)

	s.write(c, protocol.BuildResponse(m.ID, nil))

	return nil
}

func (s *Server) HandleSubmit(c *client.Client, m *protocol.Message) error {
	log.Printf("mocking submit for client: %s\n", string(c.GetID()))

	if m.SubmitParams.JobID <= 0 {
		return ErrInvalidJob
	}

	if time.Since(c.GetLastSubmitAt()) < time.Second {
		return ErrRateLimit
	}

	serverNonce, exists := s.dispatcher.GetNonce(JobID(m.SubmitParams.JobID))
	if !exists {
		return ErrInexistentServerNonce
	}

	if c.HasNonce(m.SubmitParams.ClientNonce) {
		return ErrDuplicateNonce
	}

	hasher := sha256.New()
	hasher.Write([]byte(serverNonce + m.SubmitParams.ClientNonce))
	hashBytes := hasher.Sum(nil)

	hashStr := hex.EncodeToString(hashBytes)

	if !strings.EqualFold(hashStr, m.SubmitParams.Result) {
		return ErrInvalidResult
	}

	c.UpdateTimeSubmit()
	c.SetNonce(m.SubmitParams.ClientNonce)

	s.mu.Lock()
	s.stats[c.GetUsername()]++
	s.mu.Unlock()

	s.write(c, protocol.BuildResponse(m.ID, nil))

	return nil
}
