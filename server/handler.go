package server

import (
	"fmt"
	"log"
	"strings"
	"tcp_luxor/client"
	"tcp_luxor/protocol"
)

type Handler func(c *client.Client, m *protocol.Message) error

func (s *Server) SessionHandler(c *client.Client, m *protocol.Message) error {
	return s.router.Dispatch(c, m)
}

func (s *Server) HandleAuth(client *client.Client, m *protocol.Message) error {
	if m.AuthParams == nil {
		return fmt.Errorf("missing auth params")
	}

	if strings.TrimSpace(m.AuthParams.Username) == "" {
		return fmt.Errorf("username cannot be empty")
	}

	if m.ID <= 0 {
		return fmt.Errorf("auth messages must include a proper id: %d\n", m.ID)
	}

	s.mu.Lock()
	s.clients[client.ID].Username = m.AuthParams.Username
	s.clients[client.ID].Authenticated = true
	s.mu.Unlock()

	return nil
}

func (s *Server) HandleJob(client *client.Client, m *protocol.Message) error {
	log.Printf("mocking job for client: %s\n", string(client.ID))
	return nil
}

func (s *Server) HandleSubmit(client *client.Client, m *protocol.Message) error {
	log.Printf("mocking submit for client: %s\n", string(client.ID))
	return nil
}
