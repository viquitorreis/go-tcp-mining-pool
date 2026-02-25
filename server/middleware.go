package server

import (
	"fmt"
	"tcp_luxor/client"
	"tcp_luxor/protocol"
)

type Middleware func(Handler) Handler

func (s *Server) AuthMiddleware(next Handler) Handler {
	return func(c *client.Client, m *protocol.Message) error {
		if !c.Authenticated {
			return fmt.Errorf("unauthorized: client must be authenticated first")
		}

		return next(c, m)
	}
}
