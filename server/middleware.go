package server

import (
	"tcp_luxor/client"
	"tcp_luxor/protocol"
)

type Middleware func(Handler) Handler

func (s *Server) AuthMiddleware(next Handler) Handler {
	return func(c *client.Client, m *protocol.Message) error {
		if !c.IsAuthenticated() {
			return ErrUnauthorized
		}

		return next(c, m)
	}
}
