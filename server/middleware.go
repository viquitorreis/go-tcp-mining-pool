package server

import (
	"tcp_luxor/pool/session"
	"tcp_luxor/protocol"
)

type Middleware func(Handler) Handler

func (s *Server) authMiddleware(next Handler) Handler {
	return func(s *session.Session, m *protocol.Message) error {
		if !s.IsAuthenticated() {
			return ErrUnauthorized
		}

		return next(s, m)
	}
}
