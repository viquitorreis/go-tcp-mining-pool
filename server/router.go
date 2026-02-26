package server

import (
	"log/slog"
	"tcp_luxor/client"
	"tcp_luxor/protocol"
)

type Router struct {
	routes map[protocol.Method]Handler
}

func NewRouter() *Router {
	return &Router{
		routes: make(map[protocol.Method]Handler),
	}
}

func (r *Router) Register(method protocol.Method, h Handler, middlewares ...Middleware) {
	if _, ok := r.routes[method]; ok {
		return
	}

	// middlewares de fora para dentro
	for i := len(middlewares) - 1; i >= 0; i-- {
		h = middlewares[i](h)
	}

	r.routes[method] = h
}

func (r *Router) Dispatch(c *client.Client, m *protocol.Message) error {
	h, ok := r.routes[m.Method]
	if !ok {
		slog.Error("unknown method", "method", m.Method)
		return protocol.ErrUnknownMethod
	}
	return h(c, m)
}
