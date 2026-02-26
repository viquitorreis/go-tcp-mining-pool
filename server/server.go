package server

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"sync"
	"tcp_luxor/client"
	"tcp_luxor/protocol"
	"time"
)

type Server struct {
	port       string
	clients    map[client.ClientID]*client.Client
	listener   net.Listener
	router     *Router
	dispatcher IDispatcher
	jobsChan   chan ServerJob

	wg sync.WaitGroup
	mu sync.RWMutex
}

type IServer interface {
	Start(ctx context.Context) error
	Stop()
}

func NewServer(p string) IServer {
	jobsCh := make(chan ServerJob)

	return &Server{
		port:       p,
		clients:    make(map[client.ClientID]*client.Client),
		router:     NewRouter(),
		dispatcher: NewDispatcher(time.Second*5, jobsCh),
		jobsChan:   jobsCh,
	}
}

func (s *Server) Start(ctx context.Context) error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", s.port))
	if err != nil {
		slog.Error("error starting tcp server", "error", err)
		return err
	}
	s.listener = listener

	slog.Info("Server UP and running", "port", s.port)

	s.dispatcher.Bootstrap()

	// cancelar o Accept quando o ctx terminar
	go func() {
		<-ctx.Done()
		listener.Close()
	}()

	s.RouteManager()

	go s.ListenDispatcher(ctx)

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
				slog.Error("error accepting new connection", "error", err)
				continue
			}

		}

		s.wg.Go(func() {
			s.handleClient(ctx, conn)
		})
	}
}

func (s *Server) Stop() {
	s.mu.Lock()

	for _, c := range s.clients {
		c.CloseConn()
		delete(s.clients, c.GetID())
	}

	s.mu.Unlock()

	s.wg.Wait()

	slog.Info("server stopped gracefully")
}

func (s *Server) RouteManager() {
	s.router.Register(protocol.MethodAuthorize, s.HandleAuth)

	s.router.Register(protocol.MethodSubmit, s.HandleSubmit, s.AuthMiddleware)
}

func (s *Server) handleClient(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	client := client.NewClient(len(s.clients), conn)

	s.mu.Lock()
	s.clients[client.GetID()] = client
	s.mu.Unlock()

	slog.Info("new client connected", "client_id", client.GetID())

	defer func() {
		slog.Info("client disconnected", "client_id", client.GetID())
		s.removeClient(client.GetID())
	}()

	var clientWg sync.WaitGroup
	clientWg.Go(func() {
		s.readLoop(ctx, client)
	})

	clientWg.Wait()
}

func (s *Server) readLoop(ctx context.Context, c *client.Client) {
	reader := bufio.NewReader(c.GetConn())
	for {
		select {
		case <-ctx.Done():
			slog.Warn("context expired while trying to read from client", "client_id", c.GetID())
			return
		default:
			line, err := reader.ReadBytes('\n')
			if err != nil {
				if err == io.EOF {
					return
				}

				// shutdown
				if ctx.Err() != nil {
					return
				}

				slog.Error("error while reading from client", "client_id", c.GetID(), "error", err)
				return
			}

			msg, err := protocol.Parse(line)
			if err != nil {
				slog.Warn("invalid message", "client_id", c.GetID(), "error", err)
				continue
			}

			// APAGAR
			log.Printf("client: %s sent message: id=%d method=%s params=%s\n",
				c.GetID(), msg.ID, msg.Method.ToString(), string(msg.Params))

			if err := s.SessionHandler(c, msg); err != nil {
				slog.Error("error handling session handler", "client_id", c.GetID(), "error", err)
				s.write(c, protocol.BuildResponse(msg.ID, err))

				continue
			}
		}
	}
}

func (s *Server) removeClient(id client.ClientID) {
	s.mu.Lock()
	delete(s.clients, id)
	s.mu.Unlock()
}

func (s *Server) write(c *client.Client, msg any) {
	data, err := json.Marshal(msg)
	if err != nil {
		slog.Error("error marshaling response", "client_id", c.GetID(), "error", err)
		return
	}

	data = append(data, '\n')
	_, err = c.GetConn().Write(data)
	if err != nil {
		slog.Error("error while writing message on client", "client_id", c.GetID(), "error", err)
		return
	}
}

func (s *Server) ListenDispatcher(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-s.jobsChan:
			slog.Info("new job received from dispatcher. Broadcasting to clients.")
			s.broadcastJob(job)
		}
	}
}

func (s *Server) broadcastJob(job ServerJob) {
	msg, err := protocol.BuildJobMessage(uint64(job.JobID), job.ServerNonce)
	if err != nil {
		slog.Error("error building job message", "error", err)
		return
	}

	s.mu.RLock()
	targets := make([]*client.Client, 0, len(s.clients))
	for _, c := range s.clients {
		if c.IsAuthenticated() {
			targets = append(targets, c)
		}
	}
	s.mu.RUnlock()

	for _, c := range targets {
		s.write(c, msg)
	}
}
