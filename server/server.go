package server

import (
	"bufio"
	"context"
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
	port     string
	clients  map[client.ClienID]*client.Client
	listener net.Listener
	router   *Router

	wg sync.WaitGroup
	mu sync.Mutex
}

type IServer interface {
	Start(ctx context.Context) error
	Stop()
}

func NewServer(p string) IServer {
	return &Server{
		port:    p,
		clients: make(map[client.ClienID]*client.Client),
		router:  NewRouter(),
	}
}

func (s *Server) Start(ctx context.Context) error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", s.port))
	if err != nil {
		slog.Error("error starting tcp server", "error", err)
		return err
	}
	s.listener = listener

	log.Printf("Server listening on port: %s\n", s.port)

	// cancelar o Accept quando o ctx terminar
	go func() {
		<-ctx.Done()
		listener.Close()
	}()

	go s.RouteManager()

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
		c.Conn.Close()
		delete(s.clients, c.ID)
	}

	s.mu.Unlock()

	s.wg.Wait()
	log.Println("server stopped gracefully")
}

func (s *Server) RouteManager() {
	s.router.Register(protocol.MethodAuthorize, s.HandleAuth)

	s.router.Register(protocol.MethodJob, s.HandleJob, s.AuthMiddleware)
	s.router.Register(protocol.MethodSubmit, s.HandleSubmit, s.AuthMiddleware)
}

func (s *Server) handleClient(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	client := &client.Client{
		ID:        client.NewClientID(len(s.clients)),
		Conn:      conn,
		StartedAt: time.Now(),
	}

	s.mu.Lock()
	s.clients[client.ID] = client
	s.mu.Unlock()

	log.Println("new client connected", client.ID)

	defer func() {
		log.Println("client disconnected", client.ID)
		s.removeClient(client.ID)
	}()

	var clientWg sync.WaitGroup
	clientWg.Go(func() {
		s.readLoop(ctx, client)
	})

	clientWg.Wait()
}

func (s *Server) readLoop(ctx context.Context, client *client.Client) {
	reader := bufio.NewReader(client.Conn)
	for {
		select {
		case <-ctx.Done():
			log.Println("context expired while trying to read from client")
			return
		default:
			msg, err := reader.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					return
				}

				// shutdown
				if ctx.Err() != nil {
					return
				}

				slog.Error("error while reading from client", "client_id", client.ID, "error", err)
				return
			}

			msgJSON, err := protocol.ReadJSON(msg)
			if err != nil {
				continue
			}

			log.Printf("client: %s sent message: id=%d method=%s params=%s\n",
				client.ID, msgJSON.ID, msgJSON.Method.ToString(), string(msgJSON.Params))

			if err := s.SessionHandler(client, msgJSON); err != nil {
				slog.Error("error handling session handler", "client_id", client.ID, "error", err)
				continue
			}

			s.ReadAllClients()
		}
	}
}

func (s *Server) removeClient(id client.ClienID) {
	s.mu.Lock()
	delete(s.clients, id)
	s.mu.Unlock()
}

// type Client struct {
// 	ID            uuid.UUID
// 	Conn          net.Conn
// 	Username      string
// 	Authenticated bool
// }

func (s *Server) ReadAllClients() {
	s.mu.Lock()
	for id, client := range s.clients {
		log.Printf("Client ID: %s, Username: %s, Authenticated: %t, StartedAt: %s\n",
			id, client.Username, client.Authenticated, client.StartedAt.String())
	}
	s.mu.Unlock()
}
