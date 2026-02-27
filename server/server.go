package server

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"tcp_luxor/infra/db"
	"tcp_luxor/infra/events"
	"tcp_luxor/pool/dispatcher"
	"tcp_luxor/pool/session"
	"tcp_luxor/protocol"
	"time"
)

// Server represents the TCP server. It accpets client connections, routes messages
// to the appropriate handlers, tracks submission statistics, and broadcasts
// new jobs received from the dispatcher
type Server struct {
	port       string
	clients    map[session.SessionID]*session.Session
	listener   net.Listener
	router     *Router
	dispatcher dispatcher.IDispatcher
	stats      map[string]int // username -> submssion count
	conn       *db.DB
	nextID     atomic.Uint64
	publisher  *events.Publisher
	jobsChan   chan dispatcher.ServerJob

	wg sync.WaitGroup
	mu sync.RWMutex
}

// NewServer creates a server ready to start. The db connection is injected
// rather than opened internally so tests can run without a real database
func NewServer(port string, conn *db.DB, publisher *events.Publisher) *Server {
	jobsCh := make(chan dispatcher.ServerJob)

	return &Server{
		port:       port,
		clients:    make(map[session.SessionID]*session.Session),
		router:     NewRouter(),
		dispatcher: dispatcher.NewDispatcher(time.Second*30, jobsCh),
		stats:      make(map[string]int),
		conn:       conn,
		publisher:  publisher,
		jobsChan:   jobsCh,
	}
}

func (s *Server) Start(ctx context.Context) error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", s.port))
	if err != nil {
		slog.Error("error starting tcp server", "error", err)
		return err
	}

	s.mu.Lock()
	s.listener = listener
	s.mu.Unlock()

	slog.Info("Server UP and running", "port", s.port)

	s.conn, err = db.New(ctx)
	if err != nil {
		slog.Error("error while starting database", "error", err)
		return err
	}

	s.dispatcher.Bootstrap()

	go func() {
		<-ctx.Done()
		listener.Close()
	}()

	s.routeManager()
	go s.listenDispatcher(ctx)
	go s.runStatsCollector(ctx)

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
		delete(s.clients, c.GetSessionID())
	}
	s.mu.Unlock()

	s.wg.Wait()

	slog.Info("server stopped gracefully")
}

func (s *Server) routeManager() {
	s.router.register(protocol.MethodAuthorize, s.handleAuth)

	s.router.register(protocol.MethodSubmit, s.handleSubmit, s.authMiddleware)
}

func (s *Server) handleClient(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	id := s.nextID.Add(1)
	session := session.NewSession(id, conn)

	s.mu.Lock()
	s.clients[session.GetSessionID()] = session
	s.mu.Unlock()

	slog.Info("new client connected", "session_id", session.GetSessionID())

	defer func() {
		slog.Info("client disconnected", "session_id", session.GetSessionID())
		s.removeClient(session.GetSessionID())
	}()

	var clientWg sync.WaitGroup
	clientWg.Go(func() {
		s.handleSession(ctx, session)
	})

	clientWg.Wait()
}

func (s *Server) handleSession(ctx context.Context, session *session.Session) {
	reader := bufio.NewReader(session)
	for {
		select {
		case <-ctx.Done():
			slog.Warn("context expired while trying to read from client", "session_id", session.GetSessionID())
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

				slog.Error("error while reading from client", "session_id", session.GetSessionID(), "error", err)
				return
			}

			msg, err := protocol.Parse(line)
			if err != nil {
				slog.Warn("invalid message", "session_id", session.GetSessionID(), "error", err)
				continue
			}

			if err := s.sessionHandler(session, msg); err != nil {
				slog.Error("error handling session handler", "session_id", session.GetSessionID(), "error", err)
				s.write(session, protocol.BuildResponse(msg.ID, err))

				continue
			}
		}
	}
}

func (s *Server) removeClient(id session.SessionID) {
	s.mu.Lock()
	delete(s.clients, id)
	s.mu.Unlock()
}

func (s *Server) write(se *session.Session, msg any) {
	data, err := json.Marshal(msg)
	if err != nil {
		slog.Error("error marshaling response", "session_id", se.GetSessionID(), "error", err)
		return
	}

	data = append(data, '\n')

	se.Write(data)
}

func (s *Server) listenDispatcher(ctx context.Context) {
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

func (s *Server) broadcastJob(job dispatcher.ServerJob) {
	msg, err := protocol.BuildJobMessage(uint64(job.JobID), job.ServerNonce)
	if err != nil {
		slog.Error("error building job message", "error", err)
		return
	}

	s.mu.RLock()
	targets := make([]*session.Session, 0, len(s.clients))
	for _, session := range s.clients {
		if session.IsAuthenticated() {
			targets = append(targets, session)
		}
	}
	s.mu.RUnlock()

	for _, session := range targets {
		session.Write(msg)
	}
}

func (s *Server) runStatsCollector(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.flushStats(ctx)
		}
	}
}

func (s *Server) flushStats(ctx context.Context) {
	if ctx.Err() != nil || s.conn == nil {
		return
	}

	s.mu.Lock()
	snapshot := s.stats
	s.stats = make(map[string]int)
	s.mu.Unlock()

	if len(snapshot) == 0 {
		return
	}

	now := time.Now()
	models := make([]db.SubmissionStatModel, 0, len(snapshot))

	for username, count := range snapshot {
		models = append(models, db.SubmissionStatModel{
			Username:        username,
			SubmissionCount: count,
			Timestamp:       now,
		})
	}

	ctxTimeout, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	if err := s.conn.UpsertSubmissions(ctxTimeout, models); err != nil {
		slog.Error("error flushing stats, re-queuing for next cycle", "error", err)
		s.mu.Lock()
		for _, m := range models {
			s.stats[m.Username] += m.SubmissionCount
		}
		s.mu.Unlock()
	}
}
