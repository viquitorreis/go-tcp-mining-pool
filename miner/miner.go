package miner

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"sync/atomic"
	"tcp_luxor/protocol"
	"time"
)

// miner represents a tcp client connection
type Miner struct {
	addr     string
	username string
	conn     net.Conn
	reader   *bufio.Reader

	currentJob *protocol.JobParams
	jobChan    chan protocol.JobParams

	msgID atomic.Uint64
}

func New(addr, username string) *Miner {
	return &Miner{
		addr:     addr,
		username: username,
		jobChan:  make(chan protocol.JobParams, 1),
	}
}

func (m *Miner) nextID() uint64 {
	return m.msgID.Add(1)
}

func (m *Miner) Connect(ctx context.Context) error {
	conn, err := net.Dial("tcp", m.addr)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", m.addr, err)
	}
	m.conn = conn
	m.reader = bufio.NewReader(conn)

	slog.Info("miner connected to server", "addr", m.addr)

	if err := m.authenticate(); err != nil {
		conn.Close()
		return fmt.Errorf("miner authentication failed: %w", err)
	}

	slog.Info("authenticated", "username", m.username)
	return nil
}

func (m *Miner) authenticate() error {
	msg := protocol.Message{
		ID:     m.nextID(),
		Method: protocol.MethodAuthorize,
	}

	params, _ := json.Marshal(protocol.AuthParams{Username: m.username})
	msg.Params = params

	if err := m.send(msg); err != nil {
		return err
	}

	line, err := m.reader.ReadBytes('\n')
	if err != nil {
		return fmt.Errorf("error reading auth response: %w", err)
	}

	var res protocol.Response
	if err := json.Unmarshal(line, &res); err != nil {
		return fmt.Errorf("invalid auth response: %w", err)
	}

	if !res.Result {
		return fmt.Errorf("auth rejected: %s", res.Error)
	}

	return nil
}

func (m *Miner) Run(ctx context.Context) error {
	if err := m.Connect(ctx); err != nil {
		return err
	}
	defer m.conn.Close()

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errChan := make(chan error, 2)

	go func() {
		errChan <- m.readLoop(runCtx)
	}()

	go func() {
		errChan <- m.submitLoop(runCtx)
	}()

	select {
	case err := <-errChan:
		return err
	case <-ctx.Done():
		return nil
	}
}

func (m *Miner) readLoop(ctx context.Context) error {
	for {
		line, err := m.reader.ReadBytes('\n')
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}

			return fmt.Errorf("read error: %w", err)
		}

		// received a job broadcast
		var serverMsg protocol.ServerMessage
		if err := json.Unmarshal(line, &serverMsg); err != nil {
			slog.Warn("failed to parse server message", "error", err)
			continue
		}

		if serverMsg.Method == protocol.MethodJob {
			var params protocol.JobParams
			if err := json.Unmarshal(serverMsg.Params, &params); err != nil {
				slog.Warn("failed to parse job params", "error", err)
				continue
			}

			slog.Info("received job", "job_id", params.JobID, "server_nonce", params.ServerNonce)

			// doesnt block in case submitLoop is still processing previous job, discard
			select {
			case m.jobChan <- params:
			default:
				slog.Warn("job channel full, discarding old job")
				// drains channel
				<-m.jobChan
				m.jobChan <- params
			}
		}
	}
}

func (m *Miner) submitLoop(ctx context.Context) error {
	maxWait := time.NewTicker(time.Minute)
	defer maxWait.Stop()

	var currentJob *protocol.JobParams

	for {
		select {
		case <-ctx.Done():
			return nil

		// received new job
		case job := <-m.jobChan:
			currentJob = &job
			if err := m.submit(ctx, currentJob); err != nil {
				slog.Error("submit failed", "error", err)
			}

			maxWait.Reset(time.Minute)

		case <-maxWait.C:
			if currentJob == nil {
				slog.Warn("no job received yet, waiting...")
				continue
			}

			slog.Info("1 minute timeout, resubmitting current job")

			if err := m.submit(ctx, currentJob); err != nil {
				slog.Error("resubmit failed", "error", err)
			}
		}
	}
}

func (m *Miner) submit(ctx context.Context, job *protocol.JobParams) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return fmt.Errorf("failed to generate nonce: %w", err)
	}

	clientNonce := hex.EncodeToString(b)

	h := sha256.Sum256([]byte(job.ServerNonce + clientNonce))
	result := hex.EncodeToString(h[:])

	params, _ := json.Marshal(protocol.SubmitParams{
		JobID:       job.JobID,
		ClientNonce: clientNonce,
		Result:      result,
	})

	msg := protocol.Message{
		ID:     m.nextID(),
		Method: protocol.MethodSubmit,
		Params: params,
	}

	slog.Info("submitting", "job_id", job.JobID, "client_nonce", clientNonce)
	return m.send(msg)
}

func (m *Miner) send(msg any) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal error: %w", err)
	}

	data = append(data, '\n')
	_, err = m.conn.Write(data)
	return err
}
