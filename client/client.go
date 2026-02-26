package client

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"
)

type ClientID string

type Client struct {
	id            ClientID
	conn          net.Conn
	username      string
	authenticated bool
	usedNonces    map[string]bool
	currentJobID  uint64 // TODO

	startedAt    time.Time
	lastSubmitAt time.Time

	mu      sync.RWMutex
	writeMu sync.Mutex // tcp writes only
}

func NewClient(numClients int, conn net.Conn) *Client {
	return &Client{
		id:         newClientID(numClients),
		conn:       conn,
		startedAt:  time.Now(),
		usedNonces: make(map[string]bool),
	}
}

func newClientID(numClients int) ClientID {
	host, _ := os.Hostname()
	return ClientID(fmt.Sprintf("%d_%s", numClients+1, host))
}

func (c *Client) Write(data []byte) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if _, err := c.conn.Write(data); err != nil {
		slog.Error("error writing to client", "client_id", c.id, "error", err)
	}
}

func (c *Client) GetConn() net.Conn {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.conn
}

func (c *Client) CloseConn() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.conn.Close()
}

func (c *Client) GetID() ClientID {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.id
}

func (c *Client) GetUsername() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.username
}

func (c *Client) Authenticate(username string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.username = username
	c.authenticated = true
}

func (c *Client) IsAuthenticated() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.authenticated
}

func (c *Client) GetLastSubmitAt() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lastSubmitAt
}

func (c *Client) UpdateTimeSubmit() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastSubmitAt = time.Now()
}

func (c *Client) SetNonce(nonce string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.usedNonces[nonce] = true
}

func (c *Client) HasNonce(nonce string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	_, exists := c.usedNonces[nonce]
	if exists {
		return true
	}

	return false
}
