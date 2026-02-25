package client

import (
	"fmt"
	"net"
	"os"
	"time"
)

type ClienID string

type Client struct {
	ID            ClienID
	Conn          net.Conn
	Username      string
	Authenticated bool

	StartedAt time.Time
}

func NewClientID(numClients int) ClienID {
	host, _ := os.Hostname()
	return ClienID(fmt.Sprintf("%d_%s", numClients+1, host))
}
