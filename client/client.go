package client

import (
	"fmt"
	"net"
	"os"
	"time"
)

type ClientID string

type Client struct {
	ID            ClientID
	Conn          net.Conn
	Username      string
	Authenticated bool

	StartedAt time.Time
}

func NewClientID(numClients int) ClientID {
	host, _ := os.Hostname()
	return ClientID(fmt.Sprintf("%d_%s", numClients+1, host))
}
