package client

import (
	"net"

	"github.com/google/uuid"
)

type Client struct {
	ID   uuid.UUID
	Conn net.Conn
}
