package server

import "errors"

var (
	ErrUnauthorized       = errors.New("Unauthorized")
	ErrUnknownMethod      = errors.New("Unknown method")
	ErrInvalidJob         = errors.New("Invalid task")
	ErrMinerAlreadyExists = errors.New("Miner already exists")
)
