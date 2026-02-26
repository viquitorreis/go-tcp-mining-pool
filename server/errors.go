package server

import "errors"

var (
	ErrUnauthorized       = errors.New("Unauthorized")
	ErrUnknownMethod      = errors.New("Unknown method")
	ErrInvalidJob         = errors.New("Invalid task")
	ErrJobNotFound        = errors.New("Job not found")
	ErrRateLimit          = errors.New("Submission too frequent")
	ErrDuplicateNonce     = errors.New("Duplicate submission")
	ErrInvalidResult      = errors.New("Invalid result")
	ErrTaskNotFound       = errors.New("Task does not exist")
	ErrMinerAlreadyExists = errors.New("Miner already exists")
)
