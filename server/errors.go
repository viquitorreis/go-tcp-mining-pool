package server

import "errors"

var (
	ErrUnauthorized          = errors.New("unauthorized")
	ErrUnknownMethod         = errors.New("unknown method")
	ErrInvalidJob            = errors.New("invalid task")
	ErrJobNotFound           = errors.New("job not found")
	ErrRateLimit             = errors.New("submission too frequent")
	ErrDuplicateNonce        = errors.New("duplicate submission")
	ErrInvalidResult         = errors.New("invalid result")
	ErrInexistentServerNonce = errors.New("task does not exist")
)
