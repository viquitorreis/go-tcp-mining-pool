package server

import "errors"

var (
	ErrUnauthorized       = errors.New("unauthorized")
	ErrUnknownMethod      = errors.New("unknown method")
	ErrInvalidJob         = errors.New("invalid job")
	ErrJobNotFound        = errors.New("job not found")
	ErrRateLimit          = errors.New("error too many requests")
	ErrDuplicateNonce     = errors.New("error duplicate nonce")
	ErrInvalidResult      = errors.New("submitted result is invalid")
	ErrInvalidServerNonce = errors.New("invalid server nonce")
)
