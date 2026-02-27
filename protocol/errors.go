package protocol

import "errors"

var (
	ErrRateLimit      = errors.New("Submission too frequent")
	ErrDuplicateNonce = errors.New("Duplicate submission")
	ErrInvalidResult  = errors.New("Invalid result")
	ErrTaskNotFound   = errors.New("Task does not exist")
)
