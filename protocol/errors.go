package protocol

import "errors"

var (
	ErrUnauthorized  = errors.New("unauthorized")
	ErrUnknownMethod = errors.New("unknown method")
)

func BuildErrorResponse(msgID uint64, err error) *response {
	if err != nil {
		switch err {
		case ErrUnauthorized:
			return &response{
				ID:     msgID,
				Result: false,
				Error:  err.Error(),
			}
		case ErrUnknownMethod:
			return &response{
				ID:     msgID,
				Result: false,
				Error:  err.Error(),
			}
		}
	}

	return nil
}
