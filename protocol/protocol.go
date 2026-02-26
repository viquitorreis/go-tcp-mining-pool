package protocol

import (
	"encoding/json"
	"fmt"
)

func Parse(data []byte) (*Message, error) {
	var msg Message
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, fmt.Errorf("invalid json: %w", err)
	}

	if !msg.Method.IsValid() {
		return nil, fmt.Errorf("unknown method: %s", msg.Method)
	}

	if err := msg.parseParams(); err != nil {
		return nil, fmt.Errorf("invalid params for %s: %w", msg.Method, err)
	}

	return &msg, nil
}

func (m *Message) parseParams() error {
	switch m.Method {
	case MethodAuthorize:
		var params AuthParams
		if err := json.Unmarshal(m.Params, &params); err != nil {
			return fmt.Errorf("error unmarshaling message for authorize method message_id:%d error:%w", m.ID, err)
		}

		m.AuthParams = &params
	case MethodJob:
		var params JobParams
		if err := json.Unmarshal(m.Params, &params); err != nil {
			return fmt.Errorf("error unmarshaling message for job method message_id:%d error:%w", m.ID, err)
		}

		m.JobParams = &params
	case MethodSubmit:
		var params SubmitParams
		if err := json.Unmarshal(m.Params, &params); err != nil {
			return fmt.Errorf("error unmarshaling message for submit method message_id:%d error:%w", m.ID, err)
		}

		m.SubmitParams = &params
	}

	return nil
}

func BuildResponse(id uint64, err error) *response {
	if err != nil {
		return buildErrorResponse(id, err)
	}

	return &response{
		ID:     id,
		Result: true,
	}
}

func BuildJobMessage(jobID uint64, serverNonce string) (*ServerMessage, error) {
	params, err := json.Marshal(JobParams{
		JobID:       jobID,
		ServerNonce: serverNonce,
	})

	if err != nil {
		return nil, fmt.Errorf("error building job message: %w", err)
	}

	return &ServerMessage{
		ID:     nil,
		Method: MethodJob,
		Params: params,
	}, nil
}

func buildErrorResponse(msgID uint64, err error) *response {
	if err != nil {
		return &response{
			ID:     msgID,
			Result: false,
			Error:  err.Error(),
		}
	}

	return nil
}
