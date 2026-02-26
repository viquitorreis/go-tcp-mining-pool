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

func BuildMessage(id uint64, method Method, params any) ([]byte, error) {
	raw, err := json.Marshal(params)
	if err != nil {
		return nil, fmt.Errorf("error marshaling params for %s: %w", method, err)
	}

	msg := Message{
		ID:     id,
		Method: method,
		Params: raw,
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("error marshaling message: %w", err)
	}

	return append(data, '\n'), nil
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

func BuildResponse(id uint64, err error) *Response {
	if err != nil {
		return buildErrorResponse(id, err)
	}

	return &Response{
		ID:     id,
		Result: true,
	}
}

func BuildJobMessage(jobID uint64, serverNonce string) ([]byte, error) {
	params, err := json.Marshal(JobParams{
		JobID:       jobID,
		ServerNonce: serverNonce,
	})

	if err != nil {
		return nil, fmt.Errorf("error building job message: %w", err)
	}

	msg := ServerMessage{
		ID:     nil,
		Method: MethodJob,
		Params: params,
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("error marshaling job message: %w", err)
	}

	return append(data, '\n'), nil
}

func buildErrorResponse(msgID uint64, err error) *Response {
	if err != nil {
		return &Response{
			ID:     msgID,
			Result: false,
			Error:  err.Error(),
		}
	}

	return nil
}
