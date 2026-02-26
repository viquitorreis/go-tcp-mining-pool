package protocol

import (
	"encoding/json"
	"fmt"
	"log/slog"
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
			slog.Error("error unmarshaling mesage job method", "message_id", m.ID, "error", err)
			return err
		}

		m.AuthParams = &params
	case MethodJob:
		var params JobParams
		if err := json.Unmarshal(m.Params, &params); err != nil {
			slog.Error("error unmarshaling mesage job method", "message_id", m.ID, "error", err)
			return err
		}

		m.JobParams = &params
	case MethodSubmit:
		var params SubmitParams
		if err := json.Unmarshal(m.Params, &params); err != nil {
			slog.Error("error unmarshaling mesage job method", "message_id", m.ID, "error", err)
			return err
		}

		m.SubmitParams = &params
	}

	return nil
}

func BuildAuthResponse(id uint64, result bool) Response {
	return Response{
		ID:     id,
		Result: result,
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
