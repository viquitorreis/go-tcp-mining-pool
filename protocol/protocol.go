package protocol

import (
	"encoding/json"
	"log/slog"
)

func (m *Message) ToJSON() (string, error) {
	b, err := json.Marshal(m)
	if err != nil {
		slog.Error("error marshaling to json", "message_id", m.ID, "error", err)
		return "", err
	}
	return string(b), nil
}

func ReadJSON(s string) (*Message, error) {
	var msg Message
	if err := json.Unmarshal([]byte(s), &msg); err != nil {
		slog.Error("error unmarshalling message", "err", err)
		return nil, err
	}

	switch msg.Method {
	case MethodAuthorize:
		var params AuthParams
		if err := json.Unmarshal(msg.Params, &params); err != nil {
			slog.Error("error unmarshaling mesage job method", "message_id", msg.ID, "error", err)
			return nil, err
		}

		msg.AuthParams = &params
	case MethodJob:
		var params JobParams
		if err := json.Unmarshal(msg.Params, &params); err != nil {
			slog.Error("error unmarshaling mesage job method", "message_id", msg.ID, "error", err)
			return nil, err
		}

		msg.JobParams = &params
	case MethodSubmit:
		var params SubmitParams
		if err := json.Unmarshal(msg.Params, &params); err != nil {
			slog.Error("error unmarshaling mesage job method", "message_id", msg.ID, "error", err)
			return nil, err
		}

		msg.SubmitParams = &params
	}

	return &msg, nil
}
