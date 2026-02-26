package protocol

import "encoding/json"

type Method string

const (
	MethodAuthorize Method = "authorize"
	MethodJob       Method = "job"
	MethodSubmit    Method = "submit"
)

func (m Method) IsValid() bool {
	switch m {
	case "authorize", "job", "submit":
		return true
	default:
		return false
	}
}

func (m Method) ToString() string {
	return string(m)
}

type Message struct {
	ID     uint64          `json:"id"`
	Method Method          `json:"method"`
	Params json.RawMessage `json:"params"`

	JobParams    *JobParams
	AuthParams   *AuthParams
	SubmitParams *SubmitParams
}

type response struct {
	ID     uint64 `json:"id"`
	Result bool   `json:"result"`
	Error  string `json:"error_message,omitempty"`
}

type ServerMessage struct {
	ID     *uint64         `json:"id"`
	Method Method          `json:"method"`
	Params json.RawMessage `json:"params"`
}

type AuthParams struct {
	Username string `json:"username"`
}

type JobParams struct {
	JobID       uint64 `json:"job_id"`
	ServerNonce string `json:"server_nonce"`
}

type SubmitParams struct {
	JobID       uint64 `json:"job_id"`
	ClientNonce string `json:"client_nonce"`
	Result      string `json:"result"`
}
