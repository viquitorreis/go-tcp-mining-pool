package protocol

import "encoding/json"

type Method string

const (
	MethodAuthorize = "authorize"
	MethodJob       = "job"
	MethodSubmit    = "submit"
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
	ID     string          `json:"id"`
	Method Method          `json:"method"`
	Params json.RawMessage `json:"params"`

	JobParams    *JobParams
	AuthParams   *AuthParams
	SubmitParams *SubmitParams
}

type Response struct {
	ID     int    `json:"id"`
	Result bool   `json:"result"`
	Error  string `json:"error_message"`
}

type AuthParams struct {
	Username string `json:"username"`
}

type JobParams struct {
	JobID       int    `json:"job_id"`
	ServerNonce string `json:"server_nonce`
}

type SubmitParams struct {
	JobID       int    `json:"job_id"`
	ClientNonce string `json:"client_nonce"`
	Result      string `json:"result"`
}
