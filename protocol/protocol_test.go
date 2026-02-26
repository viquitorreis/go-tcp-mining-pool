package protocol_test

import (
	"tcp_luxor/protocol"
	"testing"
)

func TestParse_ValidAuthorize(t *testing.T) {
	input := []byte(`{"id":1,"method":"authorize","params":{"username":"admin"}}` + "\n")

	msg, err := protocol.Parse(input)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if msg.Method != protocol.MethodAuthorize {
		t.Errorf("expected method authorize, got %s", msg.Method)
	}

	if msg.AuthParams == nil {
		t.Fatal("expected AuthParams to be populated")
	}

	if msg.AuthParams.Username != "admin" {
		t.Errorf("expected username admin, got %s", msg.AuthParams.Username)
	}
}

func TestParse_InvalidJSON(t *testing.T) {
	input := []byte(`{invalid json}` + "\n")

	_, err := protocol.Parse(input)
	if err == nil {
		t.Fatal("expected error for invalid JSON, got nil")
	}
}

func TestParse_UnknownMethod(t *testing.T) {
	input := []byte(`{"id":1,"method":"unknown","params":{}}` + "\n")

	_, err := protocol.Parse(input)
	if err == nil {
		t.Fatal("expected error for unknown method, got nil")
	}
}

func TestBuildJobMessage(t *testing.T) {
	data, err := protocol.BuildJobMessage(30, "abc123")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	msg, err := protocol.Parse(data)
	if err != nil {
		t.Fatalf("failed to parse built message: %v", err)
	}

	if msg.Method != protocol.MethodJob {
		t.Errorf("expected method job, got %s", msg.Method)
	}

	if msg.JobParams.JobID != 30 {
		t.Errorf("expected job_id 30, got %d", msg.JobParams.JobID)
	}

	if msg.JobParams.ServerNonce != "abc123" {
		t.Errorf("expected server_nonce abc123, got %s", msg.JobParams.ServerNonce)
	}
}
