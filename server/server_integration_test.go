package server

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"tcp_luxor/pool/dispatcher"
	"tcp_luxor/protocol"
	"testing"
	"time"
)

func TestIntegration_FullSubmitFlow(t *testing.T) {
	const serverNonce = "integration-server-nonce"
	const clientNonce = "integration-client-nonce"

	fake := newFakeDispatcher()
	fake.addJob(1, serverNonce)

	srv := NewTestServerWithDispatcher(fake)

	ctx := t.Context()
	go srv.Start(ctx)

	addr := srv.WaitForAddr(2 * time.Second)
	if addr == nil {
		t.Fatal("server did not start in time")
	}

	conn, err := net.Dial("tcp", addr.String())
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	// 1. auth
	authenticate(t, conn, reader, "integrationuser")

	// 2. calc correct hash + submit
	h := sha256.Sum256([]byte(serverNonce + clientNonce))
	sendMsg(t, conn, 2, protocol.MethodSubmit, protocol.SubmitParams{
		JobID:       1,
		ClientNonce: clientNonce,
		Result:      hex.EncodeToString(h[:]),
	})

	res := readResponse(t, reader)
	if !res.Result {
		t.Fatalf("submit failed unexpectedly: %s", res.Error)
	}
}

func TestIntegration_Submit_ErrorConditions(t *testing.T) {
	const serverNonce = "error-test-nonce"

	fake := newFakeDispatcher()
	fake.addJob(1, serverNonce)

	srv := NewTestServerWithDispatcher(fake)
	ctx := t.Context()
	go srv.Start(ctx)

	addr := srv.WaitForAddr(2 * time.Second)
	if addr == nil {
		t.Fatal("server did not start in time")
	}

	connect := func(t *testing.T, username string) (net.Conn, *bufio.Reader) {
		t.Helper()

		conn, err := net.Dial("tcp", addr.String())
		if err != nil {
			t.Fatalf("failed to connect: %v", err)
		}

		reader := bufio.NewReader(conn)

		authenticate(t, conn, reader, username)

		return conn, reader
	}

	submitRaw := func(t *testing.T, conn net.Conn, reader *bufio.Reader, jobID uint64, nonce, result string) protocol.Response {
		t.Helper()

		sendMsg(t, conn, 2, protocol.MethodSubmit, protocol.SubmitParams{
			JobID:       jobID,
			ClientNonce: nonce,
			Result:      result,
		})

		return readResponse(t, reader)
	}

	t.Run("task does not exist", func(t *testing.T) {
		conn, reader := connect(t, "user-no-task")
		defer conn.Close()

		res := submitRaw(t, conn, reader, math.MaxUint64, "anynonce", "anyhash")

		if res.Error != "Task does not exist" {
			t.Errorf("expected 'Task does not exist', got %q", res.Error)
		}
	})

	t.Run("invalid result", func(t *testing.T) {
		conn, reader := connect(t, "user-invalid-result")
		defer conn.Close()

		res := submitRaw(t, conn, reader, 1, "mynonce", "wronghash")
		if res.Error != "Invalid result" {
			t.Errorf("expected 'Invalid result', got %q", res.Error)
		}
	})

	t.Run("duplicate nonce", func(t *testing.T) {
		conn, reader := connect(t, "user-dup-nonce")
		defer conn.Close()

		h := sha256.Sum256([]byte(serverNonce + "firstnonce"))
		validResult := hex.EncodeToString(h[:])

		res := submitRaw(t, conn, reader, 1, "firstnonce", validResult)
		if !res.Result {
			t.Fatalf("first submit failed: %s", res.Error)
		}

		time.Sleep(1100 * time.Millisecond)

		res = submitRaw(t, conn, reader, 1, "firstnonce", validResult)
		if res.Error != "Duplicate submission" {
			t.Errorf("expected 'Duplicate submission', got %q", res.Error)
		}
	})

	t.Run("rate limit", func(t *testing.T) {
		conn, reader := connect(t, "user-rate-limit")
		defer conn.Close()

		h1 := sha256.Sum256([]byte(serverNonce + "nonce1"))
		h2 := sha256.Sum256([]byte(serverNonce + "nonce2"))

		res := submitRaw(t, conn, reader, 1, "nonce1", hex.EncodeToString(h1[:]))
		if !res.Result {
			t.Fatalf("first submit failed: %s", res.Error)
		}

		res = submitRaw(t, conn, reader, 1, "nonce2", hex.EncodeToString(h2[:]))
		if res.Error != "Submission too frequent" {
			t.Errorf("expected 'Submission too frequent', got %q", res.Error)
		}
	})
}

func TestRace_ConcurrentSubmits(t *testing.T) {
	const numClients = 20
	const serverNonce = "race-test-nonce"

	fake := newFakeDispatcher()
	fake.addJob(1, serverNonce)

	srv := NewTestServerWithDispatcher(fake)
	ctx := t.Context()
	go srv.Start(ctx)

	addr := srv.WaitForAddr(2 * time.Second)
	if addr == nil {
		t.Fatal("server did not start in time")
	}

	var wg sync.WaitGroup
	wg.Add(numClients)

	for i := range numClients {
		go func(i int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", addr.String())
			if err != nil {
				t.Errorf("client %d: failed to connect: %v", i, err)
				return
			}
			defer conn.Close()

			reader := bufio.NewReader(conn)
			authenticate(t, conn, reader, fmt.Sprintf("raceuser%d", i))

			clientNonce := fmt.Sprintf("racenonce%d", i)
			h := sha256.Sum256([]byte(serverNonce + clientNonce))

			sendMsg(t, conn, 2, protocol.MethodSubmit, protocol.SubmitParams{
				JobID:       1,
				ClientNonce: clientNonce,
				Result:      hex.EncodeToString(h[:]),
			})

			if _, err := reader.ReadBytes('\n'); err != nil {
				t.Errorf("client %d: failed to read submit response: %v", i, err)
			}
		}(i)
	}

	wg.Wait()
}

func TestRace_BroadcastWhileConnecting(t *testing.T) {
	fake := newFakeDispatcher()
	fake.addJob(1, "broadcast-race-nonce")

	srv := NewTestServerWithDispatcher(fake)
	ctx := t.Context()
	go srv.Start(ctx)

	addr := srv.WaitForAddr(2 * time.Second)
	if addr == nil {
		t.Fatal("server did not start in time")
	}

	stopBroadcast := make(chan struct{})
	go func() {
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopBroadcast:
				return
			case <-ticker.C:
				srv.broadcastJob(dispatcher.ServerJob{JobID: 1, ServerNonce: "broadcast-nonce"})
			}
		}
	}()

	var wg sync.WaitGroup
	for i := range 30 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", addr.String())
			if err != nil {
				return
			}

			conn.SetDeadline(time.Now().Add(50 * time.Millisecond))
			io.Copy(io.Discard, conn)
			conn.Close()
		}(i)
	}

	wg.Wait()
	close(stopBroadcast)
}

func TestRace_StatsMapConcurrentAccess(t *testing.T) {
	const serverNonce = "stats-race-nonce"

	fake := newFakeDispatcher()
	fake.addJob(1, serverNonce)

	srv := NewTestServerWithDispatcher(fake)
	ctx := t.Context()
	go srv.Start(ctx)

	addr := srv.WaitForAddr(2 * time.Second)
	if addr == nil {
		t.Fatal("server did not start in time")
	}

	conn, err := net.Dial("tcp", addr.String())
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)
	authenticate(t, conn, reader, "statsuser")

	clientNonce := "stats-client-nonce"
	h := sha256.Sum256([]byte(serverNonce + clientNonce))

	sendMsg(t, conn, 2, protocol.MethodSubmit, protocol.SubmitParams{
		JobID:       1,
		ClientNonce: clientNonce,
		Result:      hex.EncodeToString(h[:]),
	})

	readResponse(t, reader)

	srv.mu.RLock()
	count := srv.stats["statsuser"]
	srv.mu.RUnlock()

	if count != 1 {
		t.Errorf("expected stats count 1 for statsuser, got %d", count)
	}
}
