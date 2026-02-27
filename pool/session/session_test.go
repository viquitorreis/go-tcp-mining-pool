package session

import (
	"io"
	"net"
	"sync"
	"testing"
)

func TestSession_ConcurrentWrites_NoCorruption(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	sess := NewSession(1, serverConn)

	go io.Copy(io.Discard, clientConn)

	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			sess.Write([]byte(`{"id":1,"result":true}` + "\n"))
		})
	}
	wg.Wait()
}
