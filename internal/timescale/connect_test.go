package timescale

import (
	"context"
	"net"
	"testing"
	"time"
)

// A server that accepts the connection and never answers must not hang
// startup.
func TestNewTimescaleClientPingTimeout(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { _ = c.Close() })
		}
	}()

	old := pingTimeout
	pingTimeout = 200 * time.Millisecond
	t.Cleanup(func() { pingTimeout = old })

	done := make(chan error, 1)
	go func() {
		_, err := NewTimescaleClient(context.Background(),
			"postgres://u:p@"+ln.Addr().String()+"/db?sslmode=disable")
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("NewTimescaleClient: expected error from a silent server")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("NewTimescaleClient did not return within 5s")
	}
}
