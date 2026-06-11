package network

import (
	"bytes"
	"net"
	"testing"
	"time"
)

func TestConnection_FramingAndLifecycle(t *testing.T) {
	// Start server on an ephemeral port
	server := NewServer("127.0.0.1:0")

	messageChan := make(chan []byte, 10)

	server.OnNewConnection = func(conn *Connection) {
		conn.Start()
		for msg := range conn.Incoming {
			messageChan <- msg
			// Echo it back
			_ = conn.WriteMessage(msg)
		}
	}

	if err := server.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer server.Stop()

	// Get listener address to connect to
	addr := server.listener.Addr().String()

	// Connect client
	rawConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to connect to server: %v", err)
	}

	clientConn := NewConnection(rawConn, "client-1")
	clientConn.Start()

	// Send message
	testMsg := []byte("hello p2p world")
	if err := clientConn.WriteMessage(testMsg); err != nil {
		t.Fatalf("failed to write message: %v", err)
	}

	// Verify server received the message
	select {
	case recMsg := <-messageChan:
		if !bytes.Equal(recMsg, testMsg) {
			t.Errorf("expected %q, got %q", testMsg, recMsg)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for server to receive message")
	}

	// Verify client received the echo
	select {
	case echoMsg := <-clientConn.Incoming:
		if !bytes.Equal(echoMsg, testMsg) {
			t.Errorf("expected %q, got %q", testMsg, echoMsg)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for client to receive echo")
	}

	// Close client connection and verify incoming channel closes
	clientConn.Close()
	select {
	case _, ok := <-clientConn.Incoming:
		if ok {
			t.Error("expected Incoming channel to be closed")
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for client Incoming channel to close")
	}
}
