package network

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"github.com/IEEECS-VIT/p2p-model-distribution/internal/protocol"
	"google.golang.org/protobuf/proto"
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

func TestConnection_RPCRoundtrip(t *testing.T) {
	// Start server on an ephemeral port
	server := NewServer("127.0.0.1:0")

	// Set up router on server side
	router := NewRouter()
	router.Register(protocol.MessageType_MSG_HANDSHAKE_REQUEST, func(conn *Connection, env *protocol.Envelope) error {
		var req protocol.HandshakeRequest
		if err := proto.Unmarshal(env.Payload, &req); err != nil {
			return err
		}

		resp := &protocol.HandshakeResponse{
			PeerId:  "server-node",
			Success: true,
		}
		return conn.WriteResponse(env.Id, protocol.MessageType_MSG_HANDSHAKE_RESPONSE, resp)
	})

	router.Register(protocol.MessageType_MSG_GET_CHUNK_REQUEST, func(conn *Connection, env *protocol.Envelope) error {
		var req protocol.GetChunkRequest
		if err := proto.Unmarshal(env.Payload, &req); err != nil {
			return err
		}

		resp := &protocol.GetChunkResponse{
			FileId:     req.FileId,
			ChunkIndex: req.ChunkIndex,
			Success:    true,
			Data:       []byte("mock-chunk-data"),
		}
		return conn.WriteResponse(env.Id, protocol.MessageType_MSG_GET_CHUNK_RESPONSE, resp)
	})

	server.OnNewConnection = func(conn *Connection) {
		conn.SetRouter(router)
		conn.Start()
	}

	if err := server.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer server.Stop()

	// Connect client
	addr := server.listener.Addr().String()
	rawConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to connect to server: %v", err)
	}
	defer rawConn.Close()

	clientConn := NewConnection(rawConn, "client-node")
	clientConn.Start()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// 1. Test Handshake RPC
	hsReq := &protocol.HandshakeRequest{
		PeerId:  "client-node",
		Version: "1.0",
	}
	hsRespEnv, err := clientConn.SendRequest(ctx, protocol.MessageType_MSG_HANDSHAKE_REQUEST, hsReq)
	if err != nil {
		t.Fatalf("handshake request failed: %v", err)
	}

	if hsRespEnv.Type != protocol.MessageType_MSG_HANDSHAKE_RESPONSE {
		t.Errorf("expected msg type %v, got %v", protocol.MessageType_MSG_HANDSHAKE_RESPONSE, hsRespEnv.Type)
	}

	var hsResp protocol.HandshakeResponse
	if err := proto.Unmarshal(hsRespEnv.Payload, &hsResp); err != nil {
		t.Fatalf("failed to unmarshal handshake response: %v", err)
	}

	if !hsResp.Success || hsResp.PeerId != "server-node" {
		t.Errorf("handshake response details invalid: success=%v, peerID=%s", hsResp.Success, hsResp.PeerId)
	}

	// 2. Test GetChunk RPC
	chunkReq := &protocol.GetChunkRequest{
		FileId:     "test-cid-123",
		ChunkIndex: 5,
	}
	chunkRespEnv, err := clientConn.SendRequest(ctx, protocol.MessageType_MSG_GET_CHUNK_REQUEST, chunkReq)
	if err != nil {
		t.Fatalf("get chunk request failed: %v", err)
	}

	if chunkRespEnv.Type != protocol.MessageType_MSG_GET_CHUNK_RESPONSE {
		t.Errorf("expected msg type %v, got %v", protocol.MessageType_MSG_GET_CHUNK_RESPONSE, chunkRespEnv.Type)
	}

	var chunkResp protocol.GetChunkResponse
	if err := proto.Unmarshal(chunkRespEnv.Payload, &chunkResp); err != nil {
		t.Fatalf("failed to unmarshal chunk response: %v", err)
	}

	if !chunkResp.Success || !bytes.Equal(chunkResp.Data, []byte("mock-chunk-data")) || chunkResp.ChunkIndex != 5 {
		t.Errorf("chunk response data mismatch: index=%d, success=%v, data=%s", chunkResp.ChunkIndex, chunkResp.Success, string(chunkResp.Data))
	}
}
