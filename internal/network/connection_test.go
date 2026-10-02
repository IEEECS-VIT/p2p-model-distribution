package network

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/IEEECS-VIT/p2p-model-distribution/internal/protocol"
	"google.golang.org/protobuf/proto"
)

func TestFraming_RoundTrip(t *testing.T) {
	var buf bytes.Buffer
	msgs := [][]byte{[]byte("hello p2p world"), {}, bytes.Repeat([]byte{7}, 70000)}
	for _, m := range msgs {
		if err := WriteFrame(&buf, m); err != nil {
			t.Fatalf("WriteFrame: %v", err)
		}
	}
	for i, want := range msgs {
		got, err := ReadFrame(&buf)
		if err != nil {
			t.Fatalf("ReadFrame %d: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("frame %d mismatch", i)
		}
	}
}

func TestFraming_RejectsOversizedFrames(t *testing.T) {
	if err := WriteFrame(io.Discard, make([]byte, MaxMessageSize+1)); err == nil {
		t.Fatal("WriteFrame accepted an oversized frame")
	}

	header := []byte{0xFF, 0xFF, 0xFF, 0xFF}
	if _, err := ReadFrame(bytes.NewReader(header)); err == nil {
		t.Fatal("ReadFrame accepted an oversized length header")
	}
}

// startRawPeer returns a server-side Connection (with router) and the raw
// client socket talking to it, so tests can inject arbitrary bytes.
func startRawPeer(t *testing.T, router *Router) (*Connection, net.Conn) {
	t.Helper()
	server := NewServer("127.0.0.1:0")
	accepted := make(chan *Connection, 1)
	server.OnNewConnection = func(conn *Connection) {
		conn.SetRouter(router)
		conn.Start()
		accepted <- conn
	}
	if err := server.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	t.Cleanup(server.Stop)

	raw, err := net.Dial("tcp", server.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { raw.Close() })

	select {
	case c := <-accepted:
		return c, raw
	case <-time.After(2 * time.Second):
		t.Fatal("server did not accept connection")
		return nil, nil
	}
}

func TestConnection_MalformedFrameClosesConnection(t *testing.T) {
	conn, raw := startRawPeer(t, NewRouter())

	// 0xFF is never a valid protobuf field tag, so this cannot parse.
	if err := WriteFrame(raw, []byte{0xFF, 0xFF, 0xFF}); err != nil {
		t.Fatal(err)
	}

	select {
	case <-conn.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("connection stayed open after a malformed frame")
	}
}

// TestConnection_DuplicateResponsesDoNotWedgeReadLoop reproduces a peer
// answering one request many times. Previously every duplicate was pushed
// into the request's 1-slot channel, so the third one blocked the read
// loop forever and the connection stopped processing all traffic.
func TestConnection_DuplicateResponsesDoNotWedgeReadLoop(t *testing.T) {
	serverSide, clientSide := net.Pipe()
	defer serverSide.Close()

	client := NewConnection(clientSide, "server")
	client.Start()
	defer client.Close()

	// Fake server: answer the first request 10 times, then answer the
	// second request normally.
	go func() {
		for i := 0; i < 2; i++ {
			data, err := ReadFrame(serverSide)
			if err != nil {
				return
			}
			var req protocol.Envelope
			_ = proto.Unmarshal(data, &req)
			resp, _ := proto.Marshal(&protocol.Envelope{Id: req.Id, Type: protocol.MessageType_MSG_GET_CHUNK_RESPONSE})
			repeats := 1
			if i == 0 {
				repeats = 10
			}
			for j := 0; j < repeats; j++ {
				if err := WriteFrame(serverSide, resp); err != nil {
					return
				}
			}
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for i := 0; i < 2; i++ {
		if _, err := client.SendRaw(ctx, protocol.MessageType_MSG_GET_CHUNK_REQUEST, nil); err != nil {
			t.Fatalf("request %d failed: %v", i, err)
		}
	}
}

func TestConnection_PendingRequestsFailWhenConnectionCloses(t *testing.T) {
	serverSide, clientSide := net.Pipe()
	client := NewConnection(clientSide, "server")
	client.Start()

	go func() {
		_, _ = ReadFrame(serverSide) // swallow the request, then hang up
		serverSide.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := client.SendRaw(ctx, protocol.MessageType_MSG_GET_CHUNK_REQUEST, nil); err == nil || ctx.Err() != nil {
		t.Fatalf("SendRaw err = %v, want prompt connection-closed error", err)
	}

	<-client.Done()
	if _, err := client.SendRaw(ctx, protocol.MessageType_MSG_GET_CHUNK_REQUEST, nil); err == nil {
		t.Fatal("SendRaw on a closed connection succeeded")
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
