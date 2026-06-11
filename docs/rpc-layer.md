# Custom Multiplexed TCP RPC & Protobuf Layer

This document provides a detailed technical explanation of the custom peer-to-peer (P2P) RPC and messaging layer built over TCP. The layer uses Google Protocol Buffers (Protobuf) for structured message payloads, custom length-prefix framing for socket boundaries, and asynchronous multiplexing to handle concurrent request-response loops over single TCP sockets.

---

## 1. Core Architecture & Design Philosophy

In a production P2P file/model distribution network, standard application protocols like HTTP/1.1 or HTTP/2 introduce substantial framing overhead, lack pure symmetric bi-directional request dispatching out-of-the-box, or pull in heavy dependencies (e.g. gRPC or `libp2p`).

To maintain a lightweight, zero-dependency foundation, this project implements a custom P2P communication layer directly over TCP. The core requirements satisfied by this design are:
* **Message Framing:** Raw TCP is a continuous byte stream with no message boundaries. We prefix every message with a 4-byte header specifying payload length.
* **Type Safety:** Payloads are serialized using Google Protocol Buffers for robust, schema-driven serialization.
* **Multiplexing:** Multiple requests and responses are sent concurrently over a single TCP connection. A background goroutine reads incoming frames and matches responses back to their originating callers using request IDs.
* **Symmetric Routing:** A node can simultaneously act as an RPC client (sending requests) and an RPC server (routing incoming requests to registered handlers) on the same socket connection.

---

## 2. Framing Layer (Length-Prefix Boundary)

Since TCP is stream-oriented, the socket layer must know how many bytes constitute a complete application message. The project implements a length-prefix frame writer and reader in `internal/network/connection.go`:

```
┌───────────────────────────┬───────────────────────────────────────────┐
│ Length Prefix (4 Bytes)   │        Protobuf Envelope (Variable)       │
│ Big-Endian uint32         │  [Envelope ID, Message Type, Payload]     │
└───────────────────────────┴───────────────────────────────────────────┘
```

* **Writing (`WriteFrame`):** Serializes the protobuf message into bytes, calculates the length $N$, writes $N$ as a 4-byte big-endian unsigned integer (`binary.BigEndian.PutUint32`), followed by the $N$ payload bytes.
* **Reading (`ReadFrame`):** Reads exactly 4 bytes from the socket to determine the payload length $N$. Then, performs a buffered, full read (`io.ReadFull`) of exactly $N$ bytes to isolate the serialized protobuf frame.

---

## 3. Protocol Buffers Schema (`proto/p2p.proto`)

The communication schema defines standard structured message envelopes and operational payloads.

### Message Envelope
Every message sent across the network is wrapped inside an `Envelope` structure. This ensures the receiver can read, dispatch, and deserialize payloads safely.

```protobuf
syntax = "proto3";
package protocol;
option go_package = "github.com/IEEECS-VIT/p2p-model-distribution/internal/protocol";

enum MessageType {
  MSG_UNKNOWN = 0;
  MSG_HANDSHAKE_REQUEST = 1;
  MSG_HANDSHAKE_RESPONSE = 2;
  MSG_GET_METADATA_REQUEST = 3;
  MSG_GET_METADATA_RESPONSE = 4;
  MSG_GET_CHUNK_REQUEST = 5;
  MSG_GET_CHUNK_RESPONSE = 6;
}

message Envelope {
  string id = 1;          // Unique request ID (UUID) for matching responses
  MessageType type = 2;   // Enum specifying what payload is enclosed
  bytes payload = 3;      // Marshallled sub-message bytes
}
```

### Operational Message Payloads

#### 1. Handshake
Exchanged immediately upon socket establishment to authenticate identity, check network protocol versions, and exchange public parameters.
```protobuf
message HandshakeRequest {
  string peer_id = 1;
  string version = 2;
}

message HandshakeResponse {
  string peer_id = 1;
  bool success = 2;
  string error = 3;
}
```

#### 2. Get Metadata (Manifest)
Queries a peer for a file manifest JSON by its stable `FileID`. 
```protobuf
message GetMetadataRequest {
  string file_id = 1;
}

message GetMetadataResponse {
  string file_id = 1;
  bool success = 2;
  bytes metadata_json = 3; // Serialized filemeta.FileMeta structure
  string error = 4;
}
```
> **Design Note:** Rather than recreating the complex hierarchical `FileMeta` struct inside the protobuf schema, it is marshaled to JSON on the sender side and packed into `metadata_json`. This isolates Go struct field updates from wire-protocol schema drift.

#### 3. Get Chunk
Requests raw block data for a specific chunk index of a file.
```protobuf
message GetChunkRequest {
  string file_id = 1;
  int32 chunk_index = 2;
}

message GetChunkResponse {
  string file_id = 1;
  int32 chunk_index = 2;
  bool success = 3;
  bytes data = 4;        // Raw chunk content
  string error = 5;
}
```

---

## 4. Connection Multiplexing & Request Matching

The `Connection` struct (`internal/network/connection.go`) manages read/write coordination.

### Struct Anatomy
```go
type Connection struct {
	conn            net.Conn
	peerID          string
	Incoming        chan []byte
	router          *Router
	
	mu              sync.Mutex
	pendingRequests map[string]chan *protocol.Envelope // Maps RequestID -> Reply Channel
	closed          bool
}
```

### Asynchronous Read Loop (`readLoop`)
When `Start()` is called, a background goroutine executes `readLoop()`:
1. It continuously calls `ReadFrame()` to extract the raw byte payload.
2. It attempts to parse the payload as a `protocol.Envelope`.
   * **Backward Compatibility Fallback:** If parsing fails, the raw bytes are pushed directly to `Incoming` (allowing non-RPC raw framing tests to pass).
3. If it is a valid RPC envelope:
   * **Inbound Response:** If the envelope's `Id` exists in the `pendingRequests` registry map, it means a local caller is waiting for this response. The envelope is dispatched directly into the associated waiting channel.
   * **Inbound Request:** If the envelope's `Id` is not registered in our map, the envelope is treated as an incoming RPC request from the remote peer. The request is dispatched to the connection's registered `Router` for execution.

### RPC Requests (`SendRequest`)
To send an RPC and block on the response:
```go
func (c *Connection) SendRequest(ctx context.Context, msgType protocol.MessageType, reqMsg proto.Message) (*protocol.Envelope, error)
```
1. **Serialization:** Marshals the inner request message payload (`reqMsg`).
2. **Envelope Creation:** Wraps it in a `protocol.Envelope`, generating a random UUID string as the request `id`.
3. **Register Wait-Channel:** Creates a channel `ch := make(chan *protocol.Envelope, 1)` and registers it in the `pendingRequests` map under the generated `id`.
4. **Transmission:** Writes the length-prefixed frame to the TCP socket.
5. **Blocking Wait:** Listens on a select statement matching:
   * `case resp := <-ch`: Handled response received from the `readLoop`. Returns the response envelope.
   * `case <-ctx.Done()`: Context timeout or cancellation occurred. Unregisters the wait-channel to prevent leaks and returns a timeout error.

---

## 5. Request Routing (`internal/network/router.go`)

The router acts as the server-side dispatching engine. It registers callback handlers mapping specific `protocol.MessageType` enums to custom executable logic.

### Router Interface
```go
type HandlerFunc func(conn *Connection, env *protocol.Envelope) error

type Router struct {
	handlers map[protocol.MessageType]HandlerFunc
}
```

* **`Register(msgType, HandlerFunc)`:** Attaches a handler to a message type.
* **`Route(conn, envelope)`:** Invoked by the connection's `readLoop` upon receiving an unregistered envelope ID. Locates the correct handler and executes it.

### Handler Replying (`WriteResponse`)
Handlers execute their business logic and send responses using `WriteResponse`:
```go
func (c *Connection) WriteResponse(reqID string, msgType protocol.MessageType, respMsg proto.Message) error {
    // Marshals the response payload
    // Packages it into an Envelope containing the SAME reqID as the request
    // Writes it as a length-prefixed frame back to the socket
}
```

---

## 6. End-to-End Execution Sequence

This sequence diagram illustrates a downloader node requesting chunk `0` from a seeder node:

```
Downloader (Client Node)                   Seeder (Server Node)
   │                                          │
   │─── SendRequest (GetChunkRequest) ────────► (Read TCP Frame)
   │    UUID ID: "abc-123"                    │
   │                                          │─── Parse Envelope
   │                                          │─── Dispatch to Router
   │                                          │─── Execute GetChunk Handler
   │                                          │    (Load chunk index 0 from disk)
   │                                          │
   │◄── WriteResponse (GetChunkResponse) ─────│
   │    UUID ID: "abc-123"                    │
   │                                          │
(Read TCP Frame)                              │
   │                                          │
Verify envelope ID ("abc-123") in map         │
Dispatch to waiting channel                   │
Verify chunk hash & save to disk              │
```

---

## 7. Testing & CLI Drivers

### Integration Tests
A full end-to-end simulation is located at [p2p_transfer_e2e_test.go](file:///home/daksh/Documents/github/p2p-model-distribution/tests/p2p_transfer_e2e_test.go). This test:
1. Spawns a TCP server with a registered RPC router.
2. Connects a client connection to it.
3. Transits file metadata manifests and chunk arrays over the network.
4. Verifies hashes and assembles them back to check byte integrity.

### Run-Time Binary (`cmd/node/main.go`)
Allows manual local or cross-network testing.
* **Seed mode:** Chunk files and serve them over the custom TCP server.
* **Download mode:** Dial a seeder, pull metadata, request chunks, and reassemble them.
