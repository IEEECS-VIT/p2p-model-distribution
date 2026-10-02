# Network & RPC Layer

`internal/network` carries every interaction between peers. That covers DHT
traffic and file transfer, multiplexed over one authenticated connection
per peer pair.

## 1. Transport: mutual TLS 1.3

Every connection is TLS 1.3 with **both** sides presenting a certificate.
Certificates are self-signed and carry the node's ed25519 key (see
`internal/identity`):

- The **node ID** is `hex(SHA-256(ed25519 public key))`, so claiming an ID
  requires the matching private key.
- Each side checks that the peer presented exactly one currently valid,
  self-signed ed25519 certificate. The TLS handshake itself proves the peer
  holds the private key. The peer's node ID is then derived from the
  certificate and exposed as `Connection.PeerID()`.
- When dialing a node whose ID is already known (e.g. from a DHT
  response), `identity.ClientTLSConfig(expectedID)` **pins** that ID, and
  the handshake fails if anyone else answers.
- ALPN `p2p-model-distribution/2` versions the protocol, so incompatible
  nodes fail the handshake instead of exchanging garbage.

```go
srv := network.NewServer(":9000", id.ServerTLSConfig())
conn, err := network.Dial(ctx, "10.0.0.5:9000", id.ClientTLSConfig(expectedID))
```

The server runs each handshake in its own goroutine with a
`HandshakeTimeout` (10s), so a peer that connects and stalls neither
blocks the accept loop nor holds a slot for long.

## 2. Framing

TCP is a byte stream, so every message is length-prefixed:

```
+----------------------+-----------------------------+
| 4-byte length (BE)   | payload (protobuf Envelope) |
+----------------------+-----------------------------+
```

- `MaxMessageSize` (10 MiB) is enforced on both read and write. A
  manifest's chunk size is capped at 8 MiB so a chunk always fits in one
  frame.
- A frame is written with a single `Write`, and writes on a connection are
  serialized by a dedicated mutex.

## 3. Envelope and messages (`proto/p2p.proto`)

```protobuf
message Envelope {
    string id = 1;          // pairs a response with its request
    MessageType type = 2;
    bytes payload = 3;      // serialized inner message
    bool is_response = 4;   // responses are never routed to handlers
}
```

| Request | Response | Payload |
|---------|----------|---------|
| `MSG_GET_METADATA_REQUEST` | `MSG_GET_METADATA_RESPONSE` | `GetMetadataRequest` / `GetMetadataResponse` (manifest JSON) |
| `MSG_GET_CHUNK_REQUEST` | `MSG_GET_CHUNK_RESPONSE` | `GetChunkRequest` / `GetChunkResponse` |
| `MSG_DHT_PING` | `MSG_DHT_PONG` | `DHTRequest` / `DHTResponse` |
| `MSG_DHT_FIND_NODE` | `MSG_DHT_FIND_NODE_RESPONSE` | `DHTRequest` / `DHTResponse` |
| `MSG_DHT_FIND_VALUE` | `MSG_DHT_FIND_VALUE_RESPONSE` | `DHTRequest` / `DHTResponse` |
| `MSG_DHT_ADD_PROVIDER` | `MSG_DHT_ADD_PROVIDER_RESPONSE` | `DHTRequest` / `DHTResponse` |
| any | `MSG_ERROR` | `ErrorResponse` |

No message carries the sender's node ID or IP. The sender is always the
TLS-authenticated peer.

## 4. Multiplexing (`connection.go`)

Each `Connection` runs one read loop:

1. Read a frame (with `ReadIdleTimeout`, 2 min, reset per frame). If the
   frame is not a valid `Envelope`, it is a protocol violation and the
   connection is closed.
2. If `is_response` is set, look up the pending request with that `id`,
   **remove it**, and hand the response over. Duplicate, late or
   unsolicited responses are dropped, so they can never block the loop.
3. Otherwise it is a request. The loop dispatches it to the router in a
   goroutine, with at most 32 handlers in flight per connection. When that
   limit is hit the loop waits, which applies backpressure to the peer.

`SendRequest` registers a pending request, writes the envelope, and waits
for the response, the context deadline, or connection teardown (which
fails all pending requests immediately). A `MSG_ERROR` reply is returned as
a `*RemoteError`.

`Done()` is closed when a connection is torn down. The DHT connection
pool uses it to drop dead connections.

## 5. Routing (`router.go`)

```go
router := network.NewRouter()
router.Register(protocol.MessageType_MSG_GET_CHUNK_REQUEST, handler)
```

- If no handler is registered for a type, the requester gets
  `MSG_ERROR "unsupported message type"` instead of waiting for a timeout.
- If a handler fails, the requester gets a generic `MSG_ERROR "request
  failed"`. Only errors that wrap `network.ErrBadRequest` have their
  message passed on, so local paths and OS errors never leak to peers.
- `Router` is safe for concurrent use.

## 6. Resource limits

| Limit | Value | Purpose |
|-------|-------|---------|
| `DefaultMaxConnections` | 512 inbound | file descriptor / memory exhaustion |
| `HandshakeTimeout` | 10s | stalled TLS handshakes |
| `ReadIdleTimeout` | 2 min per frame | slow-loris peers |
| `WriteTimeout` | 15s per write | peers that never drain their receive window |
| handlers in flight | 32 per connection | request floods |
| `MaxMessageSize` | 10 MiB | oversized frames |

`Server.Stop()` is idempotent and closes every accepted connection,
including ones still handshaking. Persistent accept errors (e.g. `EMFILE`)
back off exponentially instead of spinning.
