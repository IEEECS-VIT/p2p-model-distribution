# DHT Peer Discovery & Unified Node CLI

This document covers the Kademlia-inspired DHT implementation, its integration with the TCP network layer, and the unified `cmd/node` CLI that combines seeding/downloading with DHT-based peer discovery.

---

## 1. DHT Overview

The DHT provides **decentralised peer discovery** and **content/provider lookup** for the P2P file distribution network. Each node maintains a routing table of known peers and can:

- Discover new peers via bootstrapping
- Announce which files/chunks it provides
- Find which peers provide a given file or chunk

---

## 2. Kademlia-Inspired Design

### 2.1 Node Identity

Each node has a `Node` struct with `ID`, `IP`, and `Port`. The node ID is either user-specified or randomly generated as an 8-byte hex string.

```
Node{ID: "a1b2c3d4", IP: "192.168.1.5", Port: 9000}
```

### 2.2 Routing Table

The routing table consists of **256 k-buckets** (`K = 20`), one for each bit of the SHA-256 hash space.

**Bucket assignment** is computed as:

1. XOR the node's own ID with the peer's ID.
2. Find the first non-zero byte, then the leading non-zero bit within that byte.
3. Each byte contributes 8 bits of index; the first set bit determines the bucket.

```
bucketIndex = leadingBitOffset(distanceBytes(selfID ⊕ peerID))
```

**Peer management per bucket:**
- **Add:** If the peer exists, it is moved to the tail (most recently seen). If the bucket is full, the oldest (head) peer is evicted (FIFO eviction).
- **ClosestNodes:** All buckets are flattened, deduplicated, sorted by XOR distance to a target ID, and the top `K` (20) are returned.

### 2.3 XOR Distance Metric

Distance between two node IDs is the XOR of their SHA-256 hashes, interpreted as a big-endian integer. Lower XOR values mean closer nodes.

```go
func distanceBytes(a, b [32]byte) [32]byte {
    var dist [32]byte
    for i := 0; i < 32; i++ {
        dist[i] = a[i] ^ b[i]
    }
    return dist
}
```

---

## 3. DHT RPC Messages

All DHT messages use **JSON serialization** inside the Protobuf `Envelope` payload. Message types are defined in `internal/protocol/dht.go`.

| Direction | Wire Type | Code | Purpose |
|---|---|---|---|
| Request  | `MSG_DHT_PING`       | 7  | Liveness check |
| Response | `MSG_DHT_PONG`       | 8  | Liveness acknowledgement |
| Request  | `MSG_DHT_FIND_NODE`  | 9  | Ask peer for K closest nodes to a target ID |
| Request  | `MSG_DHT_STORE`      | 11 | Announce that this node provides a key |
| Request  | `MSG_DHT_FIND_VALUE` | 13 | Ask peer for providers of a key + closest nodes |

### 3.1 Core Handler (`dht.HandleMessage`)

```
PING       → PONG
FIND_NODE  → ClosestPeers(targetID, K)
FIND_VALUE → ClosestPeers(targetID, K) + local ProvidersFor(key)
STORE      → RecordProvider(key, fromNodeID)
```

### 3.2 Wire Format

DHT messages are wrapped in `DHTMessageBody` and JSON-marshalled into the Protobuf envelope's `payload` field:

```go
type DHTMessageBody struct {
    Type      string   `json:"type"`
    FromID    string   `json:"from_id,omitempty"`
    TargetID  string   `json:"target_id,omitempty"`
    Key       string   `json:"key,omitempty"`
    Value     string   `json:"value,omitempty"`
    Nodes     []Node   `json:"nodes,omitempty"`
    Providers []string `json:"providers,omitempty"`
    Error     string   `json:"error,omitempty"`
}
```

### 3.3 Provider Store

A `ProviderStore` maps chunk hashes / file IDs to a list of provider node IDs:

```go
type ProviderStore struct {
    mu   sync.RWMutex
    data map[string][]string  // key → []providerID
}
```

- **`Add(key, providerID)`** — appends the provider (deduped).
- **`Get(key)`** — returns all providers for a key.
- **`Remove(key, providerID)`** — removes a specific provider.

---

## 4. DHT Service — Integration Layer

The `dht.Service` struct (`internal/dht/service.go`) wires the DHT core to the TCP network. It is the backbone of the unified node.

### 4.1 Service Lifecycle

```
NewService(nodeID, listenAddr, externalAddr, seeds)
    │
    ├── Creates DHT core (dht.DHT)
    ├── Creates TCP Server (network.Server)
    ├── Creates Router (network.Router)
    └── Registers DHT message handlers on the router
    │
    ▼
svc.Start()
    │
    ├── Starts TCP server (listen loop)
    ├── Sets OnNewConnection callback:
    │     each new connection gets the router & starts readLoop
    └── Starts background bootstrapLoop (goroutine)
    │
    ▼
svc.Stop()
    ├── Cancels context
    ├── Closes all peer connections
    └── Stops TCP server
```

### 4.2 Provider Announcement

When a file is seeded, the node calls `AnnounceProvider` for the file ID and each chunk CID:

```
AnnounceProvider(key)
    │
    ├── dht.RecordProvider(key, selfID)     // local record
    └── For each connected peer:
          Send STORE message with the key
```

### 4.3 Provider Lookup

When downloading, the node calls `FindProviders` to locate seeders:

```
FindProviders(ctx, key)
    │
    ├── Check local ProviderStore
    ├── Get K closest peers from routing table
    └── Send FIND_VALUE to each closest peer
          └── Collect provider IDs + add new nodes to routing table
```

### 4.4 Bootstrap Loop

A background goroutine runs every 60 seconds (and immediately on start):

1. Connects to each bootstrap peer address.
2. Sends `FIND_NODE` for its own node ID.
3. Adds returned nodes to the routing table.

### 4.5 Transport Adapter

The `Service.Send()` method implements `dht.Transport`, allowing the DHT core to send raw messages through the TCP network without knowing about connections:

```go
func (s *Service) Send(to string, msg []byte) error
```

---

## 5. Unified Node CLI (`cmd/node/main.go`)

This is the primary entry point that integrates DHT with file transfer over TCP.

### 5.1 Flags

| Flag | Default | Description |
|---|---|---|
| `-mode` | `"seed"` | `"seed"` or `"download"` |
| `-port` | `"9000"` | Listening TCP port |
| `-file` | `""` | Path to file to seed |
| `-data` | `"./node-data"` | Data directory for chunks/manifests |
| `-chunk-kb` | `1024` | Chunk size in KB |
| `-dht` | `false` | Enable DHT peer discovery |
| `-dht-bootstrap` | `""` | Comma-separated bootstrap peer addresses (`ip:port,...`) |
| `-dht-node-id` | `""` | Node ID (random if empty) |
| `-dht-external` | `""` | Externally advertised address (`ip:port`) |

### 5.2 Seed Mode

```
./node -mode seed -port 9000 -file ./model.bin -dht -dht-bootstrap "10.0.0.1:9000,10.0.0.2:9000"
```

**What happens:**

1. Creates a `dht.Service` with a TCP server and shared router.
2. Registers file-transfer RPC handlers on the router:
   - `MSG_GET_METADATA_REQUEST` → serve manifest
   - `MSG_GET_CHUNK_REQUEST` → serve chunk data
3. Starts the DHT service (listens on TCP, bootstraps to the network).
4. Chunks `model.bin` via `filemeta.BuildManifest` + `storage.StoreModel`:
   - Splits the file into chunks (default 1 MB)
   - SHA-256 hashes each chunk
   - Saves chunks to `<data>/chunks/<cid>/<index>.chunk`
   - Saves manifest to `<data>/manifests/<cid>.json`
5. Calls `svc.AnnounceProvider(fileID)` for the file ID and each chunk CID — propagates `STORE` messages to connected peers.
6. Waits for SIGINT/SIGTERM.

### 5.3 Download Mode

```
./node -mode download -port 9001 -dht -dht-bootstrap "10.0.0.1:9000" -file <fileID>
```

**What happens:**

1. Creates a `dht.Service`, starts it, and bootstraps to the network.
2. Waits 2 seconds for initial peer discovery.
3. Calls `svc.FindProviders(ctx, fileID)` to locate seeders for the file.
4. For each found provider:
   - Opens a TCP connection (or reuses existing DHT connection).
   - Sends `MSG_GET_METADATA_REQUEST` to fetch the manifest.
   - Iterates through chunks, sending `MSG_GET_CHUNK_REQUEST` for each index.
   - Verifies each chunk's SHA-256 hash via `filemeta.VerifyChunk`.
5. Saves chunks to disk and the manifest to `<data>/manifests/`.
6. Verifies the full file hash and reassembles via `filemeta.AssembleChunks`.
7. Outputs the reassembled file path.

---

## 6. Architecture — Seeding with DHT

```
┌───────────────┐         DHT Bootstrap          ┌───────────────┐
│  Node A       │ ◄─────── FIND_NODE ───────────► │  Bootstrap    │
│  (Seeder)     │                                  │  Node(s)     │
│               │                                  └───────────────┘
│  Port: 9000   │
│  File: model  │         DHT STORE (fileID)       ┌───────────────┐
│               │ ◄──────────────────────────────► │  Node B       │
│  svc.         │                                  │  (Peer)      │
│  Announce     │                                  │               │
│  Provider     │         TCP GetChunk Request     │  Port: 9002   │
│               │ ◄─────────────────────────────── │               │
│               │                                  │  Downloads    │
│               │ ──────── TCP GetChunk Response ─►│  model.bin    │
└───────────────┘                                  └───────────────┘
```

### Protocol Multiplexing

Both DHT messages and file-transfer RPCs share a **single TCP connection** and **single router**. The `MessageType` enum distinguishes them:

| Range | Purpose |
|---|---|
| 1–6  | File-transfer RPCs (handshake, metadata, chunk) |
| 7–14 | DHT messages (ping, find_node, store, find_value) |

When a DHT peer connects, the same connection serves both DHT maintenance and file transfer. The `dht.Service` maintains a `conns` map (`peerID → *network.Connection`) and an `addrs` map (`peerID → "ip:port"`) for address resolution.

---

## 7. Message Flow — DHT + File Transfer

```
Seeder Node                          Downloader Node
    │                                      │
    │──── MSG_DHT_FIND_NODE ──────────────►│  (bootstrap discovery)
    │◄──── MSG_DHT_FIND_NODE_RESP ─────────│
    │                                      │
    │◄──── MSG_DHT_FIND_VALUE ─────────────│  (find providers for fileID)
    │──── MSG_DHT_FIND_VALUE_RESP ────────►│
    │         (includes providers)         │
    │                                      │
    │◄──── MSG_GET_METADATA_REQUEST ───────│  (fetch manifest)
    │──── MSG_GET_METADATA_RESPONSE ──────►│
    │                                      │
    │◄──── MSG_GET_CHUNK_REQUEST(idx=0) ───│  (fetch chunk 0)
    │──── MSG_GET_CHUNK_RESPONSE ─────────►│
    │◄──── MSG_GET_CHUNK_REQUEST(idx=1) ───│  (fetch chunk 1)
    │──── MSG_GET_CHUNK_RESPONSE ─────────►│
    │           ...                        │
    │                                      │
    │◄──── MSG_DHT_PING ───────────────────│  (routing table maintenance)
    │──── MSG_DHT_PONG ──────────────────►│
```

---

## 8. Running the Unified Node

### 8.1 Prerequisites

- Go 1.21+
- Network connectivity between nodes

### 8.2 Build

```bash
go build -o node ./cmd/node/
```

### 8.3 Start a Bootstrap Node

A bootstrap node is any node running with DHT enabled. It does not need a file — it just maintains the routing table.

```bash
./node -mode seed -port 9000 -dht -dht-node-id "bootstrap"
```

### 8.4 Start a Seeder Node

```bash
./node -mode seed \
  -port 9001 \
  -file /path/to/model.bin \
  -data ./seed-data \
  -chunk-kb 1024 \
  -dht \
  -dht-bootstrap "127.0.0.1:9000" \
  -dht-node-id "seeder1"
```

Expected output:

```
→ chunking model.bin (512.0 MB)...
✓ 512 chunks written
✓ manifest CID: <cid>
→ starting DHT service on :9001...
→ bootstrapping to [127.0.0.1:9000]...
✓ DHT service ready
→ announcing provider for file <fileID>...
✓ seeding with DHT enabled
```

### 8.5 Start a Downloader Node

```bash
./node -mode download \
  -port 9002 \
  -data ./download-data \
  -dht \
  -dht-bootstrap "127.0.0.1:9000" \
  -dht-node-id "downloader1" \
  -file <fileID>
```

Expected output:

```
→ starting DHT service on :9002...
→ bootstrapping to [127.0.0.1:9000]...
✓ DHT service ready
→ looking up providers for <fileID>...
✓ found 1 provider
→ downloading manifest...
✓ manifest received (512 chunks)
→ downloading chunks: 512/512 [████████████████████] 100%
✓ all chunks verified
→ assembling file...
✓ file reassembled → ./download-data/files/<fileID>/model.bin
```

### 8.6 Network Configuration

- **Bootstrap addresses** must be reachable from all nodes.
- Use `-dht-external` if nodes are behind NAT to advertise the public address.
- Multiple bootstrap peers improve redundancy.

---

## 9. How It Works — Step by Step

### 9.1 Peer Discovery

1. Node A starts and connects to the bootstrap peer(s) via TCP.
2. A sends `FIND_NODE` with its own node ID.
3. The bootstrap responds with the `K` closest nodes it knows.
4. A adds these nodes to its routing table and may connect to them.
5. Every 60 seconds, A re-bootstraps to maintain fresh routing info.

### 9.2 Content Advertisement

1. When Node A seeds a file, it calls `AnnounceProvider(fileID)`.
2. The file ID and all chunk CIDs are recorded in A's local `ProviderStore`.
3. A sends `STORE` messages for each key to all currently connected peers.
4. Peers that receive `STORE` record A as a provider in their own `ProviderStore`.

### 9.3 Content Discovery

1. Node B wants to download the file and calls `FindProviders(fileID)`.
2. B first checks its local `ProviderStore`.
3. B queries the `K` closest peers from its routing table with `FIND_VALUE`.
4. Each peer responds with any providers it knows + its closest nodes.
5. B adds discovered providers to its routing table and retries if needed.
6. B gets the provider list and connects to a seeder for file transfer.

### 9.4 File Transfer

1. B sends `MSG_GET_METADATA_REQUEST` to the seeder.
2. The seeder responds with the JSON manifest (chunk list, hashes, sizes).
3. B iterates through chunk indices, sending `MSG_GET_CHUNK_REQUEST` for each.
4. The seeder reads the chunk from disk and returns the binary data.
5. B verifies each chunk's SHA-256 hash against the manifest.
6. After all chunks are downloaded and verified, B reassembles the file.

---

## 10. File Structure

```
<data>/
├── manifests/
│   └── <cid>.json            # File manifest (metadata + chunk list)
├── chunks/
│   └── <cid>/
│       ├── 0.chunk           # Chunk data files
│       ├── 1.chunk
│       └── ...
└── files/
    └── <fileID>/
        └── <file_name>       # Reassembled file (download mode)
```

---

## 11. Design Decisions

| Decision | Rationale |
|---|---|
| **JSON for DHT messages** | Avoids protoc dependency for schema extensions; DHT messages are small and infrequent |
| **Protobuf for file RPCs** | Schema safety and smaller encoding for potentially large metadata payloads |
| **Same TCP port for DHT + file transfer** | Simpler deployment (single port), connection reuse reduces handshake overhead |
| **Simplified iterative lookup** | `FIND_VALUE` is sent to closest peers in one hop rather than full recursive Kademlia iteration — sufficient for small-to-medium networks |
| **FIFO bucket eviction** | Oldest peer is evicted first when bucket is full, preferring fresh connections |
| **Background bootstrap loop** | Maintains routing table freshness without blocking startup |

---

## 12. Comparison: Standalone HTTP Seeder vs DHT Node

| Feature | `cmd/seed` (HTTP) | `cmd/node` (DHT) |
|---|---|---|
| Protocol | HTTP/1.1 REST | Custom TCP RPC + Protobuf |
| Peer discovery | Manual (URL) | Automatic (DHT) |
| File serving | HTTP GET | RPC over TCP |
| Use case | Testing, local sharing | Production P2P networks |
| Flags | `-file`, `-port`, `-data`, `-chunk-kb` | `-mode`, `-dht`, `-dht-bootstrap`, `-dht-node-id`, `-dht-external`, + legacy |
| Startup | Chunk + serve immediately | Chunk + bootstrap DHT + announce + serve |

---

## 13. Tests

### Unit Tests

```bash
go test ./internal/dht/... -v
go test ./... -v
go test ./internal/network/... -v
```

### E2E Test (TCP seeding with DHT)

```bash
go test ./tests/ -v -run TestE2EManualSeeding
```

This test spawns a seeder and downloader using the TCP RPC layer (with DHT integration) and verifies complete file transfer integrity.
