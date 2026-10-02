![ieeecs-template-header](https://github.com/user-attachments/assets/c3c40c85-51a2-4a5e-82a4-c32a0223e336)

<h1 align="center">P2P Model Distribution</h1>

<h4 align="center">Peer-to-peer distribution of large ML model files over an authenticated Kademlia DHT.</h4>

---

## Overview

Model weights are large, and pulling them all from a single server is slow
and expensive. This project spreads the load across peers: a model is
split into verified chunks, every node that has a file serves it, and nodes
find each other through a distributed hash table (DHT), with no central
tracker.

- **Content-addressed files.** A file's ID is the SHA-256 of its manifest.
  The ID alone is enough to check that what you received is exactly what
  you asked for, whichever peers served it.
- **Authenticated peers.** Every connection is mutual TLS 1.3. A node's ID
  is derived from its public key, so peers cannot impersonate each other.
- **Swarming.** Downloads pull chunks from every available provider in
  parallel. When a download finishes, the node seeds the file as well.
- **Resumable.** Interrupted downloads continue from the verified chunks
  already on disk.

---

## Architecture Overview

```
 ┌──────────────────────────── cmd/node (CLI) ──────────────────────────────┐
 │                               internal/node                              │
 │      ┌────────────┐   ┌───────────────┐   ┌────────────┐   ┌──────────┐  │
 │      │ downloader │   │ transfer      │   │ dht        │   │ storage  │  │
 │      │ (fetch)    │   │ (serve files) │   │ (discover) │   │ (disk)   │  │
 │      └─────┬──────┘   └──────┬────────┘   └─────┬──────┘   └────┬─────┘  │
 │            └──────────┬──────┴──────────────────┘          filemeta      │
 │                   network (mutual TLS 1.3, framed protobuf RPC)          │
 │                   identity (ed25519 key → node ID, certificates)         │
 └──────────────────────────────────────────────────────────────────────────┘
```

1. **Seeding.** `storage` splits the file into chunks, `filemeta` builds a
   manifest (chunk hashes plus the whole-file hash), and the manifest's
   SHA-256 becomes the file ID. The node announces itself as a provider of
   that ID to the 20 DHT nodes closest to it, and republishes every 10
   minutes.
2. **Discovery.** A downloader runs an iterative Kademlia lookup for the
   file ID and gets back the providers' addresses.
3. **Transfer.** The downloader fetches the manifest and checks that it
   hashes to the file ID. It then fetches chunks in parallel, spreading
   requests across providers, checks each chunk against the manifest, and
   reassembles the file. The file only appears at its destination after
   the whole-file hash matches.
4. **Seeding again.** The downloader announces itself as a provider too.

See [`docs/`](docs) for details:

- [Network and RPC layer](docs/rpc-layer.md)
- [Manifests and storage](docs/filemeta-storage.md)
- [DHT and the node](docs/dht-integration.md)
- [Security model](docs/security.md)

---

## Tech Stack

| Layer          | Technology                                          |
|----------------|-----------------------------------------------------|
| Language       | Go (standard library only, plus protobuf)           |
| Wire format    | Protocol Buffers over length-prefixed frames        |
| Transport      | TCP + mutual TLS 1.3 (self-signed ed25519 certs)    |
| Discovery      | Kademlia-style DHT (K=20, α=3)                      |
| Integrity      | SHA-256 per chunk, per file, and per manifest (CID) |
| CI             | GitHub Actions: `go vet`, build, `go test -race`    |

---

## Project Structure

```bash
cmd/node/            # CLI entry point
internal/identity/   # node key, node ID derivation, mutual TLS configs
internal/network/    # TLS server/dialer, framing, request/response multiplexing, router
internal/protocol/   # generated protobuf code (from proto/p2p.proto)
internal/dht/        # routing table, iterative lookups, provider records, connection pool
internal/filemeta/   # manifests, CIDs, validation, chunk/file verification, reassembly
internal/storage/    # on-disk layout, atomic writes, content-addressed import
internal/transfer/   # serves manifests and verified chunks to peers
internal/downloader/ # parallel, multi-provider, resumable downloads
internal/node/       # ties everything together; announces and republishes files
proto/               # protobuf schema
tests/               # end-to-end tests
docs/                # design documentation
```

---

## ⚙️ Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/IEEECS-VIT/p2p-model-distribution.git
cd p2p-model-distribution
```

### 2. Build

Requires Go (see `go.mod` for the version).

```bash
make build          # produces ./bin/node
```

### 3. Run a Small Network

```bash
# 1. A bootstrap node (any node can act as one)
./bin/node -data ./boot -listen 0.0.0.0:9000

# 2. Seed a model; note the printed File ID
./bin/node -data ./seeder -listen 0.0.0.0:9001 -bootstrap <boot-ip>:9000 -seed ./model.safetensors

# 3. Download it from another machine; the node keeps seeding afterwards
./bin/node -data ./peer -listen 0.0.0.0:9002 -bootstrap <boot-ip>:9000 \
  -download <file-id> -out ./model.safetensors
```

Each node creates `<data>/node.key` (mode `0600`) on first start. That key
is the node's identity, so keep it private and keep it across restarts. A
node seeds every complete file in its data directory when it starts.

### CLI Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-data` | `./node-data` | Node key, chunks and manifests |
| `-listen` | `:9000` | Address to accept peers on |
| `-external` | | Address to advertise if it differs from the listen address (e.g. a port forward) |
| `-bootstrap` | | Comma-separated `ip:port` list of peers to join through |
| `-seed` | | Import this file and seed it |
| `-chunk-kb` | `1024` | Chunk size for `-seed`, 1 to 8192 KiB |
| `-download` | | File ID to download |
| `-peer` | | Comma-separated provider addresses to also download from directly |
| `-out` | OS temp dir | Where to write the downloaded file |
| `-exit-after-download` | `false` | Exit instead of seeding after the download |
| `-timeout` | `0` (none) | Give up on the download after this long |
| `-log-level` | `info` | `debug`, `info`, `warn` or `error` |

Colour output is disabled when stdout is not a terminal or `NO_COLOR` is set.

---

## Git Hooks Setup

This repository uses Git hooks to enforce commit message format and block
direct pushes to `main`. After cloning, run once:

```bash
make setup
```

---

## Testing

```bash
make test           # go vet + go test -race ./...
```

The suite includes adversarial tests: substituted manifests, path
traversal file names, impersonated node IDs, peers that send duplicate
responses or malformed frames, unreachable advertised addresses, and
provider floods. It also has multi-hop lookups and a swarm test in which a
file is downloaded from a former downloader after the original seeder has
gone offline.

To regenerate the protobuf code after editing `proto/p2p.proto`:

```bash
make proto          # requires protoc and protoc-gen-go
```

---

## Known Limitations

- **NAT traversal is not implemented.** Nodes must be directly reachable,
  or have a port forward configured and advertised with `-external`.
- **Sybil resistance is limited** to per-subnet caps in the routing table
  and provider records. Node IDs come from keys, but generating keys is
  cheap.
- **No bandwidth limiting** on chunk serving.

---

## Project Status

- 🟢 In Development

---

## License

See [LICENSE](LICENSE).
