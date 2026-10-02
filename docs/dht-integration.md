# DHT Peer Discovery & the Node

`internal/dht` implements Kademlia-style peer discovery. `internal/node`
combines it with storage and file transfer into a running peer.

## 1. Identity and distance

- **Node ID**: `hex(SHA-256(ed25519 public key))` (see `internal/identity`).
  IDs are bound to keys and authenticated on every connection by mutual
  TLS.
- **Keys**: the values looked up in the DHT are file IDs (manifest CIDs).
- **Distance**: `SHA-256(a) XOR SHA-256(b)`, compared as a 256-bit
  big-endian number. Hashing both sides spreads IDs and keys uniformly
  over the same space.

## 2. Routing table

There are 256 k-buckets with `K = 20` entries each, ordered from least to
most recently seen.

- **Only verified peers.** A node enters the table only once its ID and
  address have been checked:
  - If we dialed it (ID pinned in TLS), the dialed address is verified.
  - If it connected to us, we take the IP we observe on the socket and the
    listen port it advertises, dial that address back with its ID pinned,
    and add it only if it answers a PING (`verifyInbound`, at most 16 in
    parallel).
  - Nodes listed in other peers' responses are only lookup candidates
    until we have queried them ourselves.
- **Ping before evict.** When a bucket is full, the newcomer is not
  inserted. Instead the bucket's oldest node is pinged and replaced only if
  it fails to answer (`AddNode` → `BucketFull` → `Replace`/`Touch`). This
  favours long-lived nodes and makes eclipse attacks much harder.
- **IP diversity.** At most 2 nodes per /24 (IPv4) or /48 (IPv6) per
  bucket and 10 per table. Loopback, private and link-local addresses are
  exempt.

## 3. Wire protocol

Every DHT request carries a `DHTRequest {listen_port, key}` and every
response a `DHTResponse {listen_port, closer_peers, providers}` (see
[rpc-layer.md](rpc-layer.md)).

| RPC | Handler behaviour |
|-----|-------------------|
| `PING` | liveness check |
| `FIND_NODE key` | return the K closest verified nodes to `key` |
| `FIND_VALUE key` | as `FIND_NODE`, plus known providers of `key` (with addresses) |
| `ADD_PROVIDER key` | record the **sender** as a provider of `key`, at the IP observed on the connection and its advertised listen port |

No message carries the sender's ID or IP, so a peer cannot speak for
anyone else or point others at arbitrary hosts. When a node lists itself as
a provider it uses the local IP the asking peer reached it on, or
`-external` if that is set. Every peer received over the wire is validated
(ID format, IP, port) before use.

## 4. Iterative lookup (`lookup.go`)

1. Seed a shortlist with the K closest nodes from the routing table.
2. Query the α = 3 closest unqueried nodes among the K closest live
   candidates, in parallel, each with a 5s timeout. Each candidate is
   dialed with its ID pinned.
3. Merge the returned `closer_peers` into the shortlist (at most K per
   response, shortlist bounded at 4K). Nodes that fail to answer are
   dropped; nodes that answer enter the routing table.
4. Repeat until the K closest live candidates have all been queried. A
   `FIND_VALUE` lookup also stops early once it has found K providers.

`FindProviders(ctx, key)` runs a `FIND_VALUE` lookup and returns provider
nodes with addresses, never including the local node.
`AnnounceProvider(ctx, key)` finds the K nodes closest to `key` and sends
each of them `ADD_PROVIDER`.

## 5. Provider records (`store.go`)

- Records expire after `ProviderTTL` (30 min). Expired records are pruned
  when a key is accessed and by a periodic `Sweep`.
- At most K providers per key. When a key is full, **new providers are
  refused** rather than evicting existing ones, so floods cannot push out
  providers that keep re-announcing.
- At most 2 providers per public /24 per key, at most 1,024 keys per
  provider, and at most 100,000 keys in total.

## 6. Connection pool (`pool.go`)

- Connections are keyed by authenticated node ID and removed as soon as
  they close.
- Concurrent dials to the same peer or address are merged into one dial.
- Dials pin the expected ID whenever it is known and are bounded by the
  TLS handshake timeout.
- At most 256 pooled connections. Inbound connections are pooled too, so
  their peers can be reached without a new dial.

## 7. Joining and maintenance

`Service.Start` starts a maintenance loop:

1. **Join**: dial each bootstrap address, send `FIND_NODE(self)`, then run
   a full self-lookup to fill the routing table. If the table is still
   empty, retry with exponential backoff (1s up to 1 min).
2. **Every 10 minutes**: sweep expired provider records, then run a
   self-lookup and a random-ID lookup to keep the routing table fresh. If
   the table has emptied, rejoin through the bootstrap peers.

## 8. The node (`internal/node`)

`node.Node` runs a full peer:

- the DHT service, with `transfer.Server` handlers on its router so DHT
  and file transfer share each connection;
- `AddFile(path, chunkSize)` imports a file and seeds it;
- `Download(ctx, id, out, opts)` downloads a file and then **seeds it**,
  so every downloader becomes a provider;
- on start, it seeds every complete file in the data directory;
- an announce loop announces seeded files when the node first joins the
  network, as soon as a file is added, and again every
  `RepublishInterval` (10 min, well inside the 30 min TTL).

The CLI (`cmd/node`) is a thin wrapper around `node.Node`. See the README
for its flags.

## 9. End-to-end flow

```
Seeder S                         DHT nodes                      Downloader D
   │ AddFile → CID                    │                               │
   │── lookup(CID) ──────────────────►│                               │
   │── ADD_PROVIDER(CID) to K closest►│ record S @ observed ip:port    │
   │                                  │◄── lookup FIND_VALUE(CID) ────│
   │                                  │──── providers [S @ ip:port] ──►│
   │◄──────────── TLS dial (S's ID pinned) ───────────────────────────│
   │◄──────────── GET_METADATA(CID) ──────────────────────────────────│ VerifyID(CID)
   │◄──────────── GET_CHUNK(i)... (spread across all providers) ──────│ verify each chunk
   │                                  │◄── ADD_PROVIDER(CID) ─────────│ D now seeds too
```
