# Security Model

This document describes what the system protects against, how, and what
it does not cover.

## Assets and assumptions

- **Integrity of downloaded files** is the main asset. A user who asks for
  a file ID must receive exactly that content, or get an error.
- Peers are untrusted. Any peer, including bootstrap nodes, may be
  malicious.
- The user obtains file IDs from a trusted source (the publisher). The
  system authenticates content *against* an ID; it does not decide which
  ID is the right one.

## Threats and mitigations

### Content tampering

| Threat | Mitigation |
|--------|------------|
| A provider serves a manifest for different content | The file ID is the manifest's SHA-256 (CID). Downloaders recompute it (`FileMeta.VerifyID`) and reject mismatches. |
| A provider serves corrupt or substituted chunks | Each chunk's size and SHA-256 are checked against the authenticated manifest before it is stored. Failing providers are backed off. |
| A corrupted file ends up at the destination | Reassembly writes to a temp file, checks the whole-file SHA-256, and renames the file into place only on success. |
| A node passes on its own on-disk corruption | Seeders check every chunk against the manifest before serving it, and only serve complete files whose manifest matches their ID. |

### Path traversal and local file safety

| Threat | Mitigation |
|--------|------------|
| A remote file ID such as `../../etc` | `storage.ValidFileID` restricts IDs to `[A-Za-z0-9_-]` in every store operation. |
| A remote `FileName` such as `../../.bashrc` used as the default output name | `ValidateFileName` rejects separators, `.`/`..`, control characters and invalid UTF-8, both in `Validate()` and again where the path is built. |
| Key material exposure | `node.key` is created with mode `0600` (`O_EXCL`), and the node refuses to load a key readable by group or others. |

### Identity and transport

| Threat | Mitigation |
|--------|------------|
| Eavesdropping or modification in transit | All traffic uses TLS 1.3 (minimum version enforced). |
| Impersonating a node ID | Node ID = SHA-256 of the node's ed25519 public key. Mutual TLS proves possession of the key. Dials to known nodes pin the expected ID. |
| Spoofed sender fields in DHT messages | Messages carry no sender ID or IP. Senders are identified by TLS identity and observed socket address. |
| Pointing other nodes at third-party hosts | Addresses come from the observed IP. Inbound peers are dialed back with their ID pinned before entering the routing table. |
| Incompatible or garbage clients | ALPN protocol versioning. Malformed frames close the connection. |

### Denial of service

| Threat | Mitigation |
|--------|------------|
| Connection floods | 512 inbound connections maximum. Connections over the limit are closed immediately. |
| Stalled handshakes or idle connections (slow-loris) | 10s handshake timeout, 2 min per-frame read timeout, 15s write timeout. |
| Oversized frames | 10 MiB frame limit on read and write. Chunks are capped at 8 MiB and manifests at 65,536 chunks. |
| Request floods on one connection | At most 32 handlers in flight per connection, with backpressure. |
| Wedging the read loop with duplicate or unsolicited responses | Responses are delivered at most once without blocking. Unsolicited responses are dropped and never routed. |
| Provider record floods | Per-key cap that refuses newcomers rather than evicting, plus per-/24-per-key, per-provider and global key limits, with TTL expiry. |
| Routing table poisoning or eclipse | Only verified nodes are added, ping-before-evict keeps long-lived nodes, and IP diversity limits apply per bucket and per table. |
| Leaking internals in errors | Peers receive only generic error messages. |

## Not covered (known limitations)

- **Sybil attacks.** Keys are cheap to generate. Per-subnet limits raise
  the cost, but an attacker with many IP ranges can still occupy routing
  and provider slots. Options for the future include proof-of-work node
  IDs (S/Kademlia) or disjoint lookup paths.
- **Availability.** An attacker can refuse to serve, or announce itself as
  a provider and then not serve, which slows downloads. Downloaders back
  off from failing providers and rediscover others, so integrity is never
  affected.
- **Bandwidth exhaustion.** Chunk serving has no rate limit.
- **NAT traversal** is not implemented. Nodes behind NAT need a port
  forward advertised with `-external`.
- **Privacy.** Peers can see which file IDs a node looks up and serves.
  File IDs are content hashes, so anyone who knows the content can tell
  who is sharing it.

## Reporting vulnerabilities

Please do not open public issues for security problems. Contact the
maintainers listed in `.github/CODEOWNERS` privately.
