# Seeder CLI — Testing Guide

This document covers how to build, run, and manually test the `cmd/seed` CLI end-to-end.

---

## Prerequisites

- Go 1.21+
- `curl`, `xxd`, `sha256sum` (standard on Linux/macOS)

---

## 1. Build

From the repository root:

```bash
go build -o seed ./cmd/seed/
```

Verify it compiled:

```bash
./seed --help
```

Expected output:

```
Usage of ./seed:
  -chunk-kb int
        chunk size in KB (default 1MB) (default 1024)
  -data string
        base directory for chunks and manifests (default "./data")
  -file string
        path to file to seed (omit to seed existing data only)
  -port string
        port to listen on (default "8080")
```

---

## 2. Create a test file

The seeder works with any binary file. For testing, generate a small random file:

```bash
dd if=/dev/urandom of=/tmp/test_model.bin bs=1M count=3
```

This produces a 3 MB file (6 chunks at the default 512 KB size used below).

---

## 3. Chunk a file and start the server

```bash
./seed \
  -file /tmp/test_model.bin \
  -data /tmp/seed-test-data \
  -chunk-kb 512 \
  -port 9191
```

Expected terminal output before the server blocks:

```
→ chunking test_model.bin (3.0 MB)...
✓ 6 chunks written
✓ manifest CID: <cid>
✓ manifest saved → /tmp/seed-test-data/manifests/<cid>.json

┌─────────────────────────────────────────┐
│  seeder running on :9191                 │
├─────────────────────────────────────────┤
│  manifests  /tmp/seed-test-data/manifests│
│  chunks     /tmp/seed-test-data/chunks  │
├─────────────────────────────────────────┤
│  GET /manifest/{cid}                    │
│  GET /chunk/{cid}/{index}               │
│  GET /health                            │
└─────────────────────────────────────────┘
```

Note the `<cid>` printed — you will need it for the endpoint tests below. Leave this terminal running and open a second one.

---

## 4. Test the HTTP endpoints

Set the CID from the startup output:

```bash
CID=<paste cid here>
```

### 4.1 Health check

```bash
curl -s http://localhost:9191/health
```

Expected:

```json
{"status":"ok","manifests":1}
```

### 4.2 Fetch the manifest

```bash
curl -s http://localhost:9191/manifest/$CID | python3 -m json.tool
```

Expected: a JSON object with `file_name`, `file_size`, `num_chunks`, `chunk_size`, and a `chunks` array where each entry has `index`, `cid`, `hash`, and `size`.

### 4.3 Fetch a valid chunk

```bash
curl -s http://localhost:9191/chunk/$CID/0 | wc -c
```

Expected: `524288` (512 KB).

### 4.4 Fetch the last chunk

```bash
curl -s -o /dev/null -w "HTTP %{http_code}, %{size_download} bytes\n" \
  http://localhost:9191/chunk/$CID/5
```

Expected: `HTTP 200, 524288 bytes`.

### 4.5 Out-of-range chunk index (error case)

```bash
curl -s -o /dev/null -w "HTTP %{http_code}\n" \
  http://localhost:9191/chunk/$CID/99
```

Expected: `HTTP 404`.

### 4.6 Unknown CID (error case)

```bash
curl -s -o /dev/null -w "HTTP %{http_code}\n" \
  http://localhost:9191/manifest/doesnotexist
```

Expected: `HTTP 404`.

---

## 5. Integrity check — reassemble and verify

Download all chunks in order, concatenate them, and compare the SHA256 against the original file:

```bash
for i in 0 1 2 3 4 5; do
  curl -sf http://localhost:9191/chunk/$CID/$i
done > /tmp/reassembled.bin

sha256sum /tmp/test_model.bin
sha256sum /tmp/reassembled.bin
```

Both hashes must be identical. The manifest's `model_hash` field (prefixed with `sha256:`) contains the same digest.

---

## 6. Serve-existing mode (no `-file` flag)

Stop the server (`Ctrl+C`), then restart it pointing at the same data directory but without `-file`:

```bash
./seed -data /tmp/seed-test-data -port 9192
```

The server loads the previously written manifests and chunks from disk. Confirm with:

```bash
curl -s http://localhost:9192/health
# {"status":"ok","manifests":1}

curl -s -o /dev/null -w "HTTP %{http_code}\n" \
  http://localhost:9192/manifest/$CID
# HTTP 200
```

---

## 7. Flags reference

| Flag | Default | Description |
|---|---|---|
| `-file` | _(empty)_ | Path to the file to chunk and seed. Omit to serve existing data. |
| `-data` | `./data` | Root directory for `chunks/` and `manifests/` subdirectories. |
| `-port` | `8080` | TCP port the HTTP server listens on. |
| `-chunk-kb` | `1024` | Chunk size in KB. Smaller values produce more chunks. |

---

## 8. Cleanup

```bash
kill $(pgrep -f "./seed")
rm -rf /tmp/seed-test-data /tmp/test_model.bin /tmp/reassembled.bin ./seed
```
