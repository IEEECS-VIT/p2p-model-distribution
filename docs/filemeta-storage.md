# Manifests & Storage

`internal/filemeta` defines manifests and does all hashing and
verification. `internal/storage` owns the on-disk layout.

## 1. Content-addressed manifests (v2)

```go
type ChunkMeta struct {
    Index int    `json:"index"`
    Hash  string `json:"hash"`  // "sha256:<64 hex>"
    Size  int    `json:"size"`
}

type FileMeta struct {
    FileID    string      `json:"file_id"`    // = manifest CID
    FileName  string      `json:"file_name"`  // unauthenticated metadata
    FileSize  int64       `json:"file_size"`
    ModelHash string      `json:"model_hash"` // SHA-256 of the whole file
    ChunkSize int         `json:"chunk_size"`
    NumChunks int         `json:"num_chunks"`
    Version   string      `json:"version"`    // "v2"
    CreatedAt int64       `json:"created_at"`
    Chunks    []ChunkMeta `json:"chunks"`
}
```

**The file ID is the manifest CID**: the hex SHA-256 of the canonical JSON
encoding of the content fields (`file_size`, `model_hash`, `chunk_size`,
`num_chunks`, `version`, `chunks`). It excludes `FileID` (which is the CID
itself), `FileName` and `CreatedAt`, so identical content always gets the
same ID.

A downloader asks for a file by ID. When a manifest arrives from a peer,
`FileMeta.VerifyID(id)` recomputes the CID. A match proves the manifest is
the one requested, and therefore that every chunk hash and the whole-file
hash inside it are authentic, no matter which peer served it.

### Validation

`FileMeta.Validate()` must run on every manifest from the network. It
rejects:

- unsupported versions;
- unsafe file names (path separators, `.`/`..`, control characters,
  invalid UTF-8, names over 255 bytes; see `ValidateFileName`). `FileName`
  is not covered by the CID, so it is validated before it is ever used to
  build a path;
- malformed hashes;
- `chunk_size` outside 1 KiB to 8 MiB, which keeps every chunk inside one
  network frame;
- more than `MaxChunks` (65,536) chunks, which keeps the manifest itself
  inside one frame;
- chunk indices out of order, and chunk sizes inconsistent with
  `file_size` (every chunk is `chunk_size` bytes except the last).

## 2. Building manifests

`BuildManifestWithChunkWriter` streams the source once. It hashes each
chunk and the whole file at the same time, and passes each chunk to a
writer callback. It fails if the source changes size while being read.

## 3. Verification and reassembly

- `VerifyChunk(data, hash)` checks one chunk. Downloaders also check the
  chunk length against the manifest.
- `VerifyAllChunks(dir, chunks)` hashes stored chunks in parallel and
  returns the missing or corrupt indices. It is used to resume downloads.
- `AssembleChunks(dir, out, chunks, modelHash)` concatenates the chunks
  into a temporary file next to `out` while hashing. It fsyncs the file,
  checks the whole-file hash, and only then renames it to `out`.
  **`out` never holds a partial or unverified file.**

## 4. On-disk layout (`storage`)

```
<data>/
├── node.key                 # node identity (0600)
├── .staging/                # in-progress imports (never a valid file ID)
└── <file-id>/               # file-id = manifest CID (64 hex chars)
    ├── manifest.json        # written last: marks the file complete
    └── chunks/
        ├── 0.chunk
        └── 1.chunk ...
```

- **File IDs from the network are untrusted.** Every `Store` method that
  takes a file ID checks `ValidFileID` (only `[A-Za-z0-9_-]`, at most 256
  characters), so a peer cannot escape the data directory.
- **Atomic writes.** Chunks and manifests are written to a temporary file,
  fsynced, then renamed into place.
- **Importing** (`StoreModel`): the file ID is only known once the whole
  file has been read, so chunks are written under `.staging/` and the
  directory is renamed to `<file-id>/` at the end. Importing identical
  content again is a no-op.
- **Completeness**: the manifest is always written after every chunk is in
  place. `HasCompleteFile(id)` (a manifest exists and its CID matches the
  directory name) is therefore the marker of a complete file.
  `CompleteFiles()` lists them; partial downloads are skipped.

## 5. Flows

**Seeding** (`node.AddFile` → `Store.StoreModel`): stream the source,
write chunks to staging, compute the CID, move the chunks into
`<cid>/chunks`, write the manifest, start announcing.

**Downloading** (`downloader.Download`):

1. Find providers.
2. Fetch the manifest, then `Validate` and `VerifyID` it.
3. Check chunks already on disk and keep the valid ones (resume).
4. Fetch the missing chunks in parallel from multiple providers, checking
   each chunk's size and hash before writing it.
5. Save the manifest, which marks the file complete.
6. `AssembleChunks` to the output path.
7. Start seeding.

**Serving** (`transfer.Server`): only complete files are served, and every
chunk is checked against the manifest before it is sent, so local
corruption is never passed on to other peers.
