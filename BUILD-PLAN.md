# free-s3 — Self-Contained Build Plan

> **Read this first.** This document is written so a fresh session with **no
> prior chat context** can build the whole project. Pair it with `RESEARCH.md`
> (same directory) which holds the verified per-provider API details. When in
> doubt about a provider's exact endpoint/fields, `RESEARCH.md` is the source of
> truth.

---

## 0. What we're building (the one-paragraph version)

An **S3-compatible HTTP gateway** (AWS SigV4 auth, bucket/object CRUD, multipart,
range GETs) whose *physical storage* is a pool of **free public file-hosting
sites**. Every object is split into **chunks**; each chunk is uploaded to
**R = 2–3 different free hosts** (replication); a local **SQLite** DB maps
`object → [chunk → [replica: provider, url]]`. GETs reassemble chunks, fetching
each from whichever replica answers. This is the same architecture as the
sibling project `../telegram-s3` (which uses Telegram as the backend) — we
**fork its skeleton and swap the backend**.

Why: free hosts cap file size and have no durability SLA. Chunking removes the
size cap; replication across independent hosts buys best-effort durability; a
keep-alive + self-heal loop repairs expiry/pruning.

**Personal-scale, best-effort. Not a real durability guarantee.** These are free
hobby hosts. The replication factor is the only dial.

---

## 1. Reference material (read these before coding)

| Path | What it is | How we use it |
|---|---|---|
| `../telegram-s3` | Go S3 gateway, Telegram backend. | **Fork it.** Reuse S3 API, SigV4, SQLite metadata, chunker, reader, multipart **verbatim**; replace only the storage backend. |
| `../studyon-openai/src/storage-providers.ts` | Production-tested TS provider chain (catbox, Chevereto, etc.). | **Port its upload mechanics to Go** — `fetchWithTimeout`, retry/backoff, `mapWithConcurrency`, browser-UA, body-shape validation, per-host quirks. |
| `./RESEARCH.md` | Verified provider catalog + exact APIs + gotchas. | The provider adapter spec. **Final provider set is the "✅ FINAL PROVIDER SET (locked)" section.** |

### Locked decisions (do not relitigate)
- **Language/stack:** Go. Fork `../telegram-s3`.
- **Durability:** chunk every object; replicate each chunk to **R distinct
  providers** (default R=3, configurable). Self-heal on read-404.
- **Accounts:** the operator will have accounts — catbox `userhash`, Pixeldrain
  API key, Internet Archive S3 keys, Gofile token. **Every credential is
  optional**; a provider with missing creds is simply skipped at startup.
- **Provider set:** only **simple adapters** — one-or-few plain HTTP calls,
  static credential or none, direct-URL response. **No** OAuth, client-side
  crypto, SDKs, or IPFS-CID hosts. (Dropped: GitHub, B2/R2, Google Drive,
  OneDrive, pCloud, Mega/Filen/Internxt, Pinata/Lighthouse/Filebase, 1fichier,
  mixdrop. See RESEARCH.md.)

### The final provider set (≈19, all simple adapters)
- **Permanent anchor:** Internet Archive (`PUT` + `Authorization: LOW key:secret`).
- **Durable hosts:** fileditch · pixeldrain · catbox · gofile · buzzheavier.
- **0x0 family** (`POST file=@` → plaintext URL): x0.at · envs.sh · ttm.sh · fars.ee · paste.c-net.org.
- **pomf family** (`POST files[]` → JSON `files[].url`): pomf.lain.la · uguu · tmp.ninja · doko.moe · cockfile.
- **Temp/overflow:** temp.sh · litterbox · tmpfiles.org · tmpfile.link · filebin.net.

---

## 2. Architecture: reuse vs. replace

```
                 ┌──────────────── REUSE from telegram-s3 (≈unchanged) ───────────────┐
S3 client ──────►│  internal/s3api  (router, SigV4, bucket/object/multipart handlers, │
  (aws-sdk)      │                   listing, range, CORS, error XML)                  │
                 │  internal/reader (parallel-prefetch range reader)                   │
                 │  internal/metadata (SQLite WAL store) — SCHEMA EDITED (§4)          │
                 │  internal/config  — ENV KEYS EDITED (§5)                            │
                 │  internal/cache   (location cache, optional)                        │
                 └─────────────────────────────────────────────────────────────────────┘
                                          │ storage.Backend interface (§3)
                 ┌──────────────── REPLACE (new code) ───────────────────────────────┐
                 │  internal/storage/freehost   ← NEW backend                          │
                 │    backend.go    : implements storage.Backend (chunk + replicate)   │
                 │    provider.go   : Provider interface + pool + selection/retry       │
                 │    providers/*.go: one file per host (catbox, fileditch, ia, 0x0…)  │
                 │    keepalive.go  : TTL-refresh + 404 self-heal sweep                 │
                 └─────────────────────────────────────────────────────────────────────┘
```

**Delete from the fork** (Telegram-specific, not needed):
`internal/storage/telegram/**`, the `mtproto` pool boot in `cmd/.../main.go`,
and the metadata `tg_sessions` + `bot_chunks_pending_delete` tables + the
legacy-chunk backfill (we start clean, no legacy rows).

**Keep the seams.** In telegram-s3 the only Telegram coupling in the handler is
through these helpers — they become the conversion layer to the new chunk model:
- `toMetaChunks(storage.Chunk) → metadata.Chunk`
- `metaChunkRef / chunkRefs → storage.ChunkRef`
- `reader.NewBotSource(backend, objSize, locs, streamChunkSize)` (rename
  `BotSource`→`ChunkSource`, but the shape is identical: it calls
  `backend.DownloadRange(ctx, ref, off, len)`).

Everything else in `s3api`/`reader` is backend-agnostic and compiles unchanged
once `storage.Chunk`/`ChunkRef` are redefined (§3).

---

## 3. The new storage interfaces (`internal/storage`)

Keep the **same `Backend` interface method set** as telegram-s3 so the handler
needs no logic changes — only redefine the data types it passes around.

```go
package storage

// Chunk = one contiguous slice of an object, replicated to R providers.
// Returned by Backend.Upload, persisted by the handler via metadata.
type Chunk struct {
    Seq      int
    Size     int64
    Offset   int64       // byte offset of this chunk within the object
    Replicas []Replica   // len == R (or fewer if some uploads failed but ≥1 ok)
}

type Replica struct {
    Provider    string // "catbox", "fileditch", "ia", "x0.at", ...
    Locator     string // the direct download URL (or provider-native id we can build a URL from)
    DeleteToken string // 0x0 X-Token / catbox needs userhash (global) / "" if none
}

// ChunkRef = locator to fetch or delete ONE chunk. It's just the replica list;
// the backend tries replicas in order (or races) until one returns bytes.
type ChunkRef struct {
    Size     int64
    Replicas []Replica
}

type Backend interface {
    // Upload splits body into chunks (≤ provider-min cap each), uploads each
    // chunk to R distinct providers concurrently, returns the ordered chunks.
    Upload(ctx context.Context, name, contentType string, body io.Reader) ([]Chunk, error)
    // Download returns a reader over one chunk's full content (first healthy replica).
    Download(ctx context.Context, ref ChunkRef) (io.ReadCloser, error)
    // DownloadRange returns [offset, offset+length) of one chunk (HTTP Range on a replica).
    DownloadRange(ctx context.Context, ref ChunkRef, offset, length int64) (io.ReadCloser, error)
    // Delete best-effort removes every replica of one chunk.
    Delete(ctx context.Context, ref ChunkRef) error
    // DeleteBatch best-effort removes every replica of many chunks (logs per-ref failures).
    DeleteBatch(ctx context.Context, refs []ChunkRef) error
}
```

### Provider interface (`internal/storage/freehost/provider.go`)

```go
type Provider interface {
    Name() string
    // MaxBytes is the per-file cap; the chunker never sends more than the
    // smallest MaxBytes among selected providers for a chunk.
    MaxBytes() int64
    // Durable distinguishes permanent/long-lived (anchor) hosts from temp ones,
    // for replica selection (always include ≥1 durable replica per chunk).
    Durable() bool
    // Upload stores the bytes, returns a direct download URL + optional delete token.
    Upload(ctx context.Context, data []byte, filename, contentType string) (locator, deleteToken string, err error)
    // Download fetches [offset, offset+length); length<=0 = to end. Uses HTTP Range.
    Download(ctx context.Context, locator string, offset, length int64) (io.ReadCloser, error)
    // Delete removes the file if the provider supports it (no-op + nil otherwise).
    Delete(ctx context.Context, locator, deleteToken string) error
}
```

### Backend.Upload algorithm (chunk + replicate)
1. Read `body` into chunks of `ChunkSize` bytes (config; default ~80 MiB so it
   fits under catbox's 200 MB and most caps). Stream to a temp file or buffer
   per chunk (reuse telegram-s3's chunking/buffer approach in `internal/reader`
   / its uploader splitter).
2. For each chunk, call `pool.PickReplicas(R, chunkSize)` → R distinct providers
   where **at least one is `Durable()`** and every pick has `MaxBytes ≥ chunkSize`.
3. Upload the chunk to those R providers **concurrently** (`mapWithConcurrency`,
   limit ~4–6) with per-provider retry/backoff (500ms→1s→2s ×3). A provider
   failure falls through to the next *unused* provider from the pool so we still
   reach R replicas when possible. Require **≥1 successful replica** (ideally the
   durable one) or the whole PUT fails (`502 FreeHostUploadFailed`).
4. Name each uploaded blob `<objkey-hash>.<seq>.bin` (the `.bin` matters —
   fileditch/filebin reject script extensions; see RESEARCH.md gotcha #5).
5. Return `[]storage.Chunk` with the replica list per chunk.

### Backend.Download / DownloadRange (read + self-heal)
- Iterate `ref.Replicas` (durable first). For each: `provider.Download(locator,
  off, len)`. First success wins.
- On a replica returning 404/410/connection-refused, mark it dead and try the
  next. If **all** replicas fail → return error (the handler surfaces 502).
- **Self-heal (can be v1.1):** when a read finds a chunk down to < R live
  replicas, enqueue a repair: re-fetch from a live replica, re-upload to a fresh
  provider, update the chunk's replica list in the DB. Keep the read path fast;
  do repair async.

### Replica selection policy (`pool.PickReplicas`)
- Maintain provider health (consecutive-failure counter; skip a provider that's
  failing). 
- Always include **≥1 durable** provider (IA / fileditch / pixeldrain / catbox /
  gofile / x0.at / pomf.lain.la). Fill the rest from durable-or-temp by
  round-robin, honoring `MaxBytes ≥ chunkSize`.
- Never put two replicas of the same chunk on the same provider.

---

## 4. Metadata schema changes (`internal/metadata/store.go`)

We start with **no legacy data**, so simplify the telegram schema rather than
keep it additive. Replace the per-chunk Telegram columns with a generic chunk +
a replica side-table. Drop `tg_sessions`, `bot_chunks_pending_delete`, the
`ensureColumn` Phase-3 ALTERs, and `backfillLegacyChunks`.

```sql
CREATE TABLE buckets ( name TEXT PRIMARY KEY, created_at TEXT NOT NULL );

CREATE TABLE objects (
  bucket TEXT NOT NULL, key TEXT NOT NULL,
  size INTEGER NOT NULL, etag TEXT NOT NULL, content_type TEXT NOT NULL,
  created_at TEXT NOT NULL, updated_at TEXT NOT NULL, deleted_at TEXT,
  PRIMARY KEY (bucket, key),
  FOREIGN KEY (bucket) REFERENCES buckets(name) ON DELETE CASCADE
);

CREATE TABLE object_chunks (
  bucket TEXT NOT NULL, key TEXT NOT NULL,
  part_seq INTEGER NOT NULL, size INTEGER NOT NULL, offset INTEGER NOT NULL,
  PRIMARY KEY (bucket, key, part_seq)
);

-- NEW: one row per (chunk, replica).
CREATE TABLE chunk_replicas (
  bucket TEXT NOT NULL, key TEXT NOT NULL, part_seq INTEGER NOT NULL,
  replica_idx INTEGER NOT NULL,
  provider TEXT NOT NULL, locator TEXT NOT NULL, delete_token TEXT NOT NULL DEFAULT '',
  alive INTEGER NOT NULL DEFAULT 1,         -- self-heal marks dead replicas 0
  PRIMARY KEY (bucket, key, part_seq, replica_idx)
);
CREATE INDEX idx_chunk_replicas_key ON chunk_replicas(bucket, key, part_seq);

-- Multipart mirrors the same shape:
CREATE TABLE multipart_uploads ( upload_id TEXT PRIMARY KEY, bucket TEXT, key TEXT, content_type TEXT, created_at TEXT );
CREATE TABLE multipart_parts ( upload_id TEXT, part_number INTEGER, etag TEXT, size INTEGER, PRIMARY KEY(upload_id, part_number) );
CREATE TABLE multipart_part_chunks ( upload_id TEXT, part_number INTEGER, seq INTEGER, size INTEGER, PRIMARY KEY(upload_id, part_number, seq) );
CREATE TABLE multipart_part_chunk_replicas ( upload_id TEXT, part_number INTEGER, seq INTEGER, replica_idx INTEGER, provider TEXT, locator TEXT, delete_token TEXT DEFAULT '', PRIMARY KEY(upload_id, part_number, seq, replica_idx) );
CREATE TABLE object_metadata ( bucket TEXT, key TEXT, name TEXT, value TEXT, PRIMARY KEY(bucket, key, name) );
CREATE TABLE multipart_upload_metadata ( upload_id TEXT, name TEXT, value TEXT, PRIMARY KEY(upload_id, name) );
```

`metadata.Chunk` becomes `{Seq, Size, Offset, Replicas []Replica}`. Update
`PutObject` / `GetObjectChunks` / `replaceObjectChunksTx` /
`PutMultipartPart` / `GetMultipartPartChunks` / `AllMultipartChunks` to read &
write the replica rows (a small JOIN / second query per chunk; or a single
`ORDER BY part_seq, replica_idx` query the loader folds into chunks). Keep WAL +
writer/reader split exactly as-is.

---

## 5. Config / env (`internal/config/config.go`)

Drop all `TELEGRAM_*`. Keep `LISTEN_ADDR`, `DATABASE_PATH`, `S3_ACCESS_KEY_ID`,
`S3_SECRET_ACCESS_KEY`, `PUBLIC_ENDPOINT_URL`, the multipart janitor knobs, the
stream/reader knobs. Add:

```
REPLICATION_FACTOR=3            # R
CHUNK_SIZE=80MiB                # per-chunk window (under catbox 200MB)
FREEHOST_PROVIDERS=ia,fileditch,catbox,pixeldrain,x0.at,pomf.lain.la,gofile,...
                                # which providers are enabled (in priority order)
UPLOAD_CONCURRENCY=6            # parallel chunk-replica uploads
KEEPALIVE_INTERVAL=24h         # TTL-refresh / self-heal sweep cadence (0=off)

# Per-provider credentials (ALL optional; a provider w/ missing required creds is skipped):
CATBOX_USERHASH=...            # REQUIRED for catbox from a VPS (else 412)
PIXELDRAIN_API_KEY=...
IA_ACCESS_KEY=...   IA_SECRET_KEY=...   # archive.org/account/s3.php
GOFILE_TOKEN=...               # optional (guest works but token = durable + direct links)
```

Validation: at least one **durable** provider must be enabled & credentialed, or
fail fast. Log the resolved provider list + which were skipped (missing creds).

---

## 6. Provider adapters — port from studyon-openai

Each provider is ~40–80 lines in `internal/storage/freehost/providers/<name>.go`.
Port these shared helpers from `storage-providers.ts` to Go first
(`internal/storage/freehost/httputil.go`):
- `fetchWithTimeout` → `http.Client` with per-call `context.WithTimeout`.
- Browser `User-Agent` on **every** request (const `BrowserUA`).
- Retry with exponential backoff (500ms→1s→2s, 3 attempts).
- Body-shape validation (don't trust HTTP 200 alone).

Exact per-provider request/response details are in **RESEARCH.md → "Verified API
details"**. Summary of the upload shapes:

| Provider | Upload | Direct URL out | Delete |
|---|---|---|---|
| **ia** | `PUT s3.us.archive.org/{item}/{file}` + `Authorization: LOW k:s`, `x-archive-queue-derive:0` | `archive.org/download/{item}/{file}` (deterministic) | DELETE (same auth) |
| **fileditch** | `POST new.fileditch.com/upload.php` field `file` | JSON `.url` | none |
| **catbox** | `POST catbox.moe/user/api.php` `reqtype=fileupload`+`fileToUpload`+`userhash` | plaintext body | `reqtype=deletefiles`+`userhash`+`files=` |
| **pixeldrain** | `POST /api/file` field `file`, Basic-auth key in password | `GET /api/file/{id}` (build from `.id`) | `DELETE /api/file/{id}` |
| **gofile** | `GET api.gofile.io/servers` → `POST {server}/contents/uploadfile` field `file`+token | `directlinks` API or `download/{id}` | `DELETE /contents/{id}` |
| **buzzheavier** | `PUT w.buzzheavier.com/{name}` raw body | response link | — |
| **0x0 family** (x0.at, envs.sh, ttm.sh) | `POST HOST/` field `file=@` | plaintext body; keep `X-Token` | `POST url` `token=`+`delete=` |
| **fars.ee** | `POST fars.ee/` field `c=@` | plaintext body | UUID-based |
| **paste.c-net.org** | `PUT paste.c-net.org/` raw body | plaintext body | — |
| **pomf family** (pomf.lain.la, doko.moe, cockfile) | `POST HOST/upload.php` field `files[]` | JSON `.files[0].url` | — |
| **uguu / tmp.ninja** | `POST HOST/upload` field `files[]` | JSON `.files[0].url` | — |
| **temp.sh** | `POST temp.sh/upload` field `file` | plaintext body | — |
| **litterbox** | `POST litterbox.catbox.moe/resources/internals/api.php` `reqtype=fileupload`+`time=72h`+`fileToUpload` | plaintext body | — |
| **tmpfiles** | `POST tmpfiles.org/api/v1/upload` field `file` | JSON `.data.url` → rewrite to `/dl/{id}/{name}` | — |
| **tmpfile.link** | `POST tmpfile.link/api/upload` field `file` | JSON `.downloadLink` | — |
| **filebin** | `POST filebin.net/{bin}/{file}.bin` (`Content-Length`) | `GET` same URL (302→presigned) | `DELETE /{bin}` |

**Range reads:** most hosts honor `Range:` headers on their direct URLs (CDN/S3
backed). For any that don't, `Download` falls back to fetching the whole chunk
and slicing — acceptable because chunks are ≤ CHUNK_SIZE. Detect per provider;
default to trusting `Range` and verify in the acceptance test.

---

## 7. Build phases (checklist — commit after each)

- [ ] **P0 — Fork & strip.** Copy `../telegram-s3` → `free-s3/` (keep `.git`?
      start fresh `git init`). Rename module `telegram-s3` → `free-s3` in
      `go.mod` + imports. Delete `internal/storage/telegram/**`. Make it compile
      with a **stub backend** (returns `not implemented`). `go build ./...` green.
- [ ] **P1 — Metadata reshape.** Apply §4 schema. Update `metadata.Chunk` +
      all chunk read/write funcs to the replica model. Drop tg tables + backfill.
      Unit-test PutObject/GetObjectChunks round-trip with fake replicas.
- [ ] **P2 — Storage types + conversions.** Redefine `storage.Chunk`/`ChunkRef`
      (§3). Fix handler helpers (`toMetaChunks`, `metaChunkRef`, `chunkRefs`) and
      `reader.NewBotSource`→`ChunkSource`. `go build ./...` green; s3api tests that
      don't need a live backend pass.
- [ ] **P3 — httputil + 3 providers.** Port `httputil.go` (UA, timeout, retry).
      Implement **fileditch** (no auth), **catbox** (userhash), **x0.at**
      (plaintext+token). Live smoke test each: upload 1 MB, GET it back, byte-compare.
- [ ] **P4 — freehost Backend.** Implement chunk+replicate Upload, replica-
      fallback Download/DownloadRange, Delete/DeleteBatch, the Provider pool +
      `PickReplicas`. Wire into `cmd/free-s3/main.go` (replace the mtproto boot).
      End-to-end: `aws s3 cp` a 200 MB file up, `cp` back, `cmp` identical.
- [ ] **P5 — Internet Archive + remaining providers.** Add IA (anchor),
      pixeldrain, gofile, buzzheavier, the rest of 0x0/pomf/temp tiers. Config-
      gate by `FREEHOST_PROVIDERS`. Verify replica spread in the DB.
- [ ] **P6 — Range + multipart.** Confirm range GETs work through replicas
      (reuse telegram-s3's reader + range tests). Confirm S3 multipart upload of a
      multi-GB object lands chunks+replicas correctly.
- [ ] **P7 — Keep-alive + self-heal.** `keepalive.go`: periodic sweep that
      re-reads a sample of chunks, refreshes TTL hosts, and on a dead replica
      re-uploads to refill R. On read-path 404, mark replica dead + enqueue repair.
- [ ] **P8 — Acceptance.** Run the S3 conformance suite (telegram-s3 ships
      `scripts/acceptance.sh` + uses `../s3-tests`). Document pass/fail in a
      `S3-COMPAT.md`. Ship `.env.example`, `README.md`, `Dockerfile`.

---

## 8. Testing

- **Per-provider live smoke test** (`providers/<name>_live_test.go`, behind a
  `-tags=live` build tag + env creds): upload random bytes, GET full, GET a
  range, byte-compare, delete. **Run from the actual deploy egress IP** — VPS-IP
  blocking is the #1 failure mode (catbox 412, 0x0.st refuses DC IPs; see
  RESEARCH.md gotcha #2).
- **Backend unit tests** with a fake in-memory Provider: verify chunking
  boundaries, replica count = R, ≥1 durable replica, fallback-on-failure,
  delete-all-replicas.
- **S3 conformance:** reuse telegram-s3's `scripts/acceptance.sh`,
  `scripts/run-s3-tests.ps1`, and the `../s3-tests` suite. Same surface, so most
  should pass unchanged.
- **End-to-end:** `aws --endpoint-url http://localhost:9000 s3 cp` round-trips of
  1 KB / 80 MB (1 chunk) / 500 MB (multi-chunk) / multipart; `cmp` the bytes.

---

## 9. Gotchas (full list in RESEARCH.md §"Universal gotchas")

1. **Browser `User-Agent` on every request** (403/418/1010 otherwise).
2. **VPS-IP blocking** is the #1 failure mode — smoke-test from the real egress
   IP. catbox needs `userhash`; 0x0.st is unusable from DC IPs (it's temp-tier
   anyway). Friendly: IA, fileditch, pixeldrain, gofile, buzzheavier, x0.at.
3. **Validate response body shape**, not just HTTP status (many return 200 + text error).
4. **Inactivity pruning** (pixeldrain-free ~90d, gofile, buzzheavier) → keep-alive
   re-reads; keep such hosts as secondary replicas behind a permanent anchor.
5. **Extension blocklists** (fileditch, filebin) → name chunk blobs `*.bin`.
6. **Internet Archive:** send `x-archive-queue-derive:0` (skip transcode),
   honor `?check_limit=1` (avoid 503), items ≤10 GB / <100 files → shard items by
   object or date. Opaque encrypted blobs risk "darking" — it's an anchor, not
   the sole copy.
7. Reuse studyon-openai's retry/backoff + bounded concurrency verbatim.

---

## 10. Definition of done

`aws s3` (cp/sync/ls/rm, multipart, range) works against the gateway; objects of
arbitrary size round-trip byte-identical; each chunk has ≥R replicas across
distinct hosts with ≥1 durable; killing any one provider's copies still serves
reads; keep-alive refills replicas; the S3 conformance suite passes at parity
with telegram-s3. `RESEARCH.md` + this file fully describe the system.
```
