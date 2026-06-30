# free-s3

An **S3-compatible storage gateway** (AWS SigV4 auth, bucket/object CRUD,
multipart, range GETs) whose physical backend is a pool of **free public
file-hosting sites**. Every object is split into chunks; each chunk is
replicated to **R distinct free hosts**; a local **SQLite** DB maps
`object → [chunk → [replica: provider, locator]]`. GETs reassemble chunks,
fetching each from whichever replica answers first.

> **Personal-scale, best-effort durability.** These are free hobby hosts with no
> SLA. Chunking removes per-host size caps; replication across independent hosts
> buys best-effort durability; a keep-alive + self-heal loop repairs
> expiry/pruning. The replication factor **R** is the only durability dial.

It forks the S3 surface of the sibling project `telegram-s3` (SigV4, SQLite
metadata, chunker, parallel range-reader, multipart) and replaces **only** the
storage backend.

## How it works

```
S3 client (aws-sdk / rclone / restic / boto3)
        │  SigV4, path- or vhost-style
        ▼
  internal/s3api  ── router, auth, bucket/object/multipart, range, CORS, error XML
        │  storage.Backend
        ▼
  internal/storage/freehost  ── chunk + replicate
        ├─ pool      : provider health + replica selection (>=1 durable, round-robin)
        ├─ backend   : Upload (chunk→R providers), Download (first healthy replica),
        │              Delete (all replicas), RepairChunk (self-heal)
        └─ providers : ia, fileditch, pixeldrain, catbox, gofile, buzzheavier,
                       x0.at/envs.sh/ttm.sh, pomf.lain.la/uguu/tmp.ninja/
                       doko.moe/cockfile, temp.sh/litterbox/tmpfiles/tmpfile.link,
                       paste.c-net.org, filebin.net
  internal/keepalive ── periodic TTL-refresh + self-heal sweep
  internal/metadata  ── SQLite (WAL): objects, object_chunks, chunk_replicas, multipart
```

- **Upload**: the body is split into `CHUNK_SIZE` windows; each chunk is uploaded
  to R distinct providers concurrently (durable-first), falling through to fresh
  providers on failure. ≥1 successful replica is required or the PUT fails 502.
- **Download / Range**: a parallel-prefetch reader fetches each chunk from the
  first healthy replica (HTTP Range, with client-side slicing fallback for hosts
  that ignore `Range`).
- **Self-heal**: the keep-alive sweep re-reads a rotating sample of chunks
  (resetting access-extended TTLs) and refills any chunk below R from a survivor.

## Quick start

```bash
cp .env.example .env          # fill in S3 creds + provider credentials
set -a; . ./.env; set +a
go run ./cmd/free-s3          # listens on :9000
```

Then point any S3 client at it (path-style):

```bash
export AWS_ACCESS_KEY_ID=free-s3-local AWS_SECRET_ACCESS_KEY=...  # from .env
aws --endpoint-url http://localhost:9000 s3 mb s3://my-bucket
aws --endpoint-url http://localhost:9000 s3 cp ./bigfile.iso s3://my-bucket/bigfile.iso
aws --endpoint-url http://localhost:9000 s3 cp s3://my-bucket/bigfile.iso ./out.iso
cmp ./bigfile.iso ./out.iso   # byte-identical
```

### Docker

```bash
docker build -t free-s3 .
docker run -p 9000:9000 --env-file .env -v "$PWD/data:/app/data" free-s3
```

## Configuration

See [`.env.example`](.env.example) for the full list. Key variables:

| Var | Default | Meaning |
|---|---|---|
| `S3_ACCESS_KEY_ID` / `S3_SECRET_ACCESS_KEY` | — | gateway SigV4 credentials (required) |
| `REPLICATION_FACTOR` | `3` | distinct providers per chunk (R) |
| `CHUNK_SIZE` | `80MiB` | per-chunk window (under catbox's 200 MB cap) |
| `FREEHOST_PROVIDERS` | built-in default | enabled providers, priority order |
| `UPLOAD_CONCURRENCY` | `6` | parallel chunk-replica uploads |
| `KEEPALIVE_INTERVAL` | `24h` | self-heal sweep cadence (`0` = off) |
| `CATBOX_USERHASH` | — | **required for catbox from a VPS** (anon = 412) |
| `PIXELDRAIN_API_KEY` | — | optional; longer retention |
| `IA_ACCESS_KEY` / `IA_SECRET_KEY` | — | enables the Internet Archive anchor |
| `GOFILE_TOKEN` | — | required for gofile direct links |

**Provider notes** (see [`RESEARCH.md`](RESEARCH.md) for the full catalog):

- **Datacenter-IP blocking is the #1 failure mode.** Smoke-test providers from
  your real deploy egress IP (see Testing). catbox needs a `userhash`; `0x0.st`
  is unusable from DC IPs (so we ship `x0.at` instead).
- At least one **durable** provider (IA, fileditch, pixeldrain, catbox, x0.at,
  pomf.lain.la, …) must be enabled or the gateway refuses to start.
- `gofile` and `buzzheavier` are in the registry but **not** the default order
  (docs-derived mechanics / token-gated) — enable them via `FREEHOST_PROVIDERS`
  after verifying them live.

## Testing

```bash
go test ./...            # full suite (no network) — 148 tests
go vet ./...
```

The suite covers the S3 surface (SigV4, listing/pagination, multipart, range,
conditional requests, CopyObject, CORS), the chunk-and-replicate backend
(replication, fallback, failover, delete, self-heal), every provider's
request/response shape (httptest), and full-stack PUT/GET/multipart/range
through the real backend over in-memory providers.

**Live provider smoke tests** must run from the actual deploy egress IP:

```bash
CATBOX_USERHASH=... IA_ACCESS_KEY=... IA_SECRET_KEY=... \
  go test -tags=live -run TestLive ./internal/storage/freehost/
```

**Live S3 acceptance** (against a running gateway) — see
[`S3-COMPAT.md`](S3-COMPAT.md) and `scripts/acceptance.sh`.

## License

Personal project; no warranty. Respect each host's Terms of Service — these are
soft replicas, not a place to dump anything abusive.
