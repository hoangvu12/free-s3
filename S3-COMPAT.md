# free-s3 — S3 Compatibility & Acceptance

free-s3 reuses `telegram-s3`'s S3 surface verbatim and swaps only the storage
backend, so the HTTP/S3 behavior is at parity with that project. The S3 layer is
covered by the in-repo test suite (148 tests, all passing); live provider
behavior must be verified from the real deploy egress IP.

## Supported S3 operations

| Area | Operations |
|---|---|
| **Auth** | SigV4 header auth + presigned URLs (X-Amz-Expires enforced); public unauthenticated object GET/HEAD; Accept-Encoding tolerance for CF/rclone |
| **Buckets** | CreateBucket, HeadBucket, DeleteBucket (empty-only), ListBuckets |
| **Bucket subresources** | GET `?location` / `?versioning` / `?acl` / `?cors` / `?tagging`→NoSuchTagSet; PUT/DELETE subresource no-ops |
| **Objects** | PutObject (incl. aws-chunked streaming), GetObject, HeadObject, DeleteObject |
| **Range** | RFC 7233 single byte-range (`bytes=a-b`, `bytes=-N`, `bytes=a-`), 416 on unsatisfiable, If-Range gating |
| **Conditional** | If-Match/If-None-Match/If-Modified-Since/If-Unmodified-Since (304 / 412) |
| **Metadata** | Content-Type, Content-Disposition/Encoding, Cache-Control, Expires, `x-amz-meta-*`; `response-*` override query params |
| **Copy** | CopyObject + UploadPartCopy (server-side re-stream, COPY/REPLACE metadata directive) |
| **Multipart** | Create/UploadPart/Complete/Abort/ListParts; 5 MiB min non-last part (EntityTooSmall); abandoned-upload janitor |
| **Listing** | ListObjects v1 + v2, prefix/delimiter rollup, real pagination (marker / continuation-token), `encoding-type=url` |
| **Bulk** | DeleteObjects (`POST ?delete`) |
| **CORS** | permissive preflight + `x-amz-request-id` on every response |

## Durability surface (the freehost backend)

| Property | Behavior |
|---|---|
| Chunking | object split into `CHUNK_SIZE` windows; unbounded object size |
| Replication | each chunk → R distinct providers, ≥1 durable, never two replicas on one host |
| Read | first healthy replica wins; HTTP Range with client-side slicing fallback |
| Write fallback | a failed provider falls through to the next unused one; ≥1 replica required |
| Delete | best-effort removal of every replica across all providers |
| Self-heal | keep-alive sweep verifies replicas, drops dead ones, refills below R from a survivor |

## Local conformance baseline

```bash
go test ./... -count=1     # 148 tests, all green
go vet ./...
```

Highlights:
- `internal/s3api` — 87 tests: SigV4 (incl. aws-sdk-go v2 vectors), listing &
  pagination, multipart, range, conditional, copy, CORS, vhost, subresources,
  plus full-stack PUT/GET/multipart/range through the **real** freehost backend
  over in-memory providers (`freehost_e2e_test.go`).
- `internal/storage/freehost` — 37 tests: chunk+replicate, fallback, failover,
  delete-all, durable requirement, self-heal (RepairChunk), and every provider's
  request/response shape via httptest.
- `internal/keepalive` — self-heal sweep refill/no-op/unrecoverable tolerance.

## Live acceptance (run from the deploy host)

`scripts/acceptance.sh` drives a running gateway with the real AWS CLI (and
optionally rclone/restic): bucket lifecycle, small + multipart round-trips
(byte-compared), metadata echo, conditional requests, Range, CopyObject,
subresource probes, and listing/pagination at scale. It touches only its own
throwaway bucket.

```bash
S3_ENDPOINT=http://localhost:9000 \
S3_ACCESS_KEY=free-s3-local S3_SECRET_KEY=... \
  ./scripts/acceptance.sh
```

Per-provider live smoke tests (upload / full + range GET / byte-compare /
delete), which **must** run from the real deploy egress IP because
datacenter-IP blocking is the #1 failure mode:

```bash
CATBOX_USERHASH=... IA_ACCESS_KEY=... IA_SECRET_KEY=... PIXELDRAIN_API_KEY=... \
  go test -tags=live -run TestLive ./internal/storage/freehost/
```

A provider that fails its live smoke test from your egress IP should be removed
from `FREEHOST_PROVIDERS` (the pool's health tracking will also deprioritize a
consistently-failing host automatically).

## Known limitations

- Object versioning, ACLs, bucket policies, lifecycle, and SSE are not
  implemented (single-version, public-read object model — same as telegram-s3).
- Durability is best-effort across free hosts, **not** an SLA. Keep R ≥ 2 and
  keep the Internet Archive anchor enabled.
- `gofile` / `buzzheavier` direct-link mechanics are docs-derived; verify live
  before relying on them.
