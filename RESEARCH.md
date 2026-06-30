# free-s3 — Provider Research

Goal: an **S3-compatible gateway** that stores *arbitrary* objects (any content
type, any size) on **free storage backends** — same shape as `../telegram-s3`
(Telegram backend) but with a pool of free hosts + free cloud tiers.

**Decisions locked in:**
- **Stack:** Go — fork `../telegram-s3` (reuse S3 API, SigV4, SQLite metadata,
  chunker, multipart, range-reader); replace the Telegram backend with a
  free-host provider pool.
- **Durability:** chunk every object, **replicate each chunk to R=2–3 distinct
  providers**. Survive any single host dying/expiring/IP-blocking.
- **Accounts:** user will register accounts → catbox `userhash`, Gofile token,
  Pixeldrain API key, Internet Archive S3 keys, B2/R2 keys, GitHub token, etc.
  Every credential stays optional in config.

Reference projects: `../telegram-s3` (Go, chunk-and-map architecture);
`../studyon-openai/src/storage-providers.ts` (TS, production-tested upload
mechanics, fallback chain, retry/backoff, per-host gotchas).

---

## ✅ FINAL PROVIDER SET (locked) — simple adapters only

Decision: **only providers that are one-or-few plain HTTP calls with a static
credential (or none) and a direct-URL response.** No OAuth, no client-side
crypto, no SDKs, no CID-mapping. This is the set we implement behind the
`Backend` interface, in roughly this order.

**Permanent anchor**
- **Internet Archive** — `PUT` + `Authorization: LOW key:secret`, deterministic URL (simple, NOT full SigV4).

**Anonymous durable hosts**
- **fileditch** (no auth, permanent, 100 GB) · **pixeldrain** (Basic API key) ·
  **catbox** (`userhash`) · **gofile** (2-step HTTP, token optional) ·
  **buzzheavier** (`PUT` raw).

**0x0 family** (`POST file=@` → plaintext URL)
- **x0.at** · **envs.sh** · **ttm.sh** · **fars.ee** · **paste.c-net.org**

**pomf family** (`POST files[]` → JSON url)
- **pomf.lain.la** · **uguu** · **tmp.ninja** · **doko.moe** · **cockfile**

**Temp / overflow tier**
- **temp.sh** · **litterbox** · **tmpfiles.org** · **tmpfile.link** · **filebin.net**

**Dropped as too complex / not worth it:** GitHub Release assets (repo/release
juggling), Backblaze B2 + Cloudflare R2 (account + credit-card + custom domain),
Google Drive / OneDrive / pCloud (OAuth), Mega / Filen / Internxt (E2E SDK),
Pinata / Lighthouse / Filebase / IPFS (CID layer + flaky gateways), 1fichier
(anti-bot), mixdrop (email-key + 60-day churn). Details retained below for
reference, but not in scope.

**~19 backends in scope.** Full per-provider API + gotchas below.

---

## Core design realization

Free hosts cap file size and give **no durability SLA**. Copy telegram-s3:
**chunk every object → upload each chunk to a free host → store
`(object → [chunk: provider, locator, size, offset, replicas[]])` in a metadata
DB → reassemble on GET.** Object size becomes unbounded; chunks spread +
replicate across providers so one host vanishing ≠ data loss. Replication
factor **R** is the durability dial. A keep-alive + 404→re-fetch-from-replica
→refill loop self-heals expiry/pruning.

---

# Provider catalog (verified mid-2026)

Verification: 🟢 fetched host's own API/docs · 🟡 docs-derived · 🔴 inferred.
~30 usable backends across 4 tiers; **~19 strong, datacenter-friendly.**

## Tier 0 — DURABLE CLOUD ANCHORS (accounts + real APIs · permanent/long-lived · DC-friendly)

These are the best permanent replicas. Most need a (free) account; all have
genuine HTTP/S3 APIs and true raw-bytes direct URLs.

| Provider | Free limit | Permanent? | Upload API | Direct raw URL | Notes | V |
|---|---|---|---|---|---|---|
| **Internet Archive** | no storage cap | ✅ (intent, multi-DC) | `PUT s3.us.archive.org/{item}/{file}` + `Authorization: LOW key:secret`, `x-archive-queue-derive:0` | `archive.org/download/{item}/{file}` | ⭐ best free anchor; deterministic URL, no CID. Items ≤10 GB / <100 files; can be "darked" for policy → ToS risk for opaque blobs | 🟢 |
| **GitHub Release assets** | effectively unmetered | ✅ | `POST uploads.github.com/repos/{o}/{r}/releases/{id}/assets?name=` (raw body) / `gh release upload` | `github.com/{o}/{r}/releases/download/{tag}/{asset}` →CDN | ⭐ <2 GiB/asset, 1000/release, CDN raw, most DC-friendly. ToS-fragile → spread across repos/accounts, treat as capacity not sole copy | 🟢 |
| **Backblaze B2** | 10 GB | ✅ | S3-compat (`PUT`, SigV4) or native `b2`; rclone | public bucket `fXXX.backblazeb2.com/file/{bucket}/{file}` (unauth) | best S3 cloud tier; egress free ≤3× stored then $0.01/GB (free via Cloudflare CDN). CC to activate | 🟢 |
| **Cloudflare R2** | 10 GB-mo | ✅ | S3-compat (SigV4); rclone | custom-domain public bucket / presigned (NOT `r2.dev` = dev-only) | ⭐ **zero egress** — ideal for read-heavy serving. CC to activate | 🟢 |
| **pCloud** | up to 10 GB | ✅ | public HTTP API; rclone `pcloud` (OAuth) | `getfilelink` → `https://{host}{path}` (raw, time-limited) | clean non-S3 direct URL, no E2E, DC-friendly | 🟢 |
| **Google Drive** | 15 GB | ✅ | Drive API v3 (OAuth); rclone Tier-1 | `GET /files/{id}?alt=media` (raw, bypasses scan UI) | most free GB; ToS steers blobs→GCS, 750 GB/day cap → soft replica | 🟢 |
| **OneDrive** | 5 GB | ✅ | MS Graph (OAuth); rclone Tier-1 | `/content` →302 pre-authed `downloadUrl` (Range-capable, expires min) | cleanest Range-fetch mechanism | 🟢 |
| **Filebase** | 5 GB | ✅ while in quota | `s3.filebase.io` SigV4 (IPFS-backed bucket) | `ipfs.filebase.io/ipfs/{CID}` | S3+IPFS, clean DX; records a CID per chunk | 🟢 |
| **Lighthouse.storage** | 5 GB (trial) | ✅ pay-once perpetual | `POST node.lighthouse.storage/api/v0/add` + Bearer | `gateway.lighthouse.storage/ipfs/{CID}` | only true pay-once-permanent IPFS (Filecoin endowment) | 🟡 |
| **Pinata** | 1 GB / 500 files | while in quota | `POST api.pinata.cloud/pinning/pinFileToIPFS` + Bearer JWT | `{gw}.mypinata.cloud/ipfs/{CID}` | reliable but small free quota | 🟢 |

## Tier A — ANONYMOUS DURABLE FILE HOSTS (no/low signup · arbitrary types · DC-friendly)

| Provider | Max/file | Retention | Auth | Direct raw URL | V |
|---|---|---|---|---|---|
| **fileditch.com** | **100 GB** | **permanent** | none | ✅ JSON `url` (blocks .exe/.sh/.php → name chunks `.bin`) | 🟢 |
| **pixeldrain.com** | ~20 GB | long-lived (free prunes ~90d) | API key (Basic, key in password) | ✅ `GET /api/file/{id}` | 🟢 |
| **catbox.moe** | 200 MB | permanent | `userhash` (REQUIRED from VPS — anon=`412`) | ✅ `files.catbox.moe/<id>` | 🟢 |
| **gofile.io** | unlimited | inactivity→cold | token (optional) | content code / `directlinks` API | 🟡 |
| **x0.at** | 1 GiB | 3→100 days | none; `X-Token` | ✅ plaintext (confirmed NOT DC-blocked) | 🟢 |
| **pomf.lain.la** | 1 GiB | long-term (dedic. HW) | none | ✅ JSON `files[].url` | 🟢 |
| **buzzheavier.com** | — | activity-based | optional Bearer | ✅ `PUT` raw-body, direct link | 🟡 |
| **fars.ee** | large | no default expiry (`sunset=` opt) | none (UUID for mgmt) | ✅ plaintext (field `c`, behind Cloudflare) | 🟢 |
| **paste.c-net.org** | 50 MB | 180 days (rolling on access) | none | ✅ plaintext (raw `PUT --upload-file`) | 🟢 |

## Tier B — TEMP / SCRATCH / OVERFLOW (arbitrary types · expiring · always re-derivable from a replica)

| Provider | Max/file | Retention | Direct URL | V |
|---|---|---|---|---|
| **temp.sh** | 4 GB | 3 days | ✅ body=URL | 🟢 |
| **tmp.ninja** | 10 GB | 48h | uguu-style (verify field) | 🟢¹ |
| **litterbox** (catbox) | 1 GB | 1h/12h/24h/72h | ✅ body=URL | 🟢 |
| **envs.sh** | 256 MiB | ~30→365 days | ✅ plaintext, `X-Token` | 🟡 |
| **ttm.sh** | ~256 MiB | days | ✅ plaintext, `X-Token` | 🟢 |
| **tmpfile.link** | 100 MB | 7 days | ✅ JSON `downloadLink` | 🟢 |
| **doko.moe** | 2 GB | temp | pomf `files[]` (verify) | 🟢¹ |
| **tmpfiles.org** | 100 MB | 1h–2d | ⚠️ rewrite to `/dl/{id}/{name}` | 🟢 |
| **uguu.se** | 128 MiB | 3 hours | ✅ JSON `files[].url` | 🟢 |
| **cockfile.com** | 128 MiB | 12 hours | ✅ JSON `files[].url` | 🟡 |
| **filebin.net** | large | ~6 days | ✅ `GET`→302 presigned (restricts ext) | 🟢 |
| **0x0.st** | 512 MiB | 30→365 days | ✅ plaintext — but ❌VPS-BLOCKED (residential only) | 🟢 |

¹ pomf/uguu response schema inferred — POST a 1-byte test to pin the exact
endpoint/field/JSON before wiring in.

## Conditional / account-gated (extra replicas if wanted)
- **mixdrop.ag** — email+key, arbitrary, **60-day inactivity delete**.
- **1fichier.com** — key, 100 GB, but free download caps + Cloudflare anti-bot.
- **qu.ax** — 256 MB, restricted ext (img+zip/rar/7z/tar/gz/pdf/txt), has a **Permanent** retention option.
- **GitHub raw-commit / LFS / Gist** — work but unauth raw = **60 req/hr/IP** + small LFS quota → Release assets dominate all three.
- **Mega (20GB) / Filen (10GB) / Internxt** — client-side **E2E**: no hotlink; only usable as **decrypt-on-fetch** backends via rclone/SDK.

## Excluded (don't use)
- **file.io** — auto-deletes on **first download** (one-shot) → breaks replicated reads.
- **Storj** — free tier ended 2026 ($50/mo min). **web3.storage/Storacha** — permanent-free gone. **NFT.Storage Classic** — dead (2024). **Crust** — needs CRU tokens.
- **Terabox** — no official API, anti-bot trap (1 TB is bait). **Icedrive/OpenDrive/Jottacloud** (free) — no usable API / no hotlink / tiny caps.
- Browser/JS-only: ufile.io · fileport.io · send.cm · send.now · tmpsend · dropmefiles · workupload · anonymousfiles · uploadfiles · dataupload · dbree.
- **wormhole.app / Firefox-Send forks (ffsend)** — E2E, no plain raw-bytes URL.
- Dead/parked: bashupload · oshi.at · transfer.sh (public) · anonfiles/bayfiles · most 2023-era 0x0/pomf instances (0x0.wtf, i.ode.bz, mixtape.moe, safe.moe, comfy.moe, pomf.cat…).
- **kappa.lol** — rebranded to segs.lol, flaky. **e-z.host / 0.vern.cc** — now require auth.

## Tier C — image-only (NOT for a general object store; possible future image fast-path)
**imgbb** (32 MB) · **gifyu** (100 MB) · **freeimage.host** (key `6d207e02198a...`, 64 MB) · **pixhost** (full-res, permanent) · **pasteboard** (expires ~3mo). From studyon-openai.

---

# Verified API details (the ones we'll implement first)

### Internet Archive  🟢 (best permanent anchor)
- `PUT https://s3.us.archive.org/{item}/{file}`, raw body; header `Authorization: LOW {accesskey}:{secret}` (keys: archive.org/account/s3.php)
- `x-archive-auto-make-bucket:1` create item · `x-archive-queue-derive:0` skip derive · `x-archive-size-hint:<bytes>`
- download: `GET https://archive.org/download/{item}/{file}` (deterministic, Range-capable)
- honor `?check_limit=1` (avoid 503 SlowDown); items ≤10 GB, <100 files

### GitHub Release assets  🟢 (capacity tier via `gh`)
- create release (`gh release create` / `POST /repos/{o}/{r}/releases`), then
  `POST https://uploads.github.com/repos/{o}/{r}/releases/{id}/assets?name={asset}` with raw body + `Content-Type` + Bearer token
- download: `https://github.com/{o}/{r}/releases/download/{tag}/{asset}` (302→CDN, raw)
- <2 GiB/asset, 1000 assets/release; auth reads to dodge unauth rate limits; spread across repos

### Backblaze B2 / Cloudflare R2  🟢 (S3-compatible — reuse SigV4 we already have)
- standard S3 `PUT`/`GET`; B2 public raw: `https://fXXX.backblazeb2.com/file/{bucket}/{file}`; R2 via custom-domain public bucket or presigned (avoid `r2.dev`)
- R2 = zero egress (route read-heavy traffic here); B2 egress free ≤3× stored

### fileditch.com  🟢
- `POST https://new.fileditch.com/upload.php`, multipart field `file` (single) → JSON `{success, url, filename, size}`
- blocks php/html/js/exe/apk/sh/py/bat → name chunks `.bin`/`.dat`; `temp.fileditch.com`=72h

### pixeldrain.com  🟢
- `POST /api/file` (field `file`) → `201 {id}` · or `PUT /api/file/{name}` raw
- auth: API key in HTTP Basic password: `Authorization: Basic base64(":"+key)`
- raw: `GET /api/file/{id}`; delete: `DELETE /api/file/{id}`; free prunes inactive (~90d)→keep-alive

### catbox.moe  🟢 (proven in studyon-openai)
- `POST https://catbox.moe/user/api.php`: `reqtype=fileupload`, `fileToUpload=<file>`, optional `userhash`
- plaintext `files.catbox.moe/<id>` (200 even on error→validate prefix); VPS needs `userhash` (anon=412)
- delete: `reqtype=deletefiles`, `userhash`, `files=<space-separated>`

### gofile.io  🟡 (live-verified `/servers`→`{"status":"ok"}`)
1. `GET https://api.gofile.io/servers` (≤1/10s) → `data.servers[].name`
2. `POST https://{server}.gofile.io/contents/uploadfile`, field `file`, opt `token`+`folderId`
3. `data`: `downloadPage, code, fileId, fileName, md5`; permanent link `POST /contents/{id}/directlinks`
4. delete: `DELETE /contents/{id}` + token

### 0x0 family (x0.at · envs.sh · ttm.sh · fars.ee · paste.c-net.org)  🟢
- 0x0: `POST https://HOST/` field `file=@`; opt `expires=`, `secret`, `url=`; plaintext URL + `X-Token`; delete via `token=`+`delete=`
- fars.ee variant: field `c`, expiry `sunset=<sec>`, mgmt via returned UUID (no X-Token)
- paste.c-net.org: raw `PUT --upload-file` (not multipart)
- caps differ: x0.at 1 GiB/100d ✅DC · 0x0.st 512 MiB ❌VPS-blocked

### pomf family (pomf.lain.la · cockfile · tmp.ninja · doko.moe · uguu)  🟢
- `POST https://HOST/upload.php` (uguu uses `/upload`), field `files[]=@` → JSON `{success, files:[{url, name}]}`

### temp tier (temp.sh · litterbox · tmpfiles · tmpfile.link · filebin)  🟢
- temp.sh: `POST /upload` field `file` → body=URL
- litterbox: `POST litterbox.catbox.moe/resources/internals/api.php`, `reqtype=fileupload`, `time=72h`, `fileToUpload`
- tmpfiles: `POST /api/v1/upload` field `file` → JSON `data.url`; rewrite `/dl/{id}/{name}`
- tmpfile.link: `POST /api/upload` field `file` → JSON `{downloadLink}`
- filebin: `POST /{bin}/{filename}` (`Content-Length`); `GET`→302 presigned; `DELETE /{bin}`; some ext blocked

### IPFS pinners (Filebase · Lighthouse · Pinata)  🟢🟡
- Filebase: S3 `s3.filebase.io` SigV4 → gateway `ipfs.filebase.io/ipfs/{CID}` (record CID)
- Lighthouse: `POST node.lighthouse.storage/api/v0/add` + Bearer → `gateway.lighthouse.storage/ipfs/{CID}`
- Pinata: `POST api.pinata.cloud/pinning/pinFileToIPFS` + Bearer JWT → `{gw}.mypinata.cloud/ipfs/{CID}`

---

# Universal gotchas (the provider layer must handle)

1. **Always send a browser `User-Agent`** — many hosts 403/418/1010 bot UAs.
2. **VPS/datacenter-IP blocking is the #1 hidden failure mode.** Verify from the
   gateway's *actual egress IP*. Friendly: IA, GitHub, B2, R2, pCloud, GDrive,
   OneDrive, fileditch, pixeldrain, gofile, buzzheavier, x0.at. Hostile: catbox(anon),
   0x0.st, 1fichier, krakenfiles, Terabox. → catbox needs `userhash`; 0x0.st needs proxy.
3. **Validate response *body shape*, not just HTTP status** — several return 200 with a text/HTML error.
4. **Inactivity pruning** (pixeldrain-free, gofile, mixdrop-60d, buzzheavier, IPFS-quota) →
   **keep-alive job** re-reads chunks to reset TTLs; keep these as secondaries behind permanent anchors.
5. **Extension blocklists** (fileditch, filebin) → name chunks `.bin`/`.dat`/`.txt`.
6. **Content-addressed vs path** — IPFS/Filebase return a **CID** to record per chunk; IA/S3/GitHub give URLs you control.
7. **E2E hosts (Mega/Filen/Internxt)** — no hotlink; integrate only as decrypt-on-fetch via rclone/SDK.
8. **ToS** — B2/R2/pCloud/Koofr = object-storage products (blobs in-bounds, safest); GDrive/Dropbox/IA/GitHub = soft replicas (spread load, expect possible enforcement).
9. Reuse studyon-openai patterns verbatim: `fetchWithTimeout` (AbortController),
   per-provider retry w/ exp backoff (500ms→1s→2s ×3) then fall through,
   bounded `mapWithConcurrency` for parallel chunk uploads.

---

# Replication recommendation (R=3)

Per chunk, pick R distinct providers across **independent** infrastructure:
- **Anchor 1 (permanent, deterministic):** Internet Archive **or** GitHub Release assets.
- **Anchor 2 (clean durable API):** Backblaze B2 / Cloudflare R2 / pixeldrain / fileditch.
- **Replica 3 (cheap/fast):** catbox(userhash) / x0.at / pomf.lain.la / gofile.
- **Overflow / huge / scratch:** temp.sh, tmp.ninja, litterbox, envs.sh — short TTL, always re-derivable from an anchor.

Self-healing: keep-alive re-reads inactivity-pruned hosts; on any chunk read 404,
transparently re-fetch from a surviving replica and re-upload to refill R.

**Implementation order:** start with the SigV4 hosts (IA, B2, R2 — reuse
telegram-s3's signer almost verbatim) + fileditch + catbox + the 0x0/pomf
families (trivial APIs), then layer GitHub, pixeldrain, gofile, IPFS, and the
temp tier behind the same `Backend` interface.
