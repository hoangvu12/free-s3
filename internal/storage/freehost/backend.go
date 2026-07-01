// Package freehost implements the storage.Backend over a pool of free public
// file-hosting sites. Each object is split into chunks and every chunk is
// replicated to R distinct providers; reads fetch from the first healthy
// replica. See BUILD-PLAN.md (§3) and RESEARCH.md for the provider catalog.
package freehost

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"free-s3/internal/storage"
)

// Backend is the freehost storage.Backend: it chunks an object, replicates each
// chunk to R distinct providers, and reads chunks back from the first healthy
// replica.
type Backend struct {
	pool         *pool
	chunkSize    int64
	replicas     int // R
	syncReplicas int // replicas confirmed before Upload returns; the rest replicate in the background
	uploadConc   int
	replicaRead  time.Duration   // per-replica read deadline / backstop (0 = no per-replica cap)
	hedgeDelay   time.Duration   // wait before racing the next replica on a slow lead (0 = no hedging, pure failover)
	bgCtx        context.Context // server-lifetime ctx for background replication (survives the request)
	bgWG         sync.WaitGroup  // tracks in-flight background replica uploads for graceful drain
	logger       *slog.Logger
}

// Options bundles Backend construction.
type Options struct {
	Providers         []Provider
	ChunkSize         int64
	ReplicationFactor int
	// SyncReplicas is how many replicas an upload confirms before returning; the
	// remaining (ReplicationFactor - SyncReplicas) replicate in the background so
	// a slow durable anchor (e.g. Internet Archive at ~0.5 MB/s) does not gate the
	// PUT response. <= 0 or >= ReplicationFactor means fully synchronous.
	SyncReplicas       int
	UploadConcurrency  int
	ReplicaReadTimeout time.Duration
	ReadHedgeDelay     time.Duration
	// BackgroundCtx bounds background replication lifetime; cancel it on shutdown
	// to abandon in-flight background uploads. Defaults to context.Background().
	BackgroundCtx context.Context
	Logger        *slog.Logger
}

var _ storage.Backend = (*Backend)(nil)

// New builds the backend. It requires at least one provider and at least one
// durable provider (the anchor that every chunk's replica set must be able to
// include), else it fails fast (BUILD-PLAN §5).
func New(opts Options) (*Backend, error) {
	if len(opts.Providers) == 0 {
		return nil, errors.New("freehost: no providers enabled")
	}
	if opts.ChunkSize <= 0 {
		opts.ChunkSize = 80 << 20
	}
	if opts.ReplicationFactor < 1 {
		opts.ReplicationFactor = 1
	}
	if opts.UploadConcurrency < 1 {
		opts.UploadConcurrency = 6
	}
	syncReplicas := opts.SyncReplicas
	if syncReplicas <= 0 || syncReplicas > opts.ReplicationFactor {
		syncReplicas = opts.ReplicationFactor // fully synchronous
	}
	bgCtx := opts.BackgroundCtx
	if bgCtx == nil {
		bgCtx = context.Background()
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	p := newPool(opts.Providers)
	if !p.hasDurable() {
		return nil, errors.New("freehost: at least one durable provider must be enabled")
	}
	return &Backend{
		pool:         p,
		chunkSize:    opts.ChunkSize,
		replicas:     opts.ReplicationFactor,
		syncReplicas: syncReplicas,
		uploadConc:   opts.UploadConcurrency,
		replicaRead:  opts.ReplicaReadTimeout,
		hedgeDelay:   opts.ReadHedgeDelay,
		bgCtx:        bgCtx,
		logger:       logger,
	}, nil
}

// Upload splits body into chunkSize windows and replicates each window to R
// distinct providers, FULLY SYNCHRONOUSLY (waits for all R). Used by paths that
// want every replica confirmed before returning (copy, multipart parts). It
// streams the body chunk-by-chunk so object size is unbounded by memory.
func (b *Backend) Upload(ctx context.Context, name, contentType string, body io.Reader) ([]storage.Chunk, error) {
	return b.upload(ctx, name, contentType, body, b.replicas, nil)
}

// UploadReplicated is like Upload but returns as soon as syncReplicas replicas
// per chunk are confirmed; the remaining replicas (up to R) upload in the
// BACKGROUND (server-lifetime ctx) so a slow durable anchor does not gate the
// PUT response. Each background replica that lands is delivered via
// onExtra(seq, rep) — the caller persists it (e.g. AppendChunkReplica). onExtra
// is called from background goroutines AFTER this returns; it must be safe for
// concurrent use and tolerate being called after the object is deleted/changed.
func (b *Backend) UploadReplicated(ctx context.Context, name, contentType string, body io.Reader, onExtra func(seq int, rep storage.Replica)) ([]storage.Chunk, error) {
	return b.upload(ctx, name, contentType, body, b.syncReplicas, onExtra)
}

func (b *Backend) upload(ctx context.Context, name, contentType string, body io.Reader, syncN int, onExtra func(seq int, rep storage.Replica)) ([]storage.Chunk, error) {
	nameHash := shortHash(name)
	buf := make([]byte, b.chunkSize)
	var chunks []storage.Chunk
	var offset int64

	for seq := 0; ; seq++ {
		n, rerr := io.ReadFull(body, buf)
		if n > 0 {
			data := make([]byte, n)
			copy(data, buf[:n])
			chunk, err := b.uploadChunk(ctx, nameHash, seq, offset, data, contentType, syncN, onExtra)
			if err != nil {
				return chunks, err // caller reaps the partial set
			}
			chunks = append(chunks, chunk)
			offset += int64(n)
		}
		if rerr == io.EOF || rerr == io.ErrUnexpectedEOF {
			break
		}
		if rerr != nil {
			return chunks, rerr
		}
	}
	return chunks, nil
}

// uploadChunk replicates one chunk to R distinct providers, returning once syncN
// replicas succeed and continuing the rest in the background. It launches up to
// R candidate uploads concurrently and returns the first syncN successes (the
// FASTEST providers naturally win, so the slow anchor lands in the background);
// on a failure it pulls in the next candidate. Provider uploads run on b.bgCtx
// so they survive the request — a client disconnect aborts only the wait (via
// ctx), not in-flight replication. Requires >= 1 successful replica; zero fails.
//
// When syncN >= R (the synchronous Upload path) it waits for all R and onExtra
// is never called. When syncN < R, replicas beyond syncN are delivered to
// onExtra as they land (durability tops up to R in the background).
func (b *Backend) uploadChunk(ctx context.Context, nameHash string, seq int, offset int64, data []byte, contentType string, syncN int, onExtra func(seq int, rep storage.Replica)) (storage.Chunk, error) {
	size := int64(len(data))
	cands := b.pool.candidates(size)
	if len(cands) == 0 {
		return storage.Chunk{}, fmt.Errorf("freehost: no provider can hold a %d-byte chunk", size)
	}
	filename := fmt.Sprintf("%s.%d.bin", nameHash, seq)
	if syncN < 1 || syncN > b.replicas {
		syncN = b.replicas
	}

	type outcome struct {
		rep storage.Replica
		ok  bool
	}
	results := make(chan outcome, len(cands))
	var mu sync.Mutex
	launched := 0
	// launchNext starts the next unused candidate's upload; caller holds mu.
	launchNext := func() bool {
		if launched >= len(cands) {
			return false
		}
		prov := cands[launched]
		launched++
		b.bgWG.Add(1)
		go func() {
			defer b.bgWG.Done()
			start := time.Now()
			loc, tok, err := prov.Upload(b.bgCtx, data, filename, contentType)
			elapsed := time.Since(start)
			if err != nil {
				b.pool.markFailed(prov.Name())
				b.logger.Warn("freehost: chunk replica upload failed", "provider", prov.Name(), "seq", seq, "bytes", size, "elapsed_ms", elapsed.Milliseconds(), "error", err)
				results <- outcome{}
				return
			}
			b.pool.markHealthy(prov.Name())
			b.logger.Info("freehost: chunk replica uploaded", "provider", prov.Name(), "seq", seq, "bytes", size, "elapsed_ms", elapsed.Milliseconds())
			results <- outcome{rep: storage.Replica{Provider: prov.Name(), Locator: loc, DeleteToken: tok}, ok: true}
		}()
		return true
	}

	mu.Lock()
	for launched < b.replicas && launchNext() {
	}
	inflight := launched
	mu.Unlock()

	// Synchronous phase: collect until syncN succeed (or candidates exhaust).
	var synced []storage.Replica
	collected := 0
	for len(synced) < syncN && collected < inflight {
		select {
		case <-ctx.Done():
			return storage.Chunk{}, ctx.Err() // caller gone; detached uploads finish as orphans
		case o := <-results:
			collected++
			if o.ok {
				synced = append(synced, o.rep)
			} else {
				mu.Lock()
				if launchNext() {
					inflight++
				}
				mu.Unlock()
			}
		}
	}
	if len(synced) == 0 {
		return storage.Chunk{}, fmt.Errorf("freehost: all providers failed for chunk %d", seq)
	}
	if !anyDurable(b.pool, synced) {
		b.logger.Warn("freehost: chunk sync replicas have no durable member", "seq", seq, "replicas", len(synced))
	}

	// Background phase: drain the remaining in-flight uploads (and top up toward R
	// on failures), delivering extras via onExtra. Always drain even when onExtra
	// is nil so straggler goroutines don't block on the results channel.
	remaining := inflight - collected
	if remaining > 0 {
		b.bgWG.Add(1)
		go func(total int) {
			defer b.bgWG.Done()
			for remaining > 0 {
				o := <-results
				remaining--
				if o.ok {
					total++
					if onExtra != nil {
						onExtra(seq, o.rep)
					}
				} else if total < b.replicas {
					mu.Lock()
					if launchNext() {
						remaining++
					}
					mu.Unlock()
				}
			}
		}(len(synced))
	}

	return storage.Chunk{Seq: seq, Size: size, Offset: offset, Replicas: synced}, nil
}

// WaitBackground blocks until all in-flight background replica uploads finish.
// Call on graceful shutdown (after cancelling the background ctx) so pending
// replications either complete or abort before exit.
func (b *Backend) WaitBackground() { b.bgWG.Wait() }

// Download returns the whole content of one chunk from its first healthy replica.
func (b *Backend) Download(ctx context.Context, ref storage.ChunkRef) (io.ReadCloser, error) {
	return b.openReplica(ctx, ref, 0, 0)
}

// DownloadRange returns [offset, offset+length) of one chunk from its first
// healthy replica (length <= 0 means to end).
func (b *Backend) DownloadRange(ctx context.Context, ref storage.ChunkRef, offset, length int64) (io.ReadCloser, error) {
	return b.openReplica(ctx, ref, offset, length)
}

// openReplica tries each replica in order (the handler lists alive replicas
// first) until one returns bytes. A replica whose provider is not enabled, or
// that errors, is skipped. All-fail surfaces an error the handler maps to 502.
func (b *Backend) openReplica(ctx context.Context, ref storage.ChunkRef, offset, length int64) (io.ReadCloser, error) {
	var lastErr error
	for _, rep := range ref.Replicas {
		prov := b.pool.get(rep.Provider)
		if prov == nil {
			lastErr = fmt.Errorf("freehost: provider %q not enabled", rep.Provider)
			continue
		}
		rc, err := prov.Download(ctx, rep.Locator, offset, length)
		if err != nil {
			lastErr = err
			b.logger.Warn("freehost: replica download failed", "provider", rep.Provider, "locator", rep.Locator, "error", err)
			continue
		}
		return rc, nil
	}
	if lastErr == nil {
		lastErr = errors.New("freehost: chunk has no replicas")
	}
	return nil, fmt.Errorf("freehost: all replicas failed: %w", lastErr)
}

// DownloadRangeBytes reads [offset, offset+length) of one chunk into memory
// using HEDGED reads across the chunk's replicas. length <= 0 means "to end of
// chunk". This is the hot read path's window fetch.
//
// Why hedged and not just sequential failover: free hosts are fast to WRITE but
// often throttled to READ from a datacenter IP (measured: fileditch uploads a
// 6MB chunk in ~0.3s but serves it back at ~140KB/s), and pool.candidates()
// round-robin-rotates the durable tier so the stored replica[0] is effectively
// random — we cannot statically guarantee a fast read lead. So instead of
// committing to one replica and only failing over after it times out (which, on
// a many-window object, compounds an 18s penalty per window), we:
//   - start the lead replica immediately,
//   - if it hasn't delivered the whole window within hedgeDelay, start the next
//     replica concurrently and keep racing (and likewise on each error),
//   - return the FIRST replica to deliver the full window, cancelling the rest.
//
// A slow lead therefore costs ~hedgeDelay (e.g. 2s), not the full per-replica
// deadline, and it is order-independent. Each replica is still backstopped by
// replicaRead. Because the window is bounded (the reader's prefetch size, e.g.
// 4 MiB) buffering a few concurrent copies is cheap. Read outcomes are logged
// but do NOT mutate pool health (that is upload/Verify-driven).
func (b *Backend) DownloadRangeBytes(ctx context.Context, ref storage.ChunkRef, offset, length int64) ([]byte, error) {
	type cand struct {
		name string
		prov Provider
		loc  string
	}
	var cands []cand
	for _, rep := range ref.Replicas {
		if prov := b.pool.get(rep.Provider); prov != nil {
			cands = append(cands, cand{name: rep.Provider, prov: prov, loc: rep.Locator})
		}
	}
	if len(cands) == 0 {
		return nil, errors.New("freehost: chunk has no enabled replicas")
	}

	raceCtx, cancelAll := context.WithCancel(ctx)
	defer cancelAll()

	type res struct {
		name    string
		buf     []byte
		err     error
		elapsed time.Duration
	}
	results := make(chan res, len(cands))
	launch := func(c cand) {
		go func() {
			rctx := raceCtx
			var cancel context.CancelFunc
			if b.replicaRead > 0 {
				rctx, cancel = context.WithTimeout(raceCtx, b.replicaRead)
				defer cancel()
			}
			start := time.Now()
			buf, err := readReplicaBytes(rctx, c.prov, c.loc, offset, length)
			results <- res{name: c.name, buf: buf, err: err, elapsed: time.Since(start)}
		}()
	}

	next := 0
	inflight := 0
	launch(cands[next])
	next++
	inflight++

	var timer *time.Timer
	var hedgeC <-chan time.Time
	armHedge := func() {
		if timer != nil {
			timer.Stop()
			timer = nil
		}
		if next < len(cands) && b.hedgeDelay > 0 {
			timer = time.NewTimer(b.hedgeDelay)
			hedgeC = timer.C
		} else {
			hedgeC = nil
		}
	}
	armHedge()

	var lastErr error
	for inflight > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-hedgeC:
			// Lead is slow — race the next replica concurrently instead of
			// waiting for the lead to time out.
			if next < len(cands) {
				b.logger.Debug("freehost: hedging range read", "next_provider", cands[next].name, "offset", offset)
				launch(cands[next])
				next++
				inflight++
			}
			armHedge()
		case r := <-results:
			inflight--
			if r.err == nil {
				b.logger.Debug("freehost: replica range read ok",
					"provider", r.name, "offset", offset, "bytes", len(r.buf),
					"elapsed_ms", r.elapsed.Milliseconds(), "hedged", next > 1)
				return r.buf, nil // defer cancelAll() abandons the losers
			}
			lastErr = r.err
			b.logger.Warn("freehost: replica range read failed",
				"provider", r.name, "offset", offset, "want", length,
				"got", len(r.buf), "elapsed_ms", r.elapsed.Milliseconds(), "error", r.err)
			// A replica failed: immediately bring in the next one (don't wait
			// for the hedge timer) so we keep len(cands) attempts available.
			if next < len(cands) {
				launch(cands[next])
				next++
				inflight++
				armHedge()
			}
		}
	}
	if lastErr == nil {
		lastErr = errors.New("freehost: chunk has no replicas")
	}
	return nil, fmt.Errorf("freehost: all replicas failed: %w", lastErr)
}

// readReplicaBytes opens one replica's range and reads it fully into memory,
// respecting ctx's deadline. length <= 0 reads to EOF.
func readReplicaBytes(ctx context.Context, prov Provider, locator string, offset, length int64) ([]byte, error) {
	rc, err := prov.Download(ctx, locator, offset, length)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	var rdr io.Reader = rc
	if length > 0 {
		rdr = io.LimitReader(rc, length)
	}
	return io.ReadAll(rdr)
}

// Delete best-effort removes every replica of one chunk. The first error is
// returned but every replica is still attempted.
func (b *Backend) Delete(ctx context.Context, ref storage.ChunkRef) error {
	var firstErr error
	for _, rep := range ref.Replicas {
		prov := b.pool.get(rep.Provider)
		if prov == nil {
			continue
		}
		if err := prov.Delete(ctx, rep.Locator, rep.DeleteToken); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// DeleteBatch best-effort removes every replica of many chunks, logging per-ref
// failures; the returned error is the first encountered.
func (b *Backend) DeleteBatch(ctx context.Context, refs []storage.ChunkRef) error {
	var firstErr error
	for _, ref := range refs {
		if err := b.Delete(ctx, ref); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			b.logger.Warn("freehost: delete chunk replicas failed", "error", err)
		}
	}
	return firstErr
}

// Verify checks a replica is still alive by fetching its first byte. A nil
// return means the replica served bytes; any error means it is unreachable /
// pruned. Used by the keep-alive sweep + read-path self-heal.
func (b *Backend) Verify(ctx context.Context, rep storage.Replica) error {
	prov := b.pool.get(rep.Provider)
	if prov == nil {
		return fmt.Errorf("freehost: provider %q not enabled", rep.Provider)
	}
	rc, err := prov.Download(ctx, rep.Locator, 0, 1)
	if err != nil {
		return err
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(rc, 1))
	return rc.Close()
}

// RepairChunk verifies every replica of a chunk, drops the dead ones, and — if
// fewer than R survive — re-fetches the chunk from a live replica and uploads it
// to fresh providers to refill R. It returns the updated ref and whether
// anything changed (so the caller persists only real changes). An error means
// the chunk could not be repaired (e.g. every replica is dead = data loss).
func (b *Backend) RepairChunk(ctx context.Context, ref storage.ChunkRef) (storage.ChunkRef, bool, error) {
	var live []storage.Replica
	dead := false
	for _, rep := range ref.Replicas {
		if err := b.Verify(ctx, rep); err != nil {
			dead = true
			b.logger.Warn("freehost: replica dead during sweep", "provider", rep.Provider, "locator", rep.Locator, "error", err)
			continue
		}
		live = append(live, rep)
	}
	if len(live) == 0 {
		return ref, false, errors.New("freehost: all replicas dead, chunk unrecoverable")
	}
	if len(live) >= b.replicas && !dead {
		return ref, false, nil // healthy, nothing to do
	}

	newReps := append([]storage.Replica(nil), live...)
	if len(newReps) < b.replicas {
		rc, err := b.openReplica(ctx, storage.ChunkRef{Size: ref.Size, Replicas: live}, 0, 0)
		if err != nil {
			return ref, false, err
		}
		data, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			return ref, false, err
		}
		used := map[string]bool{}
		for _, r := range newReps {
			used[r.Provider] = true
		}
		newReps = append(newReps, b.replicateExtra(ctx, data, used, b.replicas-len(newReps))...)
	}
	changed := dead || len(newReps) != len(ref.Replicas)
	return storage.ChunkRef{Size: ref.Size, Replicas: newReps}, changed, nil
}

// replicateExtra uploads data to up to `need` fresh providers not already in
// `used`, returning the new replicas (best-effort: fewer than need if providers
// are exhausted/failing).
func (b *Backend) replicateExtra(ctx context.Context, data []byte, used map[string]bool, need int) []storage.Replica {
	if need <= 0 {
		return nil
	}
	filename := fmt.Sprintf("repair-%s.bin", shortHash(string(data[:min(len(data), 64)])))
	var out []storage.Replica
	for _, prov := range b.pool.candidates(int64(len(data))) {
		if len(out) >= need {
			break
		}
		if used[prov.Name()] {
			continue
		}
		loc, tok, err := prov.Upload(ctx, data, filename, "application/octet-stream")
		if err != nil {
			b.pool.markFailed(prov.Name())
			continue
		}
		b.pool.markHealthy(prov.Name())
		out = append(out, storage.Replica{Provider: prov.Name(), Locator: loc, DeleteToken: tok})
		used[prov.Name()] = true
	}
	return out
}

// anyDurable reports whether any of the replica providers is durable.
func anyDurable(p *pool, replicas []storage.Replica) bool {
	for _, r := range replicas {
		if prov := p.get(r.Provider); prov != nil && prov.Durable() {
			return true
		}
	}
	return false
}

// shortHash is the per-object blob-name prefix: the first 8 bytes of the key's
// SHA-256, hex-encoded. Keeps blob names opaque + collision-resistant without
// leaking the object key to the free host.
func shortHash(name string) string {
	sum := sha256.Sum256([]byte(name))
	return hex.EncodeToString(sum[:8])
}
