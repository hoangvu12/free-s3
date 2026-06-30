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

	"free-s3/internal/storage"
)

// Backend is the freehost storage.Backend: it chunks an object, replicates each
// chunk to R distinct providers, and reads chunks back from the first healthy
// replica.
type Backend struct {
	pool       *pool
	chunkSize  int64
	replicas   int // R
	uploadConc int
	logger     *slog.Logger
}

// Options bundles Backend construction.
type Options struct {
	Providers         []Provider
	ChunkSize         int64
	ReplicationFactor int
	UploadConcurrency int
	Logger            *slog.Logger
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
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	p := newPool(opts.Providers)
	if !p.hasDurable() {
		return nil, errors.New("freehost: at least one durable provider must be enabled")
	}
	return &Backend{
		pool:       p,
		chunkSize:  opts.ChunkSize,
		replicas:   opts.ReplicationFactor,
		uploadConc: opts.UploadConcurrency,
		logger:     logger,
	}, nil
}

// Upload splits body into chunkSize windows and replicates each window to R
// distinct providers. It streams the body chunk-by-chunk (one chunkSize buffer
// at a time) so object size is unbounded by memory.
func (b *Backend) Upload(ctx context.Context, name, contentType string, body io.Reader) ([]storage.Chunk, error) {
	nameHash := shortHash(name)
	buf := make([]byte, b.chunkSize)
	var chunks []storage.Chunk
	var offset int64

	for seq := 0; ; seq++ {
		n, rerr := io.ReadFull(body, buf)
		if n > 0 {
			data := make([]byte, n)
			copy(data, buf[:n])
			chunk, err := b.uploadChunk(ctx, nameHash, seq, offset, data, contentType)
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

// uploadChunk replicates one chunk to up to R distinct providers, walking the
// candidate list (durable-first) in concurrency-bounded waves and falling
// through to fresh providers when one fails. It requires >= 1 successful
// replica (BUILD-PLAN §3 step 3); fewer than R is tolerated (best-effort
// durability), but zero fails the whole upload.
func (b *Backend) uploadChunk(ctx context.Context, nameHash string, seq int, offset int64, data []byte, contentType string) (storage.Chunk, error) {
	size := int64(len(data))
	cands := b.pool.candidates(size)
	if len(cands) == 0 {
		return storage.Chunk{}, fmt.Errorf("freehost: no provider can hold a %d-byte chunk", size)
	}
	filename := fmt.Sprintf("%s.%d.bin", nameHash, seq)

	var replicas []storage.Replica
	i := 0
	for i < len(cands) && len(replicas) < b.replicas {
		need := b.replicas - len(replicas)
		waveSize := need
		if waveSize > b.uploadConc {
			waveSize = b.uploadConc
		}
		if waveSize > len(cands)-i {
			waveSize = len(cands) - i
		}
		wave := cands[i : i+waveSize]
		i += waveSize

		type result struct {
			rep storage.Replica
			ok  bool
		}
		results := make([]result, len(wave))
		var wg sync.WaitGroup
		for j, prov := range wave {
			wg.Add(1)
			go func(j int, prov Provider) {
				defer wg.Done()
				loc, tok, err := prov.Upload(ctx, data, filename, contentType)
				if err != nil {
					b.pool.markFailed(prov.Name())
					b.logger.Warn("freehost: chunk replica upload failed", "provider", prov.Name(), "seq", seq, "error", err)
					return
				}
				b.pool.markHealthy(prov.Name())
				results[j] = result{rep: storage.Replica{Provider: prov.Name(), Locator: loc, DeleteToken: tok}, ok: true}
			}(j, prov)
		}
		wg.Wait()
		for _, r := range results {
			if r.ok {
				replicas = append(replicas, r.rep)
			}
		}
	}

	if len(replicas) == 0 {
		return storage.Chunk{}, fmt.Errorf("freehost: all providers failed for chunk %d", seq)
	}
	if !anyDurable(b.pool, replicas) {
		b.logger.Warn("freehost: chunk has no durable replica", "seq", seq, "replicas", len(replicas))
	}
	return storage.Chunk{Seq: seq, Size: size, Offset: offset, Replicas: replicas}, nil
}

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
