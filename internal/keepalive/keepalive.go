// Package keepalive runs the periodic TTL-refresh + self-heal sweep. Free hosts
// prune inactive files and temp tiers expire, so the sweep re-reads a rotating
// sample of chunks (resetting access-extended TTLs as a side effect) and repairs
// any chunk that has dropped below the replication factor by re-uploading from a
// surviving replica. It decouples the metadata store from the storage backend
// via the Repairer interface so it imports neither's internals.
package keepalive

import (
	"context"
	"log/slog"
	"time"

	"free-s3/internal/metadata"
	"free-s3/internal/storage"
)

// Repairer verifies + refills a chunk's replicas. *freehost.Backend implements
// it; the sweeper persists whatever it returns.
type Repairer interface {
	RepairChunk(ctx context.Context, ref storage.ChunkRef) (storage.ChunkRef, bool, error)
}

// Sweeper walks the object namespace in rotating windows, repairing chunks.
type Sweeper struct {
	store    *metadata.Store
	repairer Repairer
	interval time.Duration
	sample   int // objects processed per tick (0 = all)
	logger   *slog.Logger

	cursor int // rotating offset into the object-key list across ticks
}

// New constructs a Sweeper. sample <= 0 processes every object each tick (fine
// for small deployments); a positive sample pages through the namespace
// `sample` objects at a time so each tick is bounded.
func New(store *metadata.Store, repairer Repairer, interval time.Duration, sample int, logger *slog.Logger) *Sweeper {
	if logger == nil {
		logger = slog.Default()
	}
	return &Sweeper{store: store, repairer: repairer, interval: interval, sample: sample, logger: logger}
}

// Run sweeps on a ticker until ctx is cancelled. A non-positive interval
// disables the sweep (returns immediately).
func (s *Sweeper) Run(ctx context.Context) {
	if s.interval <= 0 {
		return
	}
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.RunOnce(ctx); err != nil && ctx.Err() == nil {
				s.logger.Warn("keepalive: sweep failed", "error", err)
			}
		}
	}
}

// RunOnce processes one window of objects: verify + repair every chunk, persist
// changes. Returns the first fatal error (per-chunk repair failures are logged,
// not fatal — a chunk with all replicas dead is unrecoverable and must not stop
// the sweep).
func (s *Sweeper) RunOnce(ctx context.Context) error {
	keys, err := s.store.AllObjectKeys(ctx)
	if err != nil {
		return err
	}
	if len(keys) == 0 {
		return nil
	}

	window := keys
	if s.sample > 0 && s.sample < len(keys) {
		start := s.cursor % len(keys)
		window = make([]metadata.ObjectKey, 0, s.sample)
		for i := 0; i < s.sample; i++ {
			window = append(window, keys[(start+i)%len(keys)])
		}
		s.cursor = (start + s.sample) % len(keys)
	}

	var repaired, lost int
	for _, k := range window {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		chunks, err := s.store.GetObjectChunks(ctx, k.Bucket, k.Key)
		if err != nil {
			s.logger.Warn("keepalive: load chunks failed", "bucket", k.Bucket, "key", k.Key, "error", err)
			continue
		}
		for _, c := range chunks {
			ref := storage.ChunkRef{Size: c.Size, Replicas: toStorageReplicas(c.Replicas)}
			newRef, changed, rerr := s.repairer.RepairChunk(ctx, ref)
			if rerr != nil {
				lost++
				s.logger.Warn("keepalive: chunk unrecoverable", "bucket", k.Bucket, "key", k.Key, "seq", c.Seq, "error", rerr)
				continue
			}
			if !changed {
				continue
			}
			if err := s.store.UpdateChunkReplicas(ctx, k.Bucket, k.Key, c.Seq, fromStorageReplicas(newRef.Replicas)); err != nil {
				s.logger.Warn("keepalive: persist repaired replicas failed", "bucket", k.Bucket, "key", k.Key, "seq", c.Seq, "error", err)
				continue
			}
			repaired++
		}
	}
	if repaired > 0 || lost > 0 {
		s.logger.Info("keepalive: sweep complete", "objects", len(window), "chunks_repaired", repaired, "chunks_lost", lost)
	}
	return nil
}

// toStorageReplicas converts persisted replicas to the backend shape, listing
// alive ones first (so RepairChunk verifies live copies before dead ones).
func toStorageReplicas(reps []metadata.Replica) []storage.Replica {
	out := make([]storage.Replica, 0, len(reps))
	for _, r := range reps {
		if r.Alive {
			out = append(out, storage.Replica{Provider: r.Provider, Locator: r.Locator, DeleteToken: r.DeleteToken})
		}
	}
	for _, r := range reps {
		if !r.Alive {
			out = append(out, storage.Replica{Provider: r.Provider, Locator: r.Locator, DeleteToken: r.DeleteToken})
		}
	}
	return out
}

// fromStorageReplicas converts a repaired replica set back to the persistence
// shape; every returned replica is live (verified or freshly uploaded).
func fromStorageReplicas(reps []storage.Replica) []metadata.Replica {
	out := make([]metadata.Replica, len(reps))
	for i, r := range reps {
		out[i] = metadata.Replica{Provider: r.Provider, Locator: r.Locator, DeleteToken: r.DeleteToken, Alive: true}
	}
	return out
}
