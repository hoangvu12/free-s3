package keepalive

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"free-s3/internal/metadata"
	"free-s3/internal/storage"
)

// fakeRepairer drives RepairChunk from a closure so tests can simulate
// refill / no-op / unrecoverable outcomes.
type fakeRepairer struct {
	fn func(ref storage.ChunkRef) (storage.ChunkRef, bool, error)
}

func (f fakeRepairer) RepairChunk(_ context.Context, ref storage.ChunkRef) (storage.ChunkRef, bool, error) {
	return f.fn(ref)
}

func discard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func seedObject(t *testing.T, s *metadata.Store, bucket, key string, replicas []metadata.Replica) {
	t.Helper()
	ctx := context.Background()
	if err := s.CreateBucket(ctx, bucket); err != nil {
		// bucket may already exist across calls in a test
		_ = err
	}
	obj := metadata.Object{Bucket: bucket, Key: key, Size: 10, ETag: "e", ContentType: "application/octet-stream"}
	chunk := metadata.Chunk{Seq: 0, Size: 10, Offset: 0, Replicas: replicas}
	if err := s.PutObject(ctx, obj, []metadata.Chunk{chunk}); err != nil {
		t.Fatalf("put: %v", err)
	}
}

func aliveRep(provider string) metadata.Replica {
	return metadata.Replica{Provider: provider, Locator: provider + "://loc", Alive: true}
}

func TestSweeperRefillsAndPersists(t *testing.T) {
	store, err := metadata.Open(filepath.Join(t.TempDir(), "k.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	seedObject(t, store, "b", "k", []metadata.Replica{aliveRep("ia"), aliveRep("fileditch")})

	// Repairer adds a third replica and reports a change.
	repairer := fakeRepairer{fn: func(ref storage.ChunkRef) (storage.ChunkRef, bool, error) {
		reps := append([]storage.Replica(nil), ref.Replicas...)
		reps = append(reps, storage.Replica{Provider: "catbox", Locator: "catbox://new"})
		return storage.ChunkRef{Size: ref.Size, Replicas: reps}, true, nil
	}}

	s := New(store, repairer, 0, 0, discard())
	if err := s.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	chunks, _ := store.GetObjectChunks(context.Background(), "b", "k")
	if len(chunks) != 1 || len(chunks[0].Replicas) != 3 {
		t.Fatalf("after sweep replicas = %+v, want 3", chunks)
	}
	for _, r := range chunks[0].Replicas {
		if !r.Alive {
			t.Fatalf("persisted replica not alive: %+v", r)
		}
	}
}

func TestSweeperNoopLeavesReplicasUnchanged(t *testing.T) {
	store, err := metadata.Open(filepath.Join(t.TempDir(), "k.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	seedObject(t, store, "b", "k", []metadata.Replica{aliveRep("ia"), aliveRep("fileditch")})

	calls := 0
	repairer := fakeRepairer{fn: func(ref storage.ChunkRef) (storage.ChunkRef, bool, error) {
		calls++
		return ref, false, nil // healthy
	}}
	s := New(store, repairer, 0, 0, discard())
	if err := s.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if calls != 1 {
		t.Fatalf("RepairChunk called %d times, want 1", calls)
	}
	chunks, _ := store.GetObjectChunks(context.Background(), "b", "k")
	if len(chunks[0].Replicas) != 2 {
		t.Fatalf("no-op changed replicas: %+v", chunks[0].Replicas)
	}
}

func TestSweeperToleratesUnrecoverableChunk(t *testing.T) {
	store, err := metadata.Open(filepath.Join(t.TempDir(), "k.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	seedObject(t, store, "b", "k1", []metadata.Replica{aliveRep("ia")})
	seedObject(t, store, "b", "k2", []metadata.Replica{aliveRep("fileditch")})

	repaired := 0
	repairer := fakeRepairer{fn: func(ref storage.ChunkRef) (storage.ChunkRef, bool, error) {
		// k1's chunk is unrecoverable; k2 succeeds.
		if ref.Replicas[0].Provider == "ia" {
			return ref, false, errors.New("all replicas dead")
		}
		repaired++
		reps := append(append([]storage.Replica(nil), ref.Replicas...), storage.Replica{Provider: "catbox", Locator: "x"})
		return storage.ChunkRef{Size: ref.Size, Replicas: reps}, true, nil
	}}
	s := New(store, repairer, 0, 0, discard())
	if err := s.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce must not fail on one unrecoverable chunk: %v", err)
	}
	if repaired != 1 {
		t.Fatalf("repaired %d, want 1 (k2)", repaired)
	}
	// k2 was still repaired despite k1 being unrecoverable.
	c2, _ := store.GetObjectChunks(context.Background(), "b", "k2")
	if len(c2[0].Replicas) != 2 {
		t.Fatalf("k2 not repaired: %+v", c2[0].Replicas)
	}
}
