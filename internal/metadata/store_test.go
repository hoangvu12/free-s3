package metadata

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func rep(provider, locator, token string) Replica {
	return Replica{Provider: provider, Locator: locator, DeleteToken: token, Alive: true}
}

func TestStoreChunksRoundTrip(t *testing.T) {
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	// WAL must be active.
	var mode string
	if err := s.read.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&mode); err != nil {
		t.Fatalf("pragma: %v", err)
	}
	if strings.ToLower(mode) != "wal" {
		t.Fatalf("journal_mode = %q, want wal", mode)
	}

	if err := s.CreateBucket(ctx, "send"); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	// Two chunks, each replicated to R distinct providers — the round-trip
	// must preserve chunk order (by seq) and per-chunk replica order (by idx).
	chunks := []Chunk{
		{Seq: 0, Size: 18, Offset: 0, Replicas: []Replica{
			rep("ia", "https://archive.org/download/item/obj.0.bin", ""),
			rep("fileditch", "https://fileditch.com/f/0", ""),
			rep("catbox", "https://files.catbox.moe/aa.bin", ""),
		}},
		{Seq: 1, Size: 7, Offset: 18, Replicas: []Replica{
			rep("ia", "https://archive.org/download/item/obj.1.bin", ""),
			rep("x0.at", "https://x0.at/abcd.bin", "tok-123"),
		}},
	}
	obj := Object{Bucket: "send", Key: "a/b.bin", Size: 25, ETag: "etag",
		ContentType: "application/octet-stream"}
	if err := s.PutObject(ctx, obj, chunks); err != nil {
		t.Fatalf("put: %v", err)
	}

	got, err := s.GetObject(ctx, "send", "a/b.bin")
	if err != nil || got.Size != 25 || got.ETag != "etag" {
		t.Fatalf("get object = %+v, %v", got, err)
	}
	gc, err := s.GetObjectChunks(ctx, "send", "a/b.bin")
	if err != nil {
		t.Fatalf("get chunks: %v", err)
	}
	if !reflect.DeepEqual(gc, chunks) {
		t.Fatalf("chunks =\n %+v\nwant\n %+v", gc, chunks)
	}
	// Spot-check the delete token survived on the x0.at replica.
	if gc[1].Replicas[1].DeleteToken != "tok-123" {
		t.Fatalf("delete token lost: %+v", gc[1].Replicas[1])
	}

	// Overwrite must replace the whole chunk map + replicas atomically.
	if err := s.PutObject(ctx, Object{Bucket: "send", Key: "a/b.bin", Size: 5, ETag: "e2",
		ContentType: "text/plain"},
		[]Chunk{{Seq: 0, Size: 5, Offset: 0, Replicas: []Replica{rep("gofile", "https://gofile.io/d/zzz", "")}}}); err != nil {
		t.Fatalf("overwrite: %v", err)
	}
	gc2, _ := s.GetObjectChunks(ctx, "send", "a/b.bin")
	if len(gc2) != 1 || len(gc2[0].Replicas) != 1 || gc2[0].Replicas[0].Provider != "gofile" {
		t.Fatalf("after overwrite chunks = %+v, want single gofile replica", gc2)
	}
	if o2, _ := s.GetObject(ctx, "send", "a/b.bin"); o2.Size != 5 {
		t.Fatalf("after overwrite size = %d, want 5", o2.Size)
	}
	// No orphaned replica rows from the prior 5-replica version.
	var nrep int
	if err := s.read.QueryRowContext(ctx, `SELECT COUNT(*) FROM chunk_replicas WHERE bucket='send' AND key='a/b.bin'`).Scan(&nrep); err != nil {
		t.Fatalf("count replicas: %v", err)
	}
	if nrep != 1 {
		t.Fatalf("orphaned replicas after overwrite: %d rows, want 1", nrep)
	}

	// Hard delete removes object row AND chunk + replica rows.
	if err := s.DeleteObject(ctx, "send", "a/b.bin"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := s.GetObject(ctx, "send", "a/b.bin"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get after delete = %v, want ErrNotFound", err)
	}
	if gc3, _ := s.GetObjectChunks(ctx, "send", "a/b.bin"); len(gc3) != 0 {
		t.Fatalf("chunks not hard-deleted: %+v", gc3)
	}
	if err := s.read.QueryRowContext(ctx, `SELECT COUNT(*) FROM chunk_replicas WHERE bucket='send' AND key='a/b.bin'`).Scan(&nrep); err != nil {
		t.Fatalf("count replicas: %v", err)
	}
	if nrep != 0 {
		t.Fatalf("replica rows not hard-deleted: %d", nrep)
	}

	// Empty object (size 0): no chunk rows.
	if err := s.PutObject(ctx, Object{Bucket: "send", Key: "empty.bin", Size: 0, ETag: "d41d", ContentType: "application/octet-stream"}, nil); err != nil {
		t.Fatalf("put empty: %v", err)
	}
	if lc, _ := s.GetObjectChunks(ctx, "send", "empty.bin"); len(lc) != 0 {
		t.Fatalf("empty object must have empty chunk list, got %+v", lc)
	}

	// DELETE is idempotent on a missing key.
	if err := s.DeleteObject(ctx, "send", "does-not-exist"); err != nil {
		t.Fatalf("delete missing key: %v", err)
	}
}

func TestMarkReplicaDead(t *testing.T) {
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()
	if err := s.CreateBucket(ctx, "b"); err != nil {
		t.Fatalf("bucket: %v", err)
	}
	chunks := []Chunk{{Seq: 0, Size: 10, Offset: 0, Replicas: []Replica{
		rep("ia", "loc0", ""), rep("catbox", "loc1", ""),
	}}}
	if err := s.PutObject(ctx, Object{Bucket: "b", Key: "k", Size: 10, ETag: "e", ContentType: "application/octet-stream"}, chunks); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := s.MarkReplicaDead(ctx, "b", "k", 0, 1); err != nil {
		t.Fatalf("mark dead: %v", err)
	}
	gc, _ := s.GetObjectChunks(ctx, "b", "k")
	if len(gc) != 1 || len(gc[0].Replicas) != 2 {
		t.Fatalf("chunks = %+v", gc)
	}
	if !gc[0].Replicas[0].Alive || gc[0].Replicas[1].Alive {
		t.Fatalf("alive flags wrong: %+v", gc[0].Replicas)
	}
}

// TestAppendChunkReplica covers the async background-replication write-back:
// an append with the matching ETag adds a replica; a stale ETag or a missing
// object is a safe no-op; and appending the same provider twice is idempotent.
func TestAppendChunkReplica(t *testing.T) {
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()
	if err := s.CreateBucket(ctx, "b"); err != nil {
		t.Fatalf("bucket: %v", err)
	}
	chunks := []Chunk{{Seq: 0, Size: 5, Offset: 0, Replicas: []Replica{rep("x0.at", "https://x0.at/a", "")}}}
	if err := s.PutObject(ctx, Object{Bucket: "b", Key: "k", Size: 5, ETag: "e1", ContentType: "application/octet-stream"}, chunks); err != nil {
		t.Fatalf("put: %v", err)
	}

	// Matching ETag → appended (idx assigned, provider now present).
	if err := s.AppendChunkReplica(ctx, "b", "k", 0, rep("ia", "https://archive.org/x", ""), "e1"); err != nil {
		t.Fatalf("append (match): %v", err)
	}
	// Idempotent: same provider again is a no-op (not a duplicate row).
	if err := s.AppendChunkReplica(ctx, "b", "k", 0, rep("ia", "https://archive.org/x", ""), "e1"); err != nil {
		t.Fatalf("append (dup): %v", err)
	}
	// Stale ETag → dropped (object was "overwritten").
	if err := s.AppendChunkReplica(ctx, "b", "k", 0, rep("catbox", "https://files.catbox.moe/y", ""), "STALE"); err != nil {
		t.Fatalf("append (stale): %v", err)
	}
	// Missing object → dropped.
	if err := s.AppendChunkReplica(ctx, "b", "missing", 0, rep("catbox", "https://files.catbox.moe/z", ""), "e1"); err != nil {
		t.Fatalf("append (missing): %v", err)
	}

	gc, err := s.GetObjectChunks(ctx, "b", "k")
	if err != nil {
		t.Fatalf("get chunks: %v", err)
	}
	provs := map[string]int{}
	for _, r := range gc[0].Replicas {
		provs[r.Provider]++
	}
	if len(gc[0].Replicas) != 2 || provs["x0.at"] != 1 || provs["ia"] != 1 || provs["catbox"] != 0 {
		t.Fatalf("replicas = %+v; want exactly x0.at + ia (stale/missing dropped)", gc[0].Replicas)
	}
}

func TestMultipartPartChunksRoundTrip(t *testing.T) {
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()
	if err := s.CreateBucket(ctx, "b"); err != nil {
		t.Fatalf("bucket: %v", err)
	}
	if err := s.CreateMultipartUpload(ctx, "up1", "b", "k", "application/octet-stream"); err != nil {
		t.Fatalf("create mpu: %v", err)
	}
	part1 := []Chunk{
		{Seq: 0, Size: 100, Replicas: []Replica{rep("ia", "p1c0a", ""), rep("fileditch", "p1c0b", "")}},
		{Seq: 1, Size: 50, Replicas: []Replica{rep("ia", "p1c1a", "")}},
	}
	part2 := []Chunk{
		{Seq: 0, Size: 30, Replicas: []Replica{rep("catbox", "p2c0a", "del"), rep("x0.at", "p2c0b", "tok")}},
	}
	if err := s.PutMultipartPart(ctx, "up1", 1, "etag1", 150, part1); err != nil {
		t.Fatalf("put part1: %v", err)
	}
	if err := s.PutMultipartPart(ctx, "up1", 2, "etag2", 30, part2); err != nil {
		t.Fatalf("put part2: %v", err)
	}

	gp1, _ := s.GetMultipartPartChunks(ctx, "up1", 1)
	if !reflect.DeepEqual(gp1, part1) {
		t.Fatalf("part1 = %+v want %+v", gp1, part1)
	}

	all, err := s.AllMultipartChunks(ctx, "up1")
	if err != nil {
		t.Fatalf("all chunks: %v", err)
	}
	// 2 chunks in part1 + 1 in part2 = 3 chunks, 5 replicas total.
	if len(all) != 3 {
		t.Fatalf("all chunks = %d, want 3", len(all))
	}
	var totReplicas int
	for _, c := range all {
		totReplicas += len(c.Replicas)
	}
	if totReplicas != 5 {
		t.Fatalf("total replicas = %d, want 5", totReplicas)
	}

	// Re-uploading part 1 replaces it (no replica leakage).
	if err := s.PutMultipartPart(ctx, "up1", 1, "etag1b", 10, []Chunk{{Seq: 0, Size: 10, Replicas: []Replica{rep("temp.sh", "new", "")}}}); err != nil {
		t.Fatalf("re-put part1: %v", err)
	}
	all2, _ := s.AllMultipartChunks(ctx, "up1")
	totReplicas = 0
	for _, c := range all2 {
		totReplicas += len(c.Replicas)
	}
	// part1 now 1 chunk/1 replica + part2 1 chunk/2 replicas = 3 replicas.
	if totReplicas != 3 {
		t.Fatalf("after re-put total replicas = %d, want 3", totReplicas)
	}

	// Finalize assembles the object from the parts' chunks.
	final := []Chunk{
		{Seq: 0, Size: 10, Offset: 0, Replicas: []Replica{rep("temp.sh", "new", "")}},
		{Seq: 1, Size: 30, Offset: 10, Replicas: []Replica{rep("catbox", "p2c0a", "del"), rep("x0.at", "p2c0b", "tok")}},
	}
	obj := Object{Bucket: "b", Key: "k", Size: 40, ETag: "final-2", ContentType: "application/octet-stream"}
	if err := s.FinalizeMultipartUpload(ctx, obj, final, "up1"); err != nil {
		t.Fatalf("finalize: %v", err)
	}
	gc, _ := s.GetObjectChunks(ctx, "b", "k")
	if !reflect.DeepEqual(gc, final) {
		t.Fatalf("finalized chunks = %+v want %+v", gc, final)
	}
	// Multipart bookkeeping is gone.
	if _, err := s.GetMultipartUpload(ctx, "up1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("mpu not deleted after finalize: %v", err)
	}
	if leftover, _ := s.AllMultipartChunks(ctx, "up1"); len(leftover) != 0 {
		t.Fatalf("multipart chunk rows leaked after finalize: %+v", leftover)
	}
}
