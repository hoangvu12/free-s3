package freehost

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"free-s3/internal/storage"
)

// fakeProvider is an in-memory Provider for backend tests.
type fakeProvider struct {
	name       string
	durable    bool
	max        int64
	failUpload bool

	mu      sync.Mutex
	blobs   map[string][]byte
	deleted map[string]bool
	uploads int
	seq     int
}

func newFakeProvider(name string, durable bool, max int64) *fakeProvider {
	return &fakeProvider{name: name, durable: durable, max: max, blobs: map[string][]byte{}, deleted: map[string]bool{}}
}

func (f *fakeProvider) Name() string    { return f.name }
func (f *fakeProvider) MaxBytes() int64 { return f.max }
func (f *fakeProvider) Durable() bool   { return f.durable }

func (f *fakeProvider) Upload(_ context.Context, data []byte, filename, _ string) (string, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.uploads++
	if f.failUpload {
		return "", "", errors.New(f.name + ": synthetic upload failure")
	}
	f.seq++
	loc := fmt.Sprintf("%s://%s#%d", f.name, filename, f.seq)
	cp := make([]byte, len(data))
	copy(cp, data)
	f.blobs[loc] = cp
	return loc, "tok-" + f.name, nil
}

func (f *fakeProvider) Download(_ context.Context, locator string, offset, length int64) (io.ReadCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.blobs[locator]
	if !ok {
		return nil, errors.New("404 " + locator)
	}
	if offset > int64(len(b)) {
		offset = int64(len(b))
	}
	b = b[offset:]
	if length > 0 && length < int64(len(b)) {
		b = b[:length]
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

func (f *fakeProvider) Delete(_ context.Context, locator, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted[locator] = true
	delete(f.blobs, locator)
	return nil
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newTestBackend(t *testing.T, r int, chunkSize int64, provs ...Provider) *Backend {
	t.Helper()
	b, err := New(Options{Providers: provs, ChunkSize: chunkSize, ReplicationFactor: r, UploadConcurrency: 4, Logger: discardLogger()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return b
}

// refOf builds a ChunkRef from an uploaded chunk (the handler does this via
// metaChunkRef in production).
func refOf(c storage.Chunk) storage.ChunkRef {
	return storage.ChunkRef{Size: c.Size, Replicas: c.Replicas}
}

func TestBackendChunkAndReplicate(t *testing.T) {
	p1 := newFakeProvider("ia", true, 1<<30)
	p2 := newFakeProvider("fileditch", true, 1<<30)
	p3 := newFakeProvider("catbox", true, 1<<30)
	b := newTestBackend(t, 3, 10, p1, p2, p3)

	payload := []byte("abcdefghijklmnopqrstuvwxy") // 25 bytes -> chunks of 10,10,5
	chunks, err := b.Upload(context.Background(), "bucket/key", "application/octet-stream", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	if len(chunks) != 3 {
		t.Fatalf("chunks = %d, want 3", len(chunks))
	}
	wantSizes := []int64{10, 10, 5}
	wantOffsets := []int64{0, 10, 20}
	var reassembled []byte
	for i, c := range chunks {
		if c.Size != wantSizes[i] || c.Offset != wantOffsets[i] || c.Seq != i {
			t.Fatalf("chunk %d = %+v, want size %d offset %d", i, c, wantSizes[i], wantOffsets[i])
		}
		if len(c.Replicas) != 3 {
			t.Fatalf("chunk %d has %d replicas, want 3", i, len(c.Replicas))
		}
		// Replicas must be on distinct providers, >= 1 durable.
		seen := map[string]bool{}
		durable := false
		for _, rep := range c.Replicas {
			if seen[rep.Provider] {
				t.Fatalf("chunk %d duplicate provider %s", i, rep.Provider)
			}
			seen[rep.Provider] = true
			if b.pool.get(rep.Provider).Durable() {
				durable = true
			}
		}
		if !durable {
			t.Fatalf("chunk %d has no durable replica", i)
		}
		// Download reassembles.
		rc, err := b.Download(context.Background(), refOf(c))
		if err != nil {
			t.Fatalf("download chunk %d: %v", i, err)
		}
		got, _ := io.ReadAll(rc)
		rc.Close()
		reassembled = append(reassembled, got...)
	}
	if !bytes.Equal(reassembled, payload) {
		t.Fatalf("reassembled %q != %q", reassembled, payload)
	}
}

func TestBackendUploadFallsThroughFailedProvider(t *testing.T) {
	d := newFakeProvider("ia", true, 1<<30)
	bad := newFakeProvider("badtemp", false, 1<<30)
	bad.failUpload = true
	good := newFakeProvider("goodtemp", false, 1<<30)
	b := newTestBackend(t, 2, 100, d, bad, good)

	chunks, err := b.Upload(context.Background(), "k", "application/octet-stream", strings.NewReader("hello world"))
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	if len(chunks) != 1 || len(chunks[0].Replicas) != 2 {
		t.Fatalf("chunks=%+v", chunks)
	}
	for _, rep := range chunks[0].Replicas {
		if rep.Provider == "badtemp" {
			t.Fatalf("failed provider made it into replicas: %+v", chunks[0].Replicas)
		}
	}
}

func TestBackendUploadFailsWhenAllProvidersFail(t *testing.T) {
	d := newFakeProvider("ia", true, 1<<30)
	d.failUpload = true
	b := newTestBackend(t, 1, 100, d)
	if _, err := b.Upload(context.Background(), "k", "ct", strings.NewReader("data")); err == nil {
		t.Fatal("expected upload error when every provider fails")
	}
}

func TestBackendDownloadFailsOverDeadReplica(t *testing.T) {
	p1 := newFakeProvider("ia", true, 1<<30)
	p2 := newFakeProvider("fileditch", true, 1<<30)
	b := newTestBackend(t, 2, 100, p1, p2)
	chunks, err := b.Upload(context.Background(), "k", "ct", strings.NewReader("payload-bytes"))
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	ref := refOf(chunks[0])

	// Kill the first replica's blob; Download must fail over to the second.
	first := ref.Replicas[0]
	b.pool.get(first.Provider).(*fakeProvider).Delete(context.Background(), first.Locator, "")

	rc, err := b.Download(context.Background(), ref)
	if err != nil {
		t.Fatalf("download after killing replica 0: %v", err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if string(got) != "payload-bytes" {
		t.Fatalf("failover read = %q", got)
	}
}

func TestBackendDeleteRemovesAllReplicas(t *testing.T) {
	p1 := newFakeProvider("ia", true, 1<<30)
	p2 := newFakeProvider("fileditch", true, 1<<30)
	b := newTestBackend(t, 2, 100, p1, p2)
	chunks, err := b.Upload(context.Background(), "k", "ct", strings.NewReader("xyz"))
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	if err := b.Delete(context.Background(), refOf(chunks[0])); err != nil {
		t.Fatalf("delete: %v", err)
	}
	for _, p := range []*fakeProvider{p1, p2} {
		p.mu.Lock()
		n := len(p.blobs)
		p.mu.Unlock()
		if n != 0 {
			t.Fatalf("provider %s still holds %d blobs after delete", p.name, n)
		}
	}
}

func TestNewRequiresDurableProvider(t *testing.T) {
	temp := newFakeProvider("temp", false, 1<<30)
	if _, err := New(Options{Providers: []Provider{temp}, ChunkSize: 100, ReplicationFactor: 1, Logger: discardLogger()}); err == nil {
		t.Fatal("expected error: no durable provider")
	}
	if _, err := New(Options{Providers: nil, ChunkSize: 100, ReplicationFactor: 1, Logger: discardLogger()}); err == nil {
		t.Fatal("expected error: no providers")
	}
}

func TestBackendSkipsProvidersTooSmallForChunk(t *testing.T) {
	big := newFakeProvider("ia", true, 1<<30)
	small := newFakeProvider("tiny", true, 4) // can't hold a 10-byte chunk
	b := newTestBackend(t, 2, 10, big, small)
	chunks, err := b.Upload(context.Background(), "k", "ct", strings.NewReader("0123456789")) // 1 chunk of 10
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	// Only "ia" can hold a 10-byte chunk, so we get 1 replica (R=2 not reachable).
	if len(chunks) != 1 || len(chunks[0].Replicas) != 1 || chunks[0].Replicas[0].Provider != "ia" {
		t.Fatalf("chunks=%+v, want single ia replica", chunks)
	}
}
