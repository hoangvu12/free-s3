package s3api

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"free-s3/internal/config"
	"free-s3/internal/metadata"
	"free-s3/internal/storage/freehost"
)

// fhRig wires the real freehost backend (over in-memory providers) behind the
// S3 handler so the full stack can be exercised end-to-end without network.
type fhRig struct {
	t     *testing.T
	h     *Handler
	store *metadata.Store
	provs []*inMemProvider
}

func newFreehostRig(t *testing.T, chunkSize int64, r int) *fhRig {
	t.Helper()
	store, err := metadata.Open(filepath.Join(t.TempDir(), "fh.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	provs := []*inMemProvider{
		newInMemProvider("ia", true),
		newInMemProvider("fileditch", true),
		newInMemProvider("catbox", true),
	}
	backend, err := freehost.New(freehost.Options{
		Providers:         []freehost.Provider{provs[0], provs[1], provs[2]},
		ChunkSize:         chunkSize,
		ReplicationFactor: r,
		UploadConcurrency: 4,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	cfg := config.Config{
		AccessKeyID: testAK, SecretAccessKey: testSecret,
		StreamConcurrency: 4, StreamBuffers: 8, StreamChunkSize: chunkSize, ChunkTimeout: 30 * time.Second,
	}
	h := NewHandler(cfg, store, backend, slog.New(slog.NewTextHandler(io.Discard, nil)))
	return &fhRig{t: t, h: h, store: store, provs: provs}
}

func (r *fhRig) do(method, target string, body []byte, headers map[string]string) *httptest.ResponseRecorder {
	r.t.Helper()
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req := httptest.NewRequest(method, "http://example.com"+target, rdr)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	if method != http.MethodGet && method != http.MethodHead {
		signHeaderAuth(req, amz(time.Now().UTC()))
	}
	rec := httptest.NewRecorder()
	r.h.ServeHTTP(rec, req)
	return rec
}

// assertReplicaSpread checks every chunk of an object has exactly r distinct
// providers with >= 1 durable.
func (r *fhRig) assertReplicaSpread(bucket, key string, wantChunks, repFactor int) {
	r.t.Helper()
	chunks, err := r.store.GetObjectChunks(context.Background(), bucket, key)
	if err != nil {
		r.t.Fatalf("get chunks: %v", err)
	}
	if wantChunks > 0 && len(chunks) != wantChunks {
		r.t.Fatalf("chunks = %d, want %d", len(chunks), wantChunks)
	}
	durableNames := map[string]bool{"ia": true, "fileditch": true, "catbox": true}
	for _, c := range chunks {
		if len(c.Replicas) != repFactor {
			r.t.Fatalf("chunk %d has %d replicas, want %d", c.Seq, len(c.Replicas), repFactor)
		}
		seen := map[string]bool{}
		durable := false
		for _, rep := range c.Replicas {
			if seen[rep.Provider] {
				r.t.Fatalf("chunk %d duplicate provider %s", c.Seq, rep.Provider)
			}
			seen[rep.Provider] = true
			if durableNames[rep.Provider] {
				durable = true
			}
		}
		if !durable {
			r.t.Fatalf("chunk %d has no durable replica", c.Seq)
		}
	}
}

// inMemProvider implements freehost.Provider in memory so the full stack
// (s3api handler -> freehost chunk/replicate backend -> providers) can be
// exercised end-to-end without any network.
type inMemProvider struct {
	name    string
	durable bool
	mu      sync.Mutex
	blobs   map[string][]byte
	seq     int
}

func newInMemProvider(name string, durable bool) *inMemProvider {
	return &inMemProvider{name: name, durable: durable, blobs: map[string][]byte{}}
}

func (p *inMemProvider) Name() string    { return p.name }
func (p *inMemProvider) MaxBytes() int64 { return 1 << 40 }
func (p *inMemProvider) Durable() bool   { return p.durable }

func (p *inMemProvider) Upload(_ context.Context, data []byte, filename, _ string) (string, string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.seq++
	loc := fmt.Sprintf("%s://%s#%d", p.name, filename, p.seq)
	cp := make([]byte, len(data))
	copy(cp, data)
	p.blobs[loc] = cp
	return loc, "", nil
}

func (p *inMemProvider) Download(_ context.Context, locator string, offset, length int64) (io.ReadCloser, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	b, ok := p.blobs[locator]
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
	out := make([]byte, len(b))
	copy(out, b)
	return io.NopCloser(bytes.NewReader(out)), nil
}

func (p *inMemProvider) Delete(_ context.Context, locator, _ string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.blobs, locator)
	return nil
}

// TestFreehostEndToEnd drives a real freehost backend (R=3, tiny chunks) through
// the S3 handler: PUT a multi-chunk object, GET it back byte-identical, GET a
// sub-range, and assert the DB recorded R distinct replicas per chunk with at
// least one durable.
func TestFreehostEndToEnd(t *testing.T) {
	store, err := metadata.Open(filepath.Join(t.TempDir(), "e2e.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	p1 := newInMemProvider("ia", true)
	p2 := newInMemProvider("fileditch", true)
	p3 := newInMemProvider("catbox", true)
	backend, err := freehost.New(freehost.Options{
		Providers:         []freehost.Provider{p1, p2, p3},
		ChunkSize:         16, // tiny: force many chunks
		ReplicationFactor: 3,
		UploadConcurrency: 3,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("backend: %v", err)
	}

	cfg := config.Config{
		AccessKeyID: testAK, SecretAccessKey: testSecret,
		StreamConcurrency: 4, StreamBuffers: 8, StreamChunkSize: 8, ChunkTimeout: 30 * time.Second,
	}
	h := NewHandler(cfg, store, backend, slog.New(slog.NewTextHandler(io.Discard, nil)))

	do := func(method, target string, body []byte, headers map[string]string) *httptest.ResponseRecorder {
		var rdr io.Reader
		if body != nil {
			rdr = bytes.NewReader(body)
		}
		req := httptest.NewRequest(method, "http://example.com"+target, rdr)
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		if method != http.MethodGet && method != http.MethodHead {
			signHeaderAuth(req, amz(time.Now().UTC()))
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}

	if rec := do(http.MethodPut, "/bkt", nil, nil); rec.Code != http.StatusOK {
		t.Fatalf("create bucket: %d %s", rec.Code, rec.Body)
	}

	payload := make([]byte, 100) // 100 bytes / 16 = 7 chunks (last is 4)
	rand.Read(payload)
	if rec := do(http.MethodPut, "/bkt/obj.bin", payload, map[string]string{"Content-Type": "application/octet-stream"}); rec.Code != http.StatusOK {
		t.Fatalf("put: %d %s", rec.Code, rec.Body)
	}

	// Full GET round-trips byte-identical.
	rec := do(http.MethodGet, "/bkt/obj.bin", nil, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("get: %d %s", rec.Code, rec.Body)
	}
	if !bytes.Equal(rec.Body.Bytes(), payload) {
		t.Fatalf("round-trip mismatch: got %d bytes want %d", rec.Body.Len(), len(payload))
	}

	// Range GET [40, 70) returns the right slice.
	rec = do(http.MethodGet, "/bkt/obj.bin", nil, map[string]string{"Range": "bytes=40-69"})
	if rec.Code != http.StatusPartialContent {
		t.Fatalf("range get: %d %s", rec.Code, rec.Body)
	}
	if !bytes.Equal(rec.Body.Bytes(), payload[40:70]) {
		t.Fatalf("range mismatch")
	}

	// DB recorded R=3 distinct replicas per chunk, >= 1 durable.
	chunks, err := store.GetObjectChunks(context.Background(), "bkt", "obj.bin")
	if err != nil {
		t.Fatalf("get chunks: %v", err)
	}
	if len(chunks) != 7 {
		t.Fatalf("chunks = %d, want 7 (100 bytes / 16)", len(chunks))
	}
	durableProviders := map[string]bool{"ia": true, "fileditch": true, "catbox": true}
	for _, c := range chunks {
		if len(c.Replicas) != 3 {
			t.Fatalf("chunk %d has %d replicas, want 3", c.Seq, len(c.Replicas))
		}
		seen := map[string]bool{}
		durable := false
		for _, r := range c.Replicas {
			if seen[r.Provider] {
				t.Fatalf("chunk %d duplicate provider %s", c.Seq, r.Provider)
			}
			seen[r.Provider] = true
			if durableProviders[r.Provider] {
				durable = true
			}
		}
		if !durable {
			t.Fatalf("chunk %d has no durable replica", c.Seq)
		}
	}

	// DELETE reaps every replica blob across all providers.
	if rec := do(http.MethodDelete, "/bkt/obj.bin", nil, nil); rec.Code != http.StatusNoContent {
		t.Fatalf("delete: %d %s", rec.Code, rec.Body)
	}
	for _, p := range []*inMemProvider{p1, p2, p3} {
		p.mu.Lock()
		n := len(p.blobs)
		p.mu.Unlock()
		if n != 0 {
			t.Fatalf("provider %s still holds %d blobs after object delete", p.name, n)
		}
	}
}

// TestFreehostRangeSweep verifies range GETs are correct through the
// parallel-prefetch reader + replica fallback, across stored-chunk boundaries
// (chunkSize 16, object 200 bytes -> 13 chunks). Covers cross-boundary, single
// byte, prefix, suffix, and full-tail ranges.
func TestFreehostRangeSweep(t *testing.T) {
	r := newFreehostRig(t, 16, 3)
	if rec := r.do(http.MethodPut, "/b", nil, nil); rec.Code != http.StatusOK {
		t.Fatalf("create bucket: %d %s", rec.Code, rec.Body)
	}
	payload := make([]byte, 200)
	for i := range payload {
		payload[i] = byte(i)
	}
	if rec := r.do(http.MethodPut, "/b/obj", payload, nil); rec.Code != http.StatusOK {
		t.Fatalf("put: %d %s", rec.Code, rec.Body)
	}
	r.assertReplicaSpread("b", "obj", 13, 3) // 200/16 = 12.5 -> 13 chunks

	cases := []struct {
		rangeHdr string
		want     []byte
	}{
		{"bytes=0-0", payload[0:1]},
		{"bytes=15-16", payload[15:17]}, // straddles chunk 0/1 boundary
		{"bytes=30-49", payload[30:50]}, // spans chunks 1,2,3
		{"bytes=100-199", payload[100:200]},
		{"bytes=-10", payload[190:200]}, // suffix range
		{"bytes=199-199", payload[199:200]},
	}
	for _, c := range cases {
		rec := r.do(http.MethodGet, "/b/obj", nil, map[string]string{"Range": c.rangeHdr})
		if rec.Code != http.StatusPartialContent {
			t.Fatalf("range %q: status %d, want 206 (%s)", c.rangeHdr, rec.Code, rec.Body)
		}
		if !bytes.Equal(rec.Body.Bytes(), c.want) {
			t.Fatalf("range %q: got %v want %v", c.rangeHdr, rec.Body.Bytes(), c.want)
		}
	}
}

// TestFreehostMultipartEndToEnd uploads a multi-part, multi-chunk object via the
// S3 multipart API backed by the real freehost backend, completes it, GETs it
// back byte-identical, and asserts the assembled object's chunks are each
// replicated R times across distinct providers.
func TestFreehostMultipartEndToEnd(t *testing.T) {
	const chunkSize = 1 << 20 // 1 MiB chunks
	r := newFreehostRig(t, chunkSize, 3)
	if rec := r.do(http.MethodPut, "/b", nil, nil); rec.Code != http.StatusOK {
		t.Fatalf("create bucket: %d %s", rec.Code, rec.Body)
	}

	rec := r.do(http.MethodPost, "/b/big.bin?uploads", nil, map[string]string{"Content-Type": "application/octet-stream"})
	if rec.Code != http.StatusOK {
		t.Fatalf("initiate mpu: %d %s", rec.Code, rec.Body)
	}
	var init initiateMultipartUploadResult
	if err := xml.Unmarshal(rec.Body.Bytes(), &init); err != nil {
		t.Fatalf("parse initiate: %v", err)
	}
	uid := init.UploadID

	// Parts: two >= 5 MiB (minPartSize) parts + a small last part.
	part1 := make([]byte, 5<<20)
	part2 := make([]byte, 5<<20)
	part3 := []byte("the final tail bytes")
	rand.Read(part1)
	rand.Read(part2)

	etags := make([]string, 3)
	for i, part := range [][]byte{part1, part2, part3} {
		rec := r.do(http.MethodPut, fmt.Sprintf("/b/big.bin?partNumber=%d&uploadId=%s", i+1, uid), part, nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("upload part %d: %d %s", i+1, rec.Code, rec.Body)
		}
		etags[i] = rec.Header().Get("ETag")
	}

	var cb bytes.Buffer
	cb.WriteString("<CompleteMultipartUpload>")
	for i, et := range etags {
		fmt.Fprintf(&cb, "<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>", i+1, et)
	}
	cb.WriteString("</CompleteMultipartUpload>")
	if rec := r.do(http.MethodPost, "/b/big.bin?uploadId="+uid, cb.Bytes(), nil); rec.Code != http.StatusOK {
		t.Fatalf("complete: %d %s", rec.Code, rec.Body)
	}

	// GET the assembled object: must be byte-identical to part1+part2+part3.
	want := append(append(append([]byte{}, part1...), part2...), part3...)
	rec = r.do(http.MethodGet, "/b/big.bin", nil, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("get: %d", rec.Code)
	}
	if !bytes.Equal(rec.Body.Bytes(), want) {
		t.Fatalf("multipart round-trip mismatch: got %d bytes want %d", rec.Body.Len(), len(want))
	}

	// Assembled object: 5+5 full-MiB chunks + 1 tail chunk = 11 chunks, each R=3.
	r.assertReplicaSpread("b", "big.bin", 11, 3)
}
