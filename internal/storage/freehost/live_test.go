//go:build live

// Live provider smoke tests. These hit the real free hosts and MUST be run from
// the actual deploy egress IP — VPS/datacenter-IP blocking is the #1 failure
// mode (RESEARCH.md gotcha #2). Run with:
//
//	CATBOX_USERHASH=... go test -tags=live -run TestLive ./internal/storage/freehost/
//
// Each test uploads ~1 MiB of random bytes, GETs the whole blob and a sub-range,
// byte-compares, then best-effort deletes.
package freehost

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"testing"
	"time"
)

func liveRoundTrip(t *testing.T, p Provider) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// 256 KiB: a correctness smoke (upload / full GET / range / delete), not a
	// throughput test. Small enough that bandwidth-throttled-but-working hosts
	// (e.g. catbox) finish inside the read timeout instead of falsely failing.
	data := make([]byte, 256<<10)
	if _, err := rand.Read(data); err != nil {
		t.Fatalf("rand: %v", err)
	}

	// Unique filename per run: providers that derive a deterministic container
	// from the name (filebin bin, IA item) permanently burn a name once its
	// container is deleted, so a fixed name fails on re-run. A random suffix
	// keeps each run's container fresh; it's harmless for the others.
	suffix := make([]byte, 4)
	rand.Read(suffix)
	filename := fmt.Sprintf("freeS3live%x.0.bin", suffix)

	loc, tok, err := p.Upload(ctx, data, filename, "application/octet-stream")
	if err != nil {
		t.Fatalf("%s upload: %v", p.Name(), err)
	}
	t.Logf("%s uploaded -> %s (token=%q)", p.Name(), loc, tok)

	// Full download.
	rc, err := p.Download(ctx, loc, 0, 0)
	if err != nil {
		t.Fatalf("%s download full: %v", p.Name(), err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, data) {
		t.Fatalf("%s full read mismatch: got %d bytes want %d", p.Name(), len(got), len(data))
	}

	// Sub-range [1000, 1500).
	rc, err = p.Download(ctx, loc, 1000, 500)
	if err != nil {
		t.Fatalf("%s download range: %v", p.Name(), err)
	}
	gotRange, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(gotRange, data[1000:1500]) {
		t.Fatalf("%s range read mismatch (len=%d)", p.Name(), len(gotRange))
	}

	if err := p.Delete(ctx, loc, tok); err != nil {
		t.Logf("%s delete (best-effort) failed: %v", p.Name(), err)
	}
}

func TestLiveFileditch(t *testing.T) { liveRoundTrip(t, NewFileditch(NewClient(0))) }

func TestLiveCatbox(t *testing.T) {
	uh := os.Getenv("CATBOX_USERHASH")
	if uh == "" {
		t.Skip("CATBOX_USERHASH unset — catbox rejects anonymous VPS uploads (412)")
	}
	liveRoundTrip(t, NewCatbox(NewClient(0), uh))
}

func TestLiveX0(t *testing.T)      { liveRoundTrip(t, NewX0(NewClient(0))) }
func TestLiveEnvsSh(t *testing.T)  { liveRoundTrip(t, NewEnvsSh(NewClient(0))) }
func TestLiveTtmSh(t *testing.T)   { liveRoundTrip(t, NewTtmSh(NewClient(0))) }
func TestLivePomf(t *testing.T)    { liveRoundTrip(t, NewPomfLainLa(NewClient(0))) }
func TestLiveTempSh(t *testing.T)  { liveRoundTrip(t, NewTempSh(NewClient(0))) }
func TestLivePaste(t *testing.T)   { liveRoundTrip(t, NewPasteCNet(NewClient(0))) }
func TestLiveFilebin(t *testing.T) { liveRoundTrip(t, NewFilebin(NewClient(0))) }

func TestLiveLitterbox(t *testing.T)   { liveRoundTrip(t, NewLitterbox(NewClient(0))) }
func TestLiveTmpfiles(t *testing.T)    { liveRoundTrip(t, NewTmpfiles(NewClient(0))) }
func TestLiveTmpfileLink(t *testing.T) { liveRoundTrip(t, NewTmpfileLink(NewClient(0))) }
func TestLiveUguu(t *testing.T)        { liveRoundTrip(t, NewUguu(NewClient(0))) }
func TestLiveTmpNinja(t *testing.T)    { liveRoundTrip(t, NewTmpNinja(NewClient(0))) }
func TestLiveDokoMoe(t *testing.T)     { liveRoundTrip(t, NewDokoMoe(NewClient(0))) }
func TestLiveCockfile(t *testing.T)    { liveRoundTrip(t, NewCockfile(NewClient(0))) }

func TestLivePixeldrain(t *testing.T) {
	liveRoundTrip(t, NewPixeldrain(NewClient(0), os.Getenv("PIXELDRAIN_API_KEY")))
}

// TestLiveIA is bespoke: IA stages uploads asynchronously (a just-PUT file 404s
// until ingested, ~tens of seconds), so the generic immediate read-back doesn't
// apply. It uses a unique filename per run (so it can't read a stale prior-run
// version from the reused item) and polls until the file serves the correct
// bytes. In production this delay is tolerated — reads fall through to other
// replicas and IA catches up — so "not ingested in time" is a skip, not a fail.
func TestLiveIA(t *testing.T) {
	ak, sk := os.Getenv("IA_ACCESS_KEY"), os.Getenv("IA_SECRET_KEY")
	if ak == "" || sk == "" {
		t.Skip("IA_ACCESS_KEY/IA_SECRET_KEY unset")
	}
	p := NewIA(NewClient(0), ak, sk)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Second)
	defer cancel()

	data := make([]byte, 256<<10)
	if _, err := rand.Read(data); err != nil {
		t.Fatalf("rand: %v", err)
	}
	suffix := make([]byte, 4)
	rand.Read(suffix)
	filename := fmt.Sprintf("freeS3live%x.0.bin", suffix)

	loc, _, err := p.Upload(ctx, data, filename, "application/octet-stream")
	if err != nil {
		t.Fatalf("ia upload: %v", err)
	}
	t.Logf("ia uploaded -> %s (ingesting; polling for availability)", loc)

	var got []byte
	ready := false
	for attempt := 0; attempt < 15 && !ready; attempt++ {
		if rc, derr := p.Download(ctx, loc, 0, 0); derr == nil {
			got, _ = io.ReadAll(rc)
			rc.Close()
			if bytes.Equal(got, data) {
				ready = true
				break
			}
		}
		select {
		case <-time.After(10 * time.Second):
		case <-ctx.Done():
			t.Fatalf("ia: context done while polling for ingestion: %v", ctx.Err())
		}
	}
	if !ready {
		t.Skipf("ia: file not ingested within window (last read %d/%d bytes) — tolerated by design", len(got), len(data))
	}
	t.Logf("ia ingested after polling; verifying range")

	rc, err := p.Download(ctx, loc, 1000, 500)
	if err != nil {
		t.Fatalf("ia download range: %v", err)
	}
	gotRange, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(gotRange, data[1000:1500]) {
		t.Fatalf("ia range read mismatch (len=%d)", len(gotRange))
	}
	if err := p.Delete(ctx, loc, ""); err != nil {
		t.Logf("ia delete (best-effort) failed: %v", err)
	}
}

func TestLiveGofile(t *testing.T) {
	tok := os.Getenv("GOFILE_TOKEN")
	if tok == "" {
		t.Skip("GOFILE_TOKEN unset — gofile needs a token for raw direct links")
	}
	liveRoundTrip(t, NewGofile(NewClient(0), tok))
}
