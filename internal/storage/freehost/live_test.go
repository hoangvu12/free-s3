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
	"io"
	"os"
	"testing"
	"time"
)

func liveRoundTrip(t *testing.T, p Provider) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	data := make([]byte, 1<<20) // 1 MiB
	if _, err := rand.Read(data); err != nil {
		t.Fatalf("rand: %v", err)
	}

	loc, tok, err := p.Upload(ctx, data, "freeS3-livetest.0.bin", "application/octet-stream")
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

func TestLiveX0(t *testing.T) { liveRoundTrip(t, NewX0(NewClient(0))) }
