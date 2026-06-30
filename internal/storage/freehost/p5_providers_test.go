package freehost

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestIAUploadDeterministicURLAndAuth(t *testing.T) {
	var gotPath, gotAuth, gotDerive string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		gotDerive = r.Header.Get("x-archive-queue-derive")
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	p := &IA{c: NewClient(0), accessKey: "AK", secretKey: "SK", s3Host: srv.URL, dlBase: "https://archive.org/download", itemPfx: "free-s3-"}
	loc, tok, err := p.Upload(context.Background(), []byte("payload"), "deadbeef.3.bin", "application/octet-stream")
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	if loc != "https://archive.org/download/free-s3-deadbeef/deadbeef.3.bin" {
		t.Fatalf("locator = %q", loc)
	}
	if tok != "" {
		t.Fatalf("ia token = %q, want empty", tok)
	}
	if gotPath != "/free-s3-deadbeef/deadbeef.3.bin" {
		t.Fatalf("PUT path = %q", gotPath)
	}
	if gotAuth != "LOW AK:SK" {
		t.Fatalf("auth = %q, want LOW AK:SK", gotAuth)
	}
	if gotDerive != "0" {
		t.Fatalf("derive header = %q, want 0", gotDerive)
	}
}

func TestPixeldrainUploadBasicAuthAndID(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusCreated)
		fmt.Fprint(w, `{"id":"abc123"}`)
	}))
	defer srv.Close()

	p := &Pixeldrain{c: NewClient(0), apiKey: "key-xyz", base: "https://pixeldrain.com", uploadEP: srv.URL}
	loc, _, err := p.Upload(context.Background(), []byte("d"), "obj.0", "application/octet-stream")
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	if loc != "https://pixeldrain.com/api/file/abc123" {
		t.Fatalf("locator = %q", loc)
	}
	if !strings.HasPrefix(gotAuth, "Basic ") {
		t.Fatalf("expected Basic auth, got %q", gotAuth)
	}
}

func TestPomfUploadParsesFilesArray(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"success":true,"files":[{"url":"https://pomf.lain.la/f/abc.bin","name":"abc.bin"}]}`)
	}))
	defer srv.Close()
	p := &pomf{c: NewClient(0), name: "pomf.lain.la", endpoint: srv.URL, maxByte: 1 << 30, durable: true}
	loc, _, err := p.Upload(context.Background(), []byte("z"), "a", "application/octet-stream")
	if err != nil || loc != "https://pomf.lain.la/f/abc.bin" {
		t.Fatalf("loc=%q err=%v", loc, err)
	}
}

func TestTempShUploadPlaintextURL(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "https://temp.sh/abcde/obj.bin")
	}))
	defer srv.Close()
	p := &TempSh{c: NewClient(0), endpoint: srv.URL}
	loc, _, err := p.Upload(context.Background(), []byte("z"), "a", "application/octet-stream")
	if err != nil || loc != "https://temp.sh/abcde/obj.bin" {
		t.Fatalf("loc=%q err=%v", loc, err)
	}
}

func TestTmpfilesRewritesToDownloadURL(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"status":"success","data":{"url":"https://tmpfiles.org/12345/obj.bin"}}`)
	}))
	defer srv.Close()
	p := &Tmpfiles{c: NewClient(0), endpoint: srv.URL}
	loc, _, err := p.Upload(context.Background(), []byte("z"), "a", "application/octet-stream")
	if err != nil || loc != "https://tmpfiles.org/dl/12345/obj.bin" {
		t.Fatalf("loc=%q err=%v", loc, err)
	}
}

func TestRegistryDefaultAndGating(t *testing.T) {
	c := NewClient(0)
	logger := discardLogger()

	// No IA / gofile creds: ia + gofile are skipped from any requested set.
	got := BuildProviders(c, []string{"ia", "gofile", "fileditch", "bogus"}, Credentials{}, logger)
	names := map[string]bool{}
	for _, p := range got {
		names[p.Name()] = true
	}
	if names["ia"] || names["gofile"] || names["bogus"] {
		t.Fatalf("expected ia/gofile/bogus skipped, got %v", names)
	}
	if !names["fileditch"] {
		t.Fatalf("fileditch should be enabled")
	}

	// With IA creds, ia is enabled and is durable (anchors the default set).
	got = BuildProviders(c, nil, Credentials{IAAccessKey: "k", IASecretKey: "s"}, logger)
	if len(got) == 0 {
		t.Fatal("default provider order produced no providers")
	}
	foundIA, durableCount := false, 0
	for _, p := range got {
		if p.Name() == "ia" {
			foundIA = true
		}
		if p.Durable() {
			durableCount++
		}
	}
	if !foundIA {
		t.Fatal("ia not enabled despite creds")
	}
	if durableCount == 0 {
		t.Fatal("default set has no durable provider")
	}
}
