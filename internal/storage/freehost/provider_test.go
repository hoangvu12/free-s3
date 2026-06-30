package freehost

import (
	"context"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// readUploadedFile pulls the named multipart file field out of a request and
// returns its bytes — lets the fakes echo the chunk back for round-trip checks.
func readUploadedFile(t *testing.T, r *http.Request, field string) []byte {
	t.Helper()
	_, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil {
		t.Fatalf("parse content-type: %v", err)
	}
	mr := multipart.NewReader(r.Body, params["boundary"])
	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("next part: %v", err)
		}
		if part.FormName() == field {
			b, _ := io.ReadAll(part)
			return b
		}
	}
	t.Fatalf("file field %q not found", field)
	return nil
}

func TestFileditchUpload(t *testing.T) {
	var gotName string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, params, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
		mr := multipart.NewReader(r.Body, params["boundary"])
		for {
			part, err := mr.NextPart()
			if err == io.EOF {
				break
			}
			if part.FormName() == "file" {
				gotName = part.FileName()
			}
		}
		if r.Header.Get("User-Agent") != BrowserUA {
			t.Errorf("missing browser UA: %q", r.Header.Get("User-Agent"))
		}
		fmt.Fprint(w, `{"success":true,"url":"https://up.fileditch.com/abc.bin"}`)
	}))
	defer srv.Close()

	p := &Fileditch{c: NewClient(0), endpoint: srv.URL}
	loc, tok, err := p.Upload(context.Background(), []byte("hello"), "obj.0", "application/octet-stream")
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	if loc != "https://up.fileditch.com/abc.bin" || tok != "" {
		t.Fatalf("loc=%q tok=%q", loc, tok)
	}
	if !strings.HasSuffix(gotName, ".bin") {
		t.Fatalf("chunk filename %q must end in .bin (extension blocklist)", gotName)
	}
}

func TestFileditchUploadFilesArrayShape(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"success":true,"files":[{"url":"https://x/y.bin"}]}`)
	}))
	defer srv.Close()
	p := &Fileditch{c: NewClient(0), endpoint: srv.URL}
	loc, _, err := p.Upload(context.Background(), []byte("z"), "a", "application/octet-stream")
	if err != nil || loc != "https://x/y.bin" {
		t.Fatalf("loc=%q err=%v", loc, err)
	}
}

func TestUploadRetriesThenFails(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		// HTTP 200 with a non-JSON error body — must be rejected, not trusted.
		fmt.Fprint(w, "upload disabled")
	}))
	defer srv.Close()
	p := &Fileditch{c: NewClient(0), endpoint: srv.URL}
	_, _, err := p.Upload(context.Background(), []byte("z"), "a", "application/octet-stream")
	if err == nil {
		t.Fatal("expected error on non-JSON 200 body")
	}
	if calls.Load() != maxAttempts {
		t.Fatalf("retried %d times, want %d", calls.Load(), maxAttempts)
	}
}

func TestCatboxUploadValidatesPrefixAndUserhash(t *testing.T) {
	var sawUserhash string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, params, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
		mr := multipart.NewReader(r.Body, params["boundary"])
		for {
			part, err := mr.NextPart()
			if err == io.EOF {
				break
			}
			if part.FormName() == "userhash" {
				b, _ := io.ReadAll(part)
				sawUserhash = string(b)
			}
		}
		fmt.Fprint(w, "https://files.catbox.moe/deadbe.bin")
	}))
	defer srv.Close()

	p := &Catbox{c: NewClient(0), userhash: "secret-hash", endpoint: srv.URL, urlPrefix: "https://files.catbox.moe/"}
	loc, tok, err := p.Upload(context.Background(), []byte("data"), "obj.1", "application/octet-stream")
	if err != nil || loc != "https://files.catbox.moe/deadbe.bin" || tok != "" {
		t.Fatalf("loc=%q tok=%q err=%v", loc, tok, err)
	}
	if sawUserhash != "secret-hash" {
		t.Fatalf("userhash not sent: %q", sawUserhash)
	}
}

func TestCatboxUploadRejectsErrorBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// catbox returns 200 + plaintext error on failure.
		fmt.Fprint(w, "412 Invalid uploader")
	}))
	defer srv.Close()
	p := &Catbox{c: NewClient(0), endpoint: srv.URL, urlPrefix: "https://files.catbox.moe/"}
	if _, _, err := p.Upload(context.Background(), []byte("d"), "a", ""); err == nil {
		t.Fatal("expected rejection of non-URL body")
	}
}

func TestX0UploadCapturesToken(t *testing.T) {
	var srvURL string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Token", "del-tok-99")
		fmt.Fprint(w, srvURL+"/file.bin")
	}))
	defer srv.Close()
	srvURL = srv.URL

	p := &zerox{c: NewClient(0), host: srv.URL, name: "x0.at", field: "file", maxByte: 1 << 30, durable: true, token: true}
	loc, tok, err := p.Upload(context.Background(), []byte("payload"), "obj.2", "application/octet-stream")
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	if loc != srv.URL+"/file.bin" || tok != "del-tok-99" {
		t.Fatalf("loc=%q tok=%q", loc, tok)
	}
}

func TestRangeGetHonorsServerRange(t *testing.T) {
	full := []byte("0123456789")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Honor Range like a CDN: serve 206 with exactly the requested window.
		if rng := r.Header.Get("Range"); rng != "" {
			var start, end int64
			fmt.Sscanf(rng, "bytes=%d-%d", &start, &end)
			if end == 0 || end >= int64(len(full)) {
				end = int64(len(full)) - 1
			}
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(full)))
			w.WriteHeader(http.StatusPartialContent)
			w.Write(full[start : end+1])
			return
		}
		w.Write(full)
	}))
	defer srv.Close()
	c := NewClient(0)
	rc, err := c.rangeGet(context.Background(), srv.URL, 3, 4, nil)
	if err != nil {
		t.Fatalf("rangeGet: %v", err)
	}
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	if string(got) != "3456" {
		t.Fatalf("range honored = %q, want 3456", got)
	}
}

func TestRangeGetEmulatesWhenServerIgnoresRange(t *testing.T) {
	full := []byte("0123456789")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Ignore Range entirely: always 200 full body.
		w.Write(full)
	}))
	defer srv.Close()
	c := NewClient(0)
	rc, err := c.rangeGet(context.Background(), srv.URL, 3, 4, nil)
	if err != nil {
		t.Fatalf("rangeGet: %v", err)
	}
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	if string(got) != "3456" {
		t.Fatalf("range emulated = %q, want 3456", got)
	}
}
