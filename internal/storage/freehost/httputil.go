package freehost

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"strings"
	"time"
)

// BrowserUA is sent on EVERY request. Many free hosts (and the Cloudflare in
// front of several) reject non-browser User-Agents with 403/418/1010 before the
// request reaches origin (RESEARCH.md gotcha #1).
const BrowserUA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
	"(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

const (
	defaultUploadTimeout   = 120 * time.Second
	defaultDownloadTimeout = 60 * time.Second
	maxAttempts            = 3
	errBodyLimit           = 2 << 10 // cap error/plaintext bodies we slurp for messages
)

// Client is the shared HTTP client for every provider. Per-call deadlines come
// from the context the backend passes in (Upload/Download), so the client has
// no global Timeout; the transport bounds idle keepalive connections.
type Client struct {
	hc *http.Client
}

// NewClient builds the shared client. maxIdlePerHost <= 0 falls back to 32.
func NewClient(maxIdlePerHost int) *Client {
	if maxIdlePerHost <= 0 {
		maxIdlePerHost = 32
	}
	tr := &http.Transport{
		MaxIdleConns:        maxIdlePerHost * 4,
		MaxIdleConnsPerHost: maxIdlePerHost,
		IdleConnTimeout:     90 * time.Second,
		ForceAttemptHTTP2:   true,
	}
	return &Client{hc: &http.Client{Transport: tr}}
}

// do issues req with the browser UA forced on (unless the caller already set a
// non-empty UA). It does not retry — callers wrap retryable work in retry().
func (c *Client) do(req *http.Request) (*http.Response, error) {
	if req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", BrowserUA)
	}
	return c.hc.Do(req)
}

// retry runs fn up to maxAttempts times with exponential backoff
// (500ms → 1s → 2s), porting studyon-openai's per-provider retry. It stops
// early if the context is cancelled. fn should be idempotent at the provider
// level (a duplicate upload just wastes a slot; the backend dedups by replica).
func retry(ctx context.Context, fn func() error) error {
	var err error
	backoff := 500 * time.Millisecond
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err = fn(); err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if attempt < maxAttempts-1 {
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
			backoff *= 2
		}
	}
	return err
}

// multipartBody builds a multipart/form-data body once so it can be replayed on
// each retry attempt (bytes.NewReader over the returned buffer). fields are the
// plain text form fields; the file part is named fileField with the given
// filename + content type. Returns the encoded body and its Content-Type header.
func multipartBody(fields map[string]string, fileField, filename, contentType string, data []byte) ([]byte, string, error) {
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	for k, v := range fields {
		if err := mw.WriteField(k, v); err != nil {
			return nil, "", err
		}
	}
	h := make(textproto.MIMEHeader)
	h.Set("Content-Disposition", fmt.Sprintf(`form-data; name=%q; filename=%q`, fileField, filename))
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	h.Set("Content-Type", contentType)
	part, err := mw.CreatePart(h)
	if err != nil {
		return nil, "", err
	}
	if _, err := part.Write(data); err != nil {
		return nil, "", err
	}
	if err := mw.Close(); err != nil {
		return nil, "", err
	}
	return buf.Bytes(), mw.FormDataContentType(), nil
}

// uploadMultipart POSTs a multipart form (built once, replayed per retry) and
// returns the trimmed response body + headers of the first attempt that passes
// validate. validate inspects (status, body) per attempt and returns an error
// to trigger a retry (hosts often return 200 + a text/JSON error body, so
// status alone is not trusted — RESEARCH.md gotcha #3).
func (c *Client) uploadMultipart(ctx context.Context, url string, fields map[string]string, fileField, filename, contentType string, data []byte, headers map[string]string, validate func(status int, body string) error) (string, http.Header, error) {
	body, ct, err := multipartBody(fields, fileField, filename, contentType, data)
	if err != nil {
		return "", nil, err
	}
	var outBody string
	var outHdr http.Header
	err = retry(ctx, func() error {
		cctx, cancel := context.WithTimeout(ctx, defaultUploadTimeout)
		defer cancel()
		req, err := http.NewRequestWithContext(cctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", ct)
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		resp, err := c.do(req)
		if err != nil {
			return err
		}
		hdr := resp.Header
		text := slurp(resp)
		if verr := validate(resp.StatusCode, text); verr != nil {
			return verr
		}
		outBody, outHdr = text, hdr
		return nil
	})
	return outBody, outHdr, err
}

// sendBytes issues a raw-body request (PUT/POST) with retry and returns the
// trimmed response body + headers of the first attempt that passes validate.
// Used by hosts that take the file as the raw request body (IA PUT, buzzheavier
// PUT, paste.c-net.org PUT, pixeldrain POST).
func (c *Client) sendBytes(ctx context.Context, method, url string, data []byte, headers map[string]string, validate func(status int, body string) error) (string, http.Header, error) {
	var outBody string
	var outHdr http.Header
	err := retry(ctx, func() error {
		cctx, cancel := context.WithTimeout(ctx, defaultUploadTimeout)
		defer cancel()
		req, err := http.NewRequestWithContext(cctx, method, url, bytes.NewReader(data))
		if err != nil {
			return err
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		resp, err := c.do(req)
		if err != nil {
			return err
		}
		hdr := resp.Header
		text := slurp(resp)
		if verr := validate(resp.StatusCode, text); verr != nil {
			return verr
		}
		outBody, outHdr = text, hdr
		return nil
	})
	return outBody, outHdr, err
}

// postForm issues a urlencoded POST (single attempt; used for deletes, which are
// best-effort) and returns the status + trimmed body.
func (c *Client) postForm(ctx context.Context, endpoint string, values map[string]string) (int, string, error) {
	form := make([]string, 0, len(values))
	for k, v := range values {
		form = append(form, url.QueryEscape(k)+"="+url.QueryEscape(v))
	}
	cctx, cancel := context.WithTimeout(ctx, defaultUploadTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(cctx, http.MethodPost, endpoint, strings.NewReader(strings.Join(form, "&")))
	if err != nil {
		return 0, "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := c.do(req)
	if err != nil {
		return 0, "", err
	}
	return resp.StatusCode, slurp(resp), nil
}

// deleteWithHeaders issues a DELETE (single attempt; deletes are best-effort)
// with the given headers and returns the status + trimmed body.
func (c *Client) deleteWithHeaders(ctx context.Context, url string, headers map[string]string) (int, string, error) {
	cctx, cancel := context.WithTimeout(ctx, defaultUploadTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(cctx, http.MethodDelete, url, nil)
	if err != nil {
		return 0, "", err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := c.do(req)
	if err != nil {
		return 0, "", err
	}
	return resp.StatusCode, slurp(resp), nil
}

// getString GETs url and returns up to maxBytes of the body as a string. Used
// by providers that must scrape an HTML landing page for the real direct link
// (fileditch). It uses the download timeout and the browser UA.
func (c *Client) getString(ctx context.Context, url string, maxBytes int64) (string, error) {
	cctx, cancel := context.WithTimeout(ctx, defaultDownloadTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(cctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	resp, err := c.do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, maxBytes))
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("freehost: GET %s -> %d", url, resp.StatusCode)
	}
	return string(b), nil
}

// slurp reads up to errBodyLimit bytes of a response body for inclusion in an
// error message (and to validate plaintext-URL responses). It always drains +
// closes so the connection can be reused.
func slurp(resp *http.Response) string {
	b, _ := io.ReadAll(io.LimitReader(resp.Body, errBodyLimit))
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return strings.TrimSpace(string(b))
}

// rangeGet performs a GET honoring an object-space [offset, offset+length)
// window via the HTTP Range header. length <= 0 means "to end". If the host
// ignores Range and returns 200, the slice is emulated client-side (RESEARCH.md:
// most hosts honor Range; the fallback keeps correctness for those that don't).
func (c *Client) rangeGet(ctx context.Context, url string, offset, length int64, extraHeaders map[string]string) (io.ReadCloser, error) {
	return c.rangeGetMethod(ctx, http.MethodGet, url, offset, length, extraHeaders)
}

// rangeGetMethod is rangeGet with an explicit HTTP method. A few hosts serve the
// file body only in response to a POST on the file URL (temp.sh's "click to
// download" form); they ignore Range and return 200, so the requested window is
// emulated client-side by the 200 branch below.
func (c *Client) rangeGetMethod(ctx context.Context, method, url string, offset, length int64, extraHeaders map[string]string) (io.ReadCloser, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultDownloadTimeout)
	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		cancel()
		return nil, err
	}
	for k, v := range extraHeaders {
		req.Header.Set(k, v)
	}
	wantRange := offset > 0 || length > 0
	if wantRange {
		if length > 0 {
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
		} else {
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
		}
	}
	resp, err := c.do(req)
	if err != nil {
		cancel()
		return nil, err
	}
	switch {
	case resp.StatusCode == http.StatusPartialContent:
		// Server honored Range — body is exactly the window.
		return &cancelReadCloser{rc: resp.Body, cancel: cancel}, nil
	case resp.StatusCode == http.StatusOK:
		// Server ignored Range (or none requested): emulate the window.
		var r io.Reader = resp.Body
		if offset > 0 {
			if _, err := io.CopyN(io.Discard, r, offset); err != nil {
				resp.Body.Close()
				cancel()
				return nil, fmt.Errorf("freehost: skip to offset %d: %w", offset, err)
			}
		}
		if length > 0 {
			r = io.LimitReader(r, length)
		}
		return &cancelReadCloser{rc: resp.Body, r: r, cancel: cancel}, nil
	default:
		msg := slurp(resp)
		cancel()
		return nil, fmt.Errorf("freehost: GET %s -> %d: %s", url, resp.StatusCode, msg)
	}
}

// cancelReadCloser ties a response body to its request context's cancel func so
// closing the reader releases the timeout. r, when set, is the (sliced) reader
// the caller should read from; rc is always the underlying body to close.
type cancelReadCloser struct {
	rc     io.ReadCloser
	r      io.Reader
	cancel context.CancelFunc
}

func (c *cancelReadCloser) Read(p []byte) (int, error) {
	if c.r != nil {
		return c.r.Read(p)
	}
	return c.rc.Read(p)
}

func (c *cancelReadCloser) Close() error {
	err := c.rc.Close()
	c.cancel()
	return err
}
