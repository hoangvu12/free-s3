package freehost

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// IA — Internet Archive, the permanent anchor. Upload is a PUT to the S3-like
// endpoint with a simple `Authorization: LOW key:secret` header (NOT full
// SigV4), the download URL is deterministic, and Range is supported. Items are
// capped at ~10 GB / <100 files, so all chunks of one object share an item
// derived from the object-key hash (BUILD-PLAN gotcha #6). `x-archive-queue-
// derive:0` skips transcoding; `x-archive-auto-make-bucket:1` creates the item.
//
// IA is an anchor, not the sole copy: opaque blobs risk policy "darking", so it
// always sits behind >= 1 other replica.
type IA struct {
	c         *Client
	accessKey string
	secretKey string
	s3Host    string // https://s3.us.archive.org
	dlBase    string // https://archive.org/download
	itemPfx   string // item-name prefix
}

func NewIA(c *Client, accessKey, secretKey string) *IA {
	return &IA{
		c:         c,
		accessKey: accessKey,
		secretKey: secretKey,
		s3Host:    "https://s3.us.archive.org",
		dlBase:    "https://archive.org/download",
		itemPfx:   "free-s3-",
	}
}

func (p *IA) Name() string    { return "ia" }
func (p *IA) MaxBytes() int64 { return 10 << 30 } // per-file practical ceiling (item <= 10 GB)
func (p *IA) Durable() bool   { return true }

// itemAndFile derives the IA item + file from the chunk filename
// "<keyhash>.<seq>.bin": all chunks of one object share item "<prefix><keyhash>"
// and are stored as the full filename inside it.
func (p *IA) itemAndFile(filename string) (item, file string) {
	keyHash := filename
	if i := strings.IndexByte(filename, '.'); i >= 0 {
		keyHash = filename[:i]
	}
	return p.itemPfx + keyHash, filename
}

func (p *IA) Upload(ctx context.Context, data []byte, filename, contentType string) (string, string, error) {
	item, file := p.itemAndFile(filename)
	url := fmt.Sprintf("%s/%s/%s", p.s3Host, item, file)
	headers := map[string]string{
		"Authorization":              fmt.Sprintf("LOW %s:%s", p.accessKey, p.secretKey),
		"x-archive-auto-make-bucket": "1",
		"x-archive-queue-derive":     "0",
		"x-archive-size-hint":        strconv.Itoa(len(data)),
		"Content-Type":               contentType,
	}
	_, _, err := p.c.sendBytes(ctx, "PUT", url, data, headers, func(status int, body string) error {
		if status < 200 || status >= 300 {
			return fmt.Errorf("ia: PUT %s -> %d: %s", url, status, body)
		}
		return nil
	})
	if err != nil {
		return "", "", err
	}
	// Deterministic download URL; no per-file delete token (delete uses creds).
	return fmt.Sprintf("%s/%s/%s", p.dlBase, item, file), "", nil
}

func (p *IA) Download(ctx context.Context, locator string, offset, length int64) (io.ReadCloser, error) {
	return p.c.rangeGet(ctx, locator, offset, length, nil)
}

func (p *IA) Delete(ctx context.Context, locator, _ string) error {
	// Map the download URL back to the S3 endpoint: .../download/{item}/{file}
	// -> {s3Host}/{item}/{file}.
	rest := strings.TrimPrefix(locator, p.dlBase+"/")
	if rest == locator {
		return nil // not an IA download URL we recognize
	}
	url := p.s3Host + "/" + rest
	status, body, err := p.c.deleteWithHeaders(ctx, url, map[string]string{
		"Authorization": fmt.Sprintf("LOW %s:%s", p.accessKey, p.secretKey),
	})
	if err != nil {
		return err
	}
	if status < 200 || status >= 300 {
		return fmt.Errorf("ia delete %s -> %d: %s", url, status, body)
	}
	return nil
}
