package freehost

import (
	"context"
	"fmt"
	"io"
	"strings"
)

// Catbox — permanent, 200 MB/file. POST multipart to user/api.php with
// reqtype=fileupload + fileToUpload (+ optional userhash). The response is the
// plaintext direct URL (files.catbox.moe/<id>); catbox returns HTTP 200 even on
// failure, so the body prefix is validated. From a VPS the userhash is REQUIRED
// (anonymous uploads get 412 — RESEARCH.md gotcha #2). Delete needs the same
// userhash (account-global, not per-file), so it is a no-op when unset.
type Catbox struct {
	c         *Client
	userhash  string
	endpoint  string // user/api.php
	urlPrefix string // expected direct-URL prefix for body validation
}

func NewCatbox(c *Client, userhash string) *Catbox {
	return &Catbox{
		c:         c,
		userhash:  strings.TrimSpace(userhash),
		endpoint:  "https://catbox.moe/user/api.php",
		urlPrefix: "https://files.catbox.moe/",
	}
}

func (p *Catbox) Name() string    { return "catbox" }
func (p *Catbox) MaxBytes() int64 { return 200 << 20 } // 200 MB
func (p *Catbox) Durable() bool   { return true }

func (p *Catbox) Upload(ctx context.Context, data []byte, filename, contentType string) (string, string, error) {
	fields := map[string]string{"reqtype": "fileupload"}
	if p.userhash != "" {
		fields["userhash"] = p.userhash
	}
	var locator string
	_, _, err := p.c.uploadMultipart(ctx, p.endpoint, fields, "fileToUpload", binName(filename), contentType, data, nil,
		func(status int, body string) error {
			if !strings.HasPrefix(body, p.urlPrefix) {
				return fmt.Errorf("catbox: HTTP %d body=%s", status, body)
			}
			locator = body
			return nil
		})
	if err != nil {
		return "", "", err
	}
	return locator, "", nil
}

func (p *Catbox) Download(ctx context.Context, locator string, offset, length int64) (io.ReadCloser, error) {
	return p.c.rangeGet(ctx, locator, offset, length, nil)
}

func (p *Catbox) Delete(ctx context.Context, locator, deleteToken string) error {
	if p.userhash == "" {
		return nil // anonymous uploads can't be deleted
	}
	id := basename(locator)
	if id == "" {
		return nil
	}
	status, body, err := p.c.postForm(ctx, p.endpoint, map[string]string{
		"reqtype":  "deletefiles",
		"userhash": p.userhash,
		"files":    id,
	})
	if err != nil {
		return err
	}
	if status < 200 || status >= 300 {
		return fmt.Errorf("catbox delete %s: HTTP %d: %s", id, status, body)
	}
	return nil
}
