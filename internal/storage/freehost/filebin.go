package freehost

import (
	"context"
	"fmt"
	"io"
	"strings"
)

// Filebin — filebin.net: PUT the raw body to /{bin}/{filename}; the same URL
// GETs (302 -> presigned, Range-capable); DELETE /{bin} drops the whole bin.
// ~6-day retention, some extensions blocked (so blobs are named .bin). The bin
// id is derived from the chunk filename so all chunks of one object share a bin.
type Filebin struct {
	c    *Client
	host string
}

func NewFilebin(c *Client) *Filebin { return &Filebin{c: c, host: "https://filebin.net"} }

func (p *Filebin) Name() string    { return "filebin.net" }
func (p *Filebin) MaxBytes() int64 { return 5 << 30 }
func (p *Filebin) Durable() bool   { return false }

// bin derives the filebin bin id from the chunk filename "<keyhash>.<seq>.bin":
// all chunks of one object land in bin "free-s3-<keyhash>".
func (p *Filebin) bin(filename string) string {
	keyHash := filename
	if i := strings.IndexByte(filename, '.'); i >= 0 {
		keyHash = filename[:i]
	}
	return "free-s3-" + keyHash
}

func (p *Filebin) Upload(ctx context.Context, data []byte, filename, contentType string) (string, string, error) {
	bin := p.bin(filename)
	url := fmt.Sprintf("%s/%s/%s", p.host, bin, binName(filename))
	headers := map[string]string{
		"Content-Type": contentType,
		"Accept":       "application/json",
	}
	_, _, err := p.c.sendBytes(ctx, "POST", url, data, headers, func(status int, body string) error {
		if status < 200 || status >= 300 {
			return fmt.Errorf("filebin: POST %s -> %d: %s", url, status, body)
		}
		return nil
	})
	if err != nil {
		return "", "", err
	}
	return url, bin, nil // deleteToken carries the bin for DELETE /{bin}
}

// filebinVerifiedCookie satisfies filebin's anti-bot gate: a bare GET of the
// file URL returns an HTML interstitial that sets `verified=<date>`; sending
// that cookie back makes filebin 302 to a presigned (Range-capable) storage URL
// with the raw bytes. The value is the fixed constant filebin issues.
const filebinVerifiedCookie = "verified=2024-05-24"

func (p *Filebin) Download(ctx context.Context, locator string, offset, length int64) (io.ReadCloser, error) {
	return p.c.rangeGet(ctx, locator, offset, length, map[string]string{"Cookie": filebinVerifiedCookie})
}

func (p *Filebin) Delete(ctx context.Context, locator, deleteToken string) error {
	bin := deleteToken
	if bin == "" {
		return nil
	}
	status, body, err := p.c.deleteWithHeaders(ctx, fmt.Sprintf("%s/%s", p.host, bin), nil)
	if err != nil {
		return err
	}
	if status < 200 || status >= 300 {
		return fmt.Errorf("filebin delete %s -> %d: %s", bin, status, body)
	}
	return nil
}
