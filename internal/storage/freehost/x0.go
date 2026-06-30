package freehost

import (
	"context"
	"fmt"
	"io"
	"strings"
)

// X0 — the 0x0-family host x0.at (1 GiB/file, 3-100 day retention, confirmed NOT
// datacenter-blocked, unlike 0x0.st — RESEARCH.md). POST multipart field "file"
// to the root; the response is the plaintext direct URL with an X-Token response
// header used for deletion. Validated as a plaintext URL (not just HTTP 200).
type X0 struct {
	c       *Client
	host    string // e.g. "https://x0.at"
	name    string
	maxByte int64
}

func NewX0(c *Client) *X0 {
	return &X0{c: c, host: "https://x0.at", name: "x0.at", maxByte: 1 << 30}
}

func (p *X0) Name() string    { return p.name }
func (p *X0) MaxBytes() int64 { return p.maxByte }
func (p *X0) Durable() bool   { return true }

func (p *X0) Upload(ctx context.Context, data []byte, filename, contentType string) (string, string, error) {
	var locator, token string
	_, hdr, err := p.c.uploadMultipart(ctx, p.host+"/", nil, "file", binName(filename), contentType, data, nil,
		func(status int, body string) error {
			if status < 200 || status >= 300 || !strings.HasPrefix(body, p.host) {
				return fmt.Errorf("%s: HTTP %d body=%s", p.name, status, body)
			}
			locator = strings.TrimSpace(body)
			return nil
		})
	if err != nil {
		return "", "", err
	}
	if hdr != nil {
		token = hdr.Get("X-Token")
	}
	return locator, token, nil
}

func (p *X0) Download(ctx context.Context, locator string, offset, length int64) (io.ReadCloser, error) {
	return p.c.rangeGet(ctx, locator, offset, length, nil)
}

func (p *X0) Delete(ctx context.Context, locator, deleteToken string) error {
	if deleteToken == "" {
		return nil
	}
	status, body, err := p.c.postForm(ctx, locator, map[string]string{
		"token":  deleteToken,
		"delete": "",
	})
	if err != nil {
		return err
	}
	if status < 200 || status >= 300 {
		return fmt.Errorf("%s delete: HTTP %d: %s", p.name, status, body)
	}
	return nil
}
