package freehost

import (
	"context"
	"fmt"
	"io"
	"strings"
)

// zerox is the generalized 0x0-family provider: POST a multipart file field to
// the host root, get a plaintext direct URL back, with an optional X-Token
// response header used for deletion (0x0 protocol: POST the file URL with
// token=+delete=). Covers x0.at, envs.sh, ttm.sh (field "file", X-Token).
//
// 0x0.st itself is intentionally absent: it refuses datacenter IPs (RESEARCH.md
// gotcha #2). x0.at is the DC-friendly member. fars.ee was removed: it is a
// ptpb/pb text pastebin ("do NOT post large files"), not a file host.
type zerox struct {
	c       *Client
	name    string
	host    string // e.g. https://x0.at
	field   string // "file" or "c"
	maxByte int64
	durable bool
	token   bool // host returns/accepts an X-Token delete token
}

// NewX0 is x0.at: 1 GiB, durable (in the BUILD-PLAN durable set), X-Token.
func NewX0(c *Client) *zerox {
	return &zerox{c: c, name: "x0.at", host: "https://x0.at", field: "file", maxByte: 1 << 30, durable: true, token: true}
}

// NewEnvsSh is envs.sh: 256 MiB, temp tier, X-Token.
func NewEnvsSh(c *Client) *zerox {
	return &zerox{c: c, name: "envs.sh", host: "https://envs.sh", field: "file", maxByte: 256 << 20, durable: false, token: true}
}

// NewTtmSh is ttm.sh: ~256 MiB, temp tier, X-Token.
func NewTtmSh(c *Client) *zerox {
	return &zerox{c: c, name: "ttm.sh", host: "https://ttm.sh", field: "file", maxByte: 256 << 20, durable: false, token: true}
}

func (p *zerox) Name() string    { return p.name }
func (p *zerox) MaxBytes() int64 { return p.maxByte }
func (p *zerox) Durable() bool   { return p.durable }

func (p *zerox) Upload(ctx context.Context, data []byte, filename, contentType string) (string, string, error) {
	var locator, token string
	_, hdr, err := p.c.uploadMultipart(ctx, p.host+"/", nil, p.field, binName(filename), contentType, data, nil,
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
	if p.token && hdr != nil {
		token = hdr.Get("X-Token")
	}
	return locator, token, nil
}

func (p *zerox) Download(ctx context.Context, locator string, offset, length int64) (io.ReadCloser, error) {
	return p.c.rangeGet(ctx, locator, offset, length, nil)
}

func (p *zerox) Delete(ctx context.Context, locator, deleteToken string) error {
	if !p.token || deleteToken == "" {
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
