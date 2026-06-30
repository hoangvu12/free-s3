package freehost

import (
	"context"
	"fmt"
	"io"
	"strings"
)

// PasteCNet — paste.c-net.org: raw PUT (curl --upload-file) returns a plaintext
// URL. 50 MB/file, 180-day rolling-on-access retention. Treated as durable-ish
// (long retention), but the small cap keeps it a secondary replica.
type PasteCNet struct {
	c    *Client
	host string
}

func NewPasteCNet(c *Client) *PasteCNet { return &PasteCNet{c: c, host: "https://paste.c-net.org"} }

func (p *PasteCNet) Name() string    { return "paste.c-net.org" }
func (p *PasteCNet) MaxBytes() int64 { return 50 << 20 } // 50 MB
func (p *PasteCNet) Durable() bool   { return true }

func (p *PasteCNet) Upload(ctx context.Context, data []byte, filename, contentType string) (string, string, error) {
	var locator string
	_, _, err := p.c.sendBytes(ctx, "PUT", p.host+"/", data, map[string]string{"Content-Type": contentType},
		func(status int, body string) error {
			if status < 200 || status >= 300 || !strings.HasPrefix(body, "http") {
				return fmt.Errorf("paste.c-net.org: HTTP %d body=%s", status, body)
			}
			locator = strings.TrimSpace(body)
			return nil
		})
	if err != nil {
		return "", "", err
	}
	return locator, "", nil
}

func (p *PasteCNet) Download(ctx context.Context, locator string, offset, length int64) (io.ReadCloser, error) {
	return p.c.rangeGet(ctx, locator, offset, length, nil)
}
func (p *PasteCNet) Delete(ctx context.Context, locator, deleteToken string) error { return nil }
