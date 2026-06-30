package freehost

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// pomf is the generalized pomf-family provider: POST a multipart "files[]" field
// to the upload endpoint, get JSON {success, files:[{url, name}]} back. Covers
// pomf.lain.la (durable, dedicated HW), uguu, tmp.ninja, doko.moe, cockfile
// (temp tiers). Endpoint path differs (uguu uses /upload, others /upload.php),
// so it is parameterized. No delete API.
//
// RESEARCH.md flags some of these response schemas as inferred — the live test
// (run from the deploy IP) is the confirmation; validation here is strict so a
// wrong guess fails loudly instead of storing a bad locator.
type pomf struct {
	c        *Client
	name     string
	endpoint string
	maxByte  int64
	durable  bool
}

func NewPomfLainLa(c *Client) *pomf {
	return &pomf{c: c, name: "pomf.lain.la", endpoint: "https://pomf.lain.la/upload.php", maxByte: 1 << 30, durable: true}
}
func NewUguu(c *Client) *pomf {
	return &pomf{c: c, name: "uguu", endpoint: "https://uguu.se/upload", maxByte: 128 << 20, durable: false}
}
func NewTmpNinja(c *Client) *pomf {
	return &pomf{c: c, name: "tmp.ninja", endpoint: "https://tmp.ninja/upload.php", maxByte: 10 << 30, durable: false}
}
func NewDokoMoe(c *Client) *pomf {
	return &pomf{c: c, name: "doko.moe", endpoint: "https://doko.moe/upload.php", maxByte: 2 << 30, durable: false}
}
func NewCockfile(c *Client) *pomf {
	return &pomf{c: c, name: "cockfile", endpoint: "https://cockfile.com/upload.php", maxByte: 128 << 20, durable: false}
}

func (p *pomf) Name() string    { return p.name }
func (p *pomf) MaxBytes() int64 { return p.maxByte }
func (p *pomf) Durable() bool   { return p.durable }

func (p *pomf) Upload(ctx context.Context, data []byte, filename, contentType string) (string, string, error) {
	var locator string
	_, _, err := p.c.uploadMultipart(ctx, p.endpoint, nil, "files[]", binName(filename), contentType, data, nil,
		func(status int, body string) error {
			if status < 200 || status >= 300 {
				return fmt.Errorf("%s: HTTP %d: %s", p.name, status, body)
			}
			var r struct {
				Success bool `json:"success"`
				Files   []struct {
					URL string `json:"url"`
				} `json:"files"`
			}
			if err := json.Unmarshal([]byte(body), &r); err != nil || len(r.Files) == 0 || !strings.HasPrefix(r.Files[0].URL, "http") {
				return fmt.Errorf("%s: no files[].url: %s", p.name, body)
			}
			locator = r.Files[0].URL
			return nil
		})
	if err != nil {
		return "", "", err
	}
	return locator, "", nil
}

func (p *pomf) Download(ctx context.Context, locator string, offset, length int64) (io.ReadCloser, error) {
	return p.c.rangeGet(ctx, locator, offset, length, nil)
}

func (p *pomf) Delete(ctx context.Context, locator, deleteToken string) error {
	return nil // pomf family has no delete API
}
