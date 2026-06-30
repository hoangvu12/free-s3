package freehost

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// Fileditch — anonymous, permanent, 100 GB/file. POST multipart field "file" to
// upload.php; the response is JSON carrying the direct URL. It blocks script
// extensions (php/html/js/exe/apk/sh/py/bat), so chunk blobs must be named
// `.bin` (RESEARCH.md gotcha #5). No delete API.
type Fileditch struct {
	c        *Client
	endpoint string
}

func NewFileditch(c *Client) *Fileditch {
	return &Fileditch{c: c, endpoint: "https://up.fileditch.com/upload.php"}
}

func (p *Fileditch) Name() string    { return "fileditch" }
func (p *Fileditch) MaxBytes() int64 { return 100 << 30 } // 100 GB
func (p *Fileditch) Durable() bool   { return true }

// fileditchResp tolerates both observed response shapes: a top-level url and a
// files[] array (different deployments report differently).
type fileditchResp struct {
	Success bool   `json:"success"`
	URL     string `json:"url"`
	Files   []struct {
		URL string `json:"url"`
	} `json:"files"`
}

func (p *Fileditch) Upload(ctx context.Context, data []byte, filename, contentType string) (string, string, error) {
	var locator string
	_, _, err := p.c.uploadMultipart(ctx, p.endpoint, nil, "file", binName(filename), contentType, data, nil,
		func(status int, body string) error {
			if status < 200 || status >= 300 {
				return fmt.Errorf("fileditch: HTTP %d: %s", status, body)
			}
			var r fileditchResp
			if err := json.Unmarshal([]byte(body), &r); err != nil {
				return fmt.Errorf("fileditch: non-JSON body: %s", body)
			}
			url := r.URL
			if url == "" && len(r.Files) > 0 {
				url = r.Files[0].URL
			}
			if !strings.HasPrefix(url, "https://") {
				return fmt.Errorf("fileditch: no url in response: %s", body)
			}
			locator = url
			return nil
		})
	if err != nil {
		return "", "", err
	}
	return locator, "", nil
}

func (p *Fileditch) Download(ctx context.Context, locator string, offset, length int64) (io.ReadCloser, error) {
	return p.c.rangeGet(ctx, locator, offset, length, nil)
}

func (p *Fileditch) Delete(ctx context.Context, locator, deleteToken string) error {
	return nil // fileditch has no delete API
}
