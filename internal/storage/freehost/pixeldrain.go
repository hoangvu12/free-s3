package freehost

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// Pixeldrain — long-lived (free prunes ~90d inactivity → keep-alive). POST
// multipart field "file"; the API key (if set) goes in HTTP Basic auth
// (username empty, key as password). Response is JSON {"id":"..."}; the raw
// bytes live at /api/file/{id} (Range-capable). Delete is DELETE /api/file/{id}.
type Pixeldrain struct {
	c        *Client
	apiKey   string
	base     string // https://pixeldrain.com
	uploadEP string // {base}/api/file
}

func NewPixeldrain(c *Client, apiKey string) *Pixeldrain {
	base := "https://pixeldrain.com"
	return &Pixeldrain{c: c, apiKey: strings.TrimSpace(apiKey), base: base, uploadEP: base + "/api/file"}
}

func (p *Pixeldrain) Name() string    { return "pixeldrain" }
func (p *Pixeldrain) MaxBytes() int64 { return 20 << 30 } // ~20 GB
func (p *Pixeldrain) Durable() bool   { return true }

func (p *Pixeldrain) basicAuth() map[string]string {
	if p.apiKey == "" {
		return nil
	}
	tok := base64.StdEncoding.EncodeToString([]byte(":" + p.apiKey))
	return map[string]string{"Authorization": "Basic " + tok}
}

func (p *Pixeldrain) Upload(ctx context.Context, data []byte, filename, contentType string) (string, string, error) {
	var id string
	_, _, err := p.c.uploadMultipart(ctx, p.uploadEP, nil, "file", binName(filename), contentType, data, p.basicAuth(),
		func(status int, body string) error {
			if status < 200 || status >= 300 {
				return fmt.Errorf("pixeldrain: HTTP %d: %s", status, body)
			}
			var r struct {
				ID string `json:"id"`
			}
			if err := json.Unmarshal([]byte(body), &r); err != nil || r.ID == "" {
				return fmt.Errorf("pixeldrain: no id in response: %s", body)
			}
			id = r.ID
			return nil
		})
	if err != nil {
		return "", "", err
	}
	return fmt.Sprintf("%s/api/file/%s", p.base, id), "", nil
}

func (p *Pixeldrain) Download(ctx context.Context, locator string, offset, length int64) (io.ReadCloser, error) {
	return p.c.rangeGet(ctx, locator, offset, length, nil)
}

func (p *Pixeldrain) Delete(ctx context.Context, locator, _ string) error {
	id := basename(locator)
	if id == "" {
		return nil
	}
	status, body, err := p.c.deleteWithHeaders(ctx, fmt.Sprintf("%s/api/file/%s", p.base, id), p.basicAuth())
	if err != nil {
		return err
	}
	if status < 200 || status >= 300 {
		return fmt.Errorf("pixeldrain delete %s -> %d: %s", id, status, body)
	}
	return nil
}
