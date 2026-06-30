package freehost

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// --- temp.sh: POST /upload field "file" -> plaintext body URL ----------------

type TempSh struct {
	c        *Client
	endpoint string
}

func NewTempSh(c *Client) *TempSh { return &TempSh{c: c, endpoint: "https://temp.sh/upload"} }

func (p *TempSh) Name() string    { return "temp.sh" }
func (p *TempSh) MaxBytes() int64 { return 4 << 30 } // 4 GB
func (p *TempSh) Durable() bool   { return false }

func (p *TempSh) Upload(ctx context.Context, data []byte, filename, contentType string) (string, string, error) {
	var locator string
	_, _, err := p.c.uploadMultipart(ctx, p.endpoint, nil, "file", binName(filename), contentType, data, nil,
		func(status int, body string) error {
			if status < 200 || status >= 300 || !strings.HasPrefix(body, "http") {
				return fmt.Errorf("temp.sh: HTTP %d body=%s", status, body)
			}
			locator = strings.TrimSpace(body)
			return nil
		})
	if err != nil {
		return "", "", err
	}
	return locator, "", nil
}

// Download POSTs to the file URL — temp.sh serves the bytes only via the page's
// "click to download" form (a plain GET returns an HTML preview). It ignores
// Range and returns the whole file (200), so rangeGetMethod emulates the window
// client-side; fine for a non-durable overflow host that rarely serves reads.
func (p *TempSh) Download(ctx context.Context, locator string, offset, length int64) (io.ReadCloser, error) {
	return p.c.rangeGetMethod(ctx, http.MethodPost, locator, offset, length, nil)
}
func (p *TempSh) Delete(ctx context.Context, locator, deleteToken string) error { return nil }

// --- litterbox (catbox temp): POST api.php reqtype=fileupload + time ----------

type Litterbox struct {
	c        *Client
	endpoint string
	ttl      string // 1h/12h/24h/72h
}

func NewLitterbox(c *Client) *Litterbox {
	return &Litterbox{c: c, endpoint: "https://litterbox.catbox.moe/resources/internals/api.php", ttl: "72h"}
}

func (p *Litterbox) Name() string    { return "litterbox" }
func (p *Litterbox) MaxBytes() int64 { return 1 << 30 } // 1 GB
func (p *Litterbox) Durable() bool   { return false }

func (p *Litterbox) Upload(ctx context.Context, data []byte, filename, contentType string) (string, string, error) {
	fields := map[string]string{"reqtype": "fileupload", "time": p.ttl}
	var locator string
	_, _, err := p.c.uploadMultipart(ctx, p.endpoint, fields, "fileToUpload", binName(filename), contentType, data, nil,
		func(status int, body string) error {
			if status < 200 || status >= 300 || !strings.HasPrefix(body, "https://litter.catbox.moe/") {
				return fmt.Errorf("litterbox: HTTP %d body=%s", status, body)
			}
			locator = strings.TrimSpace(body)
			return nil
		})
	if err != nil {
		return "", "", err
	}
	return locator, "", nil
}

func (p *Litterbox) Download(ctx context.Context, locator string, offset, length int64) (io.ReadCloser, error) {
	return p.c.rangeGet(ctx, locator, offset, length, nil)
}
func (p *Litterbox) Delete(ctx context.Context, locator, deleteToken string) error { return nil }

// --- tmpfiles.org: POST /api/v1/upload field "file" -> JSON data.url ----------

// The API returns a viewer URL (tmpfiles.org/{id}/{name}); the raw bytes live at
// tmpfiles.org/dl/{id}/{name}, so the response URL is rewritten to the /dl/ form.
type Tmpfiles struct {
	c        *Client
	endpoint string
}

func NewTmpfiles(c *Client) *Tmpfiles {
	return &Tmpfiles{c: c, endpoint: "https://tmpfiles.org/api/v1/upload"}
}

func (p *Tmpfiles) Name() string    { return "tmpfiles.org" }
func (p *Tmpfiles) MaxBytes() int64 { return 100 << 20 } // 100 MB
func (p *Tmpfiles) Durable() bool   { return false }

func (p *Tmpfiles) Upload(ctx context.Context, data []byte, filename, contentType string) (string, string, error) {
	var locator string
	_, _, err := p.c.uploadMultipart(ctx, p.endpoint, nil, "file", binName(filename), contentType, data, nil,
		func(status int, body string) error {
			var r struct {
				Data struct {
					URL string `json:"url"`
				} `json:"data"`
			}
			if err := json.Unmarshal([]byte(body), &r); err != nil || !strings.Contains(r.Data.URL, "tmpfiles.org/") {
				return fmt.Errorf("tmpfiles: HTTP %d bad body=%s", status, body)
			}
			// Rewrite https://tmpfiles.org/{id}/{name} -> /dl/{id}/{name}.
			locator = strings.Replace(r.Data.URL, "tmpfiles.org/", "tmpfiles.org/dl/", 1)
			return nil
		})
	if err != nil {
		return "", "", err
	}
	return locator, "", nil
}

func (p *Tmpfiles) Download(ctx context.Context, locator string, offset, length int64) (io.ReadCloser, error) {
	return p.c.rangeGet(ctx, locator, offset, length, nil)
}
func (p *Tmpfiles) Delete(ctx context.Context, locator, deleteToken string) error { return nil }

// --- tmpfile.link: POST /api/upload field "file" -> JSON downloadLink ---------

type TmpfileLink struct {
	c        *Client
	endpoint string
}

func NewTmpfileLink(c *Client) *TmpfileLink {
	return &TmpfileLink{c: c, endpoint: "https://tmpfile.link/api/upload"}
}

func (p *TmpfileLink) Name() string    { return "tmpfile.link" }
func (p *TmpfileLink) MaxBytes() int64 { return 100 << 20 } // 100 MB
func (p *TmpfileLink) Durable() bool   { return false }

func (p *TmpfileLink) Upload(ctx context.Context, data []byte, filename, contentType string) (string, string, error) {
	var locator string
	_, _, err := p.c.uploadMultipart(ctx, p.endpoint, nil, "file", binName(filename), contentType, data, nil,
		func(status int, body string) error {
			var r struct {
				DownloadLink string `json:"downloadLink"`
			}
			if err := json.Unmarshal([]byte(body), &r); err != nil || !strings.HasPrefix(r.DownloadLink, "http") {
				return fmt.Errorf("tmpfile.link: HTTP %d bad body=%s", status, body)
			}
			locator = r.DownloadLink
			return nil
		})
	if err != nil {
		return "", "", err
	}
	return locator, "", nil
}

func (p *TmpfileLink) Download(ctx context.Context, locator string, offset, length int64) (io.ReadCloser, error) {
	return p.c.rangeGet(ctx, locator, offset, length, nil)
}
func (p *TmpfileLink) Delete(ctx context.Context, locator, deleteToken string) error { return nil }
