package freehost

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strings"
)

// Fileditch — anonymous, permanent, 100 GB/file. POST multipart field "file" to
// upload.php; the response JSON's "url" is an HTML landing page (NOT a raw
// hotlink — fileditch stopped serving direct links in 2026). The page embeds a
// freshly-signed, expiring direct URL on a separate host, which we scrape per
// read (see Download). It blocks script extensions (php/html/js/exe/apk/sh/py/
// bat), so chunk blobs must be named `.bin` (RESEARCH.md gotcha #5). No delete API.
type Fileditch struct {
	c        *Client
	endpoint string
}

func NewFileditch(c *Client) *Fileditch {
	// Permanent storage endpoint (the old up.fileditch.com host is dead as of
	// 2026; verified live). Success JSON carries a top-level "url" pointing at
	// fileditchfiles.me; temp.fileditch.com is the 72h-expiry variant.
	return &Fileditch{c: c, endpoint: "https://new.fileditch.com/upload.php"}
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

// fileditchDirectRe matches the signed, expiring direct-download URL embedded in
// the landing page (e.g. https://<host>/path/file.bin?md5=...&expires=...). The
// host name varies/rotates, so we key off the md5= query marker, not the host.
var fileditchDirectRe = regexp.MustCompile(`https://[^"'\s]+?[?&]md5=[^"'\s]+`)

// resolveDirect fetches the landing page and extracts the current signed direct
// URL. The signed link expires, but the landing page (the stored locator) is
// stable and mints a fresh one on each load — so we scrape on every read.
func (p *Fileditch) resolveDirect(ctx context.Context, landing string) (string, error) {
	html, err := p.c.getString(ctx, landing, 1<<20)
	if err != nil {
		return "", fmt.Errorf("fileditch: fetch landing %s: %w", landing, err)
	}
	m := fileditchDirectRe.FindString(html)
	if m == "" {
		return "", fmt.Errorf("fileditch: no signed direct link in landing page %s", landing)
	}
	return strings.ReplaceAll(m, "&amp;", "&"), nil // un-HTML-escape the query
}

func (p *Fileditch) Download(ctx context.Context, locator string, offset, length int64) (io.ReadCloser, error) {
	direct, err := p.resolveDirect(ctx, locator)
	if err != nil {
		return nil, err
	}
	return p.c.rangeGet(ctx, direct, offset, length, nil)
}

func (p *Fileditch) Delete(ctx context.Context, locator, deleteToken string) error {
	return nil // fileditch has no delete API
}
