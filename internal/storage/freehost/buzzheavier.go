package freehost

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// Buzzheavier — PUT the raw body to w.buzzheavier.com/{filename}; the JSON
// response carries an id used to build the direct link. Activity-based
// retention (keep-alive re-reads). Mechanics are docs-derived (RESEARCH.md 🟡),
// so this provider is opt-in (not in the default order) until verified live
// from the deploy IP.
type Buzzheavier struct {
	c       *Client
	token   string // optional Bearer
	putHost string // https://w.buzzheavier.com
	dlBase  string // https://buzzheavier.com
}

func NewBuzzheavier(c *Client, token string) *Buzzheavier {
	return &Buzzheavier{c: c, token: token, putHost: "https://w.buzzheavier.com", dlBase: "https://buzzheavier.com"}
}

func (p *Buzzheavier) Name() string    { return "buzzheavier" }
func (p *Buzzheavier) MaxBytes() int64 { return 100 << 30 }
func (p *Buzzheavier) Durable() bool   { return true }

func (p *Buzzheavier) Upload(ctx context.Context, data []byte, filename, contentType string) (string, string, error) {
	url := p.putHost + "/" + binName(filename)
	headers := map[string]string{"Content-Type": contentType}
	if p.token != "" {
		headers["Authorization"] = "Bearer " + p.token
	}
	var id string
	_, _, err := p.c.sendBytes(ctx, "PUT", url, data, headers, func(status int, body string) error {
		if status < 200 || status >= 300 {
			return fmt.Errorf("buzzheavier: PUT %s -> %d: %s", url, status, body)
		}
		var r struct {
			Data struct {
				ID string `json:"id"`
			} `json:"data"`
		}
		if err := json.Unmarshal([]byte(body), &r); err != nil || r.Data.ID == "" {
			return fmt.Errorf("buzzheavier: no id in response: %s", body)
		}
		id = r.Data.ID
		return nil
	})
	if err != nil {
		return "", "", err
	}
	return fmt.Sprintf("%s/%s/download", p.dlBase, id), "", nil
}

func (p *Buzzheavier) Download(ctx context.Context, locator string, offset, length int64) (io.ReadCloser, error) {
	return p.c.rangeGet(ctx, locator, offset, length, nil)
}

func (p *Buzzheavier) Delete(ctx context.Context, locator, _ string) error {
	if p.token == "" {
		return nil
	}
	id := basename(strings.TrimSuffix(locator, "/download"))
	if id == "" {
		return nil
	}
	status, body, err := p.c.deleteWithHeaders(ctx, fmt.Sprintf("%s/%s", p.dlBase, id), map[string]string{"Authorization": "Bearer " + p.token})
	if err != nil {
		return err
	}
	if status < 200 || status >= 300 {
		return fmt.Errorf("buzzheavier delete %s -> %d: %s", id, status, body)
	}
	return nil
}
