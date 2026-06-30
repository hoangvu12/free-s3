package freehost

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// Gofile — 2-step upload (GET /servers to pick a store node, then POST the file
// to it). A token is REQUIRED here: gofile's raw, hotlinkable direct link comes
// from POST /contents/{id}/directlinks, which needs the account token, so the
// registry skips gofile when GOFILE_TOKEN is unset (anonymous gofile only yields
// a tokenized download page, not a raw-bytes URL). Inactivity → cold storage, so
// gofile is a secondary replica behind a permanent anchor.
type Gofile struct {
	c     *Client
	token string
	api   string // https://api.gofile.io
}

func NewGofile(c *Client, token string) *Gofile {
	return &Gofile{c: c, token: token, api: "https://api.gofile.io"}
}

func (p *Gofile) Name() string    { return "gofile" }
func (p *Gofile) MaxBytes() int64 { return 100 << 30 } // effectively large
func (p *Gofile) Durable() bool   { return true }

func (p *Gofile) bestServer(ctx context.Context) (string, error) {
	cctx, cancel := context.WithTimeout(ctx, defaultDownloadTimeout)
	defer cancel()
	req, _ := http.NewRequestWithContext(cctx, http.MethodGet, p.api+"/servers", nil)
	resp, err := p.c.do(req)
	if err != nil {
		return "", err
	}
	body := slurp(resp)
	var r struct {
		Status string `json:"status"`
		Data   struct {
			Servers []struct {
				Name string `json:"name"`
			} `json:"servers"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(body), &r); err != nil || r.Status != "ok" || len(r.Data.Servers) == 0 {
		return "", fmt.Errorf("gofile: /servers bad response: %s", body)
	}
	return r.Data.Servers[0].Name, nil
}

func (p *Gofile) Upload(ctx context.Context, data []byte, filename, contentType string) (string, string, error) {
	server, err := p.bestServer(ctx)
	if err != nil {
		return "", "", err
	}
	fields := map[string]string{"token": p.token}
	uploadURL := fmt.Sprintf("https://%s.gofile.io/contents/uploadfile", server)
	var fileID string
	_, _, err = p.c.uploadMultipart(ctx, uploadURL, fields, "file", binName(filename), contentType, data, nil,
		func(status int, body string) error {
			var r struct {
				Status string `json:"status"`
				Data   struct {
					FileID string `json:"fileId"`
				} `json:"data"`
			}
			if err := json.Unmarshal([]byte(body), &r); err != nil || r.Status != "ok" || r.Data.FileID == "" {
				return fmt.Errorf("gofile: upload bad response: HTTP %d %s", status, body)
			}
			fileID = r.Data.FileID
			return nil
		})
	if err != nil {
		return "", "", err
	}
	// Promote to a raw direct link (needs the account token).
	direct, err := p.directLink(ctx, fileID)
	if err != nil {
		return "", "", err
	}
	// Locator carries the direct URL; deleteToken carries the contentId so
	// Delete can target it (the URL alone doesn't expose it cleanly).
	return direct, fileID, nil
}

func (p *Gofile) directLink(ctx context.Context, contentID string) (string, error) {
	url := fmt.Sprintf("%s/contents/%s/directlinks", p.api, contentID)
	body, _, err := p.c.sendBytes(ctx, http.MethodPost, url, nil, map[string]string{
		"Authorization": "Bearer " + p.token,
		"Content-Type":  "application/json",
	}, func(status int, body string) error {
		if status < 200 || status >= 300 {
			return fmt.Errorf("gofile directlinks HTTP %d: %s", status, body)
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	var r struct {
		Data struct {
			DirectLink string `json:"directLink"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(body), &r); err != nil || r.Data.DirectLink == "" {
		return "", fmt.Errorf("gofile: no directLink: %s", body)
	}
	return r.Data.DirectLink, nil
}

func (p *Gofile) Download(ctx context.Context, locator string, offset, length int64) (io.ReadCloser, error) {
	return p.c.rangeGet(ctx, locator, offset, length, nil)
}

func (p *Gofile) Delete(ctx context.Context, _, deleteToken string) error {
	if deleteToken == "" {
		return nil
	}
	url := fmt.Sprintf("%s/contents/%s", p.api, deleteToken)
	status, body, err := p.c.deleteWithHeaders(ctx, url, map[string]string{"Authorization": "Bearer " + p.token})
	if err != nil {
		return err
	}
	if status < 200 || status >= 300 {
		return fmt.Errorf("gofile delete %s -> %d: %s", deleteToken, status, body)
	}
	return nil
}
