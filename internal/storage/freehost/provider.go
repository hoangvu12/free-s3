package freehost

import (
	"context"
	"io"
	"strings"
)

// binName forces a benign `.bin` extension so script-extension blocklists
// (fileditch, filebin) don't reject the chunk blob (RESEARCH.md gotcha #5).
func binName(name string) string {
	name = basename(name)
	if name == "" {
		return "chunk.bin"
	}
	if strings.HasSuffix(name, ".bin") {
		return name
	}
	return name + ".bin"
}

// basename returns the last path segment of a name or URL (after the final '/').
func basename(name string) string {
	name = strings.TrimRight(name, "/")
	if i := strings.LastIndexByte(name, '/'); i >= 0 {
		return name[i+1:]
	}
	return name
}

// Provider is one free file host behind the freehost backend. Each chunk is
// uploaded to R distinct providers (see backend.go, P4). Implementations are in
// the per-host files (fileditch.go, catbox.go, x0.go, ...) and are ~40-80 lines
// each, sharing the helpers in httputil.go.
type Provider interface {
	// Name is the stable provider id stored in chunk_replicas.provider
	// ("ia", "fileditch", "catbox", "x0.at", ...).
	Name() string
	// MaxBytes is the per-file cap. The chunker never sends a chunk larger than
	// the smallest MaxBytes among the providers selected for it.
	MaxBytes() int64
	// Durable distinguishes permanent / long-lived (anchor) hosts from temp
	// ones. Replica selection always includes >= 1 durable provider per chunk.
	Durable() bool
	// Upload stores data and returns a direct download URL (locator) plus an
	// optional per-file delete token ("" if the host has none / uses a global
	// credential). filename should carry a benign extension (.bin) to dodge
	// host extension blocklists.
	Upload(ctx context.Context, data []byte, filename, contentType string) (locator, deleteToken string, err error)
	// Download fetches [offset, offset+length) of the blob at locator via HTTP
	// Range. length <= 0 means "to end".
	Download(ctx context.Context, locator string, offset, length int64) (io.ReadCloser, error)
	// Delete removes the blob if the provider supports it (no-op + nil
	// otherwise). deleteToken is whatever Upload returned for this blob.
	Delete(ctx context.Context, locator, deleteToken string) error
}
