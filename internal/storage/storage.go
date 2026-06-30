package storage

import (
	"context"
	"io"
)

// Replica is one stored copy of a chunk on a single free host. The backend
// fetches a chunk by trying its replicas in order until one returns bytes, and
// deletes a chunk by best-effort removing every replica.
type Replica struct {
	Provider    string // "ia", "fileditch", "catbox", "x0.at", ...
	Locator     string // the direct download URL (or provider-native id we build a URL from)
	DeleteToken string // 0x0 X-Token etc.; "" if the provider has no per-file delete token
}

// Chunk = one contiguous slice of an object, replicated to R providers.
// Returned by Backend.Upload, persisted by the handler via metadata.
type Chunk struct {
	Seq      int
	Size     int64
	Offset   int64     // byte offset of this chunk's first byte within the object
	Replicas []Replica // len == R (or fewer if some uploads failed but >= 1 ok)
}

// ChunkRef = locator to fetch or delete ONE chunk. It is the chunk's replica
// list plus its size; the backend tries replicas in order (durable first) until
// one returns bytes.
type ChunkRef struct {
	Size     int64
	Replicas []Replica
}

type Backend interface {
	// Upload splits body into chunks (each <= the smallest selected provider
	// cap), uploads each chunk to R distinct providers concurrently, and
	// returns the ordered chunk list.
	Upload(ctx context.Context, name, contentType string, body io.Reader) ([]Chunk, error)
	// Download returns a reader over one chunk's full content (first healthy replica).
	Download(ctx context.Context, ref ChunkRef) (io.ReadCloser, error)
	// DownloadRange returns [offset, offset+length) of one chunk via an HTTP
	// Range on a replica. length <= 0 means "to end of chunk".
	DownloadRange(ctx context.Context, ref ChunkRef, offset, length int64) (io.ReadCloser, error)
	// Delete best-effort removes every replica of one chunk.
	Delete(ctx context.Context, ref ChunkRef) error
	// DeleteBatch best-effort removes every replica of many chunks (per-ref
	// failures are logged by the backend so the caller treats it as
	// best-effort cleanup; the returned error is the first encountered).
	DeleteBatch(ctx context.Context, refs []ChunkRef) error
}
