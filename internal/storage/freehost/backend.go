// Package freehost implements the storage.Backend over a pool of free public
// file-hosting sites. Each object is split into chunks and every chunk is
// replicated to R distinct providers; reads fetch from the first healthy
// replica. See BUILD-PLAN.md (§3) and RESEARCH.md for the provider catalog.
//
// This file currently holds a stub backend (P0): it satisfies the interface so
// the S3 gateway compiles and boots, but every storage operation returns
// ErrNotImplemented. The chunk-and-replicate implementation lands in P4.
package freehost

import (
	"context"
	"errors"
	"io"

	"free-s3/internal/storage"
)

// ErrNotImplemented is returned by every stub backend method until the real
// chunk-and-replicate backend lands (P4).
var ErrNotImplemented = errors.New("freehost: backend not implemented yet")

// Backend is the freehost storage.Backend. The stub holds no state; the real
// implementation (P4) carries the provider pool, chunk size, and concurrency
// limits.
type Backend struct{}

// New constructs the stub backend.
func New() *Backend { return &Backend{} }

// Ensure the stub satisfies the interface at compile time.
var _ storage.Backend = (*Backend)(nil)

func (b *Backend) Upload(ctx context.Context, name, contentType string, body io.Reader) ([]storage.Chunk, error) {
	return nil, ErrNotImplemented
}

func (b *Backend) Download(ctx context.Context, ref storage.ChunkRef) (io.ReadCloser, error) {
	return nil, ErrNotImplemented
}

func (b *Backend) DownloadRange(ctx context.Context, ref storage.ChunkRef, offset, length int64) (io.ReadCloser, error) {
	return nil, ErrNotImplemented
}

func (b *Backend) Delete(ctx context.Context, ref storage.ChunkRef) error {
	return ErrNotImplemented
}

func (b *Backend) DeleteBatch(ctx context.Context, refs []storage.ChunkRef) error {
	return ErrNotImplemented
}
