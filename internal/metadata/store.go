package metadata

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

var ErrNotFound = errors.New("not found")

// Store holds two *sql.DB handles to the same WAL-mode SQLite file. modernc.org/sqlite
// guidance (and the SQLite WAL model in general): a single pool with N connections
// does NOT give parallel writes — SQLite serializes the writer regardless — and tends
// to add SQLITE_BUSY contention. The right pattern is one writer connection plus a
// pool of reader connections, since WAL allows many concurrent readers and one writer
// to coexist freely.
//
// write is the sole writer (MaxOpenConns=1, BeginTx + Exec target it).
// read is the reader pool (MaxOpenConns=N, QueryContext/QueryRowContext target it).
type Store struct {
	write *sql.DB
	read  *sql.DB
}

type Bucket struct {
	Name      string
	CreatedAt time.Time
}

type Object struct {
	Bucket      string
	Key         string
	Size        int64
	ETag        string
	ContentType string
	CreatedAt   time.Time
	UpdatedAt   time.Time
	// Metadata is the optional side-table content (content-disposition,
	// content-encoding, cache-control, expires, x-amz-meta-*) — write-only
	// input to PutObject/FinalizeMultipartUpload. Reads use GetObjectMetadata
	// (a separate query; GetObject does not populate this).
	Metadata map[string]string
}

// Replica is one stored copy of a chunk on a single free host. A chunk has R
// replicas (one row per provider). Alive is the self-heal liveness flag: a
// read that finds a replica 404/410 marks it dead (0) so future reads skip it
// and the keep-alive sweep can refill R.
type Replica struct {
	Provider    string
	Locator     string // direct download URL (or provider-native id we build a URL from)
	DeleteToken string // 0x0 X-Token / "" if the provider has no per-file delete token
	Alive       bool
}

// Chunk mirrors storage.Chunk for persistence (the layers stay decoupled; the
// handler converts between them, as it already does for Object). A chunk is one
// contiguous slice of an object, replicated to R providers.
type Chunk struct {
	Seq      int
	Size     int64
	Offset   int64
	Replicas []Replica
}

// Open opens the metadata store with the default reader-pool size (8). Use
// OpenWithOptions when callers want to override via config.
func Open(path string) (*Store, error) {
	return OpenWithOptions(path, 8)
}

// OpenWithOptions opens the metadata store with an explicit reader-pool size.
// readerConns <= 0 falls back to 8. The writer pool is always 1 (SQLite's
// single-writer model). Both handles share the same WAL file and busy_timeout.
func OpenWithOptions(path string, readerConns int) (*Store, error) {
	if readerConns <= 0 {
		readerConns = 8
	}
	// WAL + a busy timeout make the single-file DB resilient to the extra
	// write pressure from chunk maps / multipart.
	dsn := path + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(ON)"

	write, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	write.SetMaxOpenConns(1)

	read, err := sql.Open("sqlite", dsn)
	if err != nil {
		write.Close()
		return nil, err
	}
	read.SetMaxOpenConns(readerConns)
	read.SetMaxIdleConns(readerConns)

	store := &Store{write: write, read: read}
	if err := store.migrate(); err != nil {
		write.Close()
		read.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error {
	rerr := s.read.Close()
	werr := s.write.Close()
	if werr != nil {
		return werr
	}
	return rerr
}

// migrate creates the free-s3 schema. We start with no legacy data (the fork
// drops every Telegram-era table and the additive backfill), so the schema is
// authored cleanly: a generic chunk table plus a per-replica side-table.
func (s *Store) migrate() error {
	_, err := s.write.Exec(`
CREATE TABLE IF NOT EXISTS buckets (
  name TEXT PRIMARY KEY,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS objects (
  bucket TEXT NOT NULL,
  key TEXT NOT NULL,
  size INTEGER NOT NULL,
  etag TEXT NOT NULL,
  content_type TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  deleted_at TEXT,
  PRIMARY KEY (bucket, key),
  FOREIGN KEY (bucket) REFERENCES buckets(name) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_objects_bucket_key ON objects(bucket, key);

CREATE TABLE IF NOT EXISTS object_chunks (
  bucket TEXT NOT NULL,
  key TEXT NOT NULL,
  part_seq INTEGER NOT NULL,
  size INTEGER NOT NULL,
  offset INTEGER NOT NULL,
  PRIMARY KEY (bucket, key, part_seq)
);

CREATE INDEX IF NOT EXISTS idx_object_chunks_key ON object_chunks(bucket, key, part_seq);

-- One row per (chunk, replica). alive is flipped to 0 by self-heal when a
-- read finds the replica gone; the keep-alive sweep refills R from a survivor.
CREATE TABLE IF NOT EXISTS chunk_replicas (
  bucket TEXT NOT NULL,
  key TEXT NOT NULL,
  part_seq INTEGER NOT NULL,
  replica_idx INTEGER NOT NULL,
  provider TEXT NOT NULL,
  locator TEXT NOT NULL,
  delete_token TEXT NOT NULL DEFAULT '',
  alive INTEGER NOT NULL DEFAULT 1,
  PRIMARY KEY (bucket, key, part_seq, replica_idx)
);

CREATE INDEX IF NOT EXISTS idx_chunk_replicas_key ON chunk_replicas(bucket, key, part_seq);

CREATE TABLE IF NOT EXISTS multipart_uploads (
  upload_id TEXT PRIMARY KEY,
  bucket TEXT NOT NULL,
  key TEXT NOT NULL,
  content_type TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS multipart_parts (
  upload_id TEXT NOT NULL,
  part_number INTEGER NOT NULL,
  etag TEXT NOT NULL,
  size INTEGER NOT NULL,
  PRIMARY KEY (upload_id, part_number)
);

CREATE TABLE IF NOT EXISTS multipart_part_chunks (
  upload_id TEXT NOT NULL,
  part_number INTEGER NOT NULL,
  seq INTEGER NOT NULL,
  size INTEGER NOT NULL,
  PRIMARY KEY (upload_id, part_number, seq)
);

CREATE TABLE IF NOT EXISTS multipart_part_chunk_replicas (
  upload_id TEXT NOT NULL,
  part_number INTEGER NOT NULL,
  seq INTEGER NOT NULL,
  replica_idx INTEGER NOT NULL,
  provider TEXT NOT NULL,
  locator TEXT NOT NULL,
  delete_token TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (upload_id, part_number, seq, replica_idx)
);

CREATE TABLE IF NOT EXISTS object_metadata (
  bucket TEXT NOT NULL,
  key TEXT NOT NULL,
  name TEXT NOT NULL,
  value TEXT NOT NULL,
  PRIMARY KEY (bucket, key, name)
);

CREATE TABLE IF NOT EXISTS multipart_upload_metadata (
  upload_id TEXT NOT NULL,
  name TEXT NOT NULL,
  value TEXT NOT NULL,
  PRIMARY KEY (upload_id, name)
);
`)
	return err
}

func (s *Store) CreateBucket(ctx context.Context, name string) error {
	_, err := s.write.ExecContext(ctx, `INSERT INTO buckets(name, created_at) VALUES(?, ?)`, name, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

func (s *Store) DeleteBucket(ctx context.Context, name string) error {
	result, err := s.write.ExecContext(ctx, `DELETE FROM buckets WHERE name = ?`, name)
	if err != nil {
		return err
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) BucketExists(ctx context.Context, name string) (bool, error) {
	var exists int
	err := s.read.QueryRowContext(ctx, `SELECT 1 FROM buckets WHERE name = ?`, name).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

func (s *Store) ListBuckets(ctx context.Context) ([]Bucket, error) {
	rows, err := s.read.QueryContext(ctx, `SELECT name, created_at FROM buckets ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var buckets []Bucket
	for rows.Next() {
		var b Bucket
		var created string
		if err := rows.Scan(&b.Name, &created); err != nil {
			return nil, err
		}
		b.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
		buckets = append(buckets, b)
	}
	return buckets, rows.Err()
}

// PutObject writes the object row and replaces its chunk map (chunks + replicas)
// atomically. An overwrite drops the prior chunk/replica rows; the caller is
// responsible for deleting the superseded blobs on the free hosts (it holds the
// backend).
func (s *Store) PutObject(ctx context.Context, obj Object, chunks []Chunk) error {
	now := time.Now().UTC()
	if obj.CreatedAt.IsZero() {
		obj.CreatedAt = now
	}
	obj.UpdatedAt = now

	tx, err := s.write.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := upsertObjectRowTx(ctx, tx, obj); err != nil {
		return err
	}
	if err := replaceObjectChunksTx(ctx, tx, obj.Bucket, obj.Key, chunks); err != nil {
		return err
	}
	if err := replaceObjectMetadataTx(ctx, tx, obj.Bucket, obj.Key, obj.Metadata); err != nil {
		return err
	}
	return tx.Commit()
}

// replaceObjectMetadata side-table mirrors the object_chunks replace pattern:
// the rows are wiped and re-inserted inside the object's own transaction, so an
// object stored without metadata simply has no rows and an overwrite never
// leaves stale metadata.
func replaceObjectMetadataTx(ctx context.Context, tx *sql.Tx, bucket, key string, kv map[string]string) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM object_metadata WHERE bucket = ? AND key = ?`, bucket, key); err != nil {
		return err
	}
	for name, value := range kv {
		if _, err := tx.ExecContext(ctx, `INSERT INTO object_metadata(bucket, key, name, value) VALUES(?, ?, ?, ?)`, bucket, key, name, value); err != nil {
			return err
		}
	}
	return nil
}

// GetObjectMetadata returns the object's side-table metadata (empty for objects
// stored without any).
func (s *Store) GetObjectMetadata(ctx context.Context, bucket, key string) (map[string]string, error) {
	rows, err := s.read.QueryContext(ctx, `SELECT name, value FROM object_metadata WHERE bucket = ? AND key = ?`, bucket, key)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	md := map[string]string{}
	for rows.Next() {
		var name, value string
		if err := rows.Scan(&name, &value); err != nil {
			return nil, err
		}
		md[name] = value
	}
	return md, rows.Err()
}

func (s *Store) GetObject(ctx context.Context, bucket, key string) (Object, error) {
	var obj Object
	var created, updated string
	err := s.read.QueryRowContext(ctx, `
SELECT bucket, key, size, etag, content_type, created_at, updated_at
FROM objects
WHERE bucket = ? AND key = ? AND deleted_at IS NULL
`, bucket, key).Scan(&obj.Bucket, &obj.Key, &obj.Size, &obj.ETag, &obj.ContentType, &created, &updated)
	if errors.Is(err, sql.ErrNoRows) {
		return Object{}, ErrNotFound
	}
	if err != nil {
		return Object{}, err
	}
	obj.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
	obj.UpdatedAt, _ = time.Parse(time.RFC3339Nano, updated)
	return obj, nil
}

// GetObjectChunks returns the ordered chunk map, each chunk carrying its full
// replica list (ordered by replica_idx). Empty objects return nil.
func (s *Store) GetObjectChunks(ctx context.Context, bucket, key string) ([]Chunk, error) {
	rows, err := s.read.QueryContext(ctx, `
SELECT part_seq, size, offset
FROM object_chunks
WHERE bucket = ? AND key = ?
ORDER BY part_seq
`, bucket, key)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var chunks []Chunk
	idxBySeq := map[int]int{}
	for rows.Next() {
		var c Chunk
		if err := rows.Scan(&c.Seq, &c.Size, &c.Offset); err != nil {
			return nil, err
		}
		idxBySeq[c.Seq] = len(chunks)
		chunks = append(chunks, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(chunks) == 0 {
		return nil, nil
	}

	rrows, err := s.read.QueryContext(ctx, `
SELECT part_seq, provider, locator, delete_token, alive
FROM chunk_replicas
WHERE bucket = ? AND key = ?
ORDER BY part_seq, replica_idx
`, bucket, key)
	if err != nil {
		return nil, err
	}
	defer rrows.Close()
	for rrows.Next() {
		var seq int
		var r Replica
		var alive int
		if err := rrows.Scan(&seq, &r.Provider, &r.Locator, &r.DeleteToken, &alive); err != nil {
			return nil, err
		}
		r.Alive = alive != 0
		if ci, ok := idxBySeq[seq]; ok {
			chunks[ci].Replicas = append(chunks[ci].Replicas, r)
		}
	}
	return chunks, rrows.Err()
}

// DeleteObject hard-deletes the object, its chunk map, and replica rows. The
// free-host blob removal is the caller's responsibility (done before this,
// while the chunk/replica list is still readable).
func (s *Store) DeleteObject(ctx context.Context, bucket, key string) error {
	tx, err := s.write.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, q := range []string{
		`DELETE FROM chunk_replicas WHERE bucket = ? AND key = ?`,
		`DELETE FROM object_chunks WHERE bucket = ? AND key = ?`,
		`DELETE FROM object_metadata WHERE bucket = ? AND key = ?`,
		`DELETE FROM objects WHERE bucket = ? AND key = ?`,
	} {
		if _, err := tx.ExecContext(ctx, q, bucket, key); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// MarkReplicaDead flips a replica's alive flag to 0 (self-heal read path). A
// missing row is not an error — the replica may already have been pruned.
func (s *Store) MarkReplicaDead(ctx context.Context, bucket, key string, partSeq, replicaIdx int) error {
	_, err := s.write.ExecContext(ctx, `UPDATE chunk_replicas SET alive = 0 WHERE bucket = ? AND key = ? AND part_seq = ? AND replica_idx = ?`,
		bucket, key, partSeq, replicaIdx)
	return err
}

// ObjectKey identifies one object for the keep-alive sweep.
type ObjectKey struct {
	Bucket string
	Key    string
}

// AllObjectKeys lists every live object (bucket, key) for the keep-alive sweep
// to iterate. Ordered so a cursor-based sweep can page deterministically.
func (s *Store) AllObjectKeys(ctx context.Context) ([]ObjectKey, error) {
	rows, err := s.read.QueryContext(ctx, `SELECT bucket, key FROM objects WHERE deleted_at IS NULL ORDER BY bucket, key`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ObjectKey
	for rows.Next() {
		var k ObjectKey
		if err := rows.Scan(&k.Bucket, &k.Key); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

// UpdateChunkReplicas atomically replaces the replica rows for one chunk (the
// self-heal write-back: surviving replicas plus any freshly re-uploaded ones,
// all marked alive). replica_idx is reassigned densely from 0.
func (s *Store) UpdateChunkReplicas(ctx context.Context, bucket, key string, partSeq int, reps []Replica) error {
	tx, err := s.write.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM chunk_replicas WHERE bucket = ? AND key = ? AND part_seq = ?`, bucket, key, partSeq); err != nil {
		return err
	}
	for i, r := range reps {
		alive := 1
		if !r.Alive {
			alive = 0
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO chunk_replicas(bucket, key, part_seq, replica_idx, provider, locator, delete_token, alive) VALUES(?, ?, ?, ?, ?, ?, ?, ?)`,
			bucket, key, partSeq, i, r.Provider, r.Locator, r.DeleteToken, alive); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// AppendChunkReplica inserts ONE additional replica row for a chunk — the async
// background-replication write-back (a slow anchor replica that landed after the
// PUT already returned 200 with its fast replicas). It assigns the next
// replica_idx and is a no-op when:
//   - the object no longer exists (deleted), or
//   - its ETag no longer matches expectETag (overwritten between the PUT
//     returning and this replica landing), or
//   - the provider is already a replica of this chunk (idempotent retry).
//
// The ETag guard is what makes async replication safe: a late replica can never
// attach to a different object version's chunk. A dropped replica (object gone /
// changed) is harmless — the keep-alive sweep refills any chunk below R.
func (s *Store) AppendChunkReplica(ctx context.Context, bucket, key string, partSeq int, r Replica, expectETag string) error {
	tx, err := s.write.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var etag string
	err = tx.QueryRowContext(ctx, `SELECT etag FROM objects WHERE bucket = ? AND key = ?`, bucket, key).Scan(&etag)
	if errors.Is(err, sql.ErrNoRows) {
		return nil // object deleted since the PUT — drop the late replica
	}
	if err != nil {
		return err
	}
	if etag != expectETag {
		return nil // object overwritten since the PUT — drop the late replica
	}

	var dup int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM chunk_replicas WHERE bucket = ? AND key = ? AND part_seq = ? AND provider = ?`,
		bucket, key, partSeq, r.Provider).Scan(&dup); err != nil {
		return err
	}
	if dup > 0 {
		return nil // already a replica (idempotent)
	}

	var nextIdx int
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(replica_idx)+1, 0) FROM chunk_replicas WHERE bucket = ? AND key = ? AND part_seq = ?`,
		bucket, key, partSeq).Scan(&nextIdx); err != nil {
		return err
	}
	alive := 1
	if !r.Alive {
		alive = 0
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO chunk_replicas(bucket, key, part_seq, replica_idx, provider, locator, delete_token, alive) VALUES(?, ?, ?, ?, ?, ?, ?, ?)`,
		bucket, key, partSeq, nextIdx, r.Provider, r.Locator, r.DeleteToken, alive); err != nil {
		return err
	}
	return tx.Commit()
}

// ListParams drives a single ListObjects / ListObjectsV2 page.
type ListParams struct {
	Bucket    string
	Prefix    string
	Delimiter string
	After     string // exclusive lower bound (marker / start-after / decoded token)
	MaxKeys   int
}

// ListPage is one page of a (optionally delimiter-rolled) listing.
type ListPage struct {
	Objects        []Object
	CommonPrefixes []string
	IsTruncated    bool
	// NextAfter is the underlying key to resume strictly-after; meaningful only
	// when IsTruncated. The handler turns it into NextMarker /
	// NextContinuationToken. It is the last key actually *consumed* (whether it
	// became a Content row or was rolled into a CommonPrefix), so the next page
	// reproduces an as-yet-unreturned prefix without duplicating data.
	NextAfter string
}

// ListObjectsPage scans keys in byte order and applies S3 delimiter rollup +
// real pagination. Prefix is a half-open range scan (not LIKE — keys may
// contain % / _), and rows are streamed lazily so a huge bucket stops as soon
// as the page is full.
func (s *Store) ListObjectsPage(ctx context.Context, p ListParams) (ListPage, error) {
	maxKeys := p.MaxKeys
	if maxKeys <= 0 || maxKeys > 1000 {
		maxKeys = 1000
	}

	query := `SELECT bucket, key, size, etag, content_type, created_at, updated_at
FROM objects WHERE bucket = ? AND deleted_at IS NULL`
	args := []any{p.Bucket}
	if p.After != "" {
		query += ` AND key > ?`
		args = append(args, p.After)
	}
	if p.Prefix != "" {
		query += ` AND key >= ?`
		args = append(args, p.Prefix)
		if hi := prefixUpperBound(p.Prefix); hi != "" {
			query += ` AND key < ?`
			args = append(args, hi)
		}
	}
	query += ` ORDER BY key`

	rows, err := s.read.QueryContext(ctx, query, args...)
	if err != nil {
		return ListPage{}, err
	}
	defer rows.Close()

	var page ListPage
	seen := map[string]bool{}
	count := 0
	for rows.Next() {
		var obj Object
		var created, updated string
		if err := rows.Scan(&obj.Bucket, &obj.Key, &obj.Size, &obj.ETag, &obj.ContentType, &created, &updated); err != nil {
			return ListPage{}, err
		}
		obj.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
		obj.UpdatedAt, _ = time.Parse(time.RFC3339Nano, updated)

		if p.Delimiter != "" {
			rel := obj.Key[len(p.Prefix):] // SQL guarantees the key starts with prefix
			if idx := strings.Index(rel, p.Delimiter); idx >= 0 {
				cp := p.Prefix + rel[:idx+len(p.Delimiter)]
				if seen[cp] {
					page.NextAfter = obj.Key // consumed, but no new result
					continue
				}
				if count == maxKeys { // a new result beyond the page → more remains
					page.IsTruncated = true
					return page, rows.Err()
				}
				seen[cp] = true
				page.CommonPrefixes = append(page.CommonPrefixes, cp)
				count++
				page.NextAfter = obj.Key
				continue
			}
		}
		if count == maxKeys {
			page.IsTruncated = true
			return page, rows.Err()
		}
		page.Objects = append(page.Objects, obj)
		count++
		page.NextAfter = obj.Key
	}
	if err := rows.Err(); err != nil {
		return ListPage{}, err
	}
	page.NextAfter = "" // exhausted the stream → no resume point
	return page, nil
}

// prefixUpperBound returns the smallest string strictly greater than every
// string starting with prefix (for a half-open [prefix, hi) byte-range scan).
// "" means unbounded above (empty prefix, or an all-0xFF prefix). SQLite's
// default TEXT collation is BINARY, so byte order == S3's UTF-8 key order.
func prefixUpperBound(prefix string) string {
	b := []byte(prefix)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] != 0xff {
			out := append([]byte(nil), b[:i]...)
			return string(append(out, b[i]+1))
		}
	}
	return ""
}

// --- Multipart upload ------------------------------------------------------

type MultipartUpload struct {
	UploadID    string
	Bucket      string
	Key         string
	ContentType string
	CreatedAt   time.Time
}

type MultipartPart struct {
	PartNumber int
	ETag       string // hex MD5 of the part bytes (unquoted)
	Size       int64
}

func (s *Store) CreateMultipartUpload(ctx context.Context, uploadID, bucket, key, contentType string) error {
	_, err := s.write.ExecContext(ctx, `INSERT INTO multipart_uploads(upload_id, bucket, key, content_type, created_at) VALUES(?, ?, ?, ?, ?)`,
		uploadID, bucket, key, contentType, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

func (s *Store) GetMultipartUpload(ctx context.Context, uploadID string) (MultipartUpload, error) {
	var u MultipartUpload
	var created string
	err := s.read.QueryRowContext(ctx, `SELECT upload_id, bucket, key, content_type, created_at FROM multipart_uploads WHERE upload_id = ?`, uploadID).
		Scan(&u.UploadID, &u.Bucket, &u.Key, &u.ContentType, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return MultipartUpload{}, ErrNotFound
	}
	if err != nil {
		return MultipartUpload{}, err
	}
	u.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
	return u, nil
}

// ListMultipartUploads returns every in-progress upload in a bucket, ordered
// by key then upload_id (the order S3's ListMultipartUploads reports).
func (s *Store) ListMultipartUploads(ctx context.Context, bucket string) ([]MultipartUpload, error) {
	rows, err := s.read.QueryContext(ctx, `SELECT upload_id, bucket, key, content_type, created_at FROM multipart_uploads WHERE bucket = ? ORDER BY key, upload_id`, bucket)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var uploads []MultipartUpload
	for rows.Next() {
		var u MultipartUpload
		var created string
		if err := rows.Scan(&u.UploadID, &u.Bucket, &u.Key, &u.ContentType, &created); err != nil {
			return nil, err
		}
		u.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
		uploads = append(uploads, u)
	}
	return uploads, rows.Err()
}

// SetMultipartCreatedAt overrides a multipart upload's created_at timestamp.
// Production never edits created_at after CreateMultipartUpload; this exists
// only so the janitor tests can stage a "stale" upload without sleeping for the
// real TTL.
func (s *Store) SetMultipartCreatedAt(ctx context.Context, uploadID string, t time.Time) error {
	_, err := s.write.ExecContext(ctx, `UPDATE multipart_uploads SET created_at = ? WHERE upload_id = ?`, t.UTC().Format(time.RFC3339Nano), uploadID)
	return err
}

// StaleMultipartUploads returns every multipart upload whose created_at is
// strictly before the cutoff. Used by the janitor; uploads within the TTL
// window are left alone (they may be live in-progress writes).
func (s *Store) StaleMultipartUploads(ctx context.Context, before time.Time) ([]MultipartUpload, error) {
	rows, err := s.read.QueryContext(ctx, `SELECT upload_id, bucket, key, content_type, created_at FROM multipart_uploads WHERE created_at < ? ORDER BY created_at`, before.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var uploads []MultipartUpload
	for rows.Next() {
		var u MultipartUpload
		var created string
		if err := rows.Scan(&u.UploadID, &u.Bucket, &u.Key, &u.ContentType, &created); err != nil {
			return nil, err
		}
		u.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
		uploads = append(uploads, u)
	}
	return uploads, rows.Err()
}

// GetMultipartPartChunks returns the chunk map of a single part, each chunk
// carrying its replica list.
func (s *Store) GetMultipartPartChunks(ctx context.Context, uploadID string, partNumber int) ([]Chunk, error) {
	rows, err := s.read.QueryContext(ctx, `SELECT seq, size FROM multipart_part_chunks WHERE upload_id = ? AND part_number = ? ORDER BY seq`, uploadID, partNumber)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var chunks []Chunk
	idxBySeq := map[int]int{}
	for rows.Next() {
		var c Chunk
		if err := rows.Scan(&c.Seq, &c.Size); err != nil {
			return nil, err
		}
		idxBySeq[c.Seq] = len(chunks)
		chunks = append(chunks, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(chunks) == 0 {
		return nil, nil
	}

	rrows, err := s.read.QueryContext(ctx, `SELECT seq, provider, locator, delete_token FROM multipart_part_chunk_replicas WHERE upload_id = ? AND part_number = ? ORDER BY seq, replica_idx`, uploadID, partNumber)
	if err != nil {
		return nil, err
	}
	defer rrows.Close()
	for rrows.Next() {
		var seq int
		var r Replica
		if err := rrows.Scan(&seq, &r.Provider, &r.Locator, &r.DeleteToken); err != nil {
			return nil, err
		}
		r.Alive = true
		if ci, ok := idxBySeq[seq]; ok {
			chunks[ci].Replicas = append(chunks[ci].Replicas, r)
		}
	}
	return chunks, rrows.Err()
}

// PutMultipartPart replaces a part and its chunk/replica list atomically
// (re-uploading the same part number is allowed by S3; the caller reaps the old
// chunks' blobs, having read them first).
func (s *Store) PutMultipartPart(ctx context.Context, uploadID string, partNumber int, etag string, size int64, chunks []Chunk) error {
	tx, err := s.write.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `INSERT INTO multipart_parts(upload_id, part_number, etag, size) VALUES(?, ?, ?, ?)
ON CONFLICT(upload_id, part_number) DO UPDATE SET etag = excluded.etag, size = excluded.size`, uploadID, partNumber, etag, size); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM multipart_part_chunk_replicas WHERE upload_id = ? AND part_number = ?`, uploadID, partNumber); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM multipart_part_chunks WHERE upload_id = ? AND part_number = ?`, uploadID, partNumber); err != nil {
		return err
	}
	for _, c := range chunks {
		if _, err := tx.ExecContext(ctx, `INSERT INTO multipart_part_chunks(upload_id, part_number, seq, size) VALUES(?, ?, ?, ?)`,
			uploadID, partNumber, c.Seq, c.Size); err != nil {
			return err
		}
		for ri, r := range c.Replicas {
			if _, err := tx.ExecContext(ctx, `INSERT INTO multipart_part_chunk_replicas(upload_id, part_number, seq, replica_idx, provider, locator, delete_token) VALUES(?, ?, ?, ?, ?, ?, ?)`,
				uploadID, partNumber, c.Seq, ri, r.Provider, r.Locator, r.DeleteToken); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

func (s *Store) ListMultipartParts(ctx context.Context, uploadID string) ([]MultipartPart, error) {
	rows, err := s.read.QueryContext(ctx, `SELECT part_number, etag, size FROM multipart_parts WHERE upload_id = ? ORDER BY part_number`, uploadID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var parts []MultipartPart
	for rows.Next() {
		var p MultipartPart
		if err := rows.Scan(&p.PartNumber, &p.ETag, &p.Size); err != nil {
			return nil, err
		}
		parts = append(parts, p)
	}
	return parts, rows.Err()
}

// AllMultipartChunks returns every chunk across all parts (for abort cleanup),
// each carrying its replica list so the caller can reap every blob.
func (s *Store) AllMultipartChunks(ctx context.Context, uploadID string) ([]Chunk, error) {
	rows, err := s.read.QueryContext(ctx, `SELECT part_number, seq, size FROM multipart_part_chunks WHERE upload_id = ? ORDER BY part_number, seq`, uploadID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	type pk struct {
		part, seq int
	}
	var chunks []Chunk
	idxByPK := map[pk]int{}
	for rows.Next() {
		var part, seq int
		var size int64
		if err := rows.Scan(&part, &seq, &size); err != nil {
			return nil, err
		}
		idxByPK[pk{part, seq}] = len(chunks)
		chunks = append(chunks, Chunk{Seq: seq, Size: size})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(chunks) == 0 {
		return nil, nil
	}

	rrows, err := s.read.QueryContext(ctx, `SELECT part_number, seq, provider, locator, delete_token FROM multipart_part_chunk_replicas WHERE upload_id = ? ORDER BY part_number, seq, replica_idx`, uploadID)
	if err != nil {
		return nil, err
	}
	defer rrows.Close()
	for rrows.Next() {
		var part, seq int
		var r Replica
		if err := rrows.Scan(&part, &seq, &r.Provider, &r.Locator, &r.DeleteToken); err != nil {
			return nil, err
		}
		r.Alive = true
		if ci, ok := idxByPK[pk{part, seq}]; ok {
			chunks[ci].Replicas = append(chunks[ci].Replicas, r)
		}
	}
	return chunks, rrows.Err()
}

// FinalizeMultipartUpload atomically materializes the assembled object (row +
// chunk map + replicas) and drops all multipart bookkeeping for the upload.
func (s *Store) FinalizeMultipartUpload(ctx context.Context, obj Object, chunks []Chunk, uploadID string) error {
	now := time.Now().UTC()
	if obj.CreatedAt.IsZero() {
		obj.CreatedAt = now
	}
	obj.UpdatedAt = now

	tx, err := s.write.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := upsertObjectRowTx(ctx, tx, obj); err != nil {
		return err
	}
	if err := replaceObjectChunksTx(ctx, tx, obj.Bucket, obj.Key, chunks); err != nil {
		return err
	}
	if err := replaceObjectMetadataTx(ctx, tx, obj.Bucket, obj.Key, obj.Metadata); err != nil {
		return err
	}
	if err := deleteMultipartTx(ctx, tx, uploadID); err != nil {
		return err
	}
	return tx.Commit()
}

// upsertObjectRowTx writes the objects row (INSERT … ON CONFLICT DO UPDATE
// clearing deleted_at). The chunk map and metadata are replaced separately by
// the caller so both PutObject and FinalizeMultipartUpload share this body.
func upsertObjectRowTx(ctx context.Context, tx *sql.Tx, obj Object) error {
	_, err := tx.ExecContext(ctx, `
INSERT INTO objects(bucket, key, size, etag, content_type, created_at, updated_at, deleted_at)
VALUES(?, ?, ?, ?, ?, ?, ?, NULL)
ON CONFLICT(bucket, key) DO UPDATE SET
  size = excluded.size, etag = excluded.etag, content_type = excluded.content_type,
  updated_at = excluded.updated_at, deleted_at = NULL
`, obj.Bucket, obj.Key, obj.Size, obj.ETag, obj.ContentType, obj.CreatedAt.Format(time.RFC3339Nano), obj.UpdatedAt.Format(time.RFC3339Nano))
	return err
}

// replaceObjectChunksTx wipes and rewrites the chunk map + replica rows for one
// object.
func replaceObjectChunksTx(ctx context.Context, tx *sql.Tx, bucket, key string, chunks []Chunk) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM chunk_replicas WHERE bucket = ? AND key = ?`, bucket, key); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM object_chunks WHERE bucket = ? AND key = ?`, bucket, key); err != nil {
		return err
	}
	for _, c := range chunks {
		if _, err := tx.ExecContext(ctx, `INSERT INTO object_chunks(bucket, key, part_seq, size, offset) VALUES(?, ?, ?, ?, ?)`,
			bucket, key, c.Seq, c.Size, c.Offset); err != nil {
			return err
		}
		for ri, r := range c.Replicas {
			alive := 1
			if !r.Alive {
				alive = 0
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO chunk_replicas(bucket, key, part_seq, replica_idx, provider, locator, delete_token, alive) VALUES(?, ?, ?, ?, ?, ?, ?, ?)`,
				bucket, key, c.Seq, ri, r.Provider, r.Locator, r.DeleteToken, alive); err != nil {
				return err
			}
		}
	}
	return nil
}

// PutMultipartUploadMetadata stores the headers captured at
// CreateMultipartUpload so FinalizeMultipartUpload can carry them onto the
// completed object (the complete request does not resend them).
func (s *Store) PutMultipartUploadMetadata(ctx context.Context, uploadID string, kv map[string]string) error {
	for name, value := range kv {
		if _, err := s.write.ExecContext(ctx, `INSERT INTO multipart_upload_metadata(upload_id, name, value) VALUES(?, ?, ?)`, uploadID, name, value); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) GetMultipartUploadMetadata(ctx context.Context, uploadID string) (map[string]string, error) {
	rows, err := s.read.QueryContext(ctx, `SELECT name, value FROM multipart_upload_metadata WHERE upload_id = ?`, uploadID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	md := map[string]string{}
	for rows.Next() {
		var name, value string
		if err := rows.Scan(&name, &value); err != nil {
			return nil, err
		}
		md[name] = value
	}
	return md, rows.Err()
}

// DeleteMultipartUpload removes all bookkeeping for an upload (abort path, after
// the caller has reaped the free-host blobs).
func (s *Store) DeleteMultipartUpload(ctx context.Context, uploadID string) error {
	tx, err := s.write.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := deleteMultipartTx(ctx, tx, uploadID); err != nil {
		return err
	}
	return tx.Commit()
}

func deleteMultipartTx(ctx context.Context, tx *sql.Tx, uploadID string) error {
	for _, q := range []string{
		`DELETE FROM multipart_part_chunk_replicas WHERE upload_id = ?`,
		`DELETE FROM multipart_part_chunks WHERE upload_id = ?`,
		`DELETE FROM multipart_parts WHERE upload_id = ?`,
		`DELETE FROM multipart_upload_metadata WHERE upload_id = ?`,
		`DELETE FROM multipart_uploads WHERE upload_id = ?`,
	} {
		if _, err := tx.ExecContext(ctx, q, uploadID); err != nil {
			return err
		}
	}
	return nil
}
