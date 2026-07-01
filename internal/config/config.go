package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	ListenAddr        string
	DatabasePath      string
	AccessKeyID       string
	SecretAccessKey   string
	PublicEndpointURL string
	// MultipartTTL is how long an in-progress multipart upload may sit
	// untouched before the janitor aborts it. MultipartSweepInterval is how
	// often the janitor runs; <= 0 disables the sweep entirely.
	MultipartTTL           time.Duration
	MultipartSweepInterval time.Duration
	// HTTPMaxIdleConnsPerHost bounds the keepalive pool the freehost provider
	// HTTP clients reuse.
	HTTPMaxIdleConnsPerHost int
	// SQLiteReaderConns bounds the read-only *sql.DB. WAL allows many
	// concurrent readers and one writer; we open a separate single-conn
	// writer pool, so this knob only governs the SELECT side.
	SQLiteReaderConns int

	// --- Object GET reader (parallel-prefetch) knobs --------------------
	// StreamConcurrency caps in-flight chunk fetches per stream; StreamBuffers
	// is the ordered delivery channel capacity; ChunkTimeout bounds each fetch;
	// StreamChunkSize is the reader's prefetch window in object-space bytes
	// (independent of the upload chunk size, typically smaller so one fetch
	// fits inside a single stored chunk).
	StreamConcurrency int
	StreamBuffers     int
	ChunkTimeout      time.Duration
	StreamChunkSize   int64
	// ReplicaReadTimeout bounds a single replica's attempt to serve one
	// prefetch window. When it fires the read fails over to the next replica
	// (within the same ChunkTimeout budget) instead of stalling the whole
	// stream — this is what keeps a slow/throttled lead host (e.g. pixeldrain
	// from a datacenter IP) from hanging a GET. Keep it < ChunkTimeout / R so
	// every replica gets a turn before the window's overall deadline.
	ReplicaReadTimeout time.Duration
	// ReadHedgeDelay is how long the reader waits for the lead replica to
	// deliver a window before ALSO racing the next replica (hedged reads). A
	// slow lead then costs ~this delay instead of the full ReplicaReadTimeout,
	// and it is order-independent (the durable tier is round-robin rotated, so
	// the stored lead replica is effectively random). 0 disables hedging.
	ReadHedgeDelay time.Duration

	// --- freehost backend knobs ----------------------------------------
	// ReplicationFactor (R) is the number of distinct providers each chunk is
	// uploaded to. ChunkSize is the per-chunk window (kept under the smallest
	// durable provider cap, e.g. catbox 200MB). FreehostProviders is the
	// enabled provider set in priority order (empty = all compiled-in).
	// UploadConcurrency bounds parallel chunk-replica uploads. KeepaliveInterval
	// is the TTL-refresh / self-heal sweep cadence (0 = off).
	ReplicationFactor int
	// SyncReplicas is how many replicas a PUT confirms before returning 200; the
	// remaining (R - SyncReplicas) replicate in the background so a slow durable
	// anchor (Internet Archive at ~0.5 MB/s) doesn't gate the response and time
	// out the proxy. Defaults to min(2, R). Set == R for fully-synchronous PUTs.
	SyncReplicas      int
	ChunkSize         int64
	FreehostProviders []string
	UploadConcurrency int
	KeepaliveInterval time.Duration

	// --- Per-provider credentials (ALL optional) -----------------------
	// A provider whose required credential is missing is skipped at startup.
	CatboxUserhash   string // REQUIRED for catbox from a VPS (else 412)
	PixeldrainAPIKey string
	IAAccessKey      string // archive.org/account/s3.php
	IASecretKey      string
	GofileToken      string // optional (guest works but token = durable + direct links)
}

func Load() (Config, error) {
	cfg := Config{
		ListenAddr:              getenv("LISTEN_ADDR", ":9000"),
		DatabasePath:            getenv("DATABASE_PATH", "free-s3.db"),
		AccessKeyID:             os.Getenv("S3_ACCESS_KEY_ID"),
		SecretAccessKey:         os.Getenv("S3_SECRET_ACCESS_KEY"),
		PublicEndpointURL:       os.Getenv("PUBLIC_ENDPOINT_URL"),
		MultipartTTL:            getDuration("MULTIPART_TTL", 7*24*time.Hour),
		MultipartSweepInterval:  getDuration("MULTIPART_SWEEP_INTERVAL", time.Hour),
		HTTPMaxIdleConnsPerHost: getInt("HTTP_MAX_IDLE_CONNS_PER_HOST", 32),
		SQLiteReaderConns:       getInt("SQLITE_READER_CONNS", 8),
		StreamConcurrency:       getInt("STREAM_CONCURRENCY", 4),
		StreamBuffers:           getInt("STREAM_BUFFERS", 8),
		ChunkTimeout:            getDuration("CHUNK_TIMEOUT", 60*time.Second),
		StreamChunkSize:         getBytes("STREAM_CHUNK_SIZE", 4<<20),
		ReplicaReadTimeout:      getDuration("REPLICA_READ_TIMEOUT", 18*time.Second),
		ReadHedgeDelay:          getDuration("READ_HEDGE_DELAY", 2*time.Second),
		ReplicationFactor:       getInt("REPLICATION_FACTOR", 3),
		SyncReplicas:            getInt("SYNC_REPLICAS", 0), // 0 => min(2, R), resolved after R is known
		ChunkSize:               getBytes("CHUNK_SIZE", 80<<20),
		FreehostProviders:       parseCSVList(os.Getenv("FREEHOST_PROVIDERS")),
		UploadConcurrency:       getInt("UPLOAD_CONCURRENCY", 6),
		KeepaliveInterval:       getDuration("KEEPALIVE_INTERVAL", 24*time.Hour),
		CatboxUserhash:          os.Getenv("CATBOX_USERHASH"),
		PixeldrainAPIKey:        os.Getenv("PIXELDRAIN_API_KEY"),
		IAAccessKey:             os.Getenv("IA_ACCESS_KEY"),
		IASecretKey:             os.Getenv("IA_SECRET_KEY"),
		GofileToken:             os.Getenv("GOFILE_TOKEN"),
	}

	if cfg.AccessKeyID == "" {
		return Config{}, errors.New("S3_ACCESS_KEY_ID is required")
	}
	if cfg.SecretAccessKey == "" {
		return Config{}, errors.New("S3_SECRET_ACCESS_KEY is required")
	}
	if cfg.ReplicationFactor < 1 {
		return Config{}, errors.New("REPLICATION_FACTOR must be >= 1")
	}

	return cfg, nil
}

// parseCSVList splits a comma-separated value, trimming whitespace around each
// entry and dropping empties (so a trailing comma or stray spaces don't produce
// empty tokens).
func parseCSVList(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func getDuration(key string, fallback time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if d, err := time.ParseDuration(value); err == nil {
			return d
		}
	}
	return fallback
}

func getInt(key string, fallback int) int {
	if value := os.Getenv(key); value != "" {
		if n, err := strconv.Atoi(value); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}

// getBytes parses a byte-count env var. Accepts plain integers ("1900000000")
// and common suffixes (KB/MB/GB decimal, KiB/MiB/GiB binary). Invalid values
// silently fall back.
func getBytes(key string, fallback int64) int64 {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	if n, err := parseBytes(value); err == nil && n > 0 {
		return n
	}
	return fallback
}

func parseBytes(s string) (int64, error) {
	s = strings.TrimSpace(s)
	// Split numeric prefix from unit suffix.
	i := 0
	for i < len(s) && (s[i] == '.' || (s[i] >= '0' && s[i] <= '9')) {
		i++
	}
	numStr := s[:i]
	unit := strings.TrimSpace(strings.ToLower(s[i:]))
	num, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return 0, err
	}
	var mult float64 = 1
	switch unit {
	case "", "b":
		mult = 1
	case "k", "kb":
		mult = 1000
	case "m", "mb":
		mult = 1000 * 1000
	case "g", "gb":
		mult = 1000 * 1000 * 1000
	case "kib":
		mult = 1024
	case "mib":
		mult = 1024 * 1024
	case "gib":
		mult = 1024 * 1024 * 1024
	default:
		return 0, fmt.Errorf("unrecognized byte unit %q", unit)
	}
	return int64(num * mult), nil
}
