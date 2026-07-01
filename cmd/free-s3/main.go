package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"free-s3/internal/config"
	"free-s3/internal/keepalive"
	"free-s3/internal/metadata"
	"free-s3/internal/s3api"
	"free-s3/internal/storage/freehost"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel()}))

	cfg, err := config.Load()
	if err != nil {
		logger.Error("load config", "error", err)
		os.Exit(1)
	}

	store, err := metadata.OpenWithOptions(cfg.DatabasePath, cfg.SQLiteReaderConns)
	if err != nil {
		logger.Error("open database", "error", err)
		os.Exit(1)
	}
	defer store.Close()

	// freehost backend: build the provider pool from config, then the
	// chunk-and-replicate backend over it.
	httpClient := freehost.NewClient(cfg.HTTPMaxIdleConnsPerHost)
	providers := freehost.BuildProviders(httpClient, cfg.FreehostProviders, freehost.Credentials{
		CatboxUserhash:   cfg.CatboxUserhash,
		PixeldrainAPIKey: cfg.PixeldrainAPIKey,
		IAAccessKey:      cfg.IAAccessKey,
		IASecretKey:      cfg.IASecretKey,
		GofileToken:      cfg.GofileToken,
	}, logger)
	for _, p := range providers {
		logger.Info("freehost provider enabled", "name", p.Name(), "durable", p.Durable(), "max_bytes", p.MaxBytes())
	}
	// bgCtx bounds background replication (the slow-anchor replica that lands
	// after a PUT returns 200); cancelled on shutdown to abandon in-flight uploads.
	bgCtx, cancelBg := context.WithCancel(context.Background())
	defer cancelBg()
	syncReplicas := cfg.SyncReplicas
	if syncReplicas <= 0 { // default: confirm the fast replicas, background the rest
		syncReplicas = 2
		if cfg.ReplicationFactor < 2 {
			syncReplicas = cfg.ReplicationFactor
		}
	}
	backend, err := freehost.New(freehost.Options{
		Providers:          providers,
		ChunkSize:          cfg.ChunkSize,
		ReplicationFactor:  cfg.ReplicationFactor,
		SyncReplicas:       syncReplicas,
		UploadConcurrency:  cfg.UploadConcurrency,
		ReplicaReadTimeout: cfg.ReplicaReadTimeout,
		ReadHedgeDelay:     cfg.ReadHedgeDelay,
		BackgroundCtx:      bgCtx,
		Logger:             logger,
	})
	if err != nil {
		logger.Error("init freehost backend", "error", err)
		os.Exit(1)
	}

	handler := s3api.NewHandler(cfg, store, backend, logger)

	// Abandoned-multipart janitor. Skipped if the sweep is disabled
	// (interval <= 0). Stops with the server on SIGINT/SIGTERM.
	janitorCtx, cancelJanitor := context.WithCancel(context.Background())
	defer cancelJanitor()
	if cfg.MultipartSweepInterval > 0 {
		go handler.RunMultipartJanitor(janitorCtx, cfg.MultipartSweepInterval, cfg.MultipartTTL)
	}

	// Keep-alive + self-heal sweep: re-reads chunks to reset TTLs and refills
	// any chunk below R from a surviving replica. Disabled when interval <= 0.
	if cfg.KeepaliveInterval > 0 {
		sweeper := keepalive.New(store, backend, cfg.KeepaliveInterval, 0, logger)
		go sweeper.Run(janitorCtx)
		logger.Info("keepalive sweep enabled", "interval", cfg.KeepaliveInterval.String())
	}

	server := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		logger.Info("server listening",
			"addr", cfg.ListenAddr,
			"replication_factor", cfg.ReplicationFactor,
			"sync_replicas", syncReplicas,
		)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server failed", "error", err)
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	cancelJanitor()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		logger.Error("shutdown failed", "error", err)
	}
	// Let in-flight background replications finish (or abort via cancelBg) so a
	// just-uploaded object isn't left below its sync-replica count on exit.
	cancelBg()
	backend.WaitBackground()
}

// logLevel reads LOG_LEVEL (debug|info|warn|error, default info). Set to debug
// to surface per-replica read timing (which host served each window, how fast,
// and whether the read was hedged) for tuning provider order from a given egress
// IP; revert to info in steady state to avoid one log line per prefetch window.
func logLevel() slog.Level {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("LOG_LEVEL"))) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
