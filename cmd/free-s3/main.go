package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"free-s3/internal/config"
	"free-s3/internal/metadata"
	"free-s3/internal/s3api"
	"free-s3/internal/storage/freehost"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

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

	// freehost backend (P0: stub; P4 wires the provider pool + chunk/replicate).
	backend := freehost.New()

	handler := s3api.NewHandler(cfg, store, backend, logger)

	// Abandoned-multipart janitor. Skipped if the sweep is disabled
	// (interval <= 0). Stops with the server on SIGINT/SIGTERM.
	janitorCtx, cancelJanitor := context.WithCancel(context.Background())
	defer cancelJanitor()
	if cfg.MultipartSweepInterval > 0 {
		go handler.RunMultipartJanitor(janitorCtx, cfg.MultipartSweepInterval, cfg.MultipartTTL)
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
}
