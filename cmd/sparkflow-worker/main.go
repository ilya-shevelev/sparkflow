// Command sparkflow-worker starts a Sparkflow worker node.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ilya-shevelev/sparkflow/internal/worker"
	"github.com/ilya-shevelev/sparkflow/pkg/executor"
	"github.com/ilya-shevelev/sparkflow/pkg/store"
)

var (
	version = "dev"
	commit  = "unknown"
)

func main() {
	var (
		id          = flag.String("id", "", "Worker ID (auto-generated if empty)")
		serverAddr  = flag.String("server-addr", "localhost:9090", "Sparkflow server gRPC address")
		concurrency = flag.Int("concurrency", 4, "Number of concurrent task executions")
		heartbeat   = flag.Duration("heartbeat", 10*time.Second, "Heartbeat interval")
		logLevel    = flag.String("log-level", "info", "Log level (debug, info, warn, error)")
		showVersion = flag.Bool("version", false, "Show version and exit")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("sparkflow-worker %s (commit: %s)\n", version, commit)
		os.Exit(0)
	}

	level := slog.LevelInfo
	switch *logLevel {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}

	// Create executor registry.
	reg := executor.DefaultRegistry()

	// Create in-memory store (will be replaced by gRPC client in production).
	st := store.NewMemoryStore()

	w, err := worker.New(worker.Config{
		ID:                *id,
		ServerAddr:        *serverAddr,
		Concurrency:       *concurrency,
		HeartbeatInterval: *heartbeat,
		LogLevel:          level,
	}, st, reg)
	if err != nil {
		slog.Error("failed to create worker", slog.Any("error", err))
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := w.Start(ctx); err != nil {
		slog.Error("failed to start worker", slog.Any("error", err))
		os.Exit(1)
	}

	// Wait for shutdown signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh

	slog.Info("received shutdown signal", slog.String("signal", sig.String()))
	cancel()

	if err := w.Stop(); err != nil {
		slog.Error("shutdown error", slog.Any("error", err))
		os.Exit(1)
	}
}
