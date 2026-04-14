// Command sparkflow-server starts the Sparkflow orchestrator server.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/ilya-shevelev/sparkflow/internal/server"
)

var (
	version = "dev"
	commit  = "unknown"
)

func main() {
	var (
		grpcAddr    = flag.String("grpc-addr", ":9090", "gRPC server listen address")
		httpAddr    = flag.String("http-addr", ":8080", "HTTP server listen address")
		metricsAddr = flag.String("metrics-addr", ":9091", "Metrics server listen address")
		dataDir     = flag.String("data-dir", "/var/lib/sparkflow", "Data directory for Raft and other state")
		storeDSN    = flag.String("store-dsn", "", "PostgreSQL connection string (empty for in-memory)")
		logLevel    = flag.String("log-level", "info", "Log level (debug, info, warn, error)")
		showVersion = flag.Bool("version", false, "Show version and exit")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("sparkflow-server %s (commit: %s)\n", version, commit)
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

	srv, err := server.New(server.Config{
		GRPCAddr:    *grpcAddr,
		HTTPAddr:    *httpAddr,
		MetricsAddr: *metricsAddr,
		DataDir:     *dataDir,
		StoreDSN:    *storeDSN,
		LogLevel:    level,
	})
	if err != nil {
		slog.Error("failed to create server", slog.Any("error", err))
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := srv.Start(ctx); err != nil {
		slog.Error("failed to start server", slog.Any("error", err))
		os.Exit(1)
	}

	// Wait for shutdown signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh

	slog.Info("received shutdown signal", slog.String("signal", sig.String()))
	cancel()

	if err := srv.Shutdown(context.Background()); err != nil {
		slog.Error("shutdown error", slog.Any("error", err))
		os.Exit(1)
	}
}
