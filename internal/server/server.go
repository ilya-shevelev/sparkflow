// Package server provides the main gRPC and HTTP server for the Sparkflow orchestrator.
package server

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	"github.com/ilya-shevelev/sparkflow/pkg/executor"
	"github.com/ilya-shevelev/sparkflow/pkg/observability"
	"github.com/ilya-shevelev/sparkflow/pkg/scheduler"
	"github.com/ilya-shevelev/sparkflow/pkg/store"
)

// Config configures the Sparkflow server.
type Config struct {
	GRPCAddr    string
	HTTPAddr    string
	MetricsAddr string
	DataDir     string
	StoreDSN    string
	LogLevel    slog.Level
}

// Server is the main Sparkflow orchestrator server.
type Server struct {
	config     Config
	grpcServer *grpc.Server
	httpServer *http.Server
	scheduler  scheduler.Scheduler
	store      store.Store
	registry   *executor.Registry
	obs        *observability.Provider
	logger     *slog.Logger
}

// New creates a new Sparkflow server.
func New(cfg Config) (*Server, error) {
	// Initialize observability.
	obs, err := observability.Setup(context.Background(), observability.Config{
		ServiceName:   "sparkflow-server",
		EnableMetrics: true,
		LogLevel:      cfg.LogLevel,
	})
	if err != nil {
		return nil, fmt.Errorf("setup observability: %w", err)
	}

	// Initialize store.
	var st store.Store
	if cfg.StoreDSN != "" {
		pgStore, err := store.NewPostgresStore(context.Background(), store.PostgresConfig{
			DSN:      cfg.StoreDSN,
			MaxConns: 20,
		})
		if err != nil {
			obs.Logger.Warn("failed to connect to PostgreSQL, using in-memory store", slog.Any("error", err))
			st = store.NewMemoryStore()
		} else {
			if err := pgStore.Migrate(context.Background()); err != nil {
				return nil, fmt.Errorf("migrate database: %w", err)
			}
			st = pgStore
		}
	} else {
		st = store.NewMemoryStore()
	}

	// Initialize scheduler.
	sch := scheduler.NewFIFOScheduler(st, scheduler.FIFOConfig{
		PollInterval: 5 * time.Second,
		Logger:       obs.Logger,
	})

	// Initialize executor registry.
	reg := executor.DefaultRegistry()

	s := &Server{
		config:   cfg,
		store:    st,
		scheduler: sch,
		registry: reg,
		obs:      obs,
		logger:   obs.Logger,
	}

	return s, nil
}

// Start starts the gRPC and HTTP servers.
func (s *Server) Start(ctx context.Context) error {
	// Start scheduler.
	if err := s.scheduler.Start(ctx); err != nil {
		return fmt.Errorf("start scheduler: %w", err)
	}

	// Start gRPC server.
	grpcLis, err := net.Listen("tcp", s.config.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen gRPC: %w", err)
	}

	s.grpcServer = grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			loggingInterceptor(s.logger),
		),
	)

	// Register gRPC health service.
	healthSvc := health.NewServer()
	healthpb.RegisterHealthServer(s.grpcServer, healthSvc)
	healthSvc.SetServingStatus("sparkflow", healthpb.HealthCheckResponse_SERVING)

	// Enable reflection for development tools.
	reflection.Register(s.grpcServer)

	go func() {
		s.logger.Info("gRPC server starting", slog.String("addr", s.config.GRPCAddr))
		if err := s.grpcServer.Serve(grpcLis); err != nil {
			s.logger.Error("gRPC server error", slog.Any("error", err))
		}
	}()

	// Start HTTP server (REST gateway + metrics).
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/readyz", s.handleReadyz)
	mux.HandleFunc("/api/v1/dags", s.handleDAGs)
	mux.HandleFunc("/api/v1/runs", s.handleRuns)

	s.httpServer = &http.Server{
		Addr:         s.config.HTTPAddr,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		s.logger.Info("HTTP server starting", slog.String("addr", s.config.HTTPAddr))
		if err := s.httpServer.ListenAndServe(); err != http.ErrServerClosed {
			s.logger.Error("HTTP server error", slog.Any("error", err))
		}
	}()

	// Start metrics server.
	if s.config.MetricsAddr != "" {
		metricsMux := http.NewServeMux()
		metricsMux.Handle("/metrics", promhttp.HandlerFor(s.obs.Registry, promhttp.HandlerOpts{}))

		go func() {
			s.logger.Info("metrics server starting", slog.String("addr", s.config.MetricsAddr))
			if err := http.ListenAndServe(s.config.MetricsAddr, metricsMux); err != nil {
				s.logger.Error("metrics server error", slog.Any("error", err))
			}
		}()
	}

	s.logger.Info("sparkflow server started successfully")
	return nil
}

// Shutdown gracefully shuts down the server.
func (s *Server) Shutdown(ctx context.Context) error {
	s.logger.Info("shutting down server")

	if s.scheduler != nil {
		_ = s.scheduler.Stop()
	}
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
	if s.httpServer != nil {
		_ = s.httpServer.Shutdown(ctx)
	}
	if s.obs != nil {
		_ = s.obs.Shutdown(ctx)
	}
	if s.store != nil {
		_ = s.store.Close()
	}

	return nil
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"healthy"}`))
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if err := s.store.Ping(r.Context()); err != nil {
		http.Error(w, `{"status":"not ready"}`, http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ready"}`))
}

func (s *Server) handleDAGs(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		dags, err := s.store.ListDAGs(r.Context(), store.DAGFilter{Limit: 100})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"count":%d}`, len(dags))
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleRuns(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		runs, err := s.store.ListRuns(r.Context(), store.RunFilter{Limit: 100})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"count":%d}`, len(runs))
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func loggingInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		duration := time.Since(start)

		logger.Info("gRPC request",
			slog.String("method", info.FullMethod),
			slog.Duration("duration", duration),
			slog.Any("error", err),
		)

		return resp, err
	}
}
