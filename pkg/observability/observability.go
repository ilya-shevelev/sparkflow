// Package observability provides OpenTelemetry tracing, Prometheus metrics,
// and structured logging setup for Sparkflow components.
package observability

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	promexporter "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"
)

// Config configures the observability stack.
type Config struct {
	ServiceName    string
	ServiceVersion string
	OTLPEndpoint   string
	LogLevel       slog.Level
	EnableTracing  bool
	EnableMetrics  bool
}

// Provider holds initialized observability components.
type Provider struct {
	TracerProvider *sdktrace.TracerProvider
	MeterProvider  *sdkmetric.MeterProvider
	Logger         *slog.Logger
	Tracer         trace.Tracer
	Meter          metric.Meter
	Registry       *prometheus.Registry
}

// Setup initializes the observability stack.
func Setup(ctx context.Context, cfg Config) (*Provider, error) {
	if cfg.ServiceName == "" {
		cfg.ServiceName = "sparkflow"
	}
	if cfg.ServiceVersion == "" {
		cfg.ServiceVersion = "dev"
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(cfg.ServiceName),
			semconv.ServiceVersionKey.String(cfg.ServiceVersion),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("create resource: %w", err)
	}

	p := &Provider{
		Registry: prometheus.NewRegistry(),
	}

	// Set up structured logging.
	logHandler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level:     cfg.LogLevel,
		AddSource: true,
	})
	p.Logger = slog.New(logHandler)

	// Set up tracing.
	if cfg.EnableTracing && cfg.OTLPEndpoint != "" {
		exporter, err := otlptracegrpc.New(ctx,
			otlptracegrpc.WithEndpoint(cfg.OTLPEndpoint),
			otlptracegrpc.WithInsecure(),
		)
		if err != nil {
			return nil, fmt.Errorf("create OTLP trace exporter: %w", err)
		}

		p.TracerProvider = sdktrace.NewTracerProvider(
			sdktrace.WithResource(res),
			sdktrace.WithBatcher(exporter,
				sdktrace.WithBatchTimeout(5*time.Second),
			),
			sdktrace.WithSampler(sdktrace.AlwaysSample()),
		)
		otel.SetTracerProvider(p.TracerProvider)
	} else {
		p.TracerProvider = sdktrace.NewTracerProvider(
			sdktrace.WithResource(res),
			sdktrace.WithSampler(sdktrace.NeverSample()),
		)
		otel.SetTracerProvider(p.TracerProvider)
	}

	p.Tracer = otel.Tracer(cfg.ServiceName)

	// Set up metrics.
	if cfg.EnableMetrics {
		promExp, err := promexporter.New(
			promexporter.WithRegisterer(p.Registry),
		)
		if err != nil {
			return nil, fmt.Errorf("create prometheus exporter: %w", err)
		}

		p.MeterProvider = sdkmetric.NewMeterProvider(
			sdkmetric.WithResource(res),
			sdkmetric.WithReader(promExp),
		)
		otel.SetMeterProvider(p.MeterProvider)
	} else {
		p.MeterProvider = sdkmetric.NewMeterProvider(
			sdkmetric.WithResource(res),
		)
		otel.SetMeterProvider(p.MeterProvider)
	}

	p.Meter = otel.Meter(cfg.ServiceName)

	return p, nil
}

// Shutdown gracefully shuts down all observability components.
func (p *Provider) Shutdown(ctx context.Context) error {
	if p.TracerProvider != nil {
		if err := p.TracerProvider.Shutdown(ctx); err != nil {
			return fmt.Errorf("shutdown tracer provider: %w", err)
		}
	}
	if p.MeterProvider != nil {
		if err := p.MeterProvider.Shutdown(ctx); err != nil {
			return fmt.Errorf("shutdown meter provider: %w", err)
		}
	}
	return nil
}

// Metrics holds common Sparkflow metrics.
type Metrics struct {
	TasksScheduled   metric.Int64Counter
	TasksCompleted   metric.Int64Counter
	TasksFailed      metric.Int64Counter
	TaskDuration     metric.Float64Histogram
	DAGRunsStarted   metric.Int64Counter
	DAGRunsCompleted metric.Int64Counter
	ActiveWorkers    metric.Int64UpDownCounter
	QueueDepth       metric.Int64UpDownCounter
}

// NewMetrics creates the standard Sparkflow metrics.
func NewMetrics(meter metric.Meter) (*Metrics, error) {
	m := &Metrics{}
	var err error

	m.TasksScheduled, err = meter.Int64Counter("sparkflow.tasks.scheduled",
		metric.WithDescription("Number of tasks scheduled for execution"),
	)
	if err != nil {
		return nil, err
	}

	m.TasksCompleted, err = meter.Int64Counter("sparkflow.tasks.completed",
		metric.WithDescription("Number of tasks completed successfully"),
	)
	if err != nil {
		return nil, err
	}

	m.TasksFailed, err = meter.Int64Counter("sparkflow.tasks.failed",
		metric.WithDescription("Number of tasks that failed"),
	)
	if err != nil {
		return nil, err
	}

	m.TaskDuration, err = meter.Float64Histogram("sparkflow.tasks.duration_ms",
		metric.WithDescription("Task execution duration in milliseconds"),
	)
	if err != nil {
		return nil, err
	}

	m.DAGRunsStarted, err = meter.Int64Counter("sparkflow.dag_runs.started",
		metric.WithDescription("Number of DAG runs started"),
	)
	if err != nil {
		return nil, err
	}

	m.DAGRunsCompleted, err = meter.Int64Counter("sparkflow.dag_runs.completed",
		metric.WithDescription("Number of DAG runs completed"),
	)
	if err != nil {
		return nil, err
	}

	m.ActiveWorkers, err = meter.Int64UpDownCounter("sparkflow.workers.active",
		metric.WithDescription("Number of active workers"),
	)
	if err != nil {
		return nil, err
	}

	m.QueueDepth, err = meter.Int64UpDownCounter("sparkflow.queue.depth",
		metric.WithDescription("Number of tasks waiting in the queue"),
	)
	if err != nil {
		return nil, err
	}

	return m, nil
}
