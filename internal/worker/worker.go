// Package worker provides the worker node implementation that receives
// and executes tasks dispatched by the scheduler.
package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/ilya-shevelev/sparkflow/pkg/dag"
	"github.com/ilya-shevelev/sparkflow/pkg/executor"
	"github.com/ilya-shevelev/sparkflow/pkg/observability"
	"github.com/ilya-shevelev/sparkflow/pkg/retry"
	"github.com/ilya-shevelev/sparkflow/pkg/store"
)

// Config configures the worker node.
type Config struct {
	ID                string
	ServerAddr        string
	Concurrency       int
	HeartbeatInterval time.Duration
	LogLevel          slog.Level
}

// Worker executes tasks dispatched by the scheduler.
type Worker struct {
	config   Config
	id       string
	registry *executor.Registry
	store    store.Store
	dlq      *retry.DeadLetterQueue
	obs      *observability.Provider
	logger   *slog.Logger
	wg       sync.WaitGroup
	stopCh   chan struct{}
	taskCh   chan *store.TaskInstance
}

// New creates a new worker node.
func New(cfg Config, st store.Store, reg *executor.Registry) (*Worker, error) {
	if cfg.ID == "" {
		cfg.ID = fmt.Sprintf("worker-%s", uuid.New().String()[:8])
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 4
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 10 * time.Second
	}

	obs, err := observability.Setup(context.Background(), observability.Config{
		ServiceName:   "sparkflow-worker",
		EnableMetrics: true,
		LogLevel:      cfg.LogLevel,
	})
	if err != nil {
		return nil, fmt.Errorf("setup observability: %w", err)
	}

	return &Worker{
		config:   cfg,
		id:       cfg.ID,
		registry: reg,
		store:    st,
		dlq:      retry.NewDeadLetterQueue(1000),
		obs:      obs,
		logger:   obs.Logger,
		stopCh:   make(chan struct{}),
		taskCh:   make(chan *store.TaskInstance, cfg.Concurrency*2),
	}, nil
}

// Start begins the worker loop.
func (w *Worker) Start(ctx context.Context) error {
	w.logger.Info("worker starting",
		slog.String("id", w.id),
		slog.Int("concurrency", w.config.Concurrency),
	)

	// Start task execution goroutines.
	for i := 0; i < w.config.Concurrency; i++ {
		w.wg.Add(1)
		go w.executionLoop(ctx, i)
	}

	// Start heartbeat.
	w.wg.Add(1)
	go w.heartbeatLoop(ctx)

	w.logger.Info("worker started", slog.String("id", w.id))
	return nil
}

// Stop gracefully shuts down the worker.
func (w *Worker) Stop() error {
	w.logger.Info("worker stopping", slog.String("id", w.id))
	close(w.stopCh)
	w.wg.Wait()
	if w.obs != nil {
		_ = w.obs.Shutdown(context.Background())
	}
	w.logger.Info("worker stopped", slog.String("id", w.id))
	return nil
}

// Submit sends a task instance to the worker for execution.
func (w *Worker) Submit(ti *store.TaskInstance) error {
	select {
	case w.taskCh <- ti:
		return nil
	default:
		return fmt.Errorf("worker %s task queue full", w.id)
	}
}

func (w *Worker) executionLoop(ctx context.Context, workerIdx int) {
	defer w.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.stopCh:
			return
		case ti := <-w.taskCh:
			w.executeTask(ctx, ti, workerIdx)
		}
	}
}

func (w *Worker) executeTask(ctx context.Context, ti *store.TaskInstance, workerIdx int) {
	log := w.logger.With(
		slog.String("task_id", ti.TaskID),
		slog.String("run_id", string(ti.RunID)),
		slog.String("dag_id", ti.DAGID),
		slog.Int("worker_idx", workerIdx),
	)

	log.Info("executing task")

	// Update task instance status.
	ti.Status = dag.StatusRunning
	ti.WorkerID = w.id
	ti.StartTime = time.Now()
	if err := w.store.UpdateTaskInstance(ctx, ti); err != nil {
		log.Error("failed to update task instance", slog.Any("error", err))
		return
	}

	// Get the DAG definition to find the task config.
	d, err := w.store.GetDAG(ctx, ti.DAGID)
	if err != nil {
		log.Error("failed to get DAG", slog.Any("error", err))
		w.failTask(ctx, ti, fmt.Sprintf("get DAG: %s", err))
		return
	}

	task, ok := d.Tasks[ti.TaskID]
	if !ok {
		w.failTask(ctx, ti, fmt.Sprintf("task %s not found in DAG", ti.TaskID))
		return
	}

	// Get the executor.
	exec, err := w.registry.Get(task.Executor)
	if err != nil {
		w.failTask(ctx, ti, fmt.Sprintf("get executor: %s", err))
		return
	}

	// Execute with retry policy.
	var result *executor.Result
	rp := task.RetryPolicy
	maxAttempts := 1
	if rp != nil {
		maxAttempts = rp.MaxRetries + 1
	}

	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			delay := time.Duration(0)
			if rp != nil {
				retryPolicy := &retry.Policy{
					InitialBackoff: rp.InitialBackoff,
					MaxBackoff:     rp.MaxBackoff,
					BackoffFactor:  rp.BackoffFactor,
					Strategy:       retry.StrategyExponential,
					Jitter:         true,
				}
				delay = retryPolicy.Delay(attempt - 1)
			}

			log.Info("retrying task",
				slog.Int("attempt", attempt+1),
				slog.Duration("delay", delay),
			)

			select {
			case <-ctx.Done():
				w.failTask(ctx, ti, "context cancelled during retry")
				return
			case <-time.After(delay):
			}

			ti.Attempt = attempt + 1
		}

		result, err = exec.Execute(ctx, task, nil)
		if err == nil && result.Status == dag.StatusSuccess {
			break
		}

		if err != nil {
			log.Warn("task execution error", slog.Int("attempt", attempt+1), slog.Any("error", err))
		} else if result != nil {
			log.Warn("task execution failed",
				slog.Int("attempt", attempt+1),
				slog.String("status", result.Status.String()),
			)
		}
	}

	// Update final status.
	ti.EndTime = time.Now()

	if result != nil {
		ti.Status = result.Status
		ti.Output = result.Output
		if result.Error != nil {
			ti.Error = result.Error.Error()
		}
	} else if err != nil {
		ti.Status = dag.StatusFailed
		ti.Error = err.Error()
	}

	if err := w.store.UpdateTaskInstance(ctx, ti); err != nil {
		log.Error("failed to update task instance after execution", slog.Any("error", err))
	}

	// If task exhausted retries, send to DLQ.
	if ti.Status == dag.StatusFailed {
		w.dlq.Push(retry.DLQEntry{
			TaskID:   ti.TaskID,
			RunID:    string(ti.RunID),
			Error:    fmt.Errorf("%s", ti.Error),
			Attempts: ti.Attempt,
		})
		log.Warn("task sent to dead letter queue", slog.Int("attempts", ti.Attempt))
	}

	log.Info("task completed",
		slog.String("status", ti.Status.String()),
		slog.Duration("duration", ti.EndTime.Sub(ti.StartTime)),
	)
}

func (w *Worker) failTask(ctx context.Context, ti *store.TaskInstance, errMsg string) {
	ti.Status = dag.StatusFailed
	ti.Error = errMsg
	ti.EndTime = time.Now()
	_ = w.store.UpdateTaskInstance(ctx, ti)
}

func (w *Worker) heartbeatLoop(ctx context.Context) {
	defer w.wg.Done()

	ticker := time.NewTicker(w.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.stopCh:
			return
		case <-ticker.C:
			w.logger.Debug("heartbeat", slog.String("worker_id", w.id))
		}
	}
}

// Pool manages a set of workers.
type Pool struct {
	workers []*Worker
	mu      sync.RWMutex
	logger  *slog.Logger
}

// NewPool creates a new worker pool.
func NewPool(logger *slog.Logger) *Pool {
	if logger == nil {
		logger = slog.Default()
	}
	return &Pool{
		logger: logger,
	}
}

// Add adds a worker to the pool.
func (p *Pool) Add(w *Worker) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.workers = append(p.workers, w)
}

// Start starts all workers in the pool.
func (p *Pool) Start(ctx context.Context) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	for _, w := range p.workers {
		if err := w.Start(ctx); err != nil {
			return fmt.Errorf("start worker %s: %w", w.id, err)
		}
	}
	return nil
}

// Stop stops all workers in the pool.
func (p *Pool) Stop() error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	for _, w := range p.workers {
		if err := w.Stop(); err != nil {
			p.logger.Error("stop worker", slog.String("id", w.id), slog.Any("error", err))
		}
	}
	return nil
}

// Size returns the number of workers in the pool.
func (p *Pool) Size() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.workers)
}

// Dispatch sends a task instance to an available worker using round-robin.
func (p *Pool) Dispatch(ti *store.TaskInstance) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.workers) == 0 {
		return fmt.Errorf("no workers available")
	}

	// Simple round-robin dispatch.
	for _, w := range p.workers {
		if err := w.Submit(ti); err == nil {
			return nil
		}
	}

	return fmt.Errorf("all workers are busy")
}
