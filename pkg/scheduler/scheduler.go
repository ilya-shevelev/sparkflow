// Package scheduler provides task scheduling strategies including FIFO,
// priority-based, and distributed scheduling with cron support.
package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/robfig/cron/v3"

	"github.com/ilya-shevelev/sparkflow/pkg/dag"
	"github.com/ilya-shevelev/sparkflow/pkg/store"
)

// Scheduler is the interface for DAG execution scheduling.
type Scheduler interface {
	// Schedule examines all DAG runs and returns task instances ready for execution.
	Schedule(ctx context.Context) ([]*store.TaskInstance, error)

	// Submit creates a new DAG run and returns the run ID.
	Submit(ctx context.Context, d *dag.DAG, params map[string]string) (store.RunID, error)

	// ReportCompletion records the completion of a task instance.
	ReportCompletion(ctx context.Context, taskID string, result *TaskResult) error

	// Start begins the scheduling loop.
	Start(ctx context.Context) error

	// Stop halts the scheduler.
	Stop() error
}

// TaskResult represents the outcome of task execution reported back to the scheduler.
type TaskResult struct {
	TaskInstanceID store.TaskInstanceID
	RunID          store.RunID
	Status         dag.Status
	Output         []byte
	Error          string
	Metrics        map[string]float64
}

// FIFOScheduler is a simple first-in-first-out scheduler.
type FIFOScheduler struct {
	store    store.Store
	logger   *slog.Logger
	mu       sync.Mutex
	cronSch  *cron.Cron
	dagIndex map[string]*dag.DAG
	stopCh   chan struct{}
	interval time.Duration
}

// FIFOConfig configures the FIFO scheduler.
type FIFOConfig struct {
	PollInterval time.Duration
	Logger       *slog.Logger
}

// NewFIFOScheduler creates a new FIFO scheduler.
func NewFIFOScheduler(st store.Store, cfg FIFOConfig) *FIFOScheduler {
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 5 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &FIFOScheduler{
		store:    st,
		logger:   cfg.Logger,
		cronSch:  cron.New(cron.WithSeconds()),
		dagIndex: make(map[string]*dag.DAG),
		stopCh:   make(chan struct{}),
		interval: cfg.PollInterval,
	}
}

func (s *FIFOScheduler) Submit(ctx context.Context, d *dag.DAG, params map[string]string) (store.RunID, error) {
	runID := store.RunID(uuid.New().String())

	run := &store.DAGRun{
		ID:        runID,
		DAGID:     d.ID,
		Status:    dag.StatusRunning,
		Params:    params,
		StartTime: time.Now(),
	}

	if err := s.store.CreateRun(ctx, run); err != nil {
		return "", fmt.Errorf("create run: %w", err)
	}

	// Create task instances for all tasks in the DAG.
	for _, task := range d.Tasks {
		ti := &store.TaskInstance{
			ID:             store.TaskInstanceID(uuid.New().String()),
			RunID:          runID,
			TaskID:         task.ID,
			DAGID:          d.ID,
			Status:         dag.StatusPending,
			Attempt:        1,
			QueuedAt:       time.Now(),
			IdempotencyKey: fmt.Sprintf("%s-%s-%s", d.ID, runID, task.ID),
		}
		if err := s.store.CreateTaskInstance(ctx, ti); err != nil {
			return "", fmt.Errorf("create task instance for %s: %w", task.ID, err)
		}
	}

	// Index the DAG for scheduling.
	s.mu.Lock()
	s.dagIndex[d.ID] = d
	s.mu.Unlock()

	s.logger.Info("submitted DAG run",
		slog.String("dag_id", d.ID),
		slog.String("run_id", string(runID)),
	)

	return runID, nil
}

func (s *FIFOScheduler) Schedule(ctx context.Context) ([]*store.TaskInstance, error) {
	// Get all running DAG runs.
	runningStatus := dag.StatusRunning
	runs, err := s.store.ListRuns(ctx, store.RunFilter{Status: &runningStatus})
	if err != nil {
		return nil, fmt.Errorf("list running runs: %w", err)
	}

	var ready []*store.TaskInstance

	for _, run := range runs {
		instances, err := s.store.ListTaskInstances(ctx, run.ID)
		if err != nil {
			s.logger.Error("list task instances", slog.String("run_id", string(run.ID)), slog.Any("error", err))
			continue
		}

		s.mu.Lock()
		d, ok := s.dagIndex[run.DAGID]
		s.mu.Unlock()

		if !ok {
			// Try to load from store.
			d, err = s.store.GetDAG(ctx, run.DAGID)
			if err != nil {
				s.logger.Error("get DAG", slog.String("dag_id", run.DAGID), slog.Any("error", err))
				continue
			}
			s.mu.Lock()
			s.dagIndex[run.DAGID] = d
			s.mu.Unlock()
		}

		// Build status map.
		statusMap := make(map[string]dag.Status)
		for _, ti := range instances {
			statusMap[ti.TaskID] = ti.Status
		}

		// Find tasks ready to execute.
		readyTasks := d.ReadyTasks(statusMap)
		for _, task := range readyTasks {
			// Find the corresponding task instance.
			for _, ti := range instances {
				if ti.TaskID == task.ID && ti.Status == dag.StatusPending {
					ti.Status = dag.StatusQueued
					ti.QueuedAt = time.Now()
					if err := s.store.UpdateTaskInstance(ctx, ti); err != nil {
						s.logger.Error("update task instance", slog.String("task_id", ti.TaskID), slog.Any("error", err))
						continue
					}
					ready = append(ready, ti)
				}
			}
		}

		// Check if the run is complete.
		allComplete := true
		anyFailed := false
		for _, ti := range instances {
			if !ti.Status.IsTerminal() && ti.Status != dag.StatusQueued {
				// Re-read to get updated status.
				current, err := s.store.GetTaskInstance(ctx, ti.ID)
				if err == nil {
					ti = current
				}
			}
			if !ti.Status.IsTerminal() {
				allComplete = false
			}
			if ti.Status == dag.StatusFailed {
				anyFailed = true
			}
		}

		if allComplete {
			if anyFailed {
				run.Status = dag.StatusFailed
			} else {
				run.Status = dag.StatusSuccess
			}
			run.EndTime = time.Now()
			if err := s.store.UpdateRun(ctx, run); err != nil {
				s.logger.Error("update run", slog.String("run_id", string(run.ID)), slog.Any("error", err))
			}
		}
	}

	return ready, nil
}

func (s *FIFOScheduler) ReportCompletion(ctx context.Context, taskID string, result *TaskResult) error {
	ti, err := s.store.GetTaskInstance(ctx, result.TaskInstanceID)
	if err != nil {
		return fmt.Errorf("get task instance: %w", err)
	}

	ti.Status = result.Status
	ti.Output = result.Output
	ti.Error = result.Error
	ti.EndTime = time.Now()

	if err := s.store.UpdateTaskInstance(ctx, ti); err != nil {
		return fmt.Errorf("update task instance: %w", err)
	}

	s.logger.Info("task completed",
		slog.String("task_id", taskID),
		slog.String("status", result.Status.String()),
	)

	return nil
}

func (s *FIFOScheduler) Start(ctx context.Context) error {
	s.logger.Info("starting FIFO scheduler", slog.Duration("poll_interval", s.interval))

	// Start cron scheduler for periodic DAGs.
	s.cronSch.Start()

	go func() {
		ticker := time.NewTicker(s.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-s.stopCh:
				return
			case <-ticker.C:
				if _, err := s.Schedule(ctx); err != nil {
					s.logger.Error("scheduling error", slog.Any("error", err))
				}
			}
		}
	}()

	return nil
}

func (s *FIFOScheduler) Stop() error {
	close(s.stopCh)
	s.cronSch.Stop()
	s.logger.Info("FIFO scheduler stopped")
	return nil
}

// RegisterDAG registers a DAG with the scheduler and sets up cron scheduling if configured.
func (s *FIFOScheduler) RegisterDAG(ctx context.Context, d *dag.DAG) error {
	// Save to store.
	if err := s.store.SaveDAG(ctx, d); err != nil {
		return fmt.Errorf("save DAG: %w", err)
	}

	s.mu.Lock()
	s.dagIndex[d.ID] = d
	s.mu.Unlock()

	// Set up cron schedule if configured.
	if d.Schedule != nil && d.Schedule.Expression != "" {
		dagCopy := d
		_, err := s.cronSch.AddFunc(d.Schedule.Expression, func() {
			if _, err := s.Submit(ctx, dagCopy, dagCopy.Config.Params); err != nil {
				s.logger.Error("cron submit failed",
					slog.String("dag_id", dagCopy.ID),
					slog.Any("error", err),
				)
			}
		})
		if err != nil {
			return fmt.Errorf("add cron schedule for DAG %s: %w", d.ID, err)
		}
		s.logger.Info("registered cron schedule",
			slog.String("dag_id", d.ID),
			slog.String("cron", d.Schedule.Expression),
		)
	}

	return nil
}

// PriorityScheduler extends FIFO with priority-based task selection.
type PriorityScheduler struct {
	*FIFOScheduler
}

// NewPriorityScheduler creates a new priority-aware scheduler.
func NewPriorityScheduler(st store.Store, cfg FIFOConfig) *PriorityScheduler {
	return &PriorityScheduler{
		FIFOScheduler: NewFIFOScheduler(st, cfg),
	}
}
