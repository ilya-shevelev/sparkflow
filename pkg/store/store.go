// Package store defines the storage interface and provides in-memory
// and PostgreSQL implementations for DAG and task run persistence.
package store

import (
	"context"
	"time"

	"github.com/ilya-shevelev/sparkflow/pkg/dag"
)

// RunID uniquely identifies a DAG run.
type RunID string

// TaskInstanceID uniquely identifies a task instance within a run.
type TaskInstanceID string

// DAGRun represents a single execution of a DAG.
type DAGRun struct {
	ID          RunID              `json:"id"`
	DAGID       string             `json:"dag_id"`
	Status      dag.Status         `json:"status"`
	Params      map[string]string  `json:"params"`
	StartTime   time.Time          `json:"start_time"`
	EndTime     time.Time          `json:"end_time"`
	CreatedAt   time.Time          `json:"created_at"`
	UpdatedAt   time.Time          `json:"updated_at"`
}

// TaskInstance represents a single execution of a task within a DAG run.
type TaskInstance struct {
	ID             TaskInstanceID `json:"id"`
	RunID          RunID          `json:"run_id"`
	TaskID         string         `json:"task_id"`
	DAGID          string         `json:"dag_id"`
	Status         dag.Status     `json:"status"`
	Attempt        int            `json:"attempt"`
	WorkerID       string         `json:"worker_id"`
	Output         []byte         `json:"output,omitempty"`
	Error          string         `json:"error,omitempty"`
	StartTime      time.Time      `json:"start_time"`
	EndTime        time.Time      `json:"end_time"`
	QueuedAt       time.Time      `json:"queued_at"`
	IdempotencyKey string         `json:"idempotency_key"`
}

// DAGFilter defines filters for querying DAGs.
type DAGFilter struct {
	Tags  []string
	Owner string
	Limit int
}

// RunFilter defines filters for querying DAG runs.
type RunFilter struct {
	DAGID  string
	Status *dag.Status
	Limit  int
	Offset int
}

// Store is the interface for persistent storage of DAGs, runs, and task instances.
type Store interface {
	// DAG operations.
	SaveDAG(ctx context.Context, d *dag.DAG) error
	GetDAG(ctx context.Context, dagID string) (*dag.DAG, error)
	ListDAGs(ctx context.Context, filter DAGFilter) ([]*dag.DAG, error)
	DeleteDAG(ctx context.Context, dagID string) error

	// DAG run operations.
	CreateRun(ctx context.Context, run *DAGRun) error
	GetRun(ctx context.Context, runID RunID) (*DAGRun, error)
	UpdateRun(ctx context.Context, run *DAGRun) error
	ListRuns(ctx context.Context, filter RunFilter) ([]*DAGRun, error)

	// Task instance operations.
	CreateTaskInstance(ctx context.Context, ti *TaskInstance) error
	GetTaskInstance(ctx context.Context, id TaskInstanceID) (*TaskInstance, error)
	UpdateTaskInstance(ctx context.Context, ti *TaskInstance) error
	ListTaskInstances(ctx context.Context, runID RunID) ([]*TaskInstance, error)
	GetTaskInstanceByIdempotencyKey(ctx context.Context, key string) (*TaskInstance, error)

	// Health.
	Ping(ctx context.Context) error
	Close() error
}
