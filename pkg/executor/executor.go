// Package executor defines the Executor interface and provides implementations
// for running tasks via shell, Python, Docker, Spark, Hadoop, and HDFS.
package executor

import (
	"context"
	"fmt"
	"time"

	"github.com/ilya-shevelev/sparkflow/pkg/dag"
)

// Result contains the outcome of task execution.
type Result struct {
	Status    dag.Status         `json:"status"`
	Output    []byte             `json:"output"`
	Error     error              `json:"error,omitempty"`
	StartTime time.Time          `json:"start_time"`
	EndTime   time.Time          `json:"end_time"`
	Metrics   map[string]float64 `json:"metrics,omitempty"`
	ExitCode  int                `json:"exit_code"`
}

// Executor is the interface that all task executors must implement.
type Executor interface {
	// Execute runs the task with the given parameters.
	Execute(ctx context.Context, task *dag.Task, params map[string]any) (*Result, error)

	// Cancel stops an in-progress task execution.
	Cancel(ctx context.Context, taskID string) error

	// Validate checks if the task configuration is valid for this executor.
	Validate(task *dag.Task) error

	// Type returns the executor type identifier.
	Type() string
}

// Registry holds available executor implementations.
type Registry struct {
	executors map[string]Executor
}

// NewRegistry creates a new executor registry.
func NewRegistry() *Registry {
	return &Registry{
		executors: make(map[string]Executor),
	}
}

// Register adds an executor to the registry.
func (r *Registry) Register(executor Executor) {
	r.executors[executor.Type()] = executor
}

// Get returns an executor by type name.
func (r *Registry) Get(executorType string) (Executor, error) {
	e, ok := r.executors[executorType]
	if !ok {
		return nil, fmt.Errorf("executor type %q not registered", executorType)
	}
	return e, nil
}

// Types returns all registered executor types.
func (r *Registry) Types() []string {
	types := make([]string, 0, len(r.executors))
	for t := range r.executors {
		types = append(types, t)
	}
	return types
}

// DefaultRegistry creates a registry with all built-in executors.
func DefaultRegistry() *Registry {
	reg := NewRegistry()
	reg.Register(NewShellExecutor())
	reg.Register(NewPythonExecutor(PythonConfig{}))
	reg.Register(NewDockerExecutor(DockerConfig{}))
	reg.Register(NewSparkExecutor(SparkConfig{}))
	reg.Register(NewHadoopExecutor(HadoopConfig{}))
	reg.Register(NewHDFSExecutor(HDFSConfig{}))
	return reg
}
