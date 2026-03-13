package executor

import (
	"context"
	"testing"
	"time"

	"github.com/ilya-shevelev/sparkflow/pkg/dag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShellExecutor_Execute_SimpleCommand(t *testing.T) {
	exec := NewShellExecutor()
	task := &dag.Task{
		ID:       "test-shell",
		Executor: "shell",
		Config: map[string]any{
			"command": "echo hello world",
		},
	}

	result, err := exec.Execute(context.Background(), task, nil)
	require.NoError(t, err)
	assert.Equal(t, dag.StatusSuccess, result.Status)
	assert.Contains(t, string(result.Output), "hello world")
	assert.Equal(t, 0, result.ExitCode)
}

func TestShellExecutor_Execute_WithParams(t *testing.T) {
	exec := NewShellExecutor()
	task := &dag.Task{
		ID:       "test-params",
		Executor: "shell",
		Config: map[string]any{
			"command": "echo {{name}} {{date}}",
		},
	}

	params := map[string]any{
		"name": "sparkflow",
		"date": "2024-01-01",
	}

	result, err := exec.Execute(context.Background(), task, params)
	require.NoError(t, err)
	assert.Equal(t, dag.StatusSuccess, result.Status)
	assert.Contains(t, string(result.Output), "sparkflow 2024-01-01")
}

func TestShellExecutor_Execute_FailingCommand(t *testing.T) {
	exec := NewShellExecutor()
	task := &dag.Task{
		ID:       "test-fail",
		Executor: "shell",
		Config: map[string]any{
			"command": "exit 42",
		},
	}

	result, err := exec.Execute(context.Background(), task, nil)
	require.NoError(t, err)
	assert.Equal(t, dag.StatusFailed, result.Status)
	assert.Equal(t, 42, result.ExitCode)
}

func TestShellExecutor_Execute_Timeout(t *testing.T) {
	exec := NewShellExecutor()
	task := &dag.Task{
		ID:       "test-timeout",
		Executor: "shell",
		Config: map[string]any{
			"command": "sleep 60",
		},
		Timeout: 100 * time.Millisecond,
	}

	result, err := exec.Execute(context.Background(), task, nil)
	require.NoError(t, err)
	assert.Equal(t, dag.StatusFailed, result.Status)
}

func TestShellExecutor_Validate(t *testing.T) {
	exec := NewShellExecutor()

	tests := []struct {
		name    string
		config  map[string]any
		wantErr bool
	}{
		{
			name:    "valid config",
			config:  map[string]any{"command": "echo hello"},
			wantErr: false,
		},
		{
			name:    "missing command",
			config:  map[string]any{},
			wantErr: true,
		},
		{
			name:    "command not string",
			config:  map[string]any{"command": 123},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task := &dag.Task{ID: "test", Executor: "shell", Config: tc.config}
			err := exec.Validate(task)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestShellExecutor_Type(t *testing.T) {
	exec := NewShellExecutor()
	assert.Equal(t, "shell", exec.Type())
}

func TestShellExecutor_Cancel(t *testing.T) {
	exec := NewShellExecutor()
	// Cancel non-existent task should return error.
	err := exec.Cancel(context.Background(), "nonexistent")
	assert.Error(t, err)
}

func TestShellExecutor_Metrics(t *testing.T) {
	exec := NewShellExecutor()
	task := &dag.Task{
		ID:       "test-metrics",
		Executor: "shell",
		Config: map[string]any{
			"command": "echo metrics",
		},
	}

	result, err := exec.Execute(context.Background(), task, nil)
	require.NoError(t, err)
	assert.Contains(t, result.Metrics, "duration_ms")
	assert.True(t, result.Metrics["duration_ms"] >= 0)
}

func TestRegistry(t *testing.T) {
	reg := NewRegistry()
	shell := NewShellExecutor()
	reg.Register(shell)

	got, err := reg.Get("shell")
	require.NoError(t, err)
	assert.Equal(t, "shell", got.Type())

	_, err = reg.Get("nonexistent")
	assert.Error(t, err)

	types := reg.Types()
	assert.Contains(t, types, "shell")
}
