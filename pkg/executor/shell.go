package executor

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/ilya-shevelev/sparkflow/pkg/dag"
)

// ShellExecutor runs shell commands as task execution.
type ShellExecutor struct {
	mu       sync.RWMutex
	commands map[string]*exec.Cmd
}

// NewShellExecutor creates a new shell executor.
func NewShellExecutor() *ShellExecutor {
	return &ShellExecutor{
		commands: make(map[string]*exec.Cmd),
	}
}

func (e *ShellExecutor) Type() string { return "shell" }

func (e *ShellExecutor) Validate(task *dag.Task) error {
	cmd, ok := task.Config["command"]
	if !ok {
		return fmt.Errorf("shell executor requires 'command' in config")
	}
	if _, ok := cmd.(string); !ok {
		return fmt.Errorf("shell executor 'command' must be a string")
	}
	return nil
}

func (e *ShellExecutor) Execute(ctx context.Context, task *dag.Task, params map[string]any) (*Result, error) {
	if err := e.Validate(task); err != nil {
		return nil, err
	}

	result := &Result{
		Status:    dag.StatusRunning,
		StartTime: time.Now(),
		Metrics:   make(map[string]float64),
	}

	command := task.Config["command"].(string)

	// Substitute parameters in command.
	for k, v := range params {
		command = strings.ReplaceAll(command, fmt.Sprintf("{{%s}}", k), fmt.Sprintf("%v", v))
	}

	// Determine shell.
	shell := "/bin/sh"
	if s, ok := task.Config["shell"]; ok {
		shell = s.(string)
	}

	// Set up context with timeout.
	execCtx := ctx
	if task.Timeout > 0 {
		var cancel context.CancelFunc
		execCtx, cancel = context.WithTimeout(ctx, task.Timeout)
		defer cancel()
	}

	// Build the command.
	cmd := exec.CommandContext(execCtx, shell, "-c", command)

	// Set working directory if specified.
	if dir, ok := task.Config["working_dir"]; ok {
		cmd.Dir = dir.(string)
	}

	// Set environment variables.
	if envMap, ok := task.Config["env"]; ok {
		if envVars, ok := envMap.(map[string]any); ok {
			for k, v := range envVars {
				cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%v", k, v))
			}
		}
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	// Track the command for cancellation.
	e.mu.Lock()
	e.commands[task.ID] = cmd
	e.mu.Unlock()

	defer func() {
		e.mu.Lock()
		delete(e.commands, task.ID)
		e.mu.Unlock()
	}()

	// Execute.
	err := cmd.Run()
	result.EndTime = time.Now()
	result.Metrics["duration_ms"] = float64(result.EndTime.Sub(result.StartTime).Milliseconds())

	// Combine stdout and stderr.
	output := stdout.Bytes()
	if stderr.Len() > 0 {
		output = append(output, '\n')
		output = append(output, stderr.Bytes()...)
	}
	result.Output = output

	if err != nil {
		result.Status = dag.StatusFailed
		result.Error = err
		if exitErr, ok := err.(*exec.ExitError); ok {
			result.ExitCode = exitErr.ExitCode()
		}
		return result, nil
	}

	result.Status = dag.StatusSuccess
	result.ExitCode = 0
	return result, nil
}

func (e *ShellExecutor) Cancel(_ context.Context, taskID string) error {
	e.mu.RLock()
	cmd, ok := e.commands[taskID]
	e.mu.RUnlock()

	if !ok {
		return fmt.Errorf("no running command for task %s", taskID)
	}

	if cmd.Process != nil {
		return cmd.Process.Kill()
	}
	return nil
}
