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

// PythonConfig configures the Python executor.
type PythonConfig struct {
	PythonPath string // Path to python binary, defaults to "python3"
	VenvPath   string // Optional virtualenv path
}

// PythonExecutor runs Python scripts as task execution.
type PythonExecutor struct {
	config   PythonConfig
	mu       sync.RWMutex
	commands map[string]*exec.Cmd
}

// NewPythonExecutor creates a new Python executor.
func NewPythonExecutor(cfg PythonConfig) *PythonExecutor {
	if cfg.PythonPath == "" {
		cfg.PythonPath = "python3"
	}
	return &PythonExecutor{
		config:   cfg,
		commands: make(map[string]*exec.Cmd),
	}
}

func (e *PythonExecutor) Type() string { return "python" }

func (e *PythonExecutor) Validate(task *dag.Task) error {
	_, hasScript := task.Config["script"]
	_, hasCode := task.Config["code"]
	if !hasScript && !hasCode {
		return fmt.Errorf("python executor requires 'script' or 'code' in config")
	}
	return nil
}

func (e *PythonExecutor) Execute(ctx context.Context, task *dag.Task, params map[string]any) (*Result, error) {
	if err := e.Validate(task); err != nil {
		return nil, err
	}

	result := &Result{
		Status:    dag.StatusRunning,
		StartTime: time.Now(),
		Metrics:   make(map[string]float64),
	}

	// Build python path.
	pythonPath := e.config.PythonPath
	if p, ok := task.Config["python_path"]; ok {
		pythonPath = p.(string)
	}
	if e.config.VenvPath != "" {
		pythonPath = e.config.VenvPath + "/bin/python"
	}

	// Set up context with timeout.
	execCtx := ctx
	if task.Timeout > 0 {
		var cancel context.CancelFunc
		execCtx, cancel = context.WithTimeout(ctx, task.Timeout)
		defer cancel()
	}

	var cmd *exec.Cmd

	if script, ok := task.Config["script"]; ok {
		// Run a Python script file.
		args := []string{script.(string)}
		if scriptArgs, ok := task.Config["args"]; ok {
			if argList, ok := scriptArgs.([]any); ok {
				for _, a := range argList {
					args = append(args, fmt.Sprintf("%v", a))
				}
			}
		}
		cmd = exec.CommandContext(execCtx, pythonPath, args...)
	} else if code, ok := task.Config["code"]; ok {
		// Run inline Python code.
		codeStr := code.(string)
		for k, v := range params {
			codeStr = strings.ReplaceAll(codeStr, fmt.Sprintf("{{%s}}", k), fmt.Sprintf("%v", v))
		}
		cmd = exec.CommandContext(execCtx, pythonPath, "-c", codeStr)
	}

	// Set working directory.
	if dir, ok := task.Config["working_dir"]; ok {
		cmd.Dir = dir.(string)
	}

	// Set environment.
	if envMap, ok := task.Config["env"]; ok {
		if envVars, ok := envMap.(map[string]any); ok {
			for k, v := range envVars {
				cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%v", k, v))
			}
		}
	}

	// Add PYTHONPATH if specified.
	if pp, ok := task.Config["pythonpath"]; ok {
		cmd.Env = append(cmd.Env, fmt.Sprintf("PYTHONPATH=%s", pp))
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	e.mu.Lock()
	e.commands[task.ID] = cmd
	e.mu.Unlock()

	defer func() {
		e.mu.Lock()
		delete(e.commands, task.ID)
		e.mu.Unlock()
	}()

	err := cmd.Run()
	result.EndTime = time.Now()
	result.Metrics["duration_ms"] = float64(result.EndTime.Sub(result.StartTime).Milliseconds())

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
	return result, nil
}

func (e *PythonExecutor) Cancel(_ context.Context, taskID string) error {
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
