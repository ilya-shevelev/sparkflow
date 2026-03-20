package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/ilya-shevelev/sparkflow/pkg/dag"
)

// SparkConfig configures the Spark executor.
type SparkConfig struct {
	SparkHome    string // Path to SPARK_HOME
	SparkSubmit  string // Path to spark-submit binary
	Master       string // Spark master URL
	ConnectHost  string // Spark Connect server host
	ConnectPort  int    // Spark Connect server port
	SparkUIURL   string // Spark UI URL for monitoring
}

// SparkExecutor submits and manages Spark jobs.
type SparkExecutor struct {
	config   SparkConfig
	mu       sync.RWMutex
	commands map[string]*exec.Cmd
	client   *http.Client
}

// NewSparkExecutor creates a new Spark executor.
func NewSparkExecutor(cfg SparkConfig) *SparkExecutor {
	if cfg.SparkSubmit == "" {
		if cfg.SparkHome != "" {
			cfg.SparkSubmit = cfg.SparkHome + "/bin/spark-submit"
		} else {
			cfg.SparkSubmit = "spark-submit"
		}
	}
	if cfg.Master == "" {
		cfg.Master = "local[*]"
	}
	if cfg.ConnectPort == 0 {
		cfg.ConnectPort = 15002
	}
	return &SparkExecutor{
		config:   cfg,
		commands: make(map[string]*exec.Cmd),
		client:   &http.Client{Timeout: 30 * time.Second},
	}
}

func (e *SparkExecutor) Type() string { return "spark" }

func (e *SparkExecutor) Validate(task *dag.Task) error {
	_, hasApp := task.Config["application"]
	_, hasClass := task.Config["class"]
	_, hasCode := task.Config["code"]
	if !hasApp && !hasClass && !hasCode {
		return fmt.Errorf("spark executor requires 'application', 'class', or 'code' in config")
	}
	return nil
}

func (e *SparkExecutor) Execute(ctx context.Context, task *dag.Task, params map[string]any) (*Result, error) {
	if err := e.Validate(task); err != nil {
		return nil, err
	}

	// Determine execution mode.
	mode := "submit"
	if m, ok := task.Config["mode"]; ok {
		mode = m.(string)
	}

	switch mode {
	case "submit":
		return e.executeSubmit(ctx, task, params)
	case "connect":
		return e.executeConnect(ctx, task, params)
	default:
		return nil, fmt.Errorf("unknown spark execution mode: %s", mode)
	}
}

func (e *SparkExecutor) executeSubmit(ctx context.Context, task *dag.Task, params map[string]any) (*Result, error) {
	result := &Result{
		Status:    dag.StatusRunning,
		StartTime: time.Now(),
		Metrics:   make(map[string]float64),
	}

	args := e.buildSubmitArgs(task, params)

	execCtx := ctx
	if task.Timeout > 0 {
		var cancel context.CancelFunc
		execCtx, cancel = context.WithTimeout(ctx, task.Timeout)
		defer cancel()
	}

	cmd := exec.CommandContext(execCtx, e.config.SparkSubmit, args...)

	// Set SPARK_HOME if configured.
	if e.config.SparkHome != "" {
		cmd.Env = append(cmd.Env, fmt.Sprintf("SPARK_HOME=%s", e.config.SparkHome))
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

	// Try to extract Spark application ID from output.
	outStr := string(output)
	if idx := strings.Index(outStr, "application_"); idx >= 0 {
		end := idx
		for end < len(outStr) && outStr[end] != '\n' && outStr[end] != ' ' && outStr[end] != ')' {
			end++
		}
		result.Metrics["spark_app_id_found"] = 1
	}

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

func (e *SparkExecutor) buildSubmitArgs(task *dag.Task, params map[string]any) []string {
	var args []string

	// Master.
	master := e.config.Master
	if m, ok := task.Config["master"]; ok {
		master = m.(string)
	}
	args = append(args, "--master", master)

	// Deploy mode.
	if mode, ok := task.Config["deploy_mode"]; ok {
		args = append(args, "--deploy-mode", mode.(string))
	}

	// Application name.
	name := task.Name
	if name == "" {
		name = task.ID
	}
	args = append(args, "--name", name)

	// Main class.
	if class, ok := task.Config["class"]; ok {
		args = append(args, "--class", class.(string))
	}

	// Driver memory.
	if mem, ok := task.Config["driver_memory"]; ok {
		args = append(args, "--driver-memory", mem.(string))
	}

	// Executor memory.
	if mem, ok := task.Config["executor_memory"]; ok {
		args = append(args, "--executor-memory", mem.(string))
	}

	// Executor cores.
	if cores, ok := task.Config["executor_cores"]; ok {
		args = append(args, "--executor-cores", fmt.Sprintf("%v", cores))
	}

	// Number of executors.
	if num, ok := task.Config["num_executors"]; ok {
		args = append(args, "--num-executors", fmt.Sprintf("%v", num))
	}

	// Packages.
	if pkgs, ok := task.Config["packages"]; ok {
		if pkgList, ok := pkgs.([]any); ok {
			strs := make([]string, len(pkgList))
			for i, p := range pkgList {
				strs[i] = fmt.Sprintf("%v", p)
			}
			args = append(args, "--packages", strings.Join(strs, ","))
		}
	}

	// Jars.
	if jars, ok := task.Config["jars"]; ok {
		if jarList, ok := jars.([]any); ok {
			strs := make([]string, len(jarList))
			for i, j := range jarList {
				strs[i] = fmt.Sprintf("%v", j)
			}
			args = append(args, "--jars", strings.Join(strs, ","))
		}
	}

	// Spark configs.
	if conf, ok := task.Config["spark_conf"]; ok {
		if confMap, ok := conf.(map[string]any); ok {
			for k, v := range confMap {
				args = append(args, "--conf", fmt.Sprintf("%s=%v", k, v))
			}
		}
	}

	// Application JAR or Python script.
	if app, ok := task.Config["application"]; ok {
		args = append(args, app.(string))
	}

	// Application arguments.
	if appArgs, ok := task.Config["app_args"]; ok {
		if argList, ok := appArgs.([]any); ok {
			for _, a := range argList {
				arg := fmt.Sprintf("%v", a)
				for k, v := range params {
					arg = strings.ReplaceAll(arg, fmt.Sprintf("{{%s}}", k), fmt.Sprintf("%v", v))
				}
				args = append(args, arg)
			}
		}
	}

	return args
}

// executeConnect uses the Spark Connect protocol (HTTP REST for compatibility).
func (e *SparkExecutor) executeConnect(ctx context.Context, task *dag.Task, params map[string]any) (*Result, error) {
	result := &Result{
		Status:    dag.StatusRunning,
		StartTime: time.Now(),
		Metrics:   make(map[string]float64),
	}

	host := e.config.ConnectHost
	if host == "" {
		host = "localhost"
	}
	port := e.config.ConnectPort

	code, ok := task.Config["code"].(string)
	if !ok {
		return nil, fmt.Errorf("spark connect mode requires 'code' in config")
	}

	// Substitute parameters.
	for k, v := range params {
		code = strings.ReplaceAll(code, fmt.Sprintf("{{%s}}", k), fmt.Sprintf("%v", v))
	}

	payload := map[string]string{
		"code":    code,
		"session": fmt.Sprintf("sparkflow-%s", task.ID),
	}
	body, _ := json.Marshal(payload)

	url := fmt.Sprintf("http://%s:%d/api/v1/execute", host, port)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create spark connect request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := e.client.Do(req)
	if err != nil {
		result.Status = dag.StatusFailed
		result.Error = fmt.Errorf("spark connect request failed: %w", err)
		result.EndTime = time.Now()
		return result, nil
	}
	defer resp.Body.Close()

	var buf bytes.Buffer
	_, _ = buf.ReadFrom(resp.Body)
	result.Output = buf.Bytes()
	result.EndTime = time.Now()
	result.Metrics["duration_ms"] = float64(result.EndTime.Sub(result.StartTime).Milliseconds())

	if resp.StatusCode >= 400 {
		result.Status = dag.StatusFailed
		result.Error = fmt.Errorf("spark connect returned status %d", resp.StatusCode)
		return result, nil
	}

	result.Status = dag.StatusSuccess
	return result, nil
}

func (e *SparkExecutor) Cancel(_ context.Context, taskID string) error {
	e.mu.RLock()
	cmd, ok := e.commands[taskID]
	e.mu.RUnlock()

	if ok && cmd.Process != nil {
		return cmd.Process.Kill()
	}

	return fmt.Errorf("no running spark job for task %s", taskID)
}
