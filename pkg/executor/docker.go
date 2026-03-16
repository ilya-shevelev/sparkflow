package executor

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/ilya-shevelev/sparkflow/pkg/dag"
)

// DockerConfig configures the Docker executor.
type DockerConfig struct {
	Host       string // Docker daemon host, defaults to unix socket
	APIVersion string // Docker API version
}

// DockerExecutor runs tasks in Docker containers.
type DockerExecutor struct {
	config     DockerConfig
	mu         sync.RWMutex
	containers map[string]string // taskID -> containerID
}

// NewDockerExecutor creates a new Docker executor.
func NewDockerExecutor(cfg DockerConfig) *DockerExecutor {
	return &DockerExecutor{
		config:     cfg,
		containers: make(map[string]string),
	}
}

func (e *DockerExecutor) Type() string { return "docker" }

func (e *DockerExecutor) Validate(task *dag.Task) error {
	if _, ok := task.Config["image"]; !ok {
		return fmt.Errorf("docker executor requires 'image' in config")
	}
	return nil
}

func (e *DockerExecutor) newClient() (*client.Client, error) {
	opts := []client.Opt{client.FromEnv}
	if e.config.Host != "" {
		opts = append(opts, client.WithHost(e.config.Host))
	}
	if e.config.APIVersion != "" {
		opts = append(opts, client.WithVersion(e.config.APIVersion))
	}
	return client.NewClientWithOpts(opts...)
}

func (e *DockerExecutor) Execute(ctx context.Context, task *dag.Task, params map[string]any) (*Result, error) {
	if err := e.Validate(task); err != nil {
		return nil, err
	}

	result := &Result{
		Status:    dag.StatusRunning,
		StartTime: time.Now(),
		Metrics:   make(map[string]float64),
	}

	cli, err := e.newClient()
	if err != nil {
		return nil, fmt.Errorf("create docker client: %w", err)
	}
	defer cli.Close()

	imageName := task.Config["image"].(string)

	// Set up context with timeout.
	execCtx := ctx
	if task.Timeout > 0 {
		var cancel context.CancelFunc
		execCtx, cancel = context.WithTimeout(ctx, task.Timeout)
		defer cancel()
	}

	// Pull image if configured.
	if pull, ok := task.Config["pull"]; ok {
		if pullBool, ok := pull.(bool); ok && pullBool {
			reader, pullErr := cli.ImagePull(execCtx, imageName, image.PullOptions{})
			if pullErr != nil {
				return nil, fmt.Errorf("pull image %s: %w", imageName, pullErr)
			}
			_, _ = io.Copy(io.Discard, reader)
			reader.Close()
		}
	}

	// Build container config.
	containerCfg := &container.Config{
		Image: imageName,
	}

	// Set command.
	if cmd, ok := task.Config["command"]; ok {
		switch v := cmd.(type) {
		case string:
			containerCfg.Cmd = []string{"/bin/sh", "-c", v}
		case []any:
			strCmd := make([]string, len(v))
			for i, c := range v {
				strCmd[i] = fmt.Sprintf("%v", c)
			}
			containerCfg.Cmd = strCmd
		}
	}

	// Set entrypoint.
	if ep, ok := task.Config["entrypoint"]; ok {
		if epStr, ok := ep.(string); ok {
			containerCfg.Entrypoint = []string{epStr}
		}
	}

	// Set environment variables.
	if envMap, ok := task.Config["env"]; ok {
		if envVars, ok := envMap.(map[string]any); ok {
			for k, v := range envVars {
				containerCfg.Env = append(containerCfg.Env, fmt.Sprintf("%s=%v", k, v))
			}
		}
	}

	// Add parameters as env vars.
	for k, v := range params {
		containerCfg.Env = append(containerCfg.Env, fmt.Sprintf("SPARKFLOW_PARAM_%s=%v", k, v))
	}

	// Set working directory.
	if dir, ok := task.Config["working_dir"]; ok {
		containerCfg.WorkingDir = dir.(string)
	}

	// Host config.
	hostCfg := &container.HostConfig{}

	// Set memory limit.
	if mem, ok := task.Config["memory"]; ok {
		if memStr, ok := mem.(string); ok {
			_ = memStr // Parse memory string to bytes.
			// Simple parsing: this would need a proper implementation for production.
		}
	}

	// Set volume bindings.
	if vols, ok := task.Config["volumes"]; ok {
		if volList, ok := vols.([]any); ok {
			binds := make([]string, len(volList))
			for i, v := range volList {
				binds[i] = fmt.Sprintf("%v", v)
			}
			hostCfg.Binds = binds
		}
	}

	// Set network mode.
	if net, ok := task.Config["network"]; ok {
		if netStr, ok := net.(string); ok {
			hostCfg.NetworkMode = container.NetworkMode(netStr)
		}
	}

	// Create container.
	containerName := fmt.Sprintf("sparkflow-%s-%d", task.ID, time.Now().UnixNano())
	resp, err := cli.ContainerCreate(execCtx, containerCfg, hostCfg, nil, nil, containerName)
	if err != nil {
		return nil, fmt.Errorf("create container: %w", err)
	}

	containerID := resp.ID

	e.mu.Lock()
	e.containers[task.ID] = containerID
	e.mu.Unlock()

	defer func() {
		e.mu.Lock()
		delete(e.containers, task.ID)
		e.mu.Unlock()

		// Clean up container unless configured to keep it.
		if keep, ok := task.Config["keep_container"]; !ok || keep != true {
			removeOpts := container.RemoveOptions{Force: true}
			_ = cli.ContainerRemove(context.Background(), containerID, removeOpts)
		}
	}()

	// Start container.
	if err := cli.ContainerStart(execCtx, containerID, container.StartOptions{}); err != nil {
		return nil, fmt.Errorf("start container: %w", err)
	}

	// Wait for completion.
	statusCh, errCh := cli.ContainerWait(execCtx, containerID, container.WaitConditionNotRunning)
	select {
	case err := <-errCh:
		if err != nil {
			result.Status = dag.StatusFailed
			result.Error = err
			result.EndTime = time.Now()
			return result, nil
		}
	case status := <-statusCh:
		result.ExitCode = int(status.StatusCode)
	}

	// Collect logs.
	logReader, err := cli.ContainerLogs(execCtx, containerID, container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
	})
	if err == nil {
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, logReader)
		logReader.Close()
		result.Output = buf.Bytes()
	}

	result.EndTime = time.Now()
	result.Metrics["duration_ms"] = float64(result.EndTime.Sub(result.StartTime).Milliseconds())

	if result.ExitCode != 0 {
		result.Status = dag.StatusFailed
		result.Error = fmt.Errorf("container exited with code %d", result.ExitCode)
	} else {
		result.Status = dag.StatusSuccess
	}

	return result, nil
}

func (e *DockerExecutor) Cancel(ctx context.Context, taskID string) error {
	e.mu.RLock()
	containerID, ok := e.containers[taskID]
	e.mu.RUnlock()

	if !ok {
		return fmt.Errorf("no running container for task %s", taskID)
	}

	cli, err := e.newClient()
	if err != nil {
		return fmt.Errorf("create docker client: %w", err)
	}
	defer cli.Close()

	timeout := 10
	stopOpts := container.StopOptions{Timeout: &timeout}
	return cli.ContainerStop(ctx, containerID, stopOpts)
}
