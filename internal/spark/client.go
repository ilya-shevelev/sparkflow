// Package spark provides Spark Connect client and spark-submit wrapper
// for submitting and monitoring Spark jobs.
package spark

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"strings"
	"time"
)

// ConnectConfig configures the Spark Connect client.
type ConnectConfig struct {
	Host    string
	Port    int
	Token   string
	Cluster string
}

// ConnectClient provides a client for the Spark Connect protocol.
type ConnectClient struct {
	config ConnectConfig
	client *http.Client
}

// NewConnectClient creates a new Spark Connect client.
func NewConnectClient(cfg ConnectConfig) *ConnectClient {
	if cfg.Host == "" {
		cfg.Host = "localhost"
	}
	if cfg.Port == 0 {
		cfg.Port = 15002
	}
	return &ConnectClient{
		config: cfg,
		client: &http.Client{Timeout: 5 * time.Minute},
	}
}

// Session represents a Spark session.
type Session struct {
	ID     string
	client *ConnectClient
}

// CreateSession creates a new Spark session.
func (c *ConnectClient) CreateSession(ctx context.Context, sessionID string) (*Session, error) {
	payload := map[string]string{
		"session_id": sessionID,
	}
	body, _ := json.Marshal(payload)

	url := fmt.Sprintf("http://%s:%d/api/v1/sessions", c.config.Host, c.config.Port)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create session request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.config.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.config.Token)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("create session: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("create session failed with status %d", resp.StatusCode)
	}

	return &Session{
		ID:     sessionID,
		client: c,
	}, nil
}

// ExecutePlan executes a Spark plan in the session.
func (s *Session) ExecutePlan(ctx context.Context, plan string) (*ExecutionResult, error) {
	payload := map[string]string{
		"session_id": s.ID,
		"plan":       plan,
	}
	body, _ := json.Marshal(payload)

	url := fmt.Sprintf("http://%s:%d/api/v1/execute", s.client.config.Host, s.client.config.Port)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create execute request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if s.client.config.Token != "" {
		req.Header.Set("Authorization", "Bearer "+s.client.config.Token)
	}

	resp, err := s.client.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute plan: %w", err)
	}
	defer resp.Body.Close()

	var result ExecutionResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode execution result: %w", err)
	}

	return &result, nil
}

// ExecutionResult represents the result of a Spark plan execution.
type ExecutionResult struct {
	Status  string         `json:"status"`
	Data    []byte         `json:"data,omitempty"`
	Schema  string         `json:"schema,omitempty"`
	Metrics map[string]any `json:"metrics,omitempty"`
	Error   string         `json:"error,omitempty"`
}

// SubmitConfig configures a spark-submit command.
type SubmitConfig struct {
	SparkHome      string
	Master         string
	DeployMode     string
	AppName        string
	Class          string
	Application    string
	DriverMemory   string
	ExecutorMemory string
	ExecutorCores  int
	NumExecutors   int
	Packages       []string
	Jars           []string
	Files          []string
	SparkConf      map[string]string
	AppArgs        []string
}

// SubmitResult holds the result of a spark-submit execution.
type SubmitResult struct {
	ExitCode      int
	Output        string
	Stderr        string
	ApplicationID string
	Duration      time.Duration
}

// Submit builds and executes a spark-submit command.
func Submit(ctx context.Context, cfg SubmitConfig) (*SubmitResult, error) {
	sparkSubmit := "spark-submit"
	if cfg.SparkHome != "" {
		sparkSubmit = cfg.SparkHome + "/bin/spark-submit"
	}

	args := buildSubmitArgs(cfg)

	cmd := exec.CommandContext(ctx, sparkSubmit, args...)

	if cfg.SparkHome != "" {
		cmd.Env = append(cmd.Env, fmt.Sprintf("SPARK_HOME=%s", cfg.SparkHome))
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	start := time.Now()
	err := cmd.Run()
	duration := time.Since(start)

	result := &SubmitResult{
		Output:   stdout.String(),
		Stderr:   stderr.String(),
		Duration: duration,
	}

	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			result.ExitCode = exitErr.ExitCode()
		} else {
			return nil, fmt.Errorf("spark-submit: %w", err)
		}
	}

	// Extract application ID from output.
	combined := result.Output + result.Stderr
	if idx := strings.Index(combined, "application_"); idx >= 0 {
		end := idx
		for end < len(combined) && combined[end] != '\n' && combined[end] != ' ' && combined[end] != ')' {
			end++
		}
		result.ApplicationID = combined[idx:end]
	}

	return result, nil
}

func buildSubmitArgs(cfg SubmitConfig) []string {
	var args []string

	if cfg.Master != "" {
		args = append(args, "--master", cfg.Master)
	}
	if cfg.DeployMode != "" {
		args = append(args, "--deploy-mode", cfg.DeployMode)
	}
	if cfg.AppName != "" {
		args = append(args, "--name", cfg.AppName)
	}
	if cfg.Class != "" {
		args = append(args, "--class", cfg.Class)
	}
	if cfg.DriverMemory != "" {
		args = append(args, "--driver-memory", cfg.DriverMemory)
	}
	if cfg.ExecutorMemory != "" {
		args = append(args, "--executor-memory", cfg.ExecutorMemory)
	}
	if cfg.ExecutorCores > 0 {
		args = append(args, "--executor-cores", fmt.Sprintf("%d", cfg.ExecutorCores))
	}
	if cfg.NumExecutors > 0 {
		args = append(args, "--num-executors", fmt.Sprintf("%d", cfg.NumExecutors))
	}
	if len(cfg.Packages) > 0 {
		args = append(args, "--packages", strings.Join(cfg.Packages, ","))
	}
	if len(cfg.Jars) > 0 {
		args = append(args, "--jars", strings.Join(cfg.Jars, ","))
	}
	if len(cfg.Files) > 0 {
		args = append(args, "--files", strings.Join(cfg.Files, ","))
	}
	for k, v := range cfg.SparkConf {
		args = append(args, "--conf", fmt.Sprintf("%s=%s", k, v))
	}
	if cfg.Application != "" {
		args = append(args, cfg.Application)
	}
	args = append(args, cfg.AppArgs...)

	return args
}

// ApplicationStatus represents the status of a Spark application.
type ApplicationStatus struct {
	AppID       string    `json:"app_id"`
	State       string    `json:"state"`
	Progress    float64   `json:"progress"`
	TrackingURL string    `json:"tracking_url"`
	StartTime   time.Time `json:"start_time"`
	EndTime     time.Time `json:"end_time"`
}

// GetApplicationStatus checks the status of a Spark application via the Spark UI REST API.
func GetApplicationStatus(ctx context.Context, sparkUIURL, appID string) (*ApplicationStatus, error) {
	url := fmt.Sprintf("%s/api/v1/applications/%s", sparkUIURL, appID)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create status request: %w", err)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("get application status: %w", err)
	}
	defer resp.Body.Close()

	var status ApplicationStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, fmt.Errorf("decode status: %w", err)
	}

	return &status, nil
}
