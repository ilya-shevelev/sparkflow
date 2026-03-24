package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/ilya-shevelev/sparkflow/pkg/dag"
)

// HDFSConfig configures the HDFS executor.
type HDFSConfig struct {
	NameNodeURL string // WebHDFS namenode URL
	User        string // HDFS user
}

// HDFSExecutor performs HDFS file operations via the WebHDFS REST API.
type HDFSExecutor struct {
	config HDFSConfig
	client *http.Client
}

// NewHDFSExecutor creates a new HDFS executor.
func NewHDFSExecutor(cfg HDFSConfig) *HDFSExecutor {
	if cfg.NameNodeURL == "" {
		cfg.NameNodeURL = "http://localhost:9870"
	}
	if cfg.User == "" {
		cfg.User = "sparkflow"
	}
	return &HDFSExecutor{
		config: cfg,
		client: &http.Client{
			Timeout: 5 * time.Minute,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				// WebHDFS uses redirects for write operations.
				return nil
			},
		},
	}
}

func (e *HDFSExecutor) Type() string { return "hdfs" }

func (e *HDFSExecutor) Validate(task *dag.Task) error {
	op, ok := task.Config["operation"]
	if !ok {
		return fmt.Errorf("hdfs executor requires 'operation' in config")
	}
	opStr, ok := op.(string)
	if !ok {
		return fmt.Errorf("hdfs executor 'operation' must be a string")
	}

	switch opStr {
	case "put", "get", "delete", "mkdir", "list", "status", "rename":
		// valid operations
	default:
		return fmt.Errorf("hdfs executor: unknown operation %q (supported: put, get, delete, mkdir, list, status, rename)", opStr)
	}

	if _, ok := task.Config["path"]; !ok {
		return fmt.Errorf("hdfs executor requires 'path' in config")
	}

	return nil
}

func (e *HDFSExecutor) Execute(ctx context.Context, task *dag.Task, params map[string]any) (*Result, error) {
	if err := e.Validate(task); err != nil {
		return nil, err
	}

	result := &Result{
		Status:    dag.StatusRunning,
		StartTime: time.Now(),
		Metrics:   make(map[string]float64),
	}

	operation := task.Config["operation"].(string)
	path := task.Config["path"].(string)

	// Substitute parameters.
	for k, v := range params {
		path = fmt.Sprintf("%s", replaceParam(path, k, v))
	}

	var err error
	var output []byte

	switch operation {
	case "put":
		err = e.putFile(ctx, path, task)
	case "get":
		output, err = e.getFile(ctx, path)
	case "delete":
		err = e.deleteFile(ctx, path, task)
	case "mkdir":
		err = e.mkdirs(ctx, path)
	case "list":
		output, err = e.listDir(ctx, path)
	case "status":
		output, err = e.fileStatus(ctx, path)
	case "rename":
		dest, ok := task.Config["destination"].(string)
		if !ok {
			err = fmt.Errorf("rename operation requires 'destination' in config")
		} else {
			err = e.rename(ctx, path, dest)
		}
	}

	result.EndTime = time.Now()
	result.Metrics["duration_ms"] = float64(result.EndTime.Sub(result.StartTime).Milliseconds())
	result.Output = output

	if err != nil {
		result.Status = dag.StatusFailed
		result.Error = err
		return result, nil
	}

	result.Status = dag.StatusSuccess
	return result, nil
}

func replaceParam(s string, key string, value any) string {
	return bytes.NewBuffer([]byte(
		fmt.Sprintf("%s", bytes.ReplaceAll([]byte(s), []byte(fmt.Sprintf("{{%s}}", key)), []byte(fmt.Sprintf("%v", value)))),
	)).String()
}

func (e *HDFSExecutor) webhdfsURL(path, op string, extraParams map[string]string) string {
	u := fmt.Sprintf("%s/webhdfs/v1%s", e.config.NameNodeURL, path)
	params := url.Values{}
	params.Set("op", op)
	params.Set("user.name", e.config.User)
	for k, v := range extraParams {
		params.Set(k, v)
	}
	return u + "?" + params.Encode()
}

func (e *HDFSExecutor) putFile(ctx context.Context, path string, task *dag.Task) error {
	// Create the file.
	createURL := e.webhdfsURL(path, "CREATE", map[string]string{"overwrite": "true"})

	var body io.Reader
	if content, ok := task.Config["content"]; ok {
		body = bytes.NewReader([]byte(fmt.Sprintf("%v", content)))
	} else if localPath, ok := task.Config["local_path"]; ok {
		_ = localPath // In production, read from local file.
		body = bytes.NewReader([]byte{})
	} else {
		body = bytes.NewReader([]byte{})
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, createURL, body)
	if err != nil {
		return fmt.Errorf("create PUT request: %w", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := e.client.Do(req)
	if err != nil {
		return fmt.Errorf("HDFS PUT request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HDFS PUT returned status %d: %s", resp.StatusCode, string(respBody))
	}

	return nil
}

func (e *HDFSExecutor) getFile(ctx context.Context, path string) ([]byte, error) {
	getURL := e.webhdfsURL(path, "OPEN", nil)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, getURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create GET request: %w", err)
	}

	resp, err := e.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HDFS GET request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("HDFS GET returned status %d: %s", resp.StatusCode, string(body))
	}

	return io.ReadAll(resp.Body)
}

func (e *HDFSExecutor) deleteFile(ctx context.Context, path string, task *dag.Task) error {
	recursive := "false"
	if rec, ok := task.Config["recursive"]; ok {
		if recBool, ok := rec.(bool); ok && recBool {
			recursive = "true"
		}
	}

	deleteURL := e.webhdfsURL(path, "DELETE", map[string]string{"recursive": recursive})

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, deleteURL, nil)
	if err != nil {
		return fmt.Errorf("create DELETE request: %w", err)
	}

	resp, err := e.client.Do(req)
	if err != nil {
		return fmt.Errorf("HDFS DELETE request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HDFS DELETE returned status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

func (e *HDFSExecutor) mkdirs(ctx context.Context, path string) error {
	mkdirURL := e.webhdfsURL(path, "MKDIRS", nil)

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, mkdirURL, nil)
	if err != nil {
		return fmt.Errorf("create MKDIRS request: %w", err)
	}

	resp, err := e.client.Do(req)
	if err != nil {
		return fmt.Errorf("HDFS MKDIRS request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HDFS MKDIRS returned status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

func (e *HDFSExecutor) listDir(ctx context.Context, path string) ([]byte, error) {
	listURL := e.webhdfsURL(path, "LISTSTATUS", nil)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, listURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create LISTSTATUS request: %w", err)
	}

	resp, err := e.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HDFS LISTSTATUS request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("HDFS LISTSTATUS returned status %d: %s", resp.StatusCode, string(body))
	}

	var result map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode HDFS listing: %w", err)
	}

	return json.MarshalIndent(result, "", "  ")
}

func (e *HDFSExecutor) fileStatus(ctx context.Context, path string) ([]byte, error) {
	statusURL := e.webhdfsURL(path, "GETFILESTATUS", nil)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, statusURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create GETFILESTATUS request: %w", err)
	}

	resp, err := e.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HDFS GETFILESTATUS request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("HDFS GETFILESTATUS returned status %d: %s", resp.StatusCode, string(body))
	}

	var result map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode HDFS file status: %w", err)
	}

	return json.MarshalIndent(result, "", "  ")
}

func (e *HDFSExecutor) rename(ctx context.Context, path, destination string) error {
	renameURL := e.webhdfsURL(path, "RENAME", map[string]string{"destination": destination})

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, renameURL, nil)
	if err != nil {
		return fmt.Errorf("create RENAME request: %w", err)
	}

	resp, err := e.client.Do(req)
	if err != nil {
		return fmt.Errorf("HDFS RENAME request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HDFS RENAME returned status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

func (e *HDFSExecutor) Cancel(_ context.Context, _ string) error {
	// HDFS operations are generally atomic and short-lived.
	// Cancellation is handled via context.
	return nil
}
