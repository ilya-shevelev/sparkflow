package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/ilya-shevelev/sparkflow/pkg/dag"
)

// HadoopConfig configures the Hadoop executor.
type HadoopConfig struct {
	ResourceManagerURL string // YARN ResourceManager URL
	TimelineServerURL  string // YARN Timeline Server URL
	User               string // Hadoop user
}

// HadoopExecutor submits and manages Hadoop MapReduce jobs via YARN REST API.
type HadoopExecutor struct {
	config HadoopConfig
	client *http.Client
}

// NewHadoopExecutor creates a new Hadoop MapReduce executor.
func NewHadoopExecutor(cfg HadoopConfig) *HadoopExecutor {
	if cfg.ResourceManagerURL == "" {
		cfg.ResourceManagerURL = "http://localhost:8088"
	}
	if cfg.User == "" {
		cfg.User = "sparkflow"
	}
	return &HadoopExecutor{
		config: cfg,
		client: &http.Client{Timeout: 60 * time.Second},
	}
}

func (e *HadoopExecutor) Type() string { return "hadoop" }

func (e *HadoopExecutor) Validate(task *dag.Task) error {
	if _, ok := task.Config["jar"]; !ok {
		return fmt.Errorf("hadoop executor requires 'jar' in config")
	}
	if _, ok := task.Config["class"]; !ok {
		return fmt.Errorf("hadoop executor requires 'class' in config")
	}
	return nil
}

// yarnNewApplicationResponse is the JSON response from creating a YARN application.
type yarnNewApplicationResponse struct {
	ApplicationID string `json:"application-id"`
}

// yarnSubmitRequest is the JSON payload for submitting a YARN application.
type yarnSubmitRequest struct {
	ApplicationID   string `json:"application-id"`
	ApplicationName string `json:"application-name"`
	ApplicationType string `json:"application-type"`
	AMContainerSpec struct {
		Commands struct {
			Command string `json:"command"`
		} `json:"commands"`
	} `json:"am-container-spec"`
	Resource struct {
		Memory int `json:"memory"`
		VCores int `json:"vCores"`
	} `json:"resource"`
}

// yarnApplicationStatus is the JSON response from checking application status.
type yarnApplicationStatus struct {
	App struct {
		ID            string  `json:"id"`
		State         string  `json:"state"`
		FinalStatus   string  `json:"finalStatus"`
		Progress      float64 `json:"progress"`
		TrackingURL   string  `json:"trackingUrl"`
		StartedTime   int64   `json:"startedTime"`
		FinishedTime  int64   `json:"finishedTime"`
		ElapsedTime   int64   `json:"elapsedTime"`
		Diagnostics   string  `json:"diagnostics"`
	} `json:"app"`
}

func (e *HadoopExecutor) Execute(ctx context.Context, task *dag.Task, params map[string]any) (*Result, error) {
	if err := e.Validate(task); err != nil {
		return nil, err
	}

	result := &Result{
		Status:    dag.StatusRunning,
		StartTime: time.Now(),
		Metrics:   make(map[string]float64),
	}

	execCtx := ctx
	if task.Timeout > 0 {
		var cancel context.CancelFunc
		execCtx, cancel = context.WithTimeout(ctx, task.Timeout)
		defer cancel()
	}

	// Step 1: Create new YARN application.
	appID, err := e.createApplication(execCtx)
	if err != nil {
		result.Status = dag.StatusFailed
		result.Error = fmt.Errorf("create YARN application: %w", err)
		result.EndTime = time.Now()
		return result, nil
	}

	result.Metrics["yarn_app_id_created"] = 1

	// Step 2: Submit the application.
	if err := e.submitApplication(execCtx, appID, task, params); err != nil {
		result.Status = dag.StatusFailed
		result.Error = fmt.Errorf("submit YARN application: %w", err)
		result.EndTime = time.Now()
		return result, nil
	}

	// Step 3: Poll for completion.
	pollInterval := 5 * time.Second
	if interval, ok := task.Config["poll_interval"]; ok {
		if intervalStr, ok := interval.(string); ok {
			if d, err := time.ParseDuration(intervalStr); err == nil {
				pollInterval = d
			}
		}
	}

	for {
		select {
		case <-execCtx.Done():
			result.Status = dag.StatusFailed
			result.Error = execCtx.Err()
			result.EndTime = time.Now()
			return result, nil
		default:
		}

		status, err := e.getApplicationStatus(execCtx, appID)
		if err != nil {
			result.Status = dag.StatusFailed
			result.Error = fmt.Errorf("check YARN application status: %w", err)
			result.EndTime = time.Now()
			return result, nil
		}

		result.Metrics["progress"] = status.App.Progress

		switch status.App.State {
		case "FINISHED":
			result.EndTime = time.Now()
			result.Metrics["duration_ms"] = float64(result.EndTime.Sub(result.StartTime).Milliseconds())
			result.Metrics["elapsed_time_ms"] = float64(status.App.ElapsedTime)

			if status.App.FinalStatus == "SUCCEEDED" {
				result.Status = dag.StatusSuccess
				result.Output = []byte(fmt.Sprintf("Application %s completed successfully\nTracking URL: %s",
					appID, status.App.TrackingURL))
			} else {
				result.Status = dag.StatusFailed
				result.Error = fmt.Errorf("YARN application %s ended with status: %s\nDiagnostics: %s",
					appID, status.App.FinalStatus, status.App.Diagnostics)
				result.Output = []byte(status.App.Diagnostics)
			}
			return result, nil

		case "KILLED":
			result.Status = dag.StatusCancelled
			result.Error = fmt.Errorf("YARN application %s was killed", appID)
			result.EndTime = time.Now()
			return result, nil

		case "FAILED":
			result.Status = dag.StatusFailed
			result.Error = fmt.Errorf("YARN application %s failed: %s", appID, status.App.Diagnostics)
			result.Output = []byte(status.App.Diagnostics)
			result.EndTime = time.Now()
			return result, nil
		}

		time.Sleep(pollInterval)
	}
}

func (e *HadoopExecutor) createApplication(ctx context.Context) (string, error) {
	url := fmt.Sprintf("%s/ws/v1/cluster/apps/new-application", e.config.ResourceManagerURL)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return "", err
	}

	resp, err := e.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("YARN returned status %d: %s", resp.StatusCode, string(body))
	}

	var result yarnNewApplicationResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("decode YARN response: %w", err)
	}

	return result.ApplicationID, nil
}

func (e *HadoopExecutor) submitApplication(ctx context.Context, appID string, task *dag.Task, params map[string]any) error {
	jar := task.Config["jar"].(string)
	class := task.Config["class"].(string)

	// Build the command.
	var cmdParts []string
	cmdParts = append(cmdParts, "$HADOOP_HOME/bin/hadoop", "jar", jar, class)

	// Add arguments.
	if args, ok := task.Config["args"]; ok {
		if argList, ok := args.([]any); ok {
			for _, a := range argList {
				arg := fmt.Sprintf("%v", a)
				for k, v := range params {
					arg = strings.ReplaceAll(arg, fmt.Sprintf("{{%s}}", k), fmt.Sprintf("%v", v))
				}
				cmdParts = append(cmdParts, arg)
			}
		}
	}

	command := strings.Join(cmdParts, " ")

	appName := task.Name
	if appName == "" {
		appName = fmt.Sprintf("sparkflow-%s", task.ID)
	}

	memory := 1024
	if mem, ok := task.Config["memory_mb"]; ok {
		if memInt, ok := mem.(int); ok {
			memory = memInt
		}
	}

	vcores := 1
	if cores, ok := task.Config["vcores"]; ok {
		if coresInt, ok := cores.(int); ok {
			vcores = coresInt
		}
	}

	submitPayload := map[string]any{
		"application-id":   appID,
		"application-name": appName,
		"application-type": "MAPREDUCE",
		"am-container-spec": map[string]any{
			"commands": map[string]string{
				"command": command,
			},
		},
		"resource": map[string]int{
			"memory": memory,
			"vCores": vcores,
		},
	}

	body, err := json.Marshal(submitPayload)
	if err != nil {
		return fmt.Errorf("marshal submit payload: %w", err)
	}

	url := fmt.Sprintf("%s/ws/v1/cluster/apps", e.config.ResourceManagerURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := e.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("YARN submit returned status %d: %s", resp.StatusCode, string(respBody))
	}

	return nil
}

func (e *HadoopExecutor) getApplicationStatus(ctx context.Context, appID string) (*yarnApplicationStatus, error) {
	url := fmt.Sprintf("%s/ws/v1/cluster/apps/%s", e.config.ResourceManagerURL, appID)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")

	resp, err := e.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("YARN status returned %d: %s", resp.StatusCode, string(body))
	}

	var status yarnApplicationStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, fmt.Errorf("decode YARN status: %w", err)
	}

	return &status, nil
}

func (e *HadoopExecutor) Cancel(ctx context.Context, taskID string) error {
	// Kill the YARN application by putting it into KILLED state.
	// In practice we would store the app ID mapping, but for now accept the app ID directly.
	url := fmt.Sprintf("%s/ws/v1/cluster/apps/%s/state", e.config.ResourceManagerURL, taskID)

	body, _ := json.Marshal(map[string]string{"state": "KILLED"})
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := e.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("YARN kill returned status %d: %s", resp.StatusCode, string(respBody))
	}

	return nil
}
