// Package hadoop provides HDFS and YARN client implementations
// using the WebHDFS and YARN Resource Manager REST APIs.
package hadoop

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// HDFSClient provides WebHDFS REST API operations.
type HDFSClient struct {
	nameNodeURL string
	user        string
	client      *http.Client
}

// NewHDFSClient creates a new HDFS client.
func NewHDFSClient(nameNodeURL, user string) *HDFSClient {
	if nameNodeURL == "" {
		nameNodeURL = "http://localhost:9870"
	}
	if user == "" {
		user = "sparkflow"
	}
	return &HDFSClient{
		nameNodeURL: nameNodeURL,
		user:        user,
		client:      &http.Client{Timeout: 5 * time.Minute},
	}
}

func (c *HDFSClient) url(path, op string, extra map[string]string) string {
	params := url.Values{}
	params.Set("op", op)
	params.Set("user.name", c.user)
	for k, v := range extra {
		params.Set(k, v)
	}
	return fmt.Sprintf("%s/webhdfs/v1%s?%s", c.nameNodeURL, path, params.Encode())
}

// FileStatus represents HDFS file metadata.
type FileStatus struct {
	Path             string `json:"pathSuffix"`
	Type             string `json:"type"`
	Length           int64  `json:"length"`
	Owner            string `json:"owner"`
	Group            string `json:"group"`
	Permission       string `json:"permission"`
	Replication      int    `json:"replication"`
	BlockSize        int64  `json:"blockSize"`
	ModificationTime int64  `json:"modificationTime"`
	AccessTime       int64  `json:"accessTime"`
}

// ListStatus lists directory contents.
func (c *HDFSClient) ListStatus(ctx context.Context, path string) ([]FileStatus, error) {
	reqURL := c.url(path, "LISTSTATUS", nil)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("list status: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("HDFS LISTSTATUS returned %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		FileStatuses struct {
			FileStatus []FileStatus `json:"FileStatus"`
		} `json:"FileStatuses"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return result.FileStatuses.FileStatus, nil
}

// GetFileStatus returns metadata for a single file or directory.
func (c *HDFSClient) GetFileStatus(ctx context.Context, path string) (*FileStatus, error) {
	reqURL := c.url(path, "GETFILESTATUS", nil)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("get file status: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("HDFS GETFILESTATUS returned %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		FileStatus FileStatus `json:"FileStatus"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return &result.FileStatus, nil
}

// MkDirs creates a directory and all parent directories.
func (c *HDFSClient) MkDirs(ctx context.Context, path string) error {
	reqURL := c.url(path, "MKDIRS", nil)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, reqURL, nil)
	if err != nil {
		return err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("mkdirs: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HDFS MKDIRS returned %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

// CreateFile creates a new file in HDFS.
func (c *HDFSClient) CreateFile(ctx context.Context, path string, data []byte, overwrite bool) error {
	extra := map[string]string{"overwrite": fmt.Sprintf("%t", overwrite)}
	reqURL := c.url(path, "CREATE", extra)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, reqURL, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HDFS CREATE returned %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

// ReadFile reads a file from HDFS.
func (c *HDFSClient) ReadFile(ctx context.Context, path string) ([]byte, error) {
	reqURL := c.url(path, "OPEN", nil)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("read file: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("HDFS OPEN returned %d: %s", resp.StatusCode, string(body))
	}

	return io.ReadAll(resp.Body)
}

// Delete removes a file or directory from HDFS.
func (c *HDFSClient) Delete(ctx context.Context, path string, recursive bool) error {
	extra := map[string]string{"recursive": fmt.Sprintf("%t", recursive)}
	reqURL := c.url(path, "DELETE", extra)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, reqURL, nil)
	if err != nil {
		return err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HDFS DELETE returned %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

// Rename moves/renames a file or directory.
func (c *HDFSClient) Rename(ctx context.Context, src, dst string) error {
	extra := map[string]string{"destination": dst}
	reqURL := c.url(src, "RENAME", extra)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, reqURL, nil)
	if err != nil {
		return err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("rename: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HDFS RENAME returned %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

// YARNClient provides YARN ResourceManager REST API operations.
type YARNClient struct {
	resourceManagerURL string
	client             *http.Client
}

// NewYARNClient creates a new YARN client.
func NewYARNClient(rmURL string) *YARNClient {
	if rmURL == "" {
		rmURL = "http://localhost:8088"
	}
	return &YARNClient{
		resourceManagerURL: rmURL,
		client:             &http.Client{Timeout: 60 * time.Second},
	}
}

// YARNApplication represents a YARN application.
type YARNApplication struct {
	ID            string  `json:"id"`
	Name          string  `json:"name"`
	State         string  `json:"state"`
	FinalStatus   string  `json:"finalStatus"`
	Progress      float64 `json:"progress"`
	TrackingURL   string  `json:"trackingUrl"`
	StartedTime   int64   `json:"startedTime"`
	FinishedTime  int64   `json:"finishedTime"`
	ElapsedTime   int64   `json:"elapsedTime"`
	Diagnostics   string  `json:"diagnostics"`
	Queue         string  `json:"queue"`
	User          string  `json:"user"`
	ApplicationType string `json:"applicationType"`
}

// GetApplication returns information about a YARN application.
func (c *YARNClient) GetApplication(ctx context.Context, appID string) (*YARNApplication, error) {
	url := fmt.Sprintf("%s/ws/v1/cluster/apps/%s", c.resourceManagerURL, appID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("get application: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("YARN returned %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		App YARNApplication `json:"app"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return &result.App, nil
}

// ListApplications lists YARN applications with optional state filter.
func (c *YARNClient) ListApplications(ctx context.Context, states []string) ([]YARNApplication, error) {
	reqURL := fmt.Sprintf("%s/ws/v1/cluster/apps", c.resourceManagerURL)
	if len(states) > 0 {
		params := url.Values{}
		for _, s := range states {
			params.Add("states", s)
		}
		reqURL += "?" + params.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("list applications: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("YARN returned %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Apps struct {
			App []YARNApplication `json:"app"`
		} `json:"apps"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return result.Apps.App, nil
}

// KillApplication terminates a YARN application.
func (c *YARNClient) KillApplication(ctx context.Context, appID string) error {
	reqURL := fmt.Sprintf("%s/ws/v1/cluster/apps/%s/state", c.resourceManagerURL, appID)
	body, _ := json.Marshal(map[string]string{"state": "KILLED"})

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, reqURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("kill application: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("YARN kill returned %d: %s", resp.StatusCode, string(respBody))
	}

	return nil
}

// ClusterInfo represents YARN cluster information.
type ClusterInfo struct {
	ID                  int64  `json:"id"`
	StartedOn           int64  `json:"startedOn"`
	State               string `json:"state"`
	HAState             string `json:"haState"`
	ResourceManagerVersion string `json:"resourceManagerVersion"`
}

// GetClusterInfo returns YARN cluster information.
func (c *YARNClient) GetClusterInfo(ctx context.Context) (*ClusterInfo, error) {
	url := fmt.Sprintf("%s/ws/v1/cluster/info", c.resourceManagerURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("get cluster info: %w", err)
	}
	defer resp.Body.Close()

	var result struct {
		ClusterInfo ClusterInfo `json:"clusterInfo"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return &result.ClusterInfo, nil
}
