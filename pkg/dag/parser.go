package dag

import (
	"fmt"
	"io"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// yamlDAG is the intermediate YAML representation of a DAG.
type yamlDAG struct {
	ID          string            `yaml:"id"`
	Name        string            `yaml:"name"`
	Description string            `yaml:"description"`
	Schedule    *yamlSchedule     `yaml:"schedule"`
	Config      *yamlConfig       `yaml:"config"`
	Tasks       []yamlTask        `yaml:"tasks"`
	Version     int               `yaml:"version"`
}

type yamlSchedule struct {
	Expression string `yaml:"expression"`
	Timezone   string `yaml:"timezone"`
	Catchup    bool   `yaml:"catchup"`
}

type yamlConfig struct {
	MaxConcurrency int               `yaml:"max_concurrency"`
	Timeout        string            `yaml:"timeout"`
	DefaultRetry   *yamlRetry        `yaml:"default_retry"`
	Params         map[string]string `yaml:"params"`
	Tags           []string          `yaml:"tags"`
	Owner          string            `yaml:"owner"`
	SLADuration    string            `yaml:"sla_duration"`
}

type yamlRetry struct {
	MaxRetries     int      `yaml:"max_retries"`
	InitialBackoff string   `yaml:"initial_backoff"`
	MaxBackoff     string   `yaml:"max_backoff"`
	BackoffFactor  float64  `yaml:"backoff_factor"`
	RetryOn        []string `yaml:"retry_on"`
}

type yamlTask struct {
	ID           string            `yaml:"id"`
	Name         string            `yaml:"name"`
	Executor     string            `yaml:"executor"`
	Config       map[string]any    `yaml:"config"`
	RetryPolicy  *yamlRetry        `yaml:"retry_policy"`
	Timeout      string            `yaml:"timeout"`
	Dependencies []string          `yaml:"dependencies"`
	Labels       map[string]string `yaml:"labels"`
	Priority     int               `yaml:"priority"`
}

// ParseFile reads and parses a DAG definition from a YAML file.
func ParseFile(path string) (*DAG, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open DAG file %s: %w", path, err)
	}
	defer f.Close()
	return Parse(f)
}

// Parse reads a DAG definition from a YAML reader.
func Parse(r io.Reader) (*DAG, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("read DAG: %w", err)
	}
	return ParseBytes(data)
}

// ParseBytes parses a DAG definition from YAML bytes.
func ParseBytes(data []byte) (*DAG, error) {
	var raw yamlDAG
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("unmarshal DAG YAML: %w", err)
	}
	return buildDAG(&raw)
}

func buildDAG(raw *yamlDAG) (*DAG, error) {
	d := NewDAG(raw.ID, raw.Name)
	d.Description = raw.Description
	d.Version = raw.Version
	if d.Version == 0 {
		d.Version = 1
	}

	// Parse schedule.
	if raw.Schedule != nil {
		d.Schedule = &CronSchedule{
			Expression: raw.Schedule.Expression,
			Timezone:   raw.Schedule.Timezone,
			Catchup:    raw.Schedule.Catchup,
		}
	}

	// Parse config.
	if raw.Config != nil {
		d.Config.MaxConcurrency = raw.Config.MaxConcurrency
		if d.Config.MaxConcurrency == 0 {
			d.Config.MaxConcurrency = 16
		}
		d.Config.Params = raw.Config.Params
		d.Config.Tags = raw.Config.Tags
		d.Config.Owner = raw.Config.Owner

		if raw.Config.Timeout != "" {
			t, err := time.ParseDuration(raw.Config.Timeout)
			if err != nil {
				return nil, fmt.Errorf("parse DAG timeout %q: %w", raw.Config.Timeout, err)
			}
			d.Config.Timeout = t
		}

		if raw.Config.SLADuration != "" {
			sla, err := time.ParseDuration(raw.Config.SLADuration)
			if err != nil {
				return nil, fmt.Errorf("parse SLA duration %q: %w", raw.Config.SLADuration, err)
			}
			d.Config.SLADuration = sla
		}

		if raw.Config.DefaultRetry != nil {
			rp, err := parseRetryPolicy(raw.Config.DefaultRetry)
			if err != nil {
				return nil, fmt.Errorf("parse default retry policy: %w", err)
			}
			d.Config.DefaultRetry = rp
		}
	}

	// Parse tasks.
	for _, rt := range raw.Tasks {
		task, err := buildTask(&rt, d.Config.DefaultRetry)
		if err != nil {
			return nil, fmt.Errorf("parse task %q: %w", rt.ID, err)
		}
		if err := d.AddTask(task); err != nil {
			return nil, err
		}
	}

	// Build edges from dependencies.
	if err := d.BuildEdgesFromDependencies(); err != nil {
		return nil, err
	}

	// Validate the constructed DAG.
	if err := d.Validate(); err != nil {
		return nil, err
	}

	return d, nil
}

func buildTask(raw *yamlTask, defaultRetry *RetryPolicy) (*Task, error) {
	task := &Task{
		ID:           raw.ID,
		Name:         raw.Name,
		Executor:     raw.Executor,
		Config:       raw.Config,
		Dependencies: raw.Dependencies,
		Labels:       raw.Labels,
		Priority:     raw.Priority,
	}

	if raw.Timeout != "" {
		t, err := time.ParseDuration(raw.Timeout)
		if err != nil {
			return nil, fmt.Errorf("parse timeout %q: %w", raw.Timeout, err)
		}
		task.Timeout = t
	}

	if raw.RetryPolicy != nil {
		rp, err := parseRetryPolicy(raw.RetryPolicy)
		if err != nil {
			return nil, err
		}
		task.RetryPolicy = rp
	} else if defaultRetry != nil {
		// Inherit DAG-level default retry policy.
		cp := *defaultRetry
		task.RetryPolicy = &cp
	}

	return task, nil
}

func parseRetryPolicy(raw *yamlRetry) (*RetryPolicy, error) {
	rp := &RetryPolicy{
		MaxRetries:    raw.MaxRetries,
		BackoffFactor: raw.BackoffFactor,
		RetryOn:       raw.RetryOn,
	}

	if rp.BackoffFactor == 0 {
		rp.BackoffFactor = 2.0
	}

	if raw.InitialBackoff != "" {
		d, err := time.ParseDuration(raw.InitialBackoff)
		if err != nil {
			return nil, fmt.Errorf("parse initial_backoff %q: %w", raw.InitialBackoff, err)
		}
		rp.InitialBackoff = d
	} else {
		rp.InitialBackoff = 10 * time.Second
	}

	if raw.MaxBackoff != "" {
		d, err := time.ParseDuration(raw.MaxBackoff)
		if err != nil {
			return nil, fmt.Errorf("parse max_backoff %q: %w", raw.MaxBackoff, err)
		}
		rp.MaxBackoff = d
	} else {
		rp.MaxBackoff = 5 * time.Minute
	}

	return rp, nil
}
