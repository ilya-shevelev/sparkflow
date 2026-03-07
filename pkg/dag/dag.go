// Package dag provides directed acyclic graph structures for workflow definitions.
// It includes YAML parsing, topological sorting, cycle detection, and dependency resolution.
package dag

import (
	"fmt"
	"sort"
	"time"
)

// Status represents the execution status of a task or DAG run.
type Status int

const (
	StatusPending Status = iota
	StatusQueued
	StatusRunning
	StatusSuccess
	StatusFailed
	StatusCancelled
	StatusSkipped
	StatusUpForRetry
)

func (s Status) String() string {
	switch s {
	case StatusPending:
		return "pending"
	case StatusQueued:
		return "queued"
	case StatusRunning:
		return "running"
	case StatusSuccess:
		return "success"
	case StatusFailed:
		return "failed"
	case StatusCancelled:
		return "cancelled"
	case StatusSkipped:
		return "skipped"
	case StatusUpForRetry:
		return "up_for_retry"
	default:
		return "unknown"
	}
}

// IsTerminal returns true if the status is a final state.
func (s Status) IsTerminal() bool {
	return s == StatusSuccess || s == StatusFailed || s == StatusCancelled || s == StatusSkipped
}

// RetryPolicy defines how a task should be retried on failure.
type RetryPolicy struct {
	MaxRetries     int           `yaml:"max_retries" json:"max_retries"`
	InitialBackoff time.Duration `yaml:"initial_backoff" json:"initial_backoff"`
	MaxBackoff     time.Duration `yaml:"max_backoff" json:"max_backoff"`
	BackoffFactor  float64       `yaml:"backoff_factor" json:"backoff_factor"`
	RetryOn        []string      `yaml:"retry_on" json:"retry_on"`
}

// DefaultRetryPolicy returns a sensible default retry policy.
func DefaultRetryPolicy() *RetryPolicy {
	return &RetryPolicy{
		MaxRetries:     3,
		InitialBackoff: 10 * time.Second,
		MaxBackoff:     5 * time.Minute,
		BackoffFactor:  2.0,
	}
}

// CronSchedule wraps a cron expression with timezone info.
type CronSchedule struct {
	Expression string `yaml:"expression" json:"expression"`
	Timezone   string `yaml:"timezone" json:"timezone"`
	Catchup    bool   `yaml:"catchup" json:"catchup"`
}

// DAGConfig holds configuration for a DAG.
type DAGConfig struct {
	MaxConcurrency int               `yaml:"max_concurrency" json:"max_concurrency"`
	Timeout        time.Duration     `yaml:"timeout" json:"timeout"`
	DefaultRetry   *RetryPolicy      `yaml:"default_retry" json:"default_retry"`
	Params         map[string]string `yaml:"params" json:"params"`
	Tags           []string          `yaml:"tags" json:"tags"`
	Owner          string            `yaml:"owner" json:"owner"`
	SLADuration    time.Duration     `yaml:"sla_duration" json:"sla_duration"`
}

// Task represents a unit of work within a DAG.
type Task struct {
	ID           string         `yaml:"id" json:"id"`
	Name         string         `yaml:"name" json:"name"`
	Executor     string         `yaml:"executor" json:"executor"`
	Config       map[string]any `yaml:"config" json:"config"`
	RetryPolicy  *RetryPolicy   `yaml:"retry_policy" json:"retry_policy"`
	Timeout      time.Duration  `yaml:"timeout" json:"timeout"`
	Dependencies []string       `yaml:"dependencies" json:"dependencies"`
	Labels       map[string]string `yaml:"labels" json:"labels"`
	Priority     int            `yaml:"priority" json:"priority"`
}

// DAG represents a directed acyclic graph of tasks.
type DAG struct {
	ID          string            `yaml:"id" json:"id"`
	Name        string            `yaml:"name" json:"name"`
	Description string            `yaml:"description" json:"description"`
	Tasks       map[string]*Task  `yaml:"tasks" json:"tasks"`
	Edges       map[string][]string `yaml:"-" json:"edges"`
	Schedule    *CronSchedule     `yaml:"schedule" json:"schedule"`
	Config      DAGConfig         `yaml:"config" json:"config"`
	Version     int               `yaml:"version" json:"version"`
	CreatedAt   time.Time         `yaml:"-" json:"created_at"`
	UpdatedAt   time.Time         `yaml:"-" json:"updated_at"`
}

// NewDAG creates a new empty DAG with the given ID and name.
func NewDAG(id, name string) *DAG {
	now := time.Now()
	return &DAG{
		ID:        id,
		Name:      name,
		Tasks:     make(map[string]*Task),
		Edges:     make(map[string][]string),
		Config:    DAGConfig{MaxConcurrency: 16},
		Version:   1,
		CreatedAt: now,
		UpdatedAt: now,
	}
}

// AddTask adds a task to the DAG.
func (d *DAG) AddTask(task *Task) error {
	if task.ID == "" {
		return fmt.Errorf("task ID must not be empty")
	}
	if _, exists := d.Tasks[task.ID]; exists {
		return fmt.Errorf("task %q already exists in DAG %q", task.ID, d.ID)
	}
	d.Tasks[task.ID] = task
	return nil
}

// AddEdge adds a dependency edge from upstream to downstream task.
func (d *DAG) AddEdge(from, to string) error {
	if _, ok := d.Tasks[from]; !ok {
		return fmt.Errorf("upstream task %q not found in DAG", from)
	}
	if _, ok := d.Tasks[to]; !ok {
		return fmt.Errorf("downstream task %q not found in DAG", to)
	}
	d.Edges[from] = append(d.Edges[from], to)
	return nil
}

// BuildEdgesFromDependencies constructs the Edges map from each task's Dependencies field.
func (d *DAG) BuildEdgesFromDependencies() error {
	d.Edges = make(map[string][]string)
	for _, task := range d.Tasks {
		for _, dep := range task.Dependencies {
			if _, ok := d.Tasks[dep]; !ok {
				return fmt.Errorf("task %q depends on unknown task %q", task.ID, dep)
			}
			d.Edges[dep] = append(d.Edges[dep], task.ID)
		}
	}
	return nil
}

// Validate checks the DAG for structural correctness.
func (d *DAG) Validate() error {
	if d.ID == "" {
		return fmt.Errorf("DAG ID must not be empty")
	}
	if len(d.Tasks) == 0 {
		return fmt.Errorf("DAG %q must have at least one task", d.ID)
	}

	// Check that all edge targets exist.
	for from, tos := range d.Edges {
		if _, ok := d.Tasks[from]; !ok {
			return fmt.Errorf("edge source task %q not found", from)
		}
		for _, to := range tos {
			if _, ok := d.Tasks[to]; !ok {
				return fmt.Errorf("edge target task %q not found (from %q)", to, from)
			}
		}
	}

	// Check for cycles.
	if cycle := d.DetectCycle(); len(cycle) > 0 {
		return fmt.Errorf("DAG %q contains a cycle: %v", d.ID, cycle)
	}

	// Validate individual tasks.
	for _, task := range d.Tasks {
		if task.Executor == "" {
			return fmt.Errorf("task %q must specify an executor", task.ID)
		}
	}

	return nil
}

// TopologicalSort returns tasks in topological order using Kahn's algorithm.
// Returns an error if the graph contains a cycle.
func (d *DAG) TopologicalSort() ([]*Task, error) {
	inDegree := make(map[string]int)
	for id := range d.Tasks {
		inDegree[id] = 0
	}

	for _, targets := range d.Edges {
		for _, t := range targets {
			inDegree[t]++
		}
	}

	var queue []string
	for id, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, id)
		}
	}
	sort.Strings(queue)

	var ordered []*Task
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		ordered = append(ordered, d.Tasks[current])

		targets := d.Edges[current]
		sort.Strings(targets)
		for _, next := range targets {
			inDegree[next]--
			if inDegree[next] == 0 {
				queue = append(queue, next)
			}
		}
	}

	if len(ordered) != len(d.Tasks) {
		return nil, fmt.Errorf("DAG contains a cycle: topological sort found %d of %d tasks", len(ordered), len(d.Tasks))
	}

	return ordered, nil
}

// DetectCycle returns the cycle path if a cycle exists, empty slice otherwise.
// Uses DFS-based cycle detection with coloring.
func (d *DAG) DetectCycle() []string {
	const (
		white = 0 // unvisited
		gray  = 1 // in current path
		black = 2 // fully processed
	)

	color := make(map[string]int)
	parent := make(map[string]string)

	for id := range d.Tasks {
		color[id] = white
	}

	var cyclePath []string

	var dfs func(node string) bool
	dfs = func(node string) bool {
		color[node] = gray

		for _, next := range d.Edges[node] {
			if color[next] == gray {
				// Found a cycle. Reconstruct path.
				cyclePath = []string{next, node}
				curr := node
				for curr != next {
					curr = parent[curr]
					if curr == "" {
						break
					}
					cyclePath = append(cyclePath, curr)
				}
				return true
			}
			if color[next] == white {
				parent[next] = node
				if dfs(next) {
					return true
				}
			}
		}

		color[node] = black
		return false
	}

	// Sort keys for deterministic output.
	ids := make([]string, 0, len(d.Tasks))
	for id := range d.Tasks {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	for _, id := range ids {
		if color[id] == white {
			if dfs(id) {
				return cyclePath
			}
		}
	}

	return nil
}

// GetRoots returns tasks with no dependencies (in-degree 0).
func (d *DAG) GetRoots() []*Task {
	inDegree := make(map[string]int)
	for id := range d.Tasks {
		inDegree[id] = 0
	}
	for _, targets := range d.Edges {
		for _, t := range targets {
			inDegree[t]++
		}
	}

	var roots []*Task
	for id, deg := range inDegree {
		if deg == 0 {
			roots = append(roots, d.Tasks[id])
		}
	}

	sort.Slice(roots, func(i, j int) bool {
		return roots[i].ID < roots[j].ID
	})
	return roots
}

// GetLeaves returns tasks with no downstream dependencies (out-degree 0).
func (d *DAG) GetLeaves() []*Task {
	hasDownstream := make(map[string]bool)
	for from := range d.Edges {
		if len(d.Edges[from]) > 0 {
			hasDownstream[from] = true
		}
	}

	var leaves []*Task
	for id, task := range d.Tasks {
		if !hasDownstream[id] {
			leaves = append(leaves, task)
		}
	}

	sort.Slice(leaves, func(i, j int) bool {
		return leaves[i].ID < leaves[j].ID
	})
	return leaves
}

// GetDownstream returns all tasks downstream of the given task ID.
func (d *DAG) GetDownstream(taskID string) []*Task {
	visited := make(map[string]bool)
	var result []*Task

	var walk func(id string)
	walk = func(id string) {
		for _, next := range d.Edges[id] {
			if !visited[next] {
				visited[next] = true
				result = append(result, d.Tasks[next])
				walk(next)
			}
		}
	}

	walk(taskID)
	sort.Slice(result, func(i, j int) bool {
		return result[i].ID < result[j].ID
	})
	return result
}

// GetUpstream returns all tasks upstream of the given task ID.
func (d *DAG) GetUpstream(taskID string) []*Task {
	// Build reverse edge map.
	reverse := make(map[string][]string)
	for from, tos := range d.Edges {
		for _, to := range tos {
			reverse[to] = append(reverse[to], from)
		}
	}

	visited := make(map[string]bool)
	var result []*Task

	var walk func(id string)
	walk = func(id string) {
		for _, prev := range reverse[id] {
			if !visited[prev] {
				visited[prev] = true
				result = append(result, d.Tasks[prev])
				walk(prev)
			}
		}
	}

	walk(taskID)
	sort.Slice(result, func(i, j int) bool {
		return result[i].ID < result[j].ID
	})
	return result
}

// ReadyTasks returns tasks that are ready to execute based on the given status map.
// A task is ready if all of its upstream dependencies have StatusSuccess.
func (d *DAG) ReadyTasks(statuses map[string]Status) []*Task {
	var ready []*Task
	for _, task := range d.Tasks {
		if statuses[task.ID] != StatusPending {
			continue
		}

		allDepsComplete := true
		for _, dep := range task.Dependencies {
			if statuses[dep] != StatusSuccess {
				allDepsComplete = false
				break
			}
		}

		if allDepsComplete {
			ready = append(ready, task)
		}
	}

	sort.Slice(ready, func(i, j int) bool {
		if ready[i].Priority != ready[j].Priority {
			return ready[i].Priority > ready[j].Priority
		}
		return ready[i].ID < ready[j].ID
	})
	return ready
}
