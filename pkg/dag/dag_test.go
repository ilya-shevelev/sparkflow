package dag

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTopologicalSort_Linear(t *testing.T) {
	d := NewDAG("test", "Test DAG")
	_ = d.AddTask(&Task{ID: "a", Executor: "shell"})
	_ = d.AddTask(&Task{ID: "b", Executor: "shell", Dependencies: []string{"a"}})
	_ = d.AddTask(&Task{ID: "c", Executor: "shell", Dependencies: []string{"b"}})
	_ = d.BuildEdgesFromDependencies()

	sorted, err := d.TopologicalSort()
	require.NoError(t, err)
	require.Len(t, sorted, 3)
	assert.Equal(t, "a", sorted[0].ID)
	assert.Equal(t, "b", sorted[1].ID)
	assert.Equal(t, "c", sorted[2].ID)
}

func TestTopologicalSort_Diamond(t *testing.T) {
	d := NewDAG("diamond", "Diamond DAG")
	_ = d.AddTask(&Task{ID: "a", Executor: "shell"})
	_ = d.AddTask(&Task{ID: "b", Executor: "shell", Dependencies: []string{"a"}})
	_ = d.AddTask(&Task{ID: "c", Executor: "shell", Dependencies: []string{"a"}})
	_ = d.AddTask(&Task{ID: "d", Executor: "shell", Dependencies: []string{"b", "c"}})
	_ = d.BuildEdgesFromDependencies()

	sorted, err := d.TopologicalSort()
	require.NoError(t, err)
	require.Len(t, sorted, 4)
	assert.Equal(t, "a", sorted[0].ID)
	// b and c can be in either order but both before d
	assert.Equal(t, "d", sorted[3].ID)
}

func TestTopologicalSort_Parallel(t *testing.T) {
	d := NewDAG("parallel", "Parallel DAG")
	_ = d.AddTask(&Task{ID: "a", Executor: "shell"})
	_ = d.AddTask(&Task{ID: "b", Executor: "shell"})
	_ = d.AddTask(&Task{ID: "c", Executor: "shell"})
	_ = d.BuildEdgesFromDependencies()

	sorted, err := d.TopologicalSort()
	require.NoError(t, err)
	require.Len(t, sorted, 3)
}

func TestDetectCycle(t *testing.T) {
	tests := []struct {
		name     string
		tasks    []*Task
		edges    map[string][]string
		hasCycle bool
	}{
		{
			name: "no cycle linear",
			tasks: []*Task{
				{ID: "a", Executor: "shell"},
				{ID: "b", Executor: "shell"},
				{ID: "c", Executor: "shell"},
			},
			edges:    map[string][]string{"a": {"b"}, "b": {"c"}},
			hasCycle: false,
		},
		{
			name: "simple cycle",
			tasks: []*Task{
				{ID: "a", Executor: "shell"},
				{ID: "b", Executor: "shell"},
			},
			edges:    map[string][]string{"a": {"b"}, "b": {"a"}},
			hasCycle: true,
		},
		{
			name: "three node cycle",
			tasks: []*Task{
				{ID: "a", Executor: "shell"},
				{ID: "b", Executor: "shell"},
				{ID: "c", Executor: "shell"},
			},
			edges:    map[string][]string{"a": {"b"}, "b": {"c"}, "c": {"a"}},
			hasCycle: true,
		},
		{
			name: "diamond no cycle",
			tasks: []*Task{
				{ID: "a", Executor: "shell"},
				{ID: "b", Executor: "shell"},
				{ID: "c", Executor: "shell"},
				{ID: "d", Executor: "shell"},
			},
			edges:    map[string][]string{"a": {"b", "c"}, "b": {"d"}, "c": {"d"}},
			hasCycle: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := NewDAG("test", "Test")
			for _, task := range tc.tasks {
				_ = d.AddTask(task)
			}
			d.Edges = tc.edges

			cycle := d.DetectCycle()
			if tc.hasCycle {
				assert.NotEmpty(t, cycle, "expected a cycle to be detected")
			} else {
				assert.Empty(t, cycle, "expected no cycle")
			}
		})
	}
}

func TestGetRootsAndLeaves(t *testing.T) {
	d := NewDAG("test", "Test")
	_ = d.AddTask(&Task{ID: "root1", Executor: "shell"})
	_ = d.AddTask(&Task{ID: "root2", Executor: "shell"})
	_ = d.AddTask(&Task{ID: "middle", Executor: "shell", Dependencies: []string{"root1", "root2"}})
	_ = d.AddTask(&Task{ID: "leaf", Executor: "shell", Dependencies: []string{"middle"}})
	_ = d.BuildEdgesFromDependencies()

	roots := d.GetRoots()
	require.Len(t, roots, 2)
	assert.Equal(t, "root1", roots[0].ID)
	assert.Equal(t, "root2", roots[1].ID)

	leaves := d.GetLeaves()
	require.Len(t, leaves, 1)
	assert.Equal(t, "leaf", leaves[0].ID)
}

func TestReadyTasks(t *testing.T) {
	d := NewDAG("test", "Test")
	_ = d.AddTask(&Task{ID: "a", Executor: "shell"})
	_ = d.AddTask(&Task{ID: "b", Executor: "shell", Dependencies: []string{"a"}})
	_ = d.AddTask(&Task{ID: "c", Executor: "shell", Dependencies: []string{"a"}})
	_ = d.AddTask(&Task{ID: "d", Executor: "shell", Dependencies: []string{"b", "c"}})
	_ = d.BuildEdgesFromDependencies()

	tests := []struct {
		name     string
		statuses map[string]Status
		expected []string
	}{
		{
			name:     "initial state",
			statuses: map[string]Status{"a": StatusPending, "b": StatusPending, "c": StatusPending, "d": StatusPending},
			expected: []string{"a"},
		},
		{
			name:     "after a completes",
			statuses: map[string]Status{"a": StatusSuccess, "b": StatusPending, "c": StatusPending, "d": StatusPending},
			expected: []string{"b", "c"},
		},
		{
			name:     "after b completes",
			statuses: map[string]Status{"a": StatusSuccess, "b": StatusSuccess, "c": StatusPending, "d": StatusPending},
			expected: []string{"c"},
		},
		{
			name:     "after b and c complete",
			statuses: map[string]Status{"a": StatusSuccess, "b": StatusSuccess, "c": StatusSuccess, "d": StatusPending},
			expected: []string{"d"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ready := d.ReadyTasks(tc.statuses)
			ids := make([]string, len(ready))
			for i, task := range ready {
				ids[i] = task.ID
			}
			assert.Equal(t, tc.expected, ids)
		})
	}
}

func TestParseYAML_Simple(t *testing.T) {
	yaml := `
id: test-dag
name: Test DAG
description: A test workflow
schedule:
  expression: "0 * * * *"
  timezone: UTC
config:
  max_concurrency: 4
  timeout: "1h"
  owner: test-user
  tags:
    - test
    - ci
tasks:
  - id: extract
    name: Extract Data
    executor: shell
    config:
      command: "echo hello"
    timeout: "5m"
  - id: transform
    name: Transform Data
    executor: python
    config:
      script: "transform.py"
    dependencies:
      - extract
  - id: load
    name: Load Data
    executor: shell
    config:
      command: "load.sh"
    dependencies:
      - transform
`
	d, err := Parse(strings.NewReader(yaml))
	require.NoError(t, err)
	assert.Equal(t, "test-dag", d.ID)
	assert.Equal(t, "Test DAG", d.Name)
	assert.Len(t, d.Tasks, 3)
	assert.Equal(t, 4, d.Config.MaxConcurrency)
	assert.Equal(t, time.Hour, d.Config.Timeout)
	assert.Equal(t, "0 * * * *", d.Schedule.Expression)

	sorted, err := d.TopologicalSort()
	require.NoError(t, err)
	assert.Equal(t, "extract", sorted[0].ID)
	assert.Equal(t, "transform", sorted[1].ID)
	assert.Equal(t, "load", sorted[2].ID)
}

func TestParseYAML_WithRetryPolicy(t *testing.T) {
	yaml := `
id: retry-dag
name: Retry Test
config:
  default_retry:
    max_retries: 5
    initial_backoff: "30s"
    max_backoff: "10m"
    backoff_factor: 3.0
tasks:
  - id: flaky-task
    name: Flaky Task
    executor: shell
    config:
      command: "might-fail.sh"
  - id: critical-task
    name: Critical Task
    executor: shell
    config:
      command: "critical.sh"
    retry_policy:
      max_retries: 10
      initial_backoff: "5s"
      max_backoff: "2m"
      backoff_factor: 2.0
`
	d, err := Parse(strings.NewReader(yaml))
	require.NoError(t, err)

	// flaky-task should inherit default retry.
	flaky := d.Tasks["flaky-task"]
	require.NotNil(t, flaky.RetryPolicy)
	assert.Equal(t, 5, flaky.RetryPolicy.MaxRetries)
	assert.Equal(t, 30*time.Second, flaky.RetryPolicy.InitialBackoff)

	// critical-task should have its own retry.
	critical := d.Tasks["critical-task"]
	require.NotNil(t, critical.RetryPolicy)
	assert.Equal(t, 10, critical.RetryPolicy.MaxRetries)
	assert.Equal(t, 5*time.Second, critical.RetryPolicy.InitialBackoff)
}

func TestParseYAML_CycleDetected(t *testing.T) {
	yaml := `
id: cycle-dag
name: Cycle DAG
tasks:
  - id: a
    executor: shell
    config:
      command: "echo a"
    dependencies:
      - c
  - id: b
    executor: shell
    config:
      command: "echo b"
    dependencies:
      - a
  - id: c
    executor: shell
    config:
      command: "echo c"
    dependencies:
      - b
`
	_, err := Parse(strings.NewReader(yaml))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cycle")
}

func TestParseYAML_MissingDependency(t *testing.T) {
	yaml := `
id: missing-dep
name: Missing Dep
tasks:
  - id: a
    executor: shell
    config:
      command: "echo a"
    dependencies:
      - nonexistent
`
	_, err := Parse(strings.NewReader(yaml))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nonexistent")
}

func TestValidate_EmptyID(t *testing.T) {
	d := &DAG{Tasks: map[string]*Task{"a": {ID: "a", Executor: "shell"}}}
	err := d.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ID must not be empty")
}

func TestValidate_NoTasks(t *testing.T) {
	d := &DAG{ID: "test", Tasks: map[string]*Task{}}
	err := d.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "at least one task")
}

func TestGetDownstream(t *testing.T) {
	d := NewDAG("test", "Test")
	_ = d.AddTask(&Task{ID: "a", Executor: "shell"})
	_ = d.AddTask(&Task{ID: "b", Executor: "shell", Dependencies: []string{"a"}})
	_ = d.AddTask(&Task{ID: "c", Executor: "shell", Dependencies: []string{"b"}})
	_ = d.AddTask(&Task{ID: "d", Executor: "shell", Dependencies: []string{"b"}})
	_ = d.BuildEdgesFromDependencies()

	downstream := d.GetDownstream("a")
	ids := make([]string, len(downstream))
	for i, task := range downstream {
		ids[i] = task.ID
	}
	assert.Equal(t, []string{"b", "c", "d"}, ids)
}

func TestGetUpstream(t *testing.T) {
	d := NewDAG("test", "Test")
	_ = d.AddTask(&Task{ID: "a", Executor: "shell"})
	_ = d.AddTask(&Task{ID: "b", Executor: "shell", Dependencies: []string{"a"}})
	_ = d.AddTask(&Task{ID: "c", Executor: "shell", Dependencies: []string{"a"}})
	_ = d.AddTask(&Task{ID: "d", Executor: "shell", Dependencies: []string{"b", "c"}})
	_ = d.BuildEdgesFromDependencies()

	upstream := d.GetUpstream("d")
	ids := make([]string, len(upstream))
	for i, task := range upstream {
		ids[i] = task.ID
	}
	assert.Equal(t, []string{"a", "b", "c"}, ids)
}

func TestStatus_String(t *testing.T) {
	tests := []struct {
		status   Status
		expected string
	}{
		{StatusPending, "pending"},
		{StatusRunning, "running"},
		{StatusSuccess, "success"},
		{StatusFailed, "failed"},
		{StatusCancelled, "cancelled"},
	}
	for _, tc := range tests {
		assert.Equal(t, tc.expected, tc.status.String())
	}
}

func TestStatus_IsTerminal(t *testing.T) {
	assert.False(t, StatusPending.IsTerminal())
	assert.False(t, StatusRunning.IsTerminal())
	assert.True(t, StatusSuccess.IsTerminal())
	assert.True(t, StatusFailed.IsTerminal())
	assert.True(t, StatusCancelled.IsTerminal())
}
