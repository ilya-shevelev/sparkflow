package scheduler

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ilya-shevelev/sparkflow/pkg/dag"
	"github.com/ilya-shevelev/sparkflow/pkg/store"
)

func TestFIFOScheduler_Submit(t *testing.T) {
	st := store.NewMemoryStore()
	sch := NewFIFOScheduler(st, FIFOConfig{})

	d := dag.NewDAG("test-dag", "Test DAG")
	_ = d.AddTask(&dag.Task{ID: "task1", Executor: "shell", Config: map[string]any{"command": "echo 1"}})
	_ = d.AddTask(&dag.Task{ID: "task2", Executor: "shell", Config: map[string]any{"command": "echo 2"}, Dependencies: []string{"task1"}})
	_ = d.BuildEdgesFromDependencies()

	ctx := context.Background()
	_ = st.SaveDAG(ctx, d)

	runID, err := sch.Submit(ctx, d, map[string]string{"date": "2024-01-01"})
	require.NoError(t, err)
	assert.NotEmpty(t, runID)

	// Verify run was created.
	run, err := st.GetRun(ctx, runID)
	require.NoError(t, err)
	assert.Equal(t, dag.StatusRunning, run.Status)
	assert.Equal(t, "test-dag", run.DAGID)

	// Verify task instances were created.
	instances, err := st.ListTaskInstances(ctx, runID)
	require.NoError(t, err)
	assert.Len(t, instances, 2)
}

func TestFIFOScheduler_Schedule(t *testing.T) {
	st := store.NewMemoryStore()
	sch := NewFIFOScheduler(st, FIFOConfig{})

	d := dag.NewDAG("test-dag", "Test DAG")
	_ = d.AddTask(&dag.Task{ID: "task1", Executor: "shell", Config: map[string]any{"command": "echo 1"}})
	_ = d.AddTask(&dag.Task{ID: "task2", Executor: "shell", Config: map[string]any{"command": "echo 2"}, Dependencies: []string{"task1"}})
	_ = d.BuildEdgesFromDependencies()

	ctx := context.Background()
	_ = st.SaveDAG(ctx, d)

	runID, err := sch.Submit(ctx, d, nil)
	require.NoError(t, err)

	// First schedule: only task1 should be ready (no dependencies).
	ready, err := sch.Schedule(ctx)
	require.NoError(t, err)
	require.Len(t, ready, 1)
	assert.Equal(t, "task1", ready[0].TaskID)

	// Mark task1 as complete.
	err = sch.ReportCompletion(ctx, "task1", &TaskResult{
		TaskInstanceID: ready[0].ID,
		RunID:          runID,
		Status:         dag.StatusSuccess,
	})
	require.NoError(t, err)

	// Second schedule: task2 should now be ready.
	ready, err = sch.Schedule(ctx)
	require.NoError(t, err)
	require.Len(t, ready, 1)
	assert.Equal(t, "task2", ready[0].TaskID)
}

func TestFIFOScheduler_RunCompletion(t *testing.T) {
	st := store.NewMemoryStore()
	sch := NewFIFOScheduler(st, FIFOConfig{})

	d := dag.NewDAG("test-dag", "Test DAG")
	_ = d.AddTask(&dag.Task{ID: "task1", Executor: "shell", Config: map[string]any{"command": "echo 1"}})
	_ = d.BuildEdgesFromDependencies()

	ctx := context.Background()
	_ = st.SaveDAG(ctx, d)

	runID, _ := sch.Submit(ctx, d, nil)

	// Schedule and complete.
	ready, _ := sch.Schedule(ctx)
	require.Len(t, ready, 1)

	_ = sch.ReportCompletion(ctx, "task1", &TaskResult{
		TaskInstanceID: ready[0].ID,
		RunID:          runID,
		Status:         dag.StatusSuccess,
	})

	// Next schedule should detect run completion.
	_, _ = sch.Schedule(ctx)

	run, err := st.GetRun(ctx, runID)
	require.NoError(t, err)
	assert.Equal(t, dag.StatusSuccess, run.Status)
}

func TestFIFOScheduler_RunFailure(t *testing.T) {
	st := store.NewMemoryStore()
	sch := NewFIFOScheduler(st, FIFOConfig{})

	d := dag.NewDAG("test-dag", "Test DAG")
	_ = d.AddTask(&dag.Task{ID: "task1", Executor: "shell", Config: map[string]any{"command": "echo 1"}})
	_ = d.BuildEdgesFromDependencies()

	ctx := context.Background()
	_ = st.SaveDAG(ctx, d)

	runID, _ := sch.Submit(ctx, d, nil)

	ready, _ := sch.Schedule(ctx)
	require.Len(t, ready, 1)

	_ = sch.ReportCompletion(ctx, "task1", &TaskResult{
		TaskInstanceID: ready[0].ID,
		RunID:          runID,
		Status:         dag.StatusFailed,
		Error:          "command failed",
	})

	_, _ = sch.Schedule(ctx)

	run, err := st.GetRun(ctx, runID)
	require.NoError(t, err)
	assert.Equal(t, dag.StatusFailed, run.Status)
}

func TestPriorityScheduler(t *testing.T) {
	st := store.NewMemoryStore()
	sch := NewPriorityScheduler(st, FIFOConfig{})

	d := dag.NewDAG("test-dag", "Test DAG")
	_ = d.AddTask(&dag.Task{ID: "low", Executor: "shell", Config: map[string]any{"command": "echo 1"}, Priority: 1})
	_ = d.AddTask(&dag.Task{ID: "high", Executor: "shell", Config: map[string]any{"command": "echo 2"}, Priority: 10})
	_ = d.BuildEdgesFromDependencies()

	ctx := context.Background()
	_ = st.SaveDAG(ctx, d)

	_, err := sch.Submit(ctx, d, nil)
	require.NoError(t, err)

	ready, err := sch.Schedule(ctx)
	require.NoError(t, err)
	require.Len(t, ready, 2)
	// Higher priority should come first.
	assert.Equal(t, "high", ready[0].TaskID)
}
