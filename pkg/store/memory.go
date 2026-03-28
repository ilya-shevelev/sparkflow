package store

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/ilya-shevelev/sparkflow/pkg/dag"
)

// MemoryStore is an in-memory implementation of the Store interface.
// Suitable for development and testing.
type MemoryStore struct {
	mu             sync.RWMutex
	dags           map[string]*dag.DAG
	runs           map[RunID]*DAGRun
	taskInstances  map[TaskInstanceID]*TaskInstance
	idempotencyIdx map[string]TaskInstanceID
}

// NewMemoryStore creates a new in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		dags:           make(map[string]*dag.DAG),
		runs:           make(map[RunID]*DAGRun),
		taskInstances:  make(map[TaskInstanceID]*TaskInstance),
		idempotencyIdx: make(map[string]TaskInstanceID),
	}
}

func (s *MemoryStore) SaveDAG(_ context.Context, d *dag.DAG) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	d.UpdatedAt = time.Now()
	if d.CreatedAt.IsZero() {
		d.CreatedAt = d.UpdatedAt
	}

	// Deep copy to avoid external mutation.
	cp := *d
	cp.Tasks = make(map[string]*dag.Task, len(d.Tasks))
	for k, v := range d.Tasks {
		t := *v
		cp.Tasks[k] = &t
	}
	cp.Edges = make(map[string][]string, len(d.Edges))
	for k, v := range d.Edges {
		edges := make([]string, len(v))
		copy(edges, v)
		cp.Edges[k] = edges
	}

	s.dags[d.ID] = &cp
	return nil
}

func (s *MemoryStore) GetDAG(_ context.Context, dagID string) (*dag.DAG, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	d, ok := s.dags[dagID]
	if !ok {
		return nil, fmt.Errorf("DAG %q not found", dagID)
	}

	cp := *d
	return &cp, nil
}

func (s *MemoryStore) ListDAGs(_ context.Context, filter DAGFilter) ([]*dag.DAG, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*dag.DAG
	for _, d := range s.dags {
		// Apply tag filter.
		if len(filter.Tags) > 0 {
			matched := false
			for _, ft := range filter.Tags {
				for _, dt := range d.Config.Tags {
					if ft == dt {
						matched = true
						break
					}
				}
				if matched {
					break
				}
			}
			if !matched {
				continue
			}
		}

		// Apply owner filter.
		if filter.Owner != "" && d.Config.Owner != filter.Owner {
			continue
		}

		cp := *d
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ID < result[j].ID
	})

	if filter.Limit > 0 && len(result) > filter.Limit {
		result = result[:filter.Limit]
	}

	return result, nil
}

func (s *MemoryStore) DeleteDAG(_ context.Context, dagID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.dags[dagID]; !ok {
		return fmt.Errorf("DAG %q not found", dagID)
	}

	delete(s.dags, dagID)
	return nil
}

func (s *MemoryStore) CreateRun(_ context.Context, run *DAGRun) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.runs[run.ID]; ok {
		return fmt.Errorf("run %q already exists", run.ID)
	}

	now := time.Now()
	run.CreatedAt = now
	run.UpdatedAt = now

	cp := *run
	s.runs[run.ID] = &cp
	return nil
}

func (s *MemoryStore) GetRun(_ context.Context, runID RunID) (*DAGRun, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	run, ok := s.runs[runID]
	if !ok {
		return nil, fmt.Errorf("run %q not found", runID)
	}

	cp := *run
	return &cp, nil
}

func (s *MemoryStore) UpdateRun(_ context.Context, run *DAGRun) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.runs[run.ID]; !ok {
		return fmt.Errorf("run %q not found", run.ID)
	}

	run.UpdatedAt = time.Now()
	cp := *run
	s.runs[run.ID] = &cp
	return nil
}

func (s *MemoryStore) ListRuns(_ context.Context, filter RunFilter) ([]*DAGRun, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*DAGRun
	for _, run := range s.runs {
		if filter.DAGID != "" && run.DAGID != filter.DAGID {
			continue
		}
		if filter.Status != nil && run.Status != *filter.Status {
			continue
		}
		cp := *run
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].CreatedAt.After(result[j].CreatedAt)
	})

	if filter.Offset > 0 && filter.Offset < len(result) {
		result = result[filter.Offset:]
	}
	if filter.Limit > 0 && len(result) > filter.Limit {
		result = result[:filter.Limit]
	}

	return result, nil
}

func (s *MemoryStore) CreateTaskInstance(_ context.Context, ti *TaskInstance) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.taskInstances[ti.ID]; ok {
		return fmt.Errorf("task instance %q already exists", ti.ID)
	}

	cp := *ti
	s.taskInstances[ti.ID] = &cp

	if ti.IdempotencyKey != "" {
		s.idempotencyIdx[ti.IdempotencyKey] = ti.ID
	}

	return nil
}

func (s *MemoryStore) GetTaskInstance(_ context.Context, id TaskInstanceID) (*TaskInstance, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ti, ok := s.taskInstances[id]
	if !ok {
		return nil, fmt.Errorf("task instance %q not found", id)
	}

	cp := *ti
	return &cp, nil
}

func (s *MemoryStore) UpdateTaskInstance(_ context.Context, ti *TaskInstance) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.taskInstances[ti.ID]; !ok {
		return fmt.Errorf("task instance %q not found", ti.ID)
	}

	cp := *ti
	s.taskInstances[ti.ID] = &cp
	return nil
}

func (s *MemoryStore) ListTaskInstances(_ context.Context, runID RunID) ([]*TaskInstance, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*TaskInstance
	for _, ti := range s.taskInstances {
		if ti.RunID == runID {
			cp := *ti
			result = append(result, &cp)
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].TaskID < result[j].TaskID
	})

	return result, nil
}

func (s *MemoryStore) GetTaskInstanceByIdempotencyKey(_ context.Context, key string) (*TaskInstance, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tiID, ok := s.idempotencyIdx[key]
	if !ok {
		return nil, fmt.Errorf("no task instance with idempotency key %q", key)
	}

	ti, ok := s.taskInstances[tiID]
	if !ok {
		return nil, fmt.Errorf("task instance %q not found (index inconsistency)", tiID)
	}

	cp := *ti
	return &cp, nil
}

func (s *MemoryStore) Ping(_ context.Context) error {
	return nil
}

func (s *MemoryStore) Close() error {
	return nil
}
