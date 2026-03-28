package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ilya-shevelev/sparkflow/pkg/dag"
)

// PostgresStore is a PostgreSQL implementation of the Store interface.
type PostgresStore struct {
	pool *pgxpool.Pool
}

// PostgresConfig configures the PostgreSQL store.
type PostgresConfig struct {
	DSN             string
	MaxConns        int32
	MinConns        int32
	MaxConnLifetime time.Duration
	MaxConnIdleTime time.Duration
}

// NewPostgresStore creates a new PostgreSQL-backed store.
func NewPostgresStore(ctx context.Context, cfg PostgresConfig) (*PostgresStore, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("parse postgres DSN: %w", err)
	}

	if cfg.MaxConns > 0 {
		poolCfg.MaxConns = cfg.MaxConns
	}
	if cfg.MinConns > 0 {
		poolCfg.MinConns = cfg.MinConns
	}
	if cfg.MaxConnLifetime > 0 {
		poolCfg.MaxConnLifetime = cfg.MaxConnLifetime
	}
	if cfg.MaxConnIdleTime > 0 {
		poolCfg.MaxConnIdleTime = cfg.MaxConnIdleTime
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	return &PostgresStore{pool: pool}, nil
}

// Migrate creates the required database schema.
func (s *PostgresStore) Migrate(ctx context.Context) error {
	schema := `
	CREATE TABLE IF NOT EXISTS dags (
		id          TEXT PRIMARY KEY,
		name        TEXT NOT NULL,
		description TEXT DEFAULT '',
		spec        JSONB NOT NULL,
		version     INTEGER DEFAULT 1,
		created_at  TIMESTAMPTZ DEFAULT NOW(),
		updated_at  TIMESTAMPTZ DEFAULT NOW()
	);

	CREATE TABLE IF NOT EXISTS dag_runs (
		id          TEXT PRIMARY KEY,
		dag_id      TEXT NOT NULL REFERENCES dags(id),
		status      INTEGER NOT NULL DEFAULT 0,
		params      JSONB DEFAULT '{}',
		start_time  TIMESTAMPTZ,
		end_time    TIMESTAMPTZ,
		created_at  TIMESTAMPTZ DEFAULT NOW(),
		updated_at  TIMESTAMPTZ DEFAULT NOW()
	);

	CREATE INDEX IF NOT EXISTS idx_dag_runs_dag_id ON dag_runs(dag_id);
	CREATE INDEX IF NOT EXISTS idx_dag_runs_status ON dag_runs(status);

	CREATE TABLE IF NOT EXISTS task_instances (
		id              TEXT PRIMARY KEY,
		run_id          TEXT NOT NULL REFERENCES dag_runs(id),
		task_id         TEXT NOT NULL,
		dag_id          TEXT NOT NULL,
		status          INTEGER NOT NULL DEFAULT 0,
		attempt         INTEGER DEFAULT 1,
		worker_id       TEXT DEFAULT '',
		output          BYTEA,
		error           TEXT DEFAULT '',
		start_time      TIMESTAMPTZ,
		end_time        TIMESTAMPTZ,
		queued_at       TIMESTAMPTZ DEFAULT NOW(),
		idempotency_key TEXT UNIQUE
	);

	CREATE INDEX IF NOT EXISTS idx_task_instances_run_id ON task_instances(run_id);
	CREATE INDEX IF NOT EXISTS idx_task_instances_status ON task_instances(status);
	CREATE INDEX IF NOT EXISTS idx_task_instances_idempotency ON task_instances(idempotency_key);
	`

	_, err := s.pool.Exec(ctx, schema)
	return err
}

func (s *PostgresStore) SaveDAG(ctx context.Context, d *dag.DAG) error {
	spec, err := json.Marshal(d)
	if err != nil {
		return fmt.Errorf("marshal DAG spec: %w", err)
	}

	query := `
	INSERT INTO dags (id, name, description, spec, version, created_at, updated_at)
	VALUES ($1, $2, $3, $4, $5, $6, $7)
	ON CONFLICT (id) DO UPDATE SET
		name = EXCLUDED.name,
		description = EXCLUDED.description,
		spec = EXCLUDED.spec,
		version = EXCLUDED.version,
		updated_at = EXCLUDED.updated_at
	`

	now := time.Now()
	_, err = s.pool.Exec(ctx, query,
		d.ID, d.Name, d.Description, spec, d.Version, now, now,
	)
	return err
}

func (s *PostgresStore) GetDAG(ctx context.Context, dagID string) (*dag.DAG, error) {
	var spec []byte
	query := `SELECT spec FROM dags WHERE id = $1`
	err := s.pool.QueryRow(ctx, query, dagID).Scan(&spec)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("DAG %q not found", dagID)
		}
		return nil, err
	}

	var d dag.DAG
	if err := json.Unmarshal(spec, &d); err != nil {
		return nil, fmt.Errorf("unmarshal DAG spec: %w", err)
	}
	return &d, nil
}

func (s *PostgresStore) ListDAGs(ctx context.Context, filter DAGFilter) ([]*dag.DAG, error) {
	query := `SELECT spec FROM dags ORDER BY id`
	args := []any{}

	if filter.Limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", filter.Limit)
	}

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []*dag.DAG
	for rows.Next() {
		var spec []byte
		if err := rows.Scan(&spec); err != nil {
			return nil, err
		}
		var d dag.DAG
		if err := json.Unmarshal(spec, &d); err != nil {
			return nil, err
		}
		result = append(result, &d)
	}
	return result, rows.Err()
}

func (s *PostgresStore) DeleteDAG(ctx context.Context, dagID string) error {
	query := `DELETE FROM dags WHERE id = $1`
	tag, err := s.pool.Exec(ctx, query, dagID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("DAG %q not found", dagID)
	}
	return nil
}

func (s *PostgresStore) CreateRun(ctx context.Context, run *DAGRun) error {
	params, _ := json.Marshal(run.Params)
	query := `
	INSERT INTO dag_runs (id, dag_id, status, params, start_time, end_time, created_at, updated_at)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`
	now := time.Now()
	_, err := s.pool.Exec(ctx, query,
		run.ID, run.DAGID, run.Status, params,
		run.StartTime, run.EndTime, now, now,
	)
	return err
}

func (s *PostgresStore) GetRun(ctx context.Context, runID RunID) (*DAGRun, error) {
	var run DAGRun
	var params []byte
	query := `SELECT id, dag_id, status, params, start_time, end_time, created_at, updated_at
	          FROM dag_runs WHERE id = $1`
	err := s.pool.QueryRow(ctx, query, runID).Scan(
		&run.ID, &run.DAGID, &run.Status, &params,
		&run.StartTime, &run.EndTime, &run.CreatedAt, &run.UpdatedAt,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("run %q not found", runID)
		}
		return nil, err
	}
	_ = json.Unmarshal(params, &run.Params)
	return &run, nil
}

func (s *PostgresStore) UpdateRun(ctx context.Context, run *DAGRun) error {
	query := `UPDATE dag_runs SET status = $1, start_time = $2, end_time = $3, updated_at = $4
	          WHERE id = $5`
	_, err := s.pool.Exec(ctx, query,
		run.Status, run.StartTime, run.EndTime, time.Now(), run.ID,
	)
	return err
}

func (s *PostgresStore) ListRuns(ctx context.Context, filter RunFilter) ([]*DAGRun, error) {
	query := `SELECT id, dag_id, status, params, start_time, end_time, created_at, updated_at
	          FROM dag_runs WHERE 1=1`
	var args []any
	argIdx := 1

	if filter.DAGID != "" {
		query += fmt.Sprintf(" AND dag_id = $%d", argIdx)
		args = append(args, filter.DAGID)
		argIdx++
	}
	if filter.Status != nil {
		query += fmt.Sprintf(" AND status = $%d", argIdx)
		args = append(args, *filter.Status)
		argIdx++
	}

	query += " ORDER BY created_at DESC"

	if filter.Offset > 0 {
		query += fmt.Sprintf(" OFFSET %d", filter.Offset)
	}
	if filter.Limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", filter.Limit)
	}

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []*DAGRun
	for rows.Next() {
		var run DAGRun
		var params []byte
		if err := rows.Scan(
			&run.ID, &run.DAGID, &run.Status, &params,
			&run.StartTime, &run.EndTime, &run.CreatedAt, &run.UpdatedAt,
		); err != nil {
			return nil, err
		}
		_ = json.Unmarshal(params, &run.Params)
		result = append(result, &run)
	}
	return result, rows.Err()
}

func (s *PostgresStore) CreateTaskInstance(ctx context.Context, ti *TaskInstance) error {
	query := `
	INSERT INTO task_instances (id, run_id, task_id, dag_id, status, attempt, worker_id, output, error, start_time, end_time, queued_at, idempotency_key)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
	`
	_, err := s.pool.Exec(ctx, query,
		ti.ID, ti.RunID, ti.TaskID, ti.DAGID, ti.Status, ti.Attempt,
		ti.WorkerID, ti.Output, ti.Error,
		ti.StartTime, ti.EndTime, ti.QueuedAt, ti.IdempotencyKey,
	)
	return err
}

func (s *PostgresStore) GetTaskInstance(ctx context.Context, id TaskInstanceID) (*TaskInstance, error) {
	var ti TaskInstance
	query := `SELECT id, run_id, task_id, dag_id, status, attempt, worker_id, output, error, start_time, end_time, queued_at, idempotency_key
	          FROM task_instances WHERE id = $1`
	err := s.pool.QueryRow(ctx, query, id).Scan(
		&ti.ID, &ti.RunID, &ti.TaskID, &ti.DAGID, &ti.Status, &ti.Attempt,
		&ti.WorkerID, &ti.Output, &ti.Error,
		&ti.StartTime, &ti.EndTime, &ti.QueuedAt, &ti.IdempotencyKey,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("task instance %q not found", id)
		}
		return nil, err
	}
	return &ti, nil
}

func (s *PostgresStore) UpdateTaskInstance(ctx context.Context, ti *TaskInstance) error {
	query := `UPDATE task_instances SET status = $1, attempt = $2, worker_id = $3, output = $4, error = $5,
	          start_time = $6, end_time = $7 WHERE id = $8`
	_, err := s.pool.Exec(ctx, query,
		ti.Status, ti.Attempt, ti.WorkerID, ti.Output, ti.Error,
		ti.StartTime, ti.EndTime, ti.ID,
	)
	return err
}

func (s *PostgresStore) ListTaskInstances(ctx context.Context, runID RunID) ([]*TaskInstance, error) {
	query := `SELECT id, run_id, task_id, dag_id, status, attempt, worker_id, output, error, start_time, end_time, queued_at, idempotency_key
	          FROM task_instances WHERE run_id = $1 ORDER BY task_id`
	rows, err := s.pool.Query(ctx, query, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []*TaskInstance
	for rows.Next() {
		var ti TaskInstance
		if err := rows.Scan(
			&ti.ID, &ti.RunID, &ti.TaskID, &ti.DAGID, &ti.Status, &ti.Attempt,
			&ti.WorkerID, &ti.Output, &ti.Error,
			&ti.StartTime, &ti.EndTime, &ti.QueuedAt, &ti.IdempotencyKey,
		); err != nil {
			return nil, err
		}
		result = append(result, &ti)
	}
	return result, rows.Err()
}

func (s *PostgresStore) GetTaskInstanceByIdempotencyKey(ctx context.Context, key string) (*TaskInstance, error) {
	var ti TaskInstance
	query := `SELECT id, run_id, task_id, dag_id, status, attempt, worker_id, output, error, start_time, end_time, queued_at, idempotency_key
	          FROM task_instances WHERE idempotency_key = $1`
	err := s.pool.QueryRow(ctx, query, key).Scan(
		&ti.ID, &ti.RunID, &ti.TaskID, &ti.DAGID, &ti.Status, &ti.Attempt,
		&ti.WorkerID, &ti.Output, &ti.Error,
		&ti.StartTime, &ti.EndTime, &ti.QueuedAt, &ti.IdempotencyKey,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("no task instance with idempotency key %q", key)
		}
		return nil, err
	}
	return &ti, nil
}

func (s *PostgresStore) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

func (s *PostgresStore) Close() error {
	s.pool.Close()
	return nil
}
