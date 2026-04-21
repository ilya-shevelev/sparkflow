# Sparkflow

[![CI](https://github.com/ilya-shevelev/sparkflow/actions/workflows/ci.yml/badge.svg)](https://github.com/ilya-shevelev/sparkflow/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/ilya-shevelev/sparkflow)](https://goreportcard.com/report/github.com/ilya-shevelev/sparkflow)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

Distributed workflow orchestrator with native Apache Spark and Hadoop integration. A lightweight Go alternative to Apache Airflow.

## Architecture

```
Client (CLI/SDK/HTTP) --> API Gateway (gRPC + REST)
  --> Scheduler Cluster (Raft consensus, 3 nodes)
    --> Worker Pool (gRPC dispatch, heartbeat)
      --> Executors: Spark | Hadoop MR | HDFS | Shell | Python | Docker
  --> PostgreSQL (metadata, DAG state, run history)
  --> OpenTelemetry (traces, metrics, logs)
```

## Features

- **DAG-based workflows** - Define tasks and dependencies in YAML or Python
- **6 built-in executors** - Shell, Python, Docker, Spark, Hadoop MapReduce, HDFS
- **Distributed scheduling** - Raft-based leader election with worker pool
- **Retry policies** - Fixed, exponential, linear backoff with dead letter queue
- **Cron scheduling** - Periodic DAG execution with cron expressions
- **Exactly-once semantics** - Idempotency keys for task execution
- **Observability** - OpenTelemetry tracing, Prometheus metrics, structured logging
- **Python SDK** - Decorator-based DAG definitions
- **Production-ready** - Health checks, graceful shutdown, Helm charts

## Quick Start

### Install

```bash
go install github.com/ilya-shevelev/sparkflow/cmd/sparkflowctl@latest
```

### Define a DAG

Create `workflow.yaml`:

```yaml
id: my-etl
name: My ETL Pipeline
schedule:
  expression: "0 2 * * *"
config:
  max_concurrency: 4
  timeout: "1h"
tasks:
  - id: extract
    name: Extract Data
    executor: shell
    config:
      command: "curl -o /tmp/data.csv https://api.example.com/data"
    timeout: "5m"

  - id: transform
    name: Transform Data
    executor: python
    config:
      script: "transform.py"
    dependencies: [extract]

  - id: load
    name: Load to Warehouse
    executor: shell
    config:
      command: "psql -c \"COPY warehouse FROM '/tmp/output.csv'\""
    dependencies: [transform]
```

### Validate and Run

```bash
# Validate the DAG
sparkflowctl validate -f workflow.yaml

# Run locally
sparkflowctl run -f workflow.yaml
```

### Start the Server

```bash
# Start with in-memory store
sparkflow-server --http-addr=:8080 --grpc-addr=:9090

# Start with PostgreSQL
sparkflow-server --store-dsn="postgres://user:pass@localhost:5432/sparkflow"

# Start a worker
sparkflow-worker --server-addr=localhost:9090 --concurrency=8
```

### Docker Compose

```bash
docker compose -f deploy/docker/docker-compose.yaml up
```

## Executors

| Executor | Description | Config Keys |
|----------|-------------|-------------|
| `shell` | Run shell commands | `command`, `shell`, `env`, `working_dir` |
| `python` | Run Python scripts | `script`, `code`, `args`, `python_path` |
| `docker` | Run in Docker containers | `image`, `command`, `env`, `volumes` |
| `spark` | Submit Spark jobs | `application`, `class`, `master`, `spark_conf` |
| `hadoop` | Submit MapReduce jobs | `jar`, `class`, `args`, `memory_mb` |
| `hdfs` | HDFS file operations | `operation`, `path`, `content`, `recursive` |

## Spark ETL Example

```yaml
id: spark-etl
name: Spark ETL Pipeline
tasks:
  - id: transform
    executor: spark
    config:
      mode: submit
      master: yarn
      deploy_mode: cluster
      application: "hdfs:///apps/etl.jar"
      class: com.example.ETL
      driver_memory: "4g"
      executor_memory: "8g"
      num_executors: 10
      spark_conf:
        spark.sql.adaptive.enabled: "true"
      app_args: ["--date", "{{date}}"]
```

## Python SDK

```python
from sparkflow import dag, task, RetryPolicy

@dag("my-pipeline", "My Pipeline", schedule="0 * * * *")
def pipeline():
    e = extract()
    t = transform(depends_on=[e])
    load(depends_on=[t])

@task(executor="shell", timeout="5m")
def extract():
    return {"command": "extract.sh"}

@task(executor="python", retry=RetryPolicy(max_retries=3))
def transform():
    return {"script": "transform.py"}

@task(executor="shell")
def load():
    return {"command": "load.sh"}

# Generate YAML
dag_builder = pipeline()
dag_builder.save("workflow.yaml")
```

## Deployment

### Helm

```bash
helm install sparkflow deploy/helm/sparkflow/ \
  --set postgresql.auth.password=secret \
  --set replicaCount.worker=3
```

### Kubernetes Architecture

```
                    +-----------------+
                    |    Ingress      |
                    +--------+--------+
                             |
                    +--------v--------+
                    | sparkflow-server|  (gRPC + HTTP)
                    | (1-3 replicas)  |
                    +--------+--------+
                             |
              +--------------+--------------+
              |                             |
    +---------v---------+         +---------v---------+
    | sparkflow-worker  |         | sparkflow-worker  |
    | (N replicas)      |         | (N replicas)      |
    +-------------------+         +-------------------+
              |
    +---------v---------+
    |    PostgreSQL      |
    +-------------------+
```

## Development

```bash
# Build
make build

# Test
make test

# Lint
make lint

# Run locally
make run-server   # terminal 1
make run-worker   # terminal 2

# Docker dev environment
make dev
```

## Configuration

| Flag | Default | Description |
|------|---------|-------------|
| `--grpc-addr` | `:9090` | gRPC listen address |
| `--http-addr` | `:8080` | HTTP listen address |
| `--metrics-addr` | `:9091` | Metrics listen address |
| `--store-dsn` | (empty) | PostgreSQL DSN (empty = in-memory) |
| `--data-dir` | `/var/lib/sparkflow` | Data directory |
| `--log-level` | `info` | Log level (debug/info/warn/error) |

## License

Apache License 2.0 - see [LICENSE](LICENSE) for details.
