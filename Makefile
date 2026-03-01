.PHONY: build test lint clean proto docker dev fmt vet

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
LDFLAGS := -ldflags "-s -w -X main.version=$(VERSION) -X main.commit=$(COMMIT)"

# Build all binaries.
build:
	go build $(LDFLAGS) -o bin/sparkflow-server ./cmd/sparkflow-server
	go build $(LDFLAGS) -o bin/sparkflow-worker ./cmd/sparkflow-worker
	go build $(LDFLAGS) -o bin/sparkflowctl ./cmd/sparkflowctl

# Run all tests.
test:
	go test -race -count=1 ./...

# Run tests with coverage.
test-cover:
	go test -race -coverprofile=coverage.out -covermode=atomic ./...
	go tool cover -func=coverage.out

# Run linter.
lint:
	golangci-lint run ./...

# Run go vet.
vet:
	go vet ./...

# Format code.
fmt:
	gofmt -s -w .
	goimports -w .

# Clean build artifacts.
clean:
	rm -rf bin/ coverage.out

# Build Docker image.
docker:
	docker build -t sparkflow:$(VERSION) -f deploy/docker/Dockerfile \
		--build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) .

# Start development environment.
dev:
	docker compose -f deploy/docker/docker-compose.yaml up --build

# Stop development environment.
dev-down:
	docker compose -f deploy/docker/docker-compose.yaml down -v

# Generate protobuf code (requires protoc and go plugins).
proto:
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		api/proto/v1/*.proto

# Run the server locally.
run-server: build
	./bin/sparkflow-server --log-level=debug

# Run a worker locally.
run-worker: build
	./bin/sparkflow-worker --log-level=debug

# Validate example DAGs.
validate-examples: build
	./bin/sparkflowctl validate -f examples/simple-dag/workflow.yaml
	./bin/sparkflowctl validate -f examples/spark-etl/workflow.yaml
	./bin/sparkflowctl validate -f examples/hadoop-wordcount/workflow.yaml

# Install tools.
tools:
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	go install golang.org/x/tools/cmd/goimports@latest
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# Show help.
help:
	@echo "Available targets:"
	@echo "  build            Build all binaries"
	@echo "  test             Run tests"
	@echo "  test-cover       Run tests with coverage"
	@echo "  lint             Run linter"
	@echo "  vet              Run go vet"
	@echo "  fmt              Format code"
	@echo "  clean            Clean build artifacts"
	@echo "  docker           Build Docker image"
	@echo "  dev              Start dev environment (docker-compose)"
	@echo "  dev-down         Stop dev environment"
	@echo "  proto            Generate protobuf code"
	@echo "  run-server       Run server locally"
	@echo "  run-worker       Run worker locally"
	@echo "  validate-examples Validate example DAG files"
	@echo "  tools            Install development tools"
