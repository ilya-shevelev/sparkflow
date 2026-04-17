# Build stage
FROM golang:1.22-bookworm AS builder

WORKDIR /build

# Cache dependency downloads.
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build.
COPY . .

ARG VERSION=dev
ARG COMMIT=unknown

RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT}" \
    -o /bin/sparkflow-server ./cmd/sparkflow-server

RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT}" \
    -o /bin/sparkflow-worker ./cmd/sparkflow-worker

RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT}" \
    -o /bin/sparkflowctl ./cmd/sparkflowctl

# Runtime stage - distroless
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /bin/sparkflow-server /usr/local/bin/sparkflow-server
COPY --from=builder /bin/sparkflow-worker /usr/local/bin/sparkflow-worker
COPY --from=builder /bin/sparkflowctl /usr/local/bin/sparkflowctl

USER nonroot:nonroot

EXPOSE 8080 9090 9091

ENTRYPOINT ["sparkflow-server"]
