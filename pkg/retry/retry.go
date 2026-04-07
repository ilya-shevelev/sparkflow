// Package retry provides retry policies with various backoff strategies
// and a dead letter queue for tasks that exhaust their retries.
package retry

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"sync"
	"time"
)

// Strategy defines the backoff strategy type.
type Strategy int

const (
	// StrategyFixed uses a fixed delay between retries.
	StrategyFixed Strategy = iota
	// StrategyExponential uses exponentially increasing delays.
	StrategyExponential
	// StrategyLinear uses linearly increasing delays.
	StrategyLinear
)

func (s Strategy) String() string {
	switch s {
	case StrategyFixed:
		return "fixed"
	case StrategyExponential:
		return "exponential"
	case StrategyLinear:
		return "linear"
	default:
		return "unknown"
	}
}

// Policy defines retry behavior for task execution.
type Policy struct {
	MaxRetries     int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	BackoffFactor  float64
	Strategy       Strategy
	Jitter         bool
	RetryableCheck func(err error) bool
}

// DefaultPolicy returns a sensible default retry policy.
func DefaultPolicy() *Policy {
	return &Policy{
		MaxRetries:     3,
		InitialBackoff: time.Second,
		MaxBackoff:     5 * time.Minute,
		BackoffFactor:  2.0,
		Strategy:       StrategyExponential,
		Jitter:         true,
	}
}

// Delay calculates the delay before the nth retry attempt (0-indexed).
func (p *Policy) Delay(attempt int) time.Duration {
	if attempt < 0 {
		return 0
	}

	var delay time.Duration

	switch p.Strategy {
	case StrategyFixed:
		delay = p.InitialBackoff

	case StrategyExponential:
		multiplier := math.Pow(p.BackoffFactor, float64(attempt))
		delay = time.Duration(float64(p.InitialBackoff) * multiplier)

	case StrategyLinear:
		delay = p.InitialBackoff + time.Duration(float64(p.InitialBackoff)*p.BackoffFactor*float64(attempt))
	}

	// Cap at max backoff.
	if delay > p.MaxBackoff {
		delay = p.MaxBackoff
	}

	// Add jitter.
	if p.Jitter && delay > 0 {
		jitterRange := float64(delay) * 0.25
		jitter := rand.Float64()*jitterRange*2 - jitterRange
		delay = time.Duration(float64(delay) + jitter)
		if delay < 0 {
			delay = 0
		}
	}

	return delay
}

// ShouldRetry determines whether a retry should be attempted.
func (p *Policy) ShouldRetry(attempt int, err error) bool {
	if attempt >= p.MaxRetries {
		return false
	}
	if p.RetryableCheck != nil {
		return p.RetryableCheck(err)
	}
	return err != nil
}

// RetryFunc is the function signature for retryable operations.
type RetryFunc func(ctx context.Context) error

// Do executes the function with retry logic according to the policy.
func (p *Policy) Do(ctx context.Context, fn RetryFunc) error {
	var lastErr error

	for attempt := 0; attempt <= p.MaxRetries; attempt++ {
		if attempt > 0 {
			delay := p.Delay(attempt - 1)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}

		lastErr = fn(ctx)
		if lastErr == nil {
			return nil
		}

		if !p.ShouldRetry(attempt, lastErr) {
			break
		}
	}

	return fmt.Errorf("exhausted %d retries: %w", p.MaxRetries, lastErr)
}

// DLQEntry represents a task that has exhausted its retries.
type DLQEntry struct {
	TaskID    string
	RunID     string
	Error     error
	Attempts  int
	CreatedAt time.Time
	Payload   map[string]any
}

// DeadLetterQueue stores tasks that have failed all retry attempts.
type DeadLetterQueue struct {
	mu      sync.RWMutex
	entries []DLQEntry
	maxSize int
}

// NewDeadLetterQueue creates a new dead letter queue with the given max size.
func NewDeadLetterQueue(maxSize int) *DeadLetterQueue {
	if maxSize <= 0 {
		maxSize = 10000
	}
	return &DeadLetterQueue{
		entries: make([]DLQEntry, 0),
		maxSize: maxSize,
	}
}

// Push adds a failed task to the DLQ.
func (q *DeadLetterQueue) Push(entry DLQEntry) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if entry.CreatedAt.IsZero() {
		entry.CreatedAt = time.Now()
	}

	// Evict oldest if at capacity.
	if len(q.entries) >= q.maxSize {
		q.entries = q.entries[1:]
	}

	q.entries = append(q.entries, entry)
}

// Pop removes and returns the oldest DLQ entry.
func (q *DeadLetterQueue) Pop() (DLQEntry, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.entries) == 0 {
		return DLQEntry{}, false
	}

	entry := q.entries[0]
	q.entries = q.entries[1:]
	return entry, true
}

// Peek returns the oldest DLQ entry without removing it.
func (q *DeadLetterQueue) Peek() (DLQEntry, bool) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	if len(q.entries) == 0 {
		return DLQEntry{}, false
	}
	return q.entries[0], true
}

// Len returns the number of entries in the DLQ.
func (q *DeadLetterQueue) Len() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.entries)
}

// List returns all DLQ entries.
func (q *DeadLetterQueue) List() []DLQEntry {
	q.mu.RLock()
	defer q.mu.RUnlock()

	result := make([]DLQEntry, len(q.entries))
	copy(result, q.entries)
	return result
}

// Clear removes all entries from the DLQ.
func (q *DeadLetterQueue) Clear() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.entries = q.entries[:0]
}
