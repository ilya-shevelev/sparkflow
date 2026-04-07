package retry

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPolicy_Delay_Exponential(t *testing.T) {
	p := &Policy{
		InitialBackoff: time.Second,
		MaxBackoff:     time.Hour,
		BackoffFactor:  2.0,
		Strategy:       StrategyExponential,
		Jitter:         false,
	}

	tests := []struct {
		attempt  int
		expected time.Duration
	}{
		{0, 1 * time.Second},
		{1, 2 * time.Second},
		{2, 4 * time.Second},
		{3, 8 * time.Second},
		{4, 16 * time.Second},
	}

	for _, tc := range tests {
		delay := p.Delay(tc.attempt)
		assert.Equal(t, tc.expected, delay, "attempt %d", tc.attempt)
	}
}

func TestPolicy_Delay_Fixed(t *testing.T) {
	p := &Policy{
		InitialBackoff: 5 * time.Second,
		MaxBackoff:     time.Hour,
		BackoffFactor:  1.0,
		Strategy:       StrategyFixed,
		Jitter:         false,
	}

	for attempt := 0; attempt < 5; attempt++ {
		delay := p.Delay(attempt)
		assert.Equal(t, 5*time.Second, delay, "attempt %d", attempt)
	}
}

func TestPolicy_Delay_Linear(t *testing.T) {
	p := &Policy{
		InitialBackoff: time.Second,
		MaxBackoff:     time.Hour,
		BackoffFactor:  1.0,
		Strategy:       StrategyLinear,
		Jitter:         false,
	}

	tests := []struct {
		attempt  int
		expected time.Duration
	}{
		{0, 1 * time.Second},
		{1, 2 * time.Second},
		{2, 3 * time.Second},
		{3, 4 * time.Second},
	}

	for _, tc := range tests {
		delay := p.Delay(tc.attempt)
		assert.Equal(t, tc.expected, delay, "attempt %d", tc.attempt)
	}
}

func TestPolicy_Delay_MaxBackoff(t *testing.T) {
	p := &Policy{
		InitialBackoff: time.Second,
		MaxBackoff:     10 * time.Second,
		BackoffFactor:  10.0,
		Strategy:       StrategyExponential,
		Jitter:         false,
	}

	delay := p.Delay(5)
	assert.Equal(t, 10*time.Second, delay)
}

func TestPolicy_Delay_Jitter(t *testing.T) {
	p := &Policy{
		InitialBackoff: time.Second,
		MaxBackoff:     time.Hour,
		BackoffFactor:  2.0,
		Strategy:       StrategyExponential,
		Jitter:         true,
	}

	// Run multiple times and check that jitter is applied (values differ).
	delays := make(map[time.Duration]bool)
	for i := 0; i < 100; i++ {
		delays[p.Delay(2)] = true
	}
	// With jitter, we should see multiple distinct values.
	assert.Greater(t, len(delays), 1, "jitter should produce varied delays")
}

func TestPolicy_ShouldRetry(t *testing.T) {
	p := &Policy{MaxRetries: 3}

	assert.True(t, p.ShouldRetry(0, errors.New("err")))
	assert.True(t, p.ShouldRetry(2, errors.New("err")))
	assert.False(t, p.ShouldRetry(3, errors.New("err")))
	assert.False(t, p.ShouldRetry(0, nil))
}

func TestPolicy_ShouldRetry_CustomCheck(t *testing.T) {
	retryableErr := errors.New("retryable")
	nonRetryableErr := errors.New("fatal")

	p := &Policy{
		MaxRetries: 5,
		RetryableCheck: func(err error) bool {
			return errors.Is(err, retryableErr)
		},
	}

	assert.True(t, p.ShouldRetry(0, retryableErr))
	assert.False(t, p.ShouldRetry(0, nonRetryableErr))
}

func TestPolicy_Do_Success(t *testing.T) {
	p := DefaultPolicy()
	p.Jitter = false
	p.InitialBackoff = time.Millisecond

	callCount := 0
	err := p.Do(context.Background(), func(ctx context.Context) error {
		callCount++
		return nil
	})

	assert.NoError(t, err)
	assert.Equal(t, 1, callCount)
}

func TestPolicy_Do_SuccessAfterRetries(t *testing.T) {
	p := &Policy{
		MaxRetries:     5,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     10 * time.Millisecond,
		BackoffFactor:  2.0,
		Strategy:       StrategyExponential,
		Jitter:         false,
	}

	callCount := 0
	err := p.Do(context.Background(), func(ctx context.Context) error {
		callCount++
		if callCount < 3 {
			return errors.New("transient")
		}
		return nil
	})

	assert.NoError(t, err)
	assert.Equal(t, 3, callCount)
}

func TestPolicy_Do_ExhaustRetries(t *testing.T) {
	p := &Policy{
		MaxRetries:     2,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     10 * time.Millisecond,
		BackoffFactor:  2.0,
		Strategy:       StrategyFixed,
		Jitter:         false,
	}

	callCount := 0
	err := p.Do(context.Background(), func(ctx context.Context) error {
		callCount++
		return errors.New("always fails")
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "exhausted")
	assert.Equal(t, 3, callCount) // initial + 2 retries
}

func TestPolicy_Do_ContextCancelled(t *testing.T) {
	p := &Policy{
		MaxRetries:     10,
		InitialBackoff: time.Second,
		MaxBackoff:     time.Minute,
		BackoffFactor:  2.0,
		Strategy:       StrategyExponential,
	}

	ctx, cancel := context.WithCancel(context.Background())
	callCount := 0

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := p.Do(ctx, func(ctx context.Context) error {
		callCount++
		return errors.New("fail")
	})

	assert.Error(t, err)
	assert.True(t, errors.Is(err, context.Canceled))
}

func TestDeadLetterQueue(t *testing.T) {
	q := NewDeadLetterQueue(100)

	assert.Equal(t, 0, q.Len())

	q.Push(DLQEntry{TaskID: "task1", Error: errors.New("err1"), Attempts: 3})
	q.Push(DLQEntry{TaskID: "task2", Error: errors.New("err2"), Attempts: 5})

	assert.Equal(t, 2, q.Len())

	// Peek.
	entry, ok := q.Peek()
	require.True(t, ok)
	assert.Equal(t, "task1", entry.TaskID)
	assert.Equal(t, 2, q.Len()) // Peek does not remove.

	// Pop.
	entry, ok = q.Pop()
	require.True(t, ok)
	assert.Equal(t, "task1", entry.TaskID)
	assert.Equal(t, 1, q.Len())

	// List.
	entries := q.List()
	assert.Len(t, entries, 1)
	assert.Equal(t, "task2", entries[0].TaskID)

	// Clear.
	q.Clear()
	assert.Equal(t, 0, q.Len())
}

func TestDeadLetterQueue_MaxSize(t *testing.T) {
	q := NewDeadLetterQueue(3)

	q.Push(DLQEntry{TaskID: "task1"})
	q.Push(DLQEntry{TaskID: "task2"})
	q.Push(DLQEntry{TaskID: "task3"})
	q.Push(DLQEntry{TaskID: "task4"})

	assert.Equal(t, 3, q.Len())

	// Oldest should have been evicted.
	entry, ok := q.Pop()
	require.True(t, ok)
	assert.Equal(t, "task2", entry.TaskID)
}

func TestStrategy_String(t *testing.T) {
	assert.Equal(t, "fixed", StrategyFixed.String())
	assert.Equal(t, "exponential", StrategyExponential.String())
	assert.Equal(t, "linear", StrategyLinear.String())
}

func TestPolicy_Delay_NegativeAttempt(t *testing.T) {
	p := DefaultPolicy()
	assert.Equal(t, time.Duration(0), p.Delay(-1))
}
