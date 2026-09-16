package queue

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// testQueue returns a Queue pointed at Redis database 15 and flushes it, so
// each test starts clean without touching database 0 where the app lives.
//
// If Redis is not running the test skips rather than fails: these are
// integration tests and an absent dependency is not a code defect. Note that
// skipped tests still report "ok" in the summary line, so read the per-test
// SKIP/PASS lines when checking a run.
func testQueue(t *testing.T) *Queue {
	t.Helper()

	q := NewWithDB("localhost:6379", 15)
	ctx := context.Background()

	if err := q.Ping(ctx); err != nil {
		t.Skipf("redis unavailable, skipping: %v", err)
	}
	if err := q.FlushDB(ctx); err != nil {
		t.Fatalf("flush failed: %v", err)
	}
	return q
}

// TestEnqueueDequeue covers the round trip: a job survives marshalling into
// Redis and back, and Dequeue returns the raw string Ack will need later.
func TestEnqueueDequeue(t *testing.T) {
	q := testQueue(t)
	ctx := context.Background()

	want := Job{ID: "abc", Type: "email", Payload: "hello"}
	if err := q.Enqueue(ctx, want); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	got, raw, err := q.Dequeue(ctx, time.Second)
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if got == nil {
		t.Fatal("expected a job, got nil")
	}
	if got.ID != want.ID || got.Payload != want.Payload {
		t.Errorf("got %+v, want %+v", got, want)
	}
	if raw == "" {
		t.Error("expected non-empty raw payload")
	}
}

// TestDequeueEmptyReturnsNil pins down the timeout contract: an empty queue is
// a nil job and a nil error, not redis.Nil leaking out to the worker loop.
func TestDequeueEmptyReturnsNil(t *testing.T) {
	q := testQueue(t)
	ctx := context.Background()

	job, _, err := q.Dequeue(ctx, time.Second)
	if err != nil {
		t.Fatalf("expected no error on empty queue, got %v", err)
	}
	if job != nil {
		t.Errorf("expected nil job, got %+v", job)
	}
}

// TestRetryIncrementsAttempts covers the non-fatal retry branch: the job is
// reported alive, lands in the delayed set, and is released from processing.
func TestRetryIncrementsAttempts(t *testing.T) {
	q := testQueue(t)
	ctx := context.Background()

	j := Job{ID: "retry-me", Type: "email", Payload: "x", Attempts: 0}
	if err := q.Enqueue(ctx, j); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	got, raw, err := q.Dequeue(ctx, time.Second)
	if err != nil || got == nil {
		t.Fatalf("dequeue: %v", err)
	}

	dead, err := q.Retry(ctx, *got, raw)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if dead {
		t.Error("job reported dead before exhausting attempts")
	}

	n, err := q.rdb.ZCard(ctx, DelayedKey).Result()
	if err != nil {
		t.Fatalf("zcard: %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1 delayed job, got %d", n)
	}

	// The retry must have acked, or the job would be counted twice.
	pending, err := q.rdb.LLen(ctx, ProcessingKey).Result()
	if err != nil {
		t.Fatalf("llen: %v", err)
	}
	if pending != 0 {
		t.Errorf("expected processing list empty after retry, got %d", pending)
	}
}

// TestRetryExhaustedGoesToDeadLetter covers the fatal branch. Seeding with
// MaxAttempts-1 means one more failure kills the job whatever MaxAttempts is
// set to, so the test does not hardcode the limit.
func TestRetryExhaustedGoesToDeadLetter(t *testing.T) {
	q := testQueue(t)
	ctx := context.Background()

	j := Job{ID: "doomed", Type: "email", Payload: "x", Attempts: MaxAttempts - 1}
	if err := q.Enqueue(ctx, j); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	got, raw, err := q.Dequeue(ctx, time.Second)
	if err != nil || got == nil {
		t.Fatalf("dequeue: %v", err)
	}

	dead, err := q.Retry(ctx, *got, raw)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if !dead {
		t.Error("exhausted job not reported as dead")
	}

	deadCount, err := q.rdb.LLen(ctx, DeadKey).Result()
	if err != nil {
		t.Fatalf("llen dead: %v", err)
	}
	if deadCount != 1 {
		t.Errorf("expected 1 dead job, got %d", deadCount)
	}

	// A dead job must not also be scheduled for another run.
	delayed, err := q.rdb.ZCard(ctx, DelayedKey).Result()
	if err != nil {
		t.Fatalf("zcard: %v", err)
	}
	if delayed != 0 {
		t.Errorf("expected no delayed jobs, got %d", delayed)
	}
}

// TestPromoteDueOnlyPromotesReadyJobs checks the scheduler's filtering: a job
// scored in the past is promoted, one scored in the future is left alone.
//
// The members are arbitrary strings rather than real jobs because PromoteDue
// moves opaque values between keys and never decodes them.
func TestPromoteDueOnlyPromotesReadyJobs(t *testing.T) {
	q := testQueue(t)
	ctx := context.Background()

	future := time.Now().Add(time.Hour).Unix()
	past := time.Now().Add(-time.Hour).Unix()

	if err := q.rdb.ZAdd(ctx, DelayedKey,
		redis.Z{Score: float64(future), Member: `{"id":"later"}`},
		redis.Z{Score: float64(past), Member: `{"id":"now"}`},
	).Err(); err != nil {
		t.Fatalf("zadd: %v", err)
	}

	n, err := q.PromoteDue(ctx)
	if err != nil {
		t.Fatalf("promote: %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1 promoted, got %d", n)
	}

	remaining, err := q.rdb.ZCard(ctx, DelayedKey).Result()
	if err != nil {
		t.Fatalf("zcard: %v", err)
	}
	if remaining != 1 {
		t.Errorf("expected 1 job still delayed, got %d", remaining)
	}
}
