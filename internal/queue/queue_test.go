package queue

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

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

	if err := q.Retry(ctx, *got, raw); err != nil {
		t.Fatalf("retry: %v", err)
	}

	n, err := q.rdb.ZCard(ctx, DelayedKey).Result()
	if err != nil {
		t.Fatalf("zcard: %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1 delayed job, got %d", n)
	}

	pending, err := q.rdb.LLen(ctx, ProcessingKey).Result()
	if err != nil {
		t.Fatalf("llen: %v", err)
	}
	if pending != 0 {
		t.Errorf("expected processing list empty after retry, got %d", pending)
	}
}

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

	if err := q.Retry(ctx, *got, raw); err != nil {
		t.Fatalf("retry: %v", err)
	}

	dead, err := q.rdb.LLen(ctx, DeadKey).Result()
	if err != nil {
		t.Fatalf("llen dead: %v", err)
	}
	if dead != 1 {
		t.Errorf("expected 1 dead job, got %d", dead)
	}

	delayed, err := q.rdb.ZCard(ctx, DelayedKey).Result()
	if err != nil {
		t.Fatalf("zcard: %v", err)
	}
	if delayed != 0 {
		t.Errorf("expected no delayed jobs, got %d", delayed)
	}
}

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
