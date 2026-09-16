// Package queue implements a Redis-backed job queue with at-least-once
// delivery, retries with exponential backoff, and a dead-letter queue.
//
// Jobs move between four Redis keys: pending -> processing -> (done | delayed
// | dead). A job is only removed from Redis once a worker has explicitly
// acknowledged it, so a crash mid-job loses nothing.
package queue

import (
	"context"
	"encoding/json"
	"math/rand"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	QueueKey      = "jobs:pending"    // list: ready to run
	ProcessingKey = "jobs:processing" // list: claimed, not yet acked
	DeadKey       = "jobs:dead"       // list: out of attempts
	DelayedKey    = "jobs:delayed"    // sorted set: scored by unix run-time

	// MaxAttempts is the total number of tries a job gets before it is moved
	// to the dead-letter queue.
	MaxAttempts = 3
)

// Job is the unit of work. It is stored in Redis as JSON.
type Job struct {
	ID        string    `json:"id"`
	Type      string    `json:"type"`
	Payload   string    `json:"payload"`
	Attempts  int       `json:"attempts"`
	CreatedAt time.Time `json:"created_at"`
}

// Queue wraps a Redis client and owns every operation on the four job keys.
// It holds no in-memory state, so multiple instances can share one Redis.
type Queue struct {
	rdb *redis.Client
}

// New returns a Queue backed by Redis database 0.
func New(addr string) *Queue {
	return NewWithDB(addr, 0)
}

// NewWithDB returns a Queue backed by a specific Redis database. Tests use
// this to run against database 15, keeping them clear of application data.
func NewWithDB(addr string, db int) *Queue {
	return &Queue{rdb: redis.NewClient(&redis.Options{Addr: addr, DB: db})}
}

// FlushDB erases the entire database this Queue is connected to. Intended for
// tests.
func (q *Queue) FlushDB(ctx context.Context) error {
	return q.rdb.FlushDB(ctx).Err()
}

// Ping reports whether Redis is reachable.
func (q *Queue) Ping(ctx context.Context) error {
	return q.rdb.Ping(ctx).Err()
}

// Enqueue marshals a job and pushes it onto the pending list.
func (q *Queue) Enqueue(ctx context.Context, j Job) error {
	data, err := json.Marshal(j)
	if err != nil {
		return err
	}
	// Push left, pop right: the queue is FIFO.
	return q.rdb.LPush(ctx, QueueKey, data).Err()
}

// Dequeue blocks until a job is available or timeout elapses. It returns the
// decoded job alongside the exact JSON string it was stored as; that string
// must be handed back to Ack or Retry.
//
// BLMove is what makes delivery reliable: claiming the job and recording the
// claim are a single atomic operation, so a job is never in flight without
// also being in the processing list. BRPop, the obvious alternative, deletes
// the job outright and loses it if the worker dies.
func (q *Queue) Dequeue(ctx context.Context, timeout time.Duration) (*Job, string, error) {
	raw, err := q.rdb.BLMove(ctx, QueueKey, ProcessingKey, "right", "left", timeout).Result()
	if err == redis.Nil {
		// Timed out with an empty queue. Not an error; the worker loops.
		return nil, "", nil
	}
	if err != nil {
		return nil, "", err
	}

	var j Job
	if err := json.Unmarshal([]byte(raw), &j); err != nil {
		return nil, "", err
	}
	return &j, raw, nil
}

// Ack removes a finished job from the processing list.
//
// It matches on the raw string the worker was given rather than re-marshalling
// the Job. Go's JSON encoder could legitimately produce different bytes for an
// equal struct, and LRem matches byte-for-byte, so re-marshalling risks a
// silent no-op that strands the job in processing forever.
func (q *Queue) Ack(ctx context.Context, raw string) error {
	return q.rdb.LRem(ctx, ProcessingKey, 1, raw).Err()
}

// Recover moves everything stranded in the processing list back to pending and
// returns how many jobs were moved. Called once at startup to pick up work
// abandoned by a previous crash.
func (q *Queue) Recover(ctx context.Context) (int, error) {
	count := 0
	for {
		_, err := q.rdb.LMove(ctx, ProcessingKey, QueueKey, "right", "left").Result()
		if err == redis.Nil {
			// Processing list is empty: everything has been moved.
			return count, nil
		}
		if err != nil {
			return count, err
		}
		count++
	}
}

// Retry increments the job's attempt count and either schedules it for a later
// run or moves it to the dead-letter queue. It reports whether the job died so
// the caller can log that, keeping logging out of this package.
//
// Both branches write the job to its new home before acking it. Acking first
// would reintroduce the loss bug: a crash in the gap would leave the job in no
// list at all. In this order a crash duplicates work instead, which is the
// tradeoff at-least-once delivery accepts.
func (q *Queue) Retry(ctx context.Context, j Job, raw string) (dead bool, err error) {
	j.Attempts++

	data, err := json.Marshal(j)
	if err != nil {
		return false, err
	}

	if j.Attempts >= MaxAttempts {
		if err := q.rdb.LPush(ctx, DeadKey, data).Err(); err != nil {
			return false, err
		}
		return true, q.Ack(ctx, raw)
	}

	// Exponential backoff: 2s after the first failure, 4s after the second.
	// With MaxAttempts at 3 the 8s branch is unreachable in normal operation.
	backoff := time.Duration(1<<uint(j.Attempts)) * time.Second

	// Spread the delay over ±10% so a batch of jobs that failed together does
	// not come back all at once.
	jitter := time.Duration(rand.Int63n(int64(backoff / 5)))
	runAt := time.Now().Add(backoff - backoff/10 + jitter).Unix()

	if err := q.rdb.ZAdd(ctx, DelayedKey, redis.Z{
		Score:  float64(runAt),
		Member: data,
	}).Err(); err != nil {
		return false, err
	}
	return false, q.Ack(ctx, raw)
}

// PromoteDue moves every delayed job whose run-time has passed onto the pending
// list and returns how many it moved. The scheduler goroutine calls this once a
// second.
func (q *Queue) PromoteDue(ctx context.Context) (int, error) {
	now := float64(time.Now().Unix())

	members, err := q.rdb.ZRangeByScore(ctx, DelayedKey, &redis.ZRangeBy{
		Min: "-inf",
		Max: strconv.FormatFloat(now, 'f', 0, 64),
	}).Result()
	if err != nil {
		return 0, err
	}

	count := 0
	for _, m := range members {
		removed, err := q.rdb.ZRem(ctx, DelayedKey, m).Result()
		if err != nil {
			return count, err
		}
		if removed == 0 {
			// Another instance removed it between the range query and here.
			// Whoever removed it owns it, so skip rather than double-promote.
			continue
		}
		if err := q.rdb.LPush(ctx, QueueKey, m).Err(); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

// Stats is a point-in-time count of each queue, served by GET /stats.
type Stats struct {
	Pending    int64 `json:"pending"`
	Processing int64 `json:"processing"`
	Delayed    int64 `json:"delayed"`
	Dead       int64 `json:"dead"`
}

// Stats counts all four keys. The reads are not atomic with respect to each
// other, so the numbers can be very slightly inconsistent under load. That is
// fine for a status display.
func (q *Queue) Stats(ctx context.Context) (Stats, error) {
	var s Stats
	var err error

	if s.Pending, err = q.rdb.LLen(ctx, QueueKey).Result(); err != nil {
		return s, err
	}
	if s.Processing, err = q.rdb.LLen(ctx, ProcessingKey).Result(); err != nil {
		return s, err
	}
	if s.Delayed, err = q.rdb.ZCard(ctx, DelayedKey).Result(); err != nil {
		return s, err
	}
	if s.Dead, err = q.rdb.LLen(ctx, DeadKey).Result(); err != nil {
		return s, err
	}
	return s, nil
}
