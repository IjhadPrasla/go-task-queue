package queue

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	QueueKey      = "jobs:pending"
	ProcessingKey = "jobs:processing"
	DeadKey       = "jobs:dead"
	MaxAttempts   = 3
	DelayedKey    = "jobs:delayed"
)

type Job struct {
	ID        string    `json:"id"`
	Type      string    `json:"type"`
	Payload   string    `json:"payload"`
	Attempts  int       `json:"attempts"`
	CreatedAt time.Time `json:"created_at"`
}

type Queue struct {
	rdb *redis.Client
}

func New(addr string) *Queue {
	return NewWithDB(addr, 0)
}

func NewWithDB(addr string, db int) *Queue {
	return &Queue{rdb: redis.NewClient(&redis.Options{Addr: addr, DB: db})}
}

func (q *Queue) FlushDB(ctx context.Context) error {
	return q.rdb.FlushDB(ctx).Err()
}

func (q *Queue) Ping(ctx context.Context) error {
	return q.rdb.Ping(ctx).Err()
}

func (q *Queue) Enqueue(ctx context.Context, j Job) error {
	data, err := json.Marshal(j)
	if err != nil {
		return err
	}
	return q.rdb.LPush(ctx, QueueKey, data).Err()
}

func (q *Queue) Dequeue(ctx context.Context, timeout time.Duration) (*Job, string, error) {
	raw, err := q.rdb.BLMove(ctx, QueueKey, ProcessingKey, "right", "left", timeout).Result()
	if err == redis.Nil {
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

func (q *Queue) Ack(ctx context.Context, raw string) error {
	return q.rdb.LRem(ctx, ProcessingKey, 1, raw).Err()
}

func (q *Queue) Recover(ctx context.Context) (int, error) {
	count := 0
	for {
		_, err := q.rdb.LMove(ctx, ProcessingKey, QueueKey, "right", "left").Result()
		if err == redis.Nil {
			return count, nil
		}
		if err != nil {
			return count, err
		}
		count++
	}
}

func (q *Queue) Retry(ctx context.Context, j Job, raw string) error {
	j.Attempts++

	data, err := json.Marshal(j)
	if err != nil {
		return err
	}

	if j.Attempts >= MaxAttempts {
		if err := q.rdb.LPush(ctx, DeadKey, data).Err(); err != nil {
			return err
		}
		return q.Ack(ctx, raw)
	}

	backoff := time.Duration(1<<uint(j.Attempts)) * time.Second
	runAt := time.Now().Add(backoff).Unix()

	if err := q.rdb.ZAdd(ctx, DelayedKey, redis.Z{
		Score:  float64(runAt),
		Member: data,
	}).Err(); err != nil {
		return err
	}
	return q.Ack(ctx, raw)
}

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
			continue
		}
		if err := q.rdb.LPush(ctx, QueueKey, m).Err(); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}
