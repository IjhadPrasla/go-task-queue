package queue

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
)

const QueueKey = "jobs:pending"

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
	return &Queue{rdb: redis.NewClient(&redis.Options{Addr: addr})}
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

func (q *Queue) Dequeue(ctx context.Context, timeout time.Duration) (*Job, error) {
	res, err := q.rdb.BRPop(ctx, timeout, QueueKey).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var j Job
	if err := json.Unmarshal([]byte(res[1]), &j); err != nil {
		return nil, err
	}
	return &j, nil
}