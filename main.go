// Command taskqueue runs the job queue: an HTTP API for submitting jobs, a
// pool of workers that execute them, and a scheduler that promotes delayed
// jobs when their backoff expires. All state lives in Redis (see the queue
// package), so the process holds nothing that can't be rebuilt on restart.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/IjhadPrasla/go-task-queue/internal/queue"
	"github.com/google/uuid"
)

// numWorkers is the size of the worker pool. Three is enough to make
// concurrency visible on the status page without flooding the logs.
const numWorkers = 3

func main() {
	// One context cancelled by SIGINT or SIGTERM drives shutdown everywhere:
	// workers and scheduler watch it, and blocking Redis calls unblock on it.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Defaults let `go run .` work with no environment set up; Docker Compose
	// supplies both variables.
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	q := queue.New(redisAddr)
	// Fail loudly at startup rather than having every worker discover this
	// separately a second later.
	if err := q.Ping(ctx); err != nil {
		log.Fatalf("redis unreachable: %v", err)
	}
	log.Println("connected to redis")

	// Claim back anything a previous run left stranded in the processing list.
	// Must happen before the workers start, or they compete with recovery.
	recovered, err := q.Recover(ctx)
	if err != nil {
		// Not fatal: the queue works, some old jobs just stay stuck.
		log.Printf("recovery failed: %v", err)
	} else if recovered > 0 {
		log.Printf("recovered %d orphaned jobs", recovered)
	}

	// The WaitGroup covers the workers and the scheduler, so shutdown can wait
	// for in-flight jobs to finish instead of killing them mid-run.
	var wg sync.WaitGroup
	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			worker(ctx, id, q)
		}(i)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		scheduler(ctx, q)
	}()

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	srv := &http.Server{Addr: ":" + port, Handler: routes(q)}
	go func() {
		// ErrServerClosed is the expected result of a graceful shutdown.
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server: %v", err)
		}
	}()
	log.Printf("listening on :%s", port)

	<-ctx.Done()
	log.Println("shutdown signal received")

	// A fresh context here: ctx is already cancelled, so reusing it would abort
	// the shutdown it's meant to bound.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	srv.Shutdown(shutdownCtx)

	// Stop accepting new requests first, then wait for the workers to drain.
	wg.Wait()
	log.Println("all workers stopped, exiting")
}

// routes builds the HTTP handler: job submission, queue stats, and the static
// status page.
func routes(q *queue.Queue) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /jobs", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Type    string `json:"type"`
			Payload string `json:"payload"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}

		j := queue.Job{
			ID:        uuid.NewString(),
			Type:      req.Type,
			Payload:   req.Payload,
			CreatedAt: time.Now(),
		}
		if err := q.Enqueue(r.Context(), j); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// 202, not 201: the job is queued, not done. The body carries the
		// generated ID back to the caller.
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(j)
	})

	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) {
		s, err := q.Stats(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(s)
	})

	// Everything else is the status page, which polls /stats once a second.
	mux.Handle("GET /", http.FileServer(http.Dir("static")))
	return mux
}

// worker pulls jobs off the queue and runs them until the context is cancelled.
// Each job ends in exactly one of three states: acked on success, rescheduled
// on failure, or dead-lettered once attempts run out.
func worker(ctx context.Context, id int, q *queue.Queue) {
	for {
		// Check for shutdown between jobs. The blocking Dequeue below also
		// returns when the context is cancelled, so this is the fast path.
		select {
		case <-ctx.Done():
			log.Printf("worker %d: stopping", id)
			return
		default:
		}

		// The 2s timeout keeps this loop responsive: an idle worker wakes up
		// regularly to re-check the shutdown signal.
		job, raw, err := q.Dequeue(ctx, 2*time.Second)
		if err != nil {
			// A cancelled context surfaces here as a Redis error. That's a
			// shutdown, not a fault.
			if ctx.Err() != nil {
				log.Printf("worker %d: stopping", id)
				return
			}
			// A real Redis error: back off briefly rather than spinning.
			log.Printf("worker %d: dequeue failed: %v", id, err)
			time.Sleep(time.Second)
			continue
		}
		if job == nil {
			// Timed out on an empty queue.
			continue
		}

		log.Printf("worker %d: processing %s (type=%s)", id, job.ID, job.Type)
		if err := process(job); err != nil {
			// Attempts is the count before this failure, hence the +1.
			log.Printf("worker %d: job %s failed (attempt %d): %v", id, job.ID, job.Attempts+1, err)

			// Retry handles the queue mechanics and reports whether the job
			// died, so the dead-letter transition gets logged here rather than
			// inside the queue package.
			dead, rerr := q.Retry(ctx, *job, raw)
			if rerr != nil {
				log.Printf("worker %d: retry failed: %v", id, rerr)
			} else if dead {
				log.Printf("worker %d: job %s exhausted retries, moved to dead letter", id, job.ID)
			}
			continue
		}

		// Ack last: until this lands the job is still in the processing list
		// and would be recovered by a restart.
		if err := q.Ack(ctx, raw); err != nil {
			log.Printf("worker %d: ack failed: %v", id, err)
		}
		log.Printf("worker %d: done %s", id, job.ID)
	}
}

// process stands in for real work. It sleeps to make concurrency observable on
// the status page and fails 30% of the time to exercise the retry and
// dead-letter paths.
func process(j *queue.Job) error {
	time.Sleep(2 * time.Second)
	if rand.Float64() < 0.3 {
		return fmt.Errorf("simulated failure")
	}
	return nil
}

// scheduler moves delayed jobs back to pending once their backoff expires.
//
// This exists so retries don't block a worker. An earlier version slept inside
// the worker instead, which held a pool slot doing nothing and made the pool
// effectively smaller during every backoff.
func scheduler(ctx context.Context, q *queue.Queue) {
	// One second is well under the shortest backoff (2s), so a job waits at
	// most a second longer than scheduled.
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("scheduler: stopping")
			return
		case <-ticker.C:
			n, err := q.PromoteDue(ctx)
			if err != nil {
				// Suppress the error if it's just the context being cancelled.
				if ctx.Err() == nil {
					log.Printf("scheduler: promote failed: %v", err)
				}
				continue
			}
			if n > 0 {
				log.Printf("scheduler: promoted %d delayed jobs", n)
			}
		}
	}
}
