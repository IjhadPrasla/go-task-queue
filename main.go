package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/IjhadPrasla/go-task-queue/internal/queue"
	"github.com/google/uuid"
)

const numWorkers = 3

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	q := queue.New("localhost:6379")
	if err := q.Ping(ctx); err != nil {
		log.Fatalf("redis unreachable: %v", err)
	}
	log.Println("connected to redis")
	recovered, err := q.Recover(ctx)
	if err != nil {
		log.Printf("recovery failed: %v", err)
	} else if recovered > 0 {
		log.Printf("recovered %d orphaned jobs", recovered)
	}

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

	srv := &http.Server{Addr: ":8080", Handler: routes(q)}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server: %v", err)
		}
	}()
	log.Println("listening on :8080")

	<-ctx.Done()
	log.Println("shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	srv.Shutdown(shutdownCtx)

	wg.Wait()
	log.Println("all workers stopped, exiting")
}

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
	return mux
}

func worker(ctx context.Context, id int, q *queue.Queue) {
	for {
		select {
		case <-ctx.Done():
			log.Printf("worker %d: stopping", id)
			return
		default:
		}

		job, raw, err := q.Dequeue(ctx, 2*time.Second)
		if err != nil {
			if ctx.Err() != nil {
				log.Printf("worker %d: stopping", id)
				return
			}
			log.Printf("worker %d: dequeue failed: %v", id, err)
			time.Sleep(time.Second)
			continue
		}
		if job == nil {
			continue
		}

		log.Printf("worker %d: processing %s (type=%s)", id, job.ID, job.Type)
		if err := process(job); err != nil {
			log.Printf("worker %d: job %s failed (attempt %d): %v", id, job.ID, job.Attempts+1, err)
			if rerr := q.Retry(ctx, *job, raw); rerr != nil {
				log.Printf("worker %d: retry failed: %v", id, rerr)
			}
			continue
		}
		if err := q.Ack(ctx, raw); err != nil {
			log.Printf("worker %d: ack failed: %v", id, err)
		}
		log.Printf("worker %d: done %s", id, job.ID)
	}
}

func process(j *queue.Job) error {
	time.Sleep(2 * time.Second)
	if rand.Float64() < 0.3 {
		return fmt.Errorf("simulated failure")
	}
	return nil
}

func scheduler(ctx context.Context, q *queue.Queue) {
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
