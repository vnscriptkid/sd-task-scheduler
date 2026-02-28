package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type CreateJobRequest struct {
	RunAt   time.Time       `json:"run_at"`   // RFC3339 e.g. "2026-03-01T10:00:00+08:00"
	JobType string          `json:"job_type"` // e.g. "email", "sync", "print"
	Payload json.RawMessage `json:"payload"`  // arbitrary JSON
}

type Job struct {
	ID      int64
	RunAt   time.Time
	JobType string
	Payload []byte
}

func main() {
	// Example: postgres://postgres:postgres@localhost:5432/scheduler?sslmode=disable
	dsn := getenv("DATABASE_URL", "postgres://postgres:postgres@localhost:5433/scheduler?sslmode=disable")
	workerID := getenv("WORKER_ID", "worker-1")
	addr := getenv("ADDR", ":8080")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("pgxpool.New: %v", err)
	}
	defer pool.Close()

	// small sanity check
	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("db ping: %v", err)
	}

	// HTTP API
	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", func(w http.ResponseWriter, r *http.Request) {
		handleCreateJob(ctx, pool, w, r)
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Worker loop
	workerCtx, workerCancel := context.WithCancel(ctx)
	go func() {
		if err := runWorker(workerCtx, pool, workerID); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("worker stopped: %v", err)
		}
	}()

	// Start server
	go func() {
		log.Printf("http listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("ListenAndServe: %v", err)
		}
	}()

	<-ctx.Done()
	log.Printf("shutting down...")

	workerCancel()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}

func handleCreateJob(ctx context.Context, pool *pgxpool.Pool, w http.ResponseWriter, r *http.Request) {
	var req CreateJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if req.JobType == "" {
		http.Error(w, "job_type required", http.StatusBadRequest)
		return
	}
	if req.RunAt.IsZero() {
		// default: run immediately
		req.RunAt = time.Now()
	}
	if len(req.Payload) == 0 {
		req.Payload = json.RawMessage(`{}`)
	}

	var id int64
	err := pool.QueryRow(ctx, `
		insert into jobs (run_at, job_type, payload)
		values ($1, $2, $3)
		returning id
	`, req.RunAt, req.JobType, req.Payload).Scan(&id)
	if err != nil {
		log.Printf("insert job: %v", err)
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("content-type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"id":     id,
		"status": "pending",
	})
}

func runWorker(ctx context.Context, pool *pgxpool.Pool, workerID string) error {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	log.Printf("worker started: %s", workerID)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Drain due jobs in small batches
			for i := 0; i < 10; i++ { // max 10 jobs per tick (POC throttle)
				job, err := claimOneDueJob(ctx, pool, workerID)
				if err != nil {
					// transient db errors -> log and continue next tick
					log.Printf("claim job error: %v", err)
					break
				}
				if job == nil {
					break // no more due jobs
				}

				if err := executeAndFinalize(ctx, pool, *job); err != nil {
					log.Printf("job %d failed to finalize: %v", job.ID, err)
				}
			}
		}
	}
}

// claimOneDueJob selects 1 due pending job, locks it, and marks as running in one transaction.
func claimOneDueJob(ctx context.Context, pool *pgxpool.Pool, workerID string) (*Job, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Select a due pending job row and lock it
	row := tx.QueryRow(ctx, `
		select id, run_at, job_type, payload
		from jobs
		where status = 'pending'
		  and run_at <= now()
		order by run_at asc, id asc
		for update
		limit 1
	`)

	var j Job
	if err := row.Scan(&j.ID, &j.RunAt, &j.JobType, &j.Payload); err != nil {
		// no rows
		if err.Error() == "no rows in result set" {
			return nil, nil
		}
		return nil, err
	}

	// Mark running + lock metadata
	_, err = tx.Exec(ctx, `
		update jobs
		set status = 'running',
		    locked_at = now(),
		    locked_by = $2
		where id = $1
	`, j.ID, workerID)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &j, nil
}

func executeAndFinalize(ctx context.Context, pool *pgxpool.Pool, job Job) error {
	start := time.Now()
	err := execute(job)
	dur := time.Since(start)

	if err == nil {
		_, uerr := pool.Exec(ctx, `
			update jobs
			set status='succeeded'
			where id=$1
		`, job.ID)
		if uerr != nil {
			return fmt.Errorf("failed to update job status to succeeded: %w", uerr)
		}
		log.Printf("job %d succeeded in %s", job.ID, dur)
		return nil
	}

	_, err = pool.Exec(ctx, `
		update jobs
		set status='failed'
		where id=$1
	`, job.ID)
	if err != nil {
		return fmt.Errorf("failed to update job status to failed: %w", err)
	}
	return nil
}

// execute is where you implement actual job handlers.
// For a POC, we do a few toy job types.
func execute(job Job) error {
	switch job.JobType {
	case "print":
		log.Printf("PRINT job %d payload=%s", job.ID, string(job.Payload))
		return nil

	case "sleep":
		// payload: {"seconds": 2}
		var p struct {
			Seconds int `json:"seconds"`
		}
		_ = json.Unmarshal(job.Payload, &p)
		if p.Seconds <= 0 {
			p.Seconds = 1
		}
		time.Sleep(time.Duration(p.Seconds) * time.Second)
		log.Printf("SLEEP job %d slept %d seconds", job.ID, p.Seconds)
		return nil

	case "fail":
		return fmt.Errorf("intentional failure for job %d", job.ID)

	default:
		// Unknown type => fail (or treat as no-op)
		return fmt.Errorf("unknown job_type=%q", job.JobType)
	}
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
