// main.go
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// ═══════════════════════════════════════════════
// Domain Types
// ═══════════════════════════════════════════════

type TaskStatus string

const (
	StatusScheduled TaskStatus = "scheduled"
	StatusPicked    TaskStatus = "picked"
	StatusRunning   TaskStatus = "running"
	StatusCompleted TaskStatus = "completed"
)

type Task struct {
	ID          string     `json:"id"`
	Function    string     `json:"function"`
	ScheduledAt time.Time  `json:"scheduled_at"`
	PickedAt    *time.Time `json:"picked_at,omitempty"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
	Status      TaskStatus `json:"status"`
}

// ═══════════════════════════════════════════════
// Events + SSE broadcaster
// ═══════════════════════════════════════════════

type Event struct {
	TS   time.Time       `json:"ts"`
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

type Broadcaster struct {
	mu   sync.Mutex
	subs map[chan Event]struct{}
}

func NewBroadcaster() *Broadcaster {
	return &Broadcaster{subs: map[chan Event]struct{}{}}
}

func (b *Broadcaster) Subscribe() chan Event {
	ch := make(chan Event, 200)
	b.mu.Lock()
	b.subs[ch] = struct{}{}
	b.mu.Unlock()
	return ch
}

func (b *Broadcaster) Unsubscribe(ch chan Event) {
	b.mu.Lock()
	delete(b.subs, ch)
	b.mu.Unlock()
	close(ch)
}

func (b *Broadcaster) Publish(typ string, v any) {
	raw, _ := json.Marshal(v)
	ev := Event{TS: time.Now(), Type: typ, Data: raw}

	b.mu.Lock()
	for ch := range b.subs {
		select {
		case ch <- ev:
		default:
			// subscriber too slow, drop event
		}
	}
	b.mu.Unlock()
}

// ═══════════════════════════════════════════════
// Broker Interface
// ═══════════════════════════════════════════════

type Broker interface {
	Enqueue(ctx context.Context, tasks []Task) error
	Dequeue(ctx context.Context) (*Task, error)
	Depth() int
}

// ═══════════════════════════════════════════════
// Picker (lean machine): DB → claim → enqueue
// ═══════════════════════════════════════════════

type Picker struct {
	db           *sql.DB
	broker       Broker
	batchSize    int
	pollInterval time.Duration
	bufferSec    int
	events       *Broadcaster
	pickerID     string
}

func NewPicker(db *sql.DB, broker Broker, events *Broadcaster, pickerID string) *Picker {
	return &Picker{
		db:           db,
		broker:       broker,
		batchSize:    100,
		pollInterval: 2 * time.Second,
		bufferSec:    5,
		events:       events,
		pickerID:     pickerID,
	}
}

func (p *Picker) PickBatch(ctx context.Context) ([]Task, error) {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	// Postgres interval math:
	// scheduled_at - bufferSec < now < scheduled_at + 30s
	query := `
		SELECT id, function, scheduled_at, status, picked_at, started_at, completed_at
		FROM tasks
		WHERE scheduled_at - make_interval(secs => $1) < NOW()
		  AND NOW() < scheduled_at + interval '30 seconds'
		  AND picked_at IS NULL
		ORDER BY scheduled_at ASC
		LIMIT $2
		FOR UPDATE SKIP LOCKED
	`

	rows, err := tx.QueryContext(ctx, query, p.bufferSec, p.batchSize)
	if err != nil {
		return nil, fmt.Errorf("select tasks: %w", err)
	}
	defer rows.Close()

	var tasks []Task
	var ids []any

	for rows.Next() {
		var (
			id uuid.UUID
			t  Task
		)
		if err := rows.Scan(&id, &t.Function, &t.ScheduledAt, &t.Status, &t.PickedAt, &t.StartedAt, &t.CompletedAt); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		t.ID = id.String()
		tasks = append(tasks, t)
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows: %w", err)
	}

	if len(tasks) == 0 {
		return nil, nil
	}

	// Claim in same tx
	placeholders := buildDollarPlaceholders(len(ids)) // $1,$2,...
	updateQuery := fmt.Sprintf(
		"UPDATE tasks SET picked_at = NOW(), status = 'picked' WHERE id IN (%s)",
		placeholders,
	)
	if _, err := tx.ExecContext(ctx, updateQuery, ids...); err != nil {
		return nil, fmt.Errorf("update picked_at: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}

	p.events.Publish("picker.claimed", map[string]any{
		"picker_id": p.pickerID,
		"count":     len(tasks),
		"task_ids":  extractIDs(tasks),
	})

	log.Printf("[picker %s] claimed %d tasks", p.pickerID, len(tasks))
	return tasks, nil
}

func (p *Picker) Run(ctx context.Context) error {
	log.Printf("[picker %s] starting poll loop", p.pickerID)
	ticker := time.NewTicker(p.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			tasks, err := p.PickBatch(ctx)
			if err != nil {
				log.Printf("[picker %s] error: %v", p.pickerID, err)
				continue
			}
			if len(tasks) > 0 {
				if err := p.broker.Enqueue(ctx, tasks); err != nil {
					log.Printf("[picker %s] broker enqueue error: %v", p.pickerID, err)
				}
			}
		}
	}
}

// ═══════════════════════════════════════════════
// Executor (bulky machine): dequeue → run → update
// ═══════════════════════════════════════════════

type Executor struct {
	db     *sql.DB
	broker Broker
	events *Broadcaster
	execID string
}

func NewExecutor(db *sql.DB, broker Broker, events *Broadcaster, execID string) *Executor {
	return &Executor{db: db, broker: broker, events: events, execID: execID}
}

func (e *Executor) Run(ctx context.Context) error {
	log.Printf("[executor %s] waiting for tasks...", e.execID)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			task, err := e.broker.Dequeue(ctx)
			if err != nil || task == nil {
				time.Sleep(200 * time.Millisecond)
				continue
			}

			e.events.Publish("broker.dequeued", map[string]any{
				"executor_id": e.execID,
				"task_id":     task.ID,
				"depth":       e.broker.Depth(),
			})

			// Mark running
			_, err = e.db.ExecContext(ctx,
				"UPDATE tasks SET started_at = NOW(), status = 'running' WHERE id = $1",
				task.ID, // pgx can cast uuid from string
			)
			if err != nil {
				log.Printf("[executor %s] failed to mark running: %v", e.execID, err)
				continue
			}

			e.events.Publish("executor.started", map[string]any{
				"executor_id": e.execID,
				"task_id":     task.ID,
				"function":    task.Function,
			})

			// Execute actual task (POC)
			log.Printf("[executor %s] running task %s (fn=%s)", e.execID, task.ID, task.Function)
			err = executeFunction(ctx, task.Function)

			status := StatusCompleted
			if err != nil {
				log.Printf("[executor %s] task %s failed: %v", e.execID, task.ID, err)
				// POC: still mark completed; extend later with failed/retry
			}

			_, _ = e.db.ExecContext(ctx,
				"UPDATE tasks SET completed_at = NOW(), status = $1 WHERE id = $2",
				status, task.ID,
			)

			e.events.Publish("executor.completed", map[string]any{
				"executor_id": e.execID,
				"task_id":     task.ID,
				"status":      status,
			})

			log.Printf("[executor %s] task %s completed", e.execID, task.ID)
		}
	}
}

func executeFunction(ctx context.Context, fn string) error {
	// Placeholder: do HTTP/gRPC/shell, etc.
	_ = ctx
	_ = fn
	time.Sleep(150 * time.Millisecond)
	return nil
}

// ═══════════════════════════════════════════════
// Postgres Schema
// ═══════════════════════════════════════════════

const createTableSQL = `
CREATE TABLE IF NOT EXISTS tasks (
  id           uuid PRIMARY KEY,
  function     text NOT NULL,
  scheduled_at timestamptz NOT NULL,
  picked_at    timestamptz NULL,
  started_at   timestamptz NULL,
  completed_at timestamptz NULL,
  status       text NOT NULL DEFAULT 'scheduled'
    CHECK (status IN ('scheduled','picked','running','completed'))
);

CREATE INDEX IF NOT EXISTS idx_picker ON tasks (scheduled_at, picked_at);
CREATE INDEX IF NOT EXISTS idx_status ON tasks (status);
`

// ═══════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════

func extractIDs(ts []Task) []string {
	out := make([]string, 0, len(ts))
	for _, t := range ts {
		out = append(out, t.ID)
	}
	return out
}

func buildDollarPlaceholders(n int) string {
	var sb strings.Builder
	for i := 0; i < n; i++ {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(fmt.Sprintf("$%d", i+1))
	}
	return sb.String()
}

// ═══════════════════════════════════════════════
// In-memory broker (instrumented)
// ═══════════════════════════════════════════════

type InMemoryBroker struct {
	ch     chan Task
	events *Broadcaster
}

func NewInMemoryBroker(size int, events *Broadcaster) *InMemoryBroker {
	return &InMemoryBroker{ch: make(chan Task, size), events: events}
}

func (b *InMemoryBroker) Enqueue(_ context.Context, tasks []Task) error {
	for _, t := range tasks {
		b.ch <- t
		b.events.Publish("broker.enqueued", map[string]any{
			"task_id": t.ID,
			"depth":   b.Depth(),
		})
	}
	return nil
}

func (b *InMemoryBroker) Dequeue(_ context.Context) (*Task, error) {
	select {
	case t := <-b.ch:
		return &t, nil
	default:
		return nil, nil
	}
}

func (b *InMemoryBroker) Depth() int { return len(b.ch) }

// ═══════════════════════════════════════════════
// HTTP Server
// ═══════════════════════════════════════════════

type Server struct {
	db     *sql.DB
	events *Broadcaster
	broker Broker
}

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc("/api/tasks", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			s.handleCreateTask(w, r)
		case http.MethodGet:
			s.handleListTasks(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/metrics", s.handleMetrics)
	mux.HandleFunc("/api/events", s.handleEventsSSE)

	// Simple CORS for local dev
	return withCORS(mux)
}

func (s *Server) handleCreateTask(w http.ResponseWriter, r *http.Request) {
	type req struct {
		Function    string `json:"function"`
		ScheduledAt string `json:"scheduled_at"` // RFC3339 / ISO
	}
	var body req
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(body.Function) == "" {
		http.Error(w, "function is required", http.StatusBadRequest)
		return
	}

	sched, err := time.Parse(time.RFC3339, body.ScheduledAt)
	if err != nil {
		http.Error(w, "scheduled_at must be RFC3339 (e.g. 2026-03-01T12:00:05+08:00 or 2026-03-01T04:00:05Z)", http.StatusBadRequest)
		return
	}

	id := uuid.New()
	t := Task{
		ID:          id.String(),
		Function:    body.Function,
		ScheduledAt: sched,
		Status:      StatusScheduled,
	}

	_, err = s.db.Exec(
		"INSERT INTO tasks (id, function, scheduled_at, status) VALUES ($1, $2, $3, 'scheduled')",
		id, t.Function, t.ScheduledAt,
	)
	if err != nil {
		http.Error(w, "db insert failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	s.events.Publish("task.scheduled", t)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(t)
}

func (s *Server) handleListTasks(w http.ResponseWriter, r *http.Request) {
	limit := 200
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 1000 {
			limit = n
		}
	}
	status := r.URL.Query().Get("status")

	q := `SELECT id, function, scheduled_at, picked_at, started_at, completed_at, status FROM tasks`
	args := []any{}
	idx := 1

	if status != "" {
		q += fmt.Sprintf(" WHERE status = $%d", idx)
		args = append(args, status)
		idx++
	}

	q += fmt.Sprintf(" ORDER BY scheduled_at DESC LIMIT $%d", idx)
	args = append(args, limit)

	rows, err := s.db.Query(q, args...)
	if err != nil {
		http.Error(w, "db query failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var out []Task
	for rows.Next() {
		var (
			id uuid.UUID
			t  Task
		)
		if err := rows.Scan(&id, &t.Function, &t.ScheduledAt, &t.PickedAt, &t.StartedAt, &t.CompletedAt, &t.Status); err != nil {
			http.Error(w, "scan failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		t.ID = id.String()
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		http.Error(w, "rows failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	type resp struct {
		Now      time.Time      `json:"now"`
		Broker   map[string]any `json:"broker"`
		ByStatus map[string]int `json:"by_status"`
	}

	byStatus := map[string]int{
		string(StatusScheduled): 0,
		string(StatusPicked):    0,
		string(StatusRunning):   0,
		string(StatusCompleted): 0,
	}

	rows, err := s.db.Query(`SELECT status, COUNT(*) FROM tasks GROUP BY status`)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var st string
			var c int
			_ = rows.Scan(&st, &c)
			byStatus[st] = c
		}
	}

	out := resp{
		Now: time.Now(),
		Broker: map[string]any{
			"depth": s.broker.Depth(),
		},
		ByStatus: byStatus,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

func (s *Server) handleEventsSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ch := s.events.Subscribe()
	defer s.events.Unsubscribe(ch)

	// initial hello
	fmt.Fprintf(w, "event: hello\ndata: %s\n\n", `{"ok":true}`)
	flusher.Flush()

	notify := r.Context().Done()
	for {
		select {
		case <-notify:
			return
		case ev := <-ch:
			payload, _ := json.Marshal(ev)
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev.Type, payload)
			flusher.Flush()
		}
	}
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// ═══════════════════════════════════════════════
// Main
// ═══════════════════════════════════════════════

func main() {
	// Example DSN:
	// postgres://user:pass@localhost:5433/scheduler?sslmode=disable
	dsn := "postgres://postgres:postgres@localhost:5433/scheduler?sslmode=disable"

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Good practice for long-running apps
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(30 * time.Minute)

	if _, err := db.Exec(createTableSQL); err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	events := NewBroadcaster()
	broker := NewInMemoryBroker(10000, events)

	// Start 3 pickers (lean machines)
	for i := 0; i < 3; i++ {
		go NewPicker(db, broker, events, fmt.Sprintf("picker-%d", i+1)).Run(ctx)
	}

	// Start 2 executors (bulky machines)
	for i := 0; i < 2; i++ {
		go NewExecutor(db, broker, events, fmt.Sprintf("exec-%d", i+1)).Run(ctx)
	}

	// HTTP server
	srv := &Server{db: db, events: events, broker: broker}
	go func() {
		addr := ":8080"
		log.Println("[http] listening on", addr)
		if err := http.ListenAndServe(addr, srv.routes()); err != nil {
			log.Fatal(err)
		}
	}()

	log.Println("[main] scheduler running. Ctrl+C to stop.")
	select {}
}
