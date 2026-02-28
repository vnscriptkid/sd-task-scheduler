import { useEffect, useMemo, useRef, useState } from "react";
import { createTask, getMetrics, listTasks, openEventStream, type Metrics, type Task, type TaskStatus } from "./api";
import "./styles.css";

function fmt(ts?: string | null) {
  if (!ts) return "-";
  const d = new Date(ts);
  return isNaN(d.getTime()) ? ts : d.toLocaleString();
}

function badge(status: TaskStatus) {
  return <span className={`badge badge-${status}`}>{status}</span>;
}

type LiveEvent = {
  at: string;
  type: string;
  payload: any;
};

export default function App() {
  const [fn, setFn] = useState("send_email");
  const [scheduledAt, setScheduledAt] = useState(() => {
    // default: now + 5s
    const d = new Date(Date.now() + 5000);
    // datetime-local expects no timezone; keep it local and convert to ISO later
    const pad = (n: number) => String(n).padStart(2, "0");
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
  });

  const [tasks, setTasks] = useState<Task[]>([]);
  const [statusFilter, setStatusFilter] = useState<string>("");
  const [metrics, setMetrics] = useState<Metrics | null>(null);
  const [error, setError] = useState<string>("");

  const [events, setEvents] = useState<LiveEvent[]>([]);
  const eventsRef = useRef<HTMLDivElement | null>(null);

  const limit = 300;

  const refreshTasks = async () => {
    const data = await listTasks({ limit, status: statusFilter || undefined });
    setTasks(data);
  };

  useEffect(() => {
    refreshTasks().catch((e) => setError(String(e)));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [statusFilter]);

  useEffect(() => {
    const t = setInterval(() => {
      refreshTasks().catch(() => {});
    }, 1500);
    return () => clearInterval(t);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [statusFilter]);

  useEffect(() => {
    const t = setInterval(() => {
      getMetrics().then(setMetrics).catch(() => {});
    }, 1000);
    return () => clearInterval(t);
  }, []);

  useEffect(() => {
    const close = openEventStream((type, payload) => {
      setEvents((prev) => {
        const next = [
          { at: new Date().toLocaleTimeString(), type, payload },
          ...prev,
        ].slice(0, 200);
        return next;
      });

      // optimistic refresh on key events
      if (type !== "hello" && type !== "sse.error") {
        refreshTasks().catch(() => {});
        getMetrics().then(setMetrics).catch(() => {});
      }
    });

    return close;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    // auto scroll to top is already fine since we prepend; but keep it stable
    if (eventsRef.current) eventsRef.current.scrollTop = 0;
  }, [events]);

  const byStatus = useMemo(() => {
    const map: Record<string, number> = { scheduled: 0, picked: 0, running: 0, completed: 0 };
    for (const t of tasks) map[t.status] = (map[t.status] ?? 0) + 1;
    return map;
  }, [tasks]);

  const schedule = async () => {
    setError("");
    // Convert datetime-local (no tz) -> ISO with local offset
    const d = new Date(scheduledAt);
    if (isNaN(d.getTime())) {
      setError("Invalid scheduled time");
      return;
    }
    const iso = d.toISOString(); // backend parses RFC3339; ISO here is UTC but valid
    try {
      await createTask({ function: fn, scheduled_at: iso });
      setFn(fn);
      refreshTasks();
    } catch (e: any) {
      setError(String(e?.message ?? e));
    }
  };

  return (
    <div className="page">
      <header className="header">
        <div>
          <h1>Task Scheduler POC</h1>
          <p className="muted">
            Real-time visibility into picker → broker → executor using SSE.
          </p>
        </div>

        <div className="metrics">
          <div className="metric">
            <div className="metric-label">Broker depth</div>
            <div className="metric-value">{metrics?.broker?.depth ?? "-"}</div>
          </div>
          <div className="metric">
            <div className="metric-label">Scheduled</div>
            <div className="metric-value">{metrics?.by_status?.scheduled ?? byStatus.scheduled}</div>
          </div>
          <div className="metric">
            <div className="metric-label">Picked</div>
            <div className="metric-value">{metrics?.by_status?.picked ?? byStatus.picked}</div>
          </div>
          <div className="metric">
            <div className="metric-label">Running</div>
            <div className="metric-value">{metrics?.by_status?.running ?? byStatus.running}</div>
          </div>
          <div className="metric">
            <div className="metric-label">Completed</div>
            <div className="metric-value">{metrics?.by_status?.completed ?? byStatus.completed}</div>
          </div>
        </div>
      </header>

      {error && <div className="error">{error}</div>}

      <div className="grid">
        <section className="card">
          <h2>Schedule a task</h2>
          <div className="formRow">
            <label>Function</label>
            <input value={fn} onChange={(e) => setFn(e.target.value)} placeholder="send_email" />
          </div>
          <div className="formRow">
            <label>Scheduled at (local)</label>
            <input type="datetime-local" step="1" value={scheduledAt} onChange={(e) => setScheduledAt(e.target.value)} />
          </div>
          <div className="formRow">
            <button onClick={schedule}>Create Task</button>
          </div>

          <div className="hint">
            Tip: schedule tasks within ~30s window around “now”, because picker query uses a ±window.
          </div>
        </section>

        <section className="card">
          <h2>Live events (behind the scenes)</h2>
          <div className="events" ref={eventsRef}>
            {events.map((e, idx) => (
              <div key={idx} className="eventLine">
                <span className="eventAt">{e.at}</span>
                <span className="eventType">{e.type}</span>
                <span className="eventPayload">{JSON.stringify(e.payload)}</span>
              </div>
            ))}
          </div>
        </section>

        <section className="card span2">
          <div className="rowBetween">
            <h2>Tasks</h2>
            <div className="filters">
              <label className="muted">Status:</label>
              <select value={statusFilter} onChange={(e) => setStatusFilter(e.target.value)}>
                <option value="">all</option>
                <option value="scheduled">scheduled</option>
                <option value="picked">picked</option>
                <option value="running">running</option>
                <option value="completed">completed</option>
              </select>
              <button onClick={() => refreshTasks()}>Refresh</button>
            </div>
          </div>

          <div className="tableWrap">
            <table>
              <thead>
                <tr>
                  <th>ID</th>
                  <th>Function</th>
                  <th>Status</th>
                  <th>Scheduled</th>
                  <th>Picked</th>
                  <th>Started</th>
                  <th>Completed</th>
                </tr>
              </thead>
              <tbody>
                {tasks.map((t) => (
                  <tr key={t.id}>
                    <td className="mono">{t.id.slice(0, 8)}…</td>
                    <td>{t.function}</td>
                    <td>{badge(t.status)}</td>
                    <td>{fmt(t.scheduled_at)}</td>
                    <td>{fmt(t.picked_at)}</td>
                    <td>{fmt(t.started_at)}</td>
                    <td>{fmt(t.completed_at)}</td>
                  </tr>
                ))}
                {tasks.length === 0 && (
                  <tr>
                    <td colSpan={7} className="muted">No tasks yet.</td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </section>
      </div>

      <footer className="footer muted">
        API base: <span className="mono">{import.meta.env.VITE_API_BASE ?? "http://localhost:8080"}</span>
      </footer>
    </div>
  );
}