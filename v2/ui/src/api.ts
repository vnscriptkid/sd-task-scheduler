export type TaskStatus = "scheduled" | "picked" | "running" | "completed";

export type Task = {
  id: string;
  function: string;
  scheduled_at: string;
  picked_at?: string | null;
  started_at?: string | null;
  completed_at?: string | null;
  status: TaskStatus;
};

export type Metrics = {
  now: string;
  broker: { depth: number };
  by_status: Record<TaskStatus, number>;
};

const BASE = import.meta.env.VITE_API_BASE ?? "http://localhost:8080";

export async function createTask(input: { function: string; scheduled_at: string }): Promise<Task> {
  const res = await fetch(`${BASE}/api/tasks`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(input),
  });
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

export async function listTasks(params?: { limit?: number; status?: string }): Promise<Task[]> {
  const usp = new URLSearchParams();
  if (params?.limit) usp.set("limit", String(params.limit));
  if (params?.status) usp.set("status", params.status);
  const res = await fetch(`${BASE}/api/tasks?${usp.toString()}`);
  if (!res.ok) throw new Error(await res.text());
  const data = await res.json();
  return Array.isArray(data) ? data : [];
}

export async function getMetrics(): Promise<Metrics> {
  const res = await fetch(`${BASE}/api/metrics`);
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

export function openEventStream(onEvent: (type: string, payload: any) => void) {
  const es = new EventSource(`${BASE}/api/events`);

  es.addEventListener("hello", () => {
    onEvent("hello", { ok: true });
  });

  // Generic handler for any custom event types:
  const handler = (e: MessageEvent) => {
    try {
      const parsed = JSON.parse(e.data);
      onEvent((e as any).type ?? "message", parsed);
    } catch {
      onEvent("message", e.data);
    }
  };

  // We don’t know all event names ahead; attach a few we do:
  const known = [
    "task.scheduled",
    "picker.claimed",
    "broker.enqueued",
    "broker.dequeued",
    "executor.started",
    "executor.completed",
  ];
  known.forEach((name) => es.addEventListener(name, handler));

  es.onerror = () => {
    onEvent("sse.error", { note: "SSE connection error (will auto-retry)" });
  };

  return () => es.close();
}