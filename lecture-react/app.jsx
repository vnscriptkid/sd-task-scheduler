import { useState } from "react";

const phases = [
  {
    id: 0,
    title: "The Problem",
    subtitle: "What are we building?",
  },
  {
    id: 1,
    title: "V1 — Storage Design",
    subtitle: "Start from what you know: the data",
  },
  {
    id: 2,
    title: "V1 — Task State Machine",
    subtitle: "Every task has a lifecycle",
  },
  {
    id: 3,
    title: "V2 — Naive Architecture",
    subtitle: "One machine does everything",
  },
  {
    id: 4,
    title: "V3 — Separation of Concerns",
    subtitle: "Pickers ≠ Executors",
  },
  {
    id: 5,
    title: "V4 — The Picker Query",
    subtitle: "The magic SQL with SKIP LOCKED",
  },
  {
    id: 6,
    title: "V5 — Auto-scaling Pickers",
    subtitle: "Predictability enables scaling",
  },
  {
    id: 7,
    title: "Final — Full Sequence Diagram",
    subtitle: "End-to-end flow",
  },
  {
    id: 8,
    title: "Go Code Demo",
    subtitle: "Working implementation",
  },
];

// ─── SVG Diagram Components ───

function ProblemDiagram() {
  return (
    <svg viewBox="0 0 700 260" style={{ width: "100%", maxWidth: 700 }}>
      <defs>
        <marker id="ah" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto">
          <polygon points="0 0, 8 3, 0 6" fill="#c084fc" />
        </marker>
      </defs>
      <rect x="20" y="80" width="140" height="60" rx="8" fill="#1e1b2e" stroke="#c084fc" strokeWidth="1.5" />
      <text x="90" y="108" textAnchor="middle" fill="#e2daf5" fontSize="13" fontFamily="monospace">User / Client</text>
      <text x="90" y="126" textAnchor="middle" fill="#9b8abf" fontSize="10" fontFamily="monospace">Submit task</text>

      <line x1="160" y1="110" x2="240" y2="110" stroke="#c084fc" strokeWidth="1.5" markerEnd="url(#ah)" />
      <text x="200" y="100" textAnchor="middle" fill="#9b8abf" fontSize="9" fontFamily="monospace">API call</text>

      <rect x="245" y="60" width="200" height="100" rx="10" fill="#1a1730" stroke="#7c3aed" strokeWidth="2" strokeDasharray="6 3" />
      <text x="345" y="90" textAnchor="middle" fill="#c084fc" fontSize="14" fontWeight="bold" fontFamily="monospace">??? Scheduler ???</text>
      <text x="345" y="112" textAnchor="middle" fill="#9b8abf" fontSize="10" fontFamily="monospace">Store → Pick → Execute</text>
      <text x="345" y="130" textAnchor="middle" fill="#facc15" fontSize="10" fontFamily="monospace">30s SLA guarantee</text>

      <line x1="445" y1="110" x2="530" y2="110" stroke="#c084fc" strokeWidth="1.5" markerEnd="url(#ah)" />
      <text x="487" y="100" textAnchor="middle" fill="#9b8abf" fontSize="9" fontFamily="monospace">execute</text>

      <rect x="535" y="80" width="140" height="60" rx="8" fill="#1e1b2e" stroke="#22c55e" strokeWidth="1.5" />
      <text x="605" y="108" textAnchor="middle" fill="#e2daf5" fontSize="13" fontFamily="monospace">Task Runs</text>
      <text x="605" y="126" textAnchor="middle" fill="#6ee7b7" fontSize="10" fontFamily="monospace">at scheduled time</text>

      <rect x="160" y="190" width="380" height="50" rx="8" fill="#1c1625" stroke="#facc15" strokeWidth="1" />
      <text x="350" y="212" textAnchor="middle" fill="#facc15" fontSize="11" fontFamily="monospace">SLA: Task starts executing within 30s of scheduled_at</text>
      <text x="350" y="228" textAnchor="middle" fill="#9b8abf" fontSize="10" fontFamily="monospace">Minute-level granularity · One-time + Cron · Inspired by Dkron / AWS CloudWatch</text>
    </svg>
  );
}

function StorageDiagram() {
  const fields = [
    { name: "id", type: "UUID", why: "Unique task identifier" },
    { name: "function", type: "TEXT", why: "What to execute (endpoint/handler)" },
    { name: "scheduled_at", type: "TIMESTAMP", why: "When to execute" },
    { name: "picked_at", type: "TIMESTAMP", why: "When a picker claimed it ★" },
    { name: "started_at", type: "TIMESTAMP", why: "When executor began work" },
    { name: "completed_at", type: "TIMESTAMP", why: "When executor finished" },
    { name: "status", type: "ENUM", why: "Derived but convenient for queries" },
  ];
  return (
    <svg viewBox="0 0 700 340" style={{ width: "100%", maxWidth: 700 }}>
      <rect x="100" y="10" width="500" height="320" rx="10" fill="#13111d" stroke="#7c3aed" strokeWidth="2" />
      <text x="350" y="40" textAnchor="middle" fill="#c084fc" fontSize="15" fontWeight="bold" fontFamily="monospace">tasks (MySQL Table)</text>
      <line x1="120" y1="52" x2="580" y2="52" stroke="#3b2d63" strokeWidth="1" />
      {fields.map((f, i) => {
        const y = 72 + i * 38;
        const isNew = f.name === "picked_at";
        return (
          <g key={f.name}>
            <rect x="120" y={y - 12} width="460" height="32" rx="4" fill={isNew ? "#2d1f4e" : "#1a1530"} stroke={isNew ? "#facc15" : "#2d2545"} strokeWidth={isNew ? 1.5 : 0.5} />
            <text x="140" y={y + 8} fill={isNew ? "#facc15" : "#e2daf5"} fontSize="12" fontFamily="monospace" fontWeight="bold">{f.name}</text>
            <text x="300" y={y + 8} fill="#9b8abf" fontSize="11" fontFamily="monospace">{f.type}</text>
            <text x="420" y={y + 8} fill={isNew ? "#facc15" : "#6b5e8a"} fontSize="10" fontFamily="monospace">{f.why}</text>
          </g>
        );
      })}
    </svg>
  );
}

function StateMachineDiagram() {
  return (
    <svg viewBox="0 0 720 200" style={{ width: "100%", maxWidth: 720 }}>
      <defs>
        <marker id="ah2" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto">
          <polygon points="0 0, 8 3, 0 6" fill="#c084fc" />
        </marker>
      </defs>
      {/* States */}
      <rect x="20" y="70" width="120" height="50" rx="25" fill="#1e1b2e" stroke="#94a3b8" strokeWidth="2" />
      <text x="80" y="100" textAnchor="middle" fill="#94a3b8" fontSize="13" fontFamily="monospace">SCHEDULED</text>

      <rect x="200" y="70" width="120" height="50" rx="25" fill="#1e1b2e" stroke="#facc15" strokeWidth="2" />
      <text x="260" y="100" textAnchor="middle" fill="#facc15" fontSize="13" fontFamily="monospace">PICKED</text>

      <rect x="390" y="70" width="120" height="50" rx="25" fill="#1e1b2e" stroke="#f97316" strokeWidth="2" />
      <text x="450" y="100" textAnchor="middle" fill="#f97316" fontSize="13" fontFamily="monospace">RUNNING</text>

      <rect x="570" y="70" width="130" height="50" rx="25" fill="#1e1b2e" stroke="#22c55e" strokeWidth="2" />
      <text x="635" y="100" textAnchor="middle" fill="#22c55e" fontSize="13" fontFamily="monospace">COMPLETED</text>

      {/* Arrows */}
      <line x1="140" y1="95" x2="195" y2="95" stroke="#c084fc" strokeWidth="1.5" markerEnd="url(#ah2)" />
      <text x="167" y="85" textAnchor="middle" fill="#9b8abf" fontSize="8" fontFamily="monospace">picker claims</text>

      <line x1="320" y1="95" x2="385" y2="95" stroke="#c084fc" strokeWidth="1.5" markerEnd="url(#ah2)" />
      <text x="352" y="85" textAnchor="middle" fill="#9b8abf" fontSize="8" fontFamily="monospace">executor starts</text>

      <line x1="510" y1="95" x2="565" y2="95" stroke="#c084fc" strokeWidth="1.5" markerEnd="url(#ah2)" />
      <text x="537" y="85" textAnchor="middle" fill="#9b8abf" fontSize="8" fontFamily="monospace">task finishes</text>

      {/* Timestamps */}
      <text x="80" y="145" textAnchor="middle" fill="#6b5e8a" fontSize="9" fontFamily="monospace">scheduled_at set</text>
      <text x="260" y="145" textAnchor="middle" fill="#6b5e8a" fontSize="9" fontFamily="monospace">picked_at set</text>
      <text x="450" y="145" textAnchor="middle" fill="#6b5e8a" fontSize="9" fontFamily="monospace">started_at set</text>
      <text x="635" y="145" textAnchor="middle" fill="#6b5e8a" fontSize="9" fontFamily="monospace">completed_at set</text>

      <text x="360" y="180" textAnchor="middle" fill="#7c3aed" fontSize="10" fontFamily="monospace">
        Each transition updates exactly one timestamp column
      </text>
    </svg>
  );
}

function NaiveArchDiagram() {
  return (
    <svg viewBox="0 0 620 300" style={{ width: "100%", maxWidth: 620 }}>
      <defs>
        <marker id="ah3" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto">
          <polygon points="0 0, 8 3, 0 6" fill="#c084fc" />
        </marker>
      </defs>
      {/* User */}
      <rect x="20" y="100" width="110" height="50" rx="8" fill="#1e1b2e" stroke="#c084fc" strokeWidth="1.5" />
      <text x="75" y="130" textAnchor="middle" fill="#e2daf5" fontSize="12" fontFamily="monospace">User</text>

      <line x1="130" y1="125" x2="190" y2="125" stroke="#c084fc" strokeWidth="1.5" markerEnd="url(#ah3)" />

      {/* Single Worker */}
      <rect x="195" y="60" width="190" height="130" rx="10" fill="#1a1730" stroke="#ef4444" strokeWidth="2" />
      <text x="290" y="90" textAnchor="middle" fill="#ef4444" fontSize="13" fontWeight="bold" fontFamily="monospace">Single Worker</text>
      <text x="290" y="112" textAnchor="middle" fill="#9b8abf" fontSize="10" fontFamily="monospace">1. Query DB</text>
      <text x="290" y="128" textAnchor="middle" fill="#9b8abf" fontSize="10" fontFamily="monospace">2. Lock rows</text>
      <text x="290" y="144" textAnchor="middle" fill="#9b8abf" fontSize="10" fontFamily="monospace">3. Execute task</text>
      <text x="290" y="160" textAnchor="middle" fill="#9b8abf" fontSize="10" fontFamily="monospace">4. Update status</text>

      <line x1="385" y1="125" x2="440" y2="125" stroke="#c084fc" strokeWidth="1.5" markerEnd="url(#ah3)" />

      {/* DB */}
      <ellipse cx="510" cy="125" rx="70" ry="35" fill="#1e1b2e" stroke="#7c3aed" strokeWidth="1.5" />
      <text x="510" y="122" textAnchor="middle" fill="#c084fc" fontSize="12" fontFamily="monospace">MySQL</text>
      <text x="510" y="138" textAnchor="middle" fill="#9b8abf" fontSize="9" fontFamily="monospace">tasks table</text>

      {/* Problems */}
      <rect x="130" y="220" width="370" height="65" rx="8" fill="#2a1520" stroke="#ef4444" strokeWidth="1" />
      <text x="315" y="242" textAnchor="middle" fill="#ef4444" fontSize="12" fontWeight="bold" fontFamily="monospace">⚠ Problems with V2</text>
      <text x="315" y="260" textAnchor="middle" fill="#fca5a5" fontSize="10" fontFamily="monospace">• GPU tasks block lightweight tasks (no separation)</text>
      <text x="315" y="274" textAnchor="middle" fill="#fca5a5" fontSize="10" fontFamily="monospace">• Can't scale reads and execution independently</text>
    </svg>
  );
}

function SeparatedArchDiagram() {
  return (
    <svg viewBox="0 0 760 380" style={{ width: "100%", maxWidth: 760 }}>
      <defs>
        <marker id="ah4" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto">
          <polygon points="0 0, 8 3, 0 6" fill="#c084fc" />
        </marker>
        <marker id="ah4g" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto">
          <polygon points="0 0, 8 3, 0 6" fill="#22c55e" />
        </marker>
      </defs>

      {/* User */}
      <rect x="10" y="140" width="90" height="45" rx="8" fill="#1e1b2e" stroke="#c084fc" strokeWidth="1.5" />
      <text x="55" y="167" textAnchor="middle" fill="#e2daf5" fontSize="11" fontFamily="monospace">User</text>
      <line x1="100" y1="162" x2="145" y2="162" stroke="#c084fc" strokeWidth="1.5" markerEnd="url(#ah4)" />

      {/* API */}
      <rect x="150" y="140" width="80" height="45" rx="8" fill="#1e1b2e" stroke="#c084fc" strokeWidth="1.5" />
      <text x="190" y="167" textAnchor="middle" fill="#e2daf5" fontSize="11" fontFamily="monospace">API</text>
      <line x1="230" y1="162" x2="275" y2="162" stroke="#c084fc" strokeWidth="1.5" markerEnd="url(#ah4)" />

      {/* DB */}
      <ellipse cx="340" cy="162" rx="55" ry="30" fill="#1e1b2e" stroke="#7c3aed" strokeWidth="1.5" />
      <text x="340" y="158" textAnchor="middle" fill="#c084fc" fontSize="11" fontFamily="monospace">MySQL</text>
      <text x="340" y="172" textAnchor="middle" fill="#9b8abf" fontSize="8" fontFamily="monospace">tasks</text>

      {/* Pickers */}
      <line x1="395" y1="152" x2="440" y2="100" stroke="#facc15" strokeWidth="1.5" markerEnd="url(#ah4)" />
      <line x1="395" y1="162" x2="440" y2="162" stroke="#facc15" strokeWidth="1.5" markerEnd="url(#ah4)" />
      <line x1="395" y1="172" x2="440" y2="224" stroke="#facc15" strokeWidth="1.5" markerEnd="url(#ah4)" />

      {[80, 142, 204].map((y, i) => (
        <g key={i}>
          <rect x="445" y={y} width="95" height="40" rx="6" fill="#1e1b2e" stroke="#facc15" strokeWidth="1.5" />
          <text x="492" y={y + 16} textAnchor="middle" fill="#facc15" fontSize="10" fontFamily="monospace">Picker {i + 1}</text>
          <text x="492" y={y + 30} textAnchor="middle" fill="#9b8abf" fontSize="8" fontFamily="monospace">lean machine</text>
        </g>
      ))}

      {/* Broker */}
      <line x1="540" y1="100" x2="575" y2="162" stroke="#c084fc" strokeWidth="1.5" markerEnd="url(#ah4)" />
      <line x1="540" y1="162" x2="575" y2="162" stroke="#c084fc" strokeWidth="1.5" markerEnd="url(#ah4)" />
      <line x1="540" y1="224" x2="575" y2="162" stroke="#c084fc" strokeWidth="1.5" markerEnd="url(#ah4)" />

      <rect x="580" y="135" width="80" height="55" rx="8" fill="#1a1730" stroke="#7c3aed" strokeWidth="2" />
      <text x="620" y="158" textAnchor="middle" fill="#c084fc" fontSize="11" fontFamily="monospace">Broker</text>
      <text x="620" y="174" textAnchor="middle" fill="#9b8abf" fontSize="8" fontFamily="monospace">SQS/Kafka</text>

      {/* Executors */}
      <line x1="660" y1="152" x2="695" y2="100" stroke="#22c55e" strokeWidth="1.5" markerEnd="url(#ah4g)" />
      <line x1="660" y1="162" x2="695" y2="224" stroke="#22c55e" strokeWidth="1.5" markerEnd="url(#ah4g)" />

      {[80, 204].map((y, i) => (
        <g key={i}>
          <rect x="700" y={y} width="50" height="40" rx="6" fill="#1e1b2e" stroke="#22c55e" strokeWidth="1.5" />
          <text x="725" y={y + 16} textAnchor="middle" fill="#22c55e" fontSize="8" fontFamily="monospace">Exec</text>
          <text x="725" y={y + 30} textAnchor="middle" fill="#6ee7b7" fontSize="7" fontFamily="monospace">GPU/CPU</text>
        </g>
      ))}

      {/* Labels */}
      <rect x="120" y="300" width="520" height="65" rx="8" fill="#162016" stroke="#22c55e" strokeWidth="1" />
      <text x="380" y="322" textAnchor="middle" fill="#22c55e" fontSize="11" fontWeight="bold" fontFamily="monospace">✓ Why separate Pickers & Executors?</text>
      <text x="380" y="340" textAnchor="middle" fill="#bbf7d0" fontSize="9" fontFamily="monospace">Executors are bulky (GPU/high-mem) — don't waste them querying a DB</text>
      <text x="380" y="354" textAnchor="middle" fill="#bbf7d0" fontSize="9" fontFamily="monospace">Pickers are lean — scale independently for high read velocity</text>
    </svg>
  );
}

function PickerQueryDiagram() {
  return (
    <svg viewBox="0 0 740 420" style={{ width: "100%", maxWidth: 740 }}>
      <defs>
        <marker id="ah5" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto">
          <polygon points="0 0, 8 3, 0 6" fill="#facc15" />
        </marker>
      </defs>
      {/* DB rows */}
      <text x="20" y="25" fill="#c084fc" fontSize="13" fontWeight="bold" fontFamily="monospace">MySQL — tasks table (conceptual view)</text>
      {["Task A — 10:00:00 ✓ due", "Task B — 10:00:00 ✓ due", "Task C — 10:00:15 ✓ due", "Task D — 09:58:00 ✗ past SLA", "Task E — 10:05:00 ✗ future"].map((t, i) => {
        const due = t.includes("✓");
        return (
          <g key={i}>
            <rect x="20" y={40 + i * 32} width="360" height="26" rx="4" fill={due ? "#1e2a1e" : "#2a1e1e"} stroke={due ? "#22c55e44" : "#ef444444"} strokeWidth="1" />
            <text x="35" y={58 + i * 32} fill={due ? "#6ee7b7" : "#fca5a5"} fontSize="11" fontFamily="monospace">{t}</text>
          </g>
        );
      })}

      {/* Pickers competing */}
      <text x="430" y="25" fill="#facc15" fontSize="13" fontWeight="bold" fontFamily="monospace">3 Pickers fire simultaneously</text>

      {["Picker 1 → LIMIT 100", "Picker 2 → LIMIT 100", "Picker 3 → LIMIT 100"].map((t, i) => (
        <g key={i}>
          <rect x="430" y={40 + i * 45} width="280" height="36" rx="6" fill="#1e1b2e" stroke="#facc15" strokeWidth="1.5" />
          <text x="570" y={63 + i * 45} textAnchor="middle" fill="#facc15" fontSize="11" fontFamily="monospace">{t}</text>
        </g>
      ))}

      {/* SKIP LOCKED explanation */}
      <rect x="430" y="185" width="280" height="30" rx="6" fill="#2d1f4e" stroke="#c084fc" strokeWidth="1.5" />
      <text x="570" y="205" textAnchor="middle" fill="#c084fc" fontSize="12" fontWeight="bold" fontFamily="monospace">FOR UPDATE SKIP LOCKED</text>

      {/* Arrow explanations */}
      <line x1="500" y1="220" x2="500" y2="260" stroke="#facc15" strokeWidth="1" markerEnd="url(#ah5)" />
      <rect x="420" y="265" width="300" height="130" rx="8" fill="#1a1730" stroke="#3b2d63" strokeWidth="1" />
      <text x="570" y="288" textAnchor="middle" fill="#facc15" fontSize="11" fontWeight="bold" fontFamily="monospace">What SKIP LOCKED does:</text>
      <text x="440" y="310" fill="#e2daf5" fontSize="10" fontFamily="monospace">Picker 1 locks rows 1-100</text>
      <text x="440" y="328" fill="#e2daf5" fontSize="10" fontFamily="monospace">Picker 2 SKIPS 1-100, locks 101-200</text>
      <text x="440" y="346" fill="#e2daf5" fontSize="10" fontFamily="monospace">Picker 3 SKIPS 1-200, locks 201-300</text>
      <text x="440" y="370" fill="#22c55e" fontSize="10" fontFamily="monospace">→ No waiting! 3x throughput!</text>

      {/* Formula */}
      <rect x="20" y="225" width="370" height="80" rx="8" fill="#2a1520" stroke="#ef4444" strokeWidth="1" />
      <text x="205" y="250" textAnchor="middle" fill="#ef4444" fontSize="12" fontWeight="bold" fontFamily="monospace">The Pattern (from Airline Tickets)</text>
      <text x="205" y="272" textAnchor="middle" fill="#fca5a5" fontSize="11" fontFamily="monospace">Fixed Inventory + Contention = Lock</text>
      <text x="205" y="290" textAnchor="middle" fill="#fca5a5" fontSize="10" fontFamily="monospace">SKIP LOCKED breaks the contention</text>

      <rect x="20" y="320" width="370" height="80" rx="8" fill="#162016" stroke="#22c55e" strokeWidth="1" />
      <text x="205" y="345" textAnchor="middle" fill="#22c55e" fontSize="12" fontWeight="bold" fontFamily="monospace">After SELECT → UPDATE in same txn</text>
      <text x="205" y="367" textAnchor="middle" fill="#bbf7d0" fontSize="10" fontFamily="monospace">UPDATE tasks SET picked_at = NOW()</text>
      <text x="205" y="385" textAnchor="middle" fill="#bbf7d0" fontSize="10" fontFamily="monospace">WHERE id IN (...selected ids...)</text>
    </svg>
  );
}

function AutoscaleDiagram() {
  return (
    <svg viewBox="0 0 700 300" style={{ width: "100%", maxWidth: 700 }}>
      <defs>
        <marker id="ah6" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto">
          <polygon points="0 0, 8 3, 0 6" fill="#c084fc" />
        </marker>
      </defs>

      {/* Orchestrator */}
      <rect x="220" y="10" width="250" height="60" rx="10" fill="#1e1b2e" stroke="#7c3aed" strokeWidth="2" />
      <text x="345" y="35" textAnchor="middle" fill="#c084fc" fontSize="13" fontWeight="bold" fontFamily="monospace">Orchestrator / Autoscaler</text>
      <text x="345" y="55" textAnchor="middle" fill="#9b8abf" fontSize="9" fontFamily="monospace">Runs 5 min ahead — forecasts load</text>

      <line x1="345" y1="70" x2="345" y2="105" stroke="#c084fc" strokeWidth="1.5" markerEnd="url(#ah6)" />

      {/* Logic box */}
      <rect x="140" y="110" width="420" height="80" rx="8" fill="#1a1730" stroke="#3b2d63" strokeWidth="1" />
      <text x="350" y="135" textAnchor="middle" fill="#facc15" fontSize="11" fontWeight="bold" fontFamily="monospace">Scaling Formula</text>
      <text x="350" y="155" textAnchor="middle" fill="#e2daf5" fontSize="10" fontFamily="monospace">tasks_in_next_10min = SELECT COUNT(*) WHERE scheduled_at BETWEEN NOW() AND +10m</text>
      <text x="350" y="175" textAnchor="middle" fill="#e2daf5" fontSize="10" fontFamily="monospace">pickers_needed = ceil(tasks_in_next_10min / (100 × picks_per_second))</text>

      {/* Why LIMIT matters */}
      <rect x="50" y="215" width="280" height="75" rx="8" fill="#162016" stroke="#22c55e" strokeWidth="1" />
      <text x="190" y="238" textAnchor="middle" fill="#22c55e" fontSize="11" fontWeight="bold" fontFamily="monospace">Why LIMIT 100 matters here</text>
      <text x="190" y="258" textAnchor="middle" fill="#bbf7d0" fontSize="9" fontFamily="monospace">With LIMIT → predictable query time</text>
      <text x="190" y="273" textAnchor="middle" fill="#bbf7d0" fontSize="9" fontFamily="monospace">Predictable time → reliable capacity math</text>

      <rect x="370" y="215" width="280" height="75" rx="8" fill="#2a1520" stroke="#ef4444" strokeWidth="1" />
      <text x="510" y="238" textAnchor="middle" fill="#ef4444" fontSize="11" fontWeight="bold" fontFamily="monospace">Without LIMIT</text>
      <text x="510" y="258" textAnchor="middle" fill="#fca5a5" fontSize="9" fontFamily="monospace">1 task? 1ms. 1M tasks? 10 minutes.</text>
      <text x="510" y="273" textAnchor="middle" fill="#fca5a5" fontSize="9" fontFamily="monospace">Unpredictable → can't autoscale</text>
    </svg>
  );
}

function SequenceDiagram() {
  const actors = [
    { x: 80, label: "User", color: "#c084fc" },
    { x: 210, label: "API", color: "#c084fc" },
    { x: 340, label: "MySQL", color: "#7c3aed" },
    { x: 470, label: "Picker", color: "#facc15" },
    { x: 600, label: "Broker", color: "#c084fc" },
    { x: 700, label: "Executor", color: "#22c55e" },
  ];
  const messages = [
    { from: 0, to: 1, label: "POST /tasks", y: 70 },
    { from: 1, to: 2, label: "INSERT task", y: 95 },
    { from: 2, to: 1, label: "OK (id)", y: 120, dashed: true },
    { from: 1, to: 0, label: "201 Created", y: 145, dashed: true },
    { from: 3, to: 2, label: "SELECT...SKIP LOCKED", y: 185, note: "every few seconds" },
    { from: 2, to: 3, label: "rows (batch)", y: 210, dashed: true },
    { from: 3, to: 2, label: "UPDATE picked_at", y: 235 },
    { from: 3, to: 4, label: "enqueue tasks", y: 260 },
    { from: 5, to: 4, label: "poll / consume", y: 295 },
    { from: 4, to: 5, label: "task payload", y: 320, dashed: true },
    { from: 5, to: 2, label: "UPDATE started_at", y: 345 },
    { from: 5, to: 5, label: "execute(fn)", y: 370, self: true },
    { from: 5, to: 2, label: "UPDATE completed_at", y: 395 },
  ];
  return (
    <svg viewBox="0 0 780 430" style={{ width: "100%", maxWidth: 780 }}>
      <defs>
        <marker id="ah7" markerWidth="7" markerHeight="5" refX="7" refY="2.5" orient="auto">
          <polygon points="0 0, 7 2.5, 0 5" fill="#c084fc" />
        </marker>
        <marker id="ah7d" markerWidth="7" markerHeight="5" refX="7" refY="2.5" orient="auto">
          <polygon points="0 0, 7 2.5, 0 5" fill="#6b5e8a" />
        </marker>
      </defs>
      {/* Actor headers & lifelines */}
      {actors.map((a, i) => (
        <g key={i}>
          <rect x={a.x - 35} y="10" width="70" height="28" rx="5" fill="#1e1b2e" stroke={a.color} strokeWidth="1.5" />
          <text x={a.x} y="29" textAnchor="middle" fill={a.color} fontSize="10" fontFamily="monospace">{a.label}</text>
          <line x1={a.x} y1="38" x2={a.x} y2="420" stroke="#2d2545" strokeWidth="1" strokeDasharray="4 3" />
        </g>
      ))}
      {/* Messages */}
      {messages.map((m, i) => {
        if (m.self) {
          const ax = actors[m.from].x;
          return (
            <g key={i}>
              <path d={`M ${ax} ${m.y} L ${ax + 30} ${m.y} L ${ax + 30} ${m.y + 15} L ${ax} ${m.y + 15}`} fill="none" stroke="#22c55e" strokeWidth="1" markerEnd="url(#ah7)" />
              <text x={ax + 35} y={m.y + 10} fill="#6ee7b7" fontSize="8" fontFamily="monospace">{m.label}</text>
            </g>
          );
        }
        const x1 = actors[m.from].x;
        const x2 = actors[m.to].x;
        const right = x2 > x1;
        return (
          <g key={i}>
            <line x1={x1} y1={m.y} x2={x2} y2={m.y} stroke={m.dashed ? "#6b5e8a" : "#c084fc"} strokeWidth="1" strokeDasharray={m.dashed ? "4 2" : "none"} markerEnd={m.dashed ? "url(#ah7d)" : "url(#ah7)"} />
            <text x={(x1 + x2) / 2} y={m.y - 4} textAnchor="middle" fill={m.dashed ? "#6b5e8a" : "#e2daf5"} fontSize="8" fontFamily="monospace">{m.label}</text>
            {m.note && <text x={(x1 + x2) / 2} y={m.y + 10} textAnchor="middle" fill="#facc1599" fontSize="7" fontFamily="monospace" fontStyle="italic">{m.note}</text>}
          </g>
        );
      })}
    </svg>
  );
}

// ─── Content sections ───

const content = {
  0: () => (
    <div>
      <ProblemDiagram />
      <h3 style={styles.h3}>Problem Statement</h3>
      <p style={styles.p}>
        Build a <strong>distributed task scheduler</strong> where anyone can schedule a task for execution at a specific time.
        The system must guarantee a <strong>30-second SLA</strong> — if a task is scheduled for 10:00:00, it must start executing before 10:00:30.
      </p>
      <h3 style={styles.h3}>Constraints & Scope</h3>
      <p style={styles.p}>
        <strong>Minute-level granularity</strong> — users schedule at hour:minute, not seconds. This is standard for systems like AWS CloudWatch Events and Dkron (our inspiration).
        We start with <strong>one-time fixed-time execution</strong> only; cron support is an extension we'll add later.
        We <strong>skip retries</strong> — they're a wrapper on top of the core system, not the hard problem.
      </p>
      <h3 style={styles.h3}>The 3-Word Hint</h3>
      <div style={styles.hint}>
        <span style={{ color: "#facc15", fontSize: 18, fontWeight: "bold" }}>Store → Pick → Execute</span>
        <p style={{ ...styles.p, marginTop: 8, color: "#bbf7d0" }}>
          Start from the input (storage), think about the output (execution), then solve the middle (picking).
        </p>
      </div>
      <h3 style={styles.h3}>Key Intuition</h3>
      <p style={styles.p}>
        Distributed task scheduler, message broker, and flash sale systems look unrelated — but they share nearly the same design.
        The core challenge is always: <em>how do you pick items from a shared pool under contention with strict latency guarantees?</em>
      </p>
    </div>
  ),
  1: () => (
    <div>
      <StorageDiagram />
      <h3 style={styles.h3}>Decision: "When in doubt, start with storage"</h3>
      <p style={styles.p}>
        The lecture's principle: when you're unclear about architecture, <strong>start with the database schema</strong>. It forces you to think about what data you actually have and need.
      </p>

      <h3 style={styles.h3}>Field-by-Field Evolution</h3>
      <div style={styles.evolution}>
        <div style={styles.evoStep}>
          <span style={styles.evoLabel}>Round 1 — Obvious fields</span>
          <code style={styles.code}>id, function, scheduled_at</code>
          <p style={styles.evoText}>The bare minimum: what task, when to run it. Function could be an HTTP endpoint, a handler name, etc.</p>
        </div>
        <div style={styles.evoStep}>
          <span style={styles.evoLabel}>Round 2 — "Is it running?"</span>
          <code style={styles.code}>+ status (enum: scheduled/running/completed)</code>
          <p style={styles.evoText}>We need lifecycle visibility. But the lecture debates: is status redundant if we have timestamps?</p>
        </div>
        <div style={styles.evoStep}>
          <span style={styles.evoLabel}>Round 3 — SLA timestamps</span>
          <code style={styles.code}>+ started_at, completed_at</code>
          <p style={styles.evoText}>To verify the 30s SLA, we need to know exactly when execution started. <code>completed_at - started_at</code> gives execution duration for capacity planning.</p>
        </div>
        <div style={styles.evoStep}>
          <span style={{ ...styles.evoLabel, color: "#facc15" }}>Round 4 — The missing column ★</span>
          <code style={{ ...styles.code, borderColor: "#facc15" }}>+ picked_at</code>
          <p style={styles.evoText}>This is the one most people miss! When a picker claims a task, it sets <code>picked_at</code>. This lets other pickers <strong>skip already-claimed tasks</strong> — the key to parallel picking.</p>
        </div>
      </div>

      <h3 style={styles.h3}>Why SQL (MySQL)?</h3>
      <p style={styles.p}>
        <strong>Atomicity</strong> — we need row-level locking when multiple pickers compete for tasks.
        <strong>Transactional updates</strong> — SELECT + UPDATE in one transaction guarantees no double-picking.
        NoSQL could work, but SQL's ACID properties make the picking query much simpler to reason about.
      </p>

      <h3 style={styles.h3}>Status: Useful but Derived</h3>
      <p style={styles.p}>
        The lecture concludes status is "ease of life" — helpful for UI filtering and simple queries, but logically redundant.
        If <code>started_at</code> is NULL → task hasn't started. If <code>completed_at</code> is non-null → task is done.
        You <strong>should not rely on status over timestamps</strong> for critical logic, because timestamps are the source of truth.
      </p>
    </div>
  ),
  2: () => (
    <div>
      <StateMachineDiagram />
      <h3 style={styles.h3}>Task Lifecycle</h3>
      <p style={styles.p}>
        Every task goes through exactly four states. Each transition is triggered by a different component of the system, and each sets exactly one timestamp column:
      </p>
      <div style={styles.evolution}>
        <div style={styles.evoStep}>
          <span style={{ ...styles.evoLabel, color: "#94a3b8" }}>SCHEDULED</span>
          <p style={styles.evoText}>User submits task via API. <code>scheduled_at</code> is set. The task sits in MySQL waiting.</p>
        </div>
        <div style={styles.evoStep}>
          <span style={{ ...styles.evoLabel, color: "#facc15" }}>PICKED</span>
          <p style={styles.evoText}>A picker claims the task. <code>picked_at = NOW()</code>. Other pickers will now skip this row.</p>
        </div>
        <div style={styles.evoStep}>
          <span style={{ ...styles.evoLabel, color: "#f97316" }}>RUNNING</span>
          <p style={styles.evoText}>An executor dequeues the task from the broker and begins work. <code>started_at = NOW()</code>.</p>
        </div>
        <div style={styles.evoStep}>
          <span style={{ ...styles.evoLabel, color: "#22c55e" }}>COMPLETED</span>
          <p style={styles.evoText}>Executor finishes. <code>completed_at = NOW()</code>. SLA check: <code>started_at - scheduled_at &lt; 30s</code>.</p>
        </div>
      </div>
      <h3 style={styles.h3}>Why 4 states, not 3?</h3>
      <p style={styles.p}>
        The PICKED state is the crucial addition. Without it, there's no way to prevent two pickers from grabbing the same task. It's the <strong>claim marker</strong> that enables parallel picking without duplication.
      </p>
    </div>
  ),
  3: () => (
    <div>
      <NaiveArchDiagram />
      <h3 style={styles.h3}>V2: The Naive "Single Worker" Approach</h3>
      <p style={styles.p}>
        The simplest architecture: one worker machine queries MySQL, picks tasks, and executes them. This is what you might build first in a prototype.
      </p>
      <h3 style={styles.h3}>Why This Breaks</h3>
      <div style={styles.evolution}>
        <div style={styles.evoStep}>
          <span style={{ ...styles.evoLabel, color: "#ef4444" }}>Problem 1: Resource mismatch</span>
          <p style={styles.evoText}>Some tasks need GPUs. Some need heavy CPU. Some are memory-intensive. A single worker type can't efficiently handle all of these. You'd need GPU machines doing lightweight DB queries — wasteful.</p>
        </div>
        <div style={styles.evoStep}>
          <span style={{ ...styles.evoLabel, color: "#ef4444" }}>Problem 2: Scaling coupling</span>
          <p style={styles.evoText}>If you have 1M tasks at 10:00 AM, you need massive read throughput AND massive execution capacity. But these scale differently — reads are I/O bound, execution is CPU/GPU bound. Coupling them means you can't scale one without the other.</p>
        </div>
        <div style={styles.evoStep}>
          <span style={{ ...styles.evoLabel, color: "#ef4444" }}>Problem 3: Blocking</span>
          <p style={styles.evoText}>A long-running GPU task blocks the worker from picking new tasks. Your 30s SLA goes out the window for queued tasks.</p>
        </div>
      </div>
      <h3 style={styles.h3}>The Insight</h3>
      <p style={styles.p}>
        This leads to the <strong>separation of concerns</strong> principle: don't make the same machine responsible for both reading from DB and executing heavy tasks.
      </p>
    </div>
  ),
  4: () => (
    <div>
      <SeparatedArchDiagram />
      <h3 style={styles.h3}>V3: Pickers ≠ Executors</h3>
      <p style={styles.p}>
        The key architectural decision: split the system into three distinct roles with a message broker in between.
      </p>
      <div style={styles.evolution}>
        <div style={styles.evoStep}>
          <span style={{ ...styles.evoLabel, color: "#facc15" }}>Pickers (Lean machines)</span>
          <p style={styles.evoText}>Small, cheap machines. Their only job: query MySQL, claim tasks, and enqueue them to a broker. They don't execute anything. This means you can have many of them without expensive hardware.</p>
        </div>
        <div style={styles.evoStep}>
          <span style={{ ...styles.evoLabel, color: "#c084fc" }}>Message Broker (SQS/Kafka)</span>
          <p style={styles.evoText}>Decouples picking from execution. Provides buffering — if executors are temporarily slow, tasks queue up rather than being lost. Enables different executor pools for different task types.</p>
        </div>
        <div style={styles.evoStep}>
          <span style={{ ...styles.evoLabel, color: "#22c55e" }}>Executors (Bulky machines)</span>
          <p style={styles.evoText}>GPU/high-CPU/high-memory machines. They just consume from the broker and execute. No DB querying. You can have GPU executors, CPU executors, memory-optimized executors — each consuming from different queues.</p>
        </div>
      </div>
      <h3 style={styles.h3}>Trade-offs</h3>
      <p style={styles.p}>
        <strong>Pro:</strong> Independent scaling. Pickers scale with task count, executors scale with task heaviness. Resource-appropriate machines for each role.
      </p>
      <p style={styles.p}>
        <strong>Con:</strong> More moving parts. The broker adds a hop of latency (usually negligible). You need to manage three component types instead of one.
      </p>
    </div>
  ),
  5: () => (
    <div>
      <PickerQueryDiagram />
      <h3 style={styles.h3}>The Magic Query</h3>
      <div style={{ ...styles.codeBlock, marginBottom: 16 }}>
        <pre style={{ margin: 0, color: "#e2daf5", fontSize: 12, lineHeight: 1.6 }}>
{`SELECT * FROM tasks
WHERE scheduled_at - INTERVAL 5 SECOND < NOW()
  AND NOW() < scheduled_at + INTERVAL 30 SECOND
  AND picked_at IS NULL
ORDER BY scheduled_at ASC
LIMIT 100
FOR UPDATE SKIP LOCKED`}
        </pre>
      </div>

      <h3 style={styles.h3}>Clause-by-Clause Breakdown</h3>
      <div style={styles.evolution}>
        <div style={styles.evoStep}>
          <span style={styles.evoLabel}>Time window with buffer</span>
          <code style={styles.code}>scheduled_at - 5s &lt; NOW() &lt; scheduled_at + 30s</code>
          <p style={styles.evoText}><strong>Why -5s?</strong> Clock sync issues between machines, plus the task still needs to travel through broker to executor. Picking slightly early gives a buffer. <strong>Why +30s?</strong> That's our SLA — don't bother picking tasks past their deadline.</p>
        </div>
        <div style={styles.evoStep}>
          <span style={{ ...styles.evoLabel, color: "#facc15" }}>picked_at IS NULL</span>
          <p style={styles.evoText}>Only pick tasks that haven't been claimed by another picker. This is why <code>picked_at</code> was the "missing column" — without it, multiple pickers would grab the same task.</p>
        </div>
        <div style={styles.evoStep}>
          <span style={styles.evoLabel}>ORDER BY scheduled_at ASC</span>
          <p style={styles.evoText}>Fairness — execute tasks in the order they were scheduled. Tasks closest to their deadline get picked first.</p>
        </div>
        <div style={styles.evoStep}>
          <span style={{ ...styles.evoLabel, color: "#22c55e" }}>LIMIT 100</span>
          <p style={styles.evoText}><strong>Critical for predictability.</strong> If a picker always reads exactly 100 rows, you know exactly how long each query takes. This makes autoscaling math possible — without LIMIT, query time is unpredictable (1ms for 1 task, 10min for 1M tasks).</p>
        </div>
        <div style={styles.evoStep}>
          <span style={{ ...styles.evoLabel, color: "#c084fc" }}>FOR UPDATE SKIP LOCKED ★★★</span>
          <p style={styles.evoText}>
            <strong>FOR UPDATE</strong> locks the selected rows so other transactions can't modify them.
            <strong>SKIP LOCKED</strong> is the game-changer: instead of waiting for locked rows (which would serialize all pickers), it <em>skips</em> them and grabs the next unlocked rows.
            This is the exact same pattern from airline ticket booking — fixed inventory + contention = locks; SKIP LOCKED breaks the contention.
          </p>
        </div>
      </div>

      <h3 style={styles.h3}>After SELECT → UPDATE in same transaction</h3>
      <div style={{ ...styles.codeBlock, marginBottom: 16 }}>
        <pre style={{ margin: 0, color: "#e2daf5", fontSize: 12 }}>
{`UPDATE tasks SET picked_at = NOW()
WHERE id IN (...selected_task_ids...);
-- then COMMIT; and enqueue to broker`}
        </pre>
      </div>
    </div>
  ),
  6: () => (
    <div>
      <AutoscaleDiagram />
      <h3 style={styles.h3}>How Many Pickers Do We Need?</h3>
      <p style={styles.p}>
        Sometimes zero tasks are scheduled. Sometimes millions. The number of pickers must auto-scale.
        An <strong>orchestrator process</strong> runs ahead of time (e.g., every 5 minutes) and forecasts load.
      </p>
      <div style={styles.evolution}>
        <div style={styles.evoStep}>
          <span style={styles.evoLabel}>Step 1: Count upcoming tasks</span>
          <code style={styles.code}>SELECT COUNT(*) FROM tasks WHERE scheduled_at BETWEEN NOW() AND NOW() + 10min</code>
          <p style={styles.evoText}>Look ahead 10 minutes to see how many tasks are coming.</p>
        </div>
        <div style={styles.evoStep}>
          <span style={styles.evoLabel}>Step 2: Calculate pickers needed</span>
          <p style={styles.evoText}>If each picker handles 100 tasks/query and does ~10 queries/second, one picker can handle ~1000 tasks/second. For 3M tasks in 10 minutes, you need roughly <code>3M / (1000 × 600s) ≈ 5 pickers</code>. The exact math depends on your measured query latency — which is predictable because of <code>LIMIT 100</code>.</p>
        </div>
        <div style={styles.evoStep}>
          <span style={styles.evoLabel}>Step 3: Scale proactively</span>
          <p style={styles.evoText}>Spin up pickers <strong>before</strong> the load hits. This is proactive scaling, not reactive. You know what's coming because tasks are pre-scheduled.</p>
        </div>
      </div>
      <h3 style={styles.h3}>Why LIMIT 100 Is the Enabler</h3>
      <p style={styles.p}>
        Without <code>LIMIT</code>, query time depends on how many rows match — could be 1ms or 10 minutes. With <code>LIMIT 100</code>, query time is always roughly the same (e.g., 5-15ms). This predictability is what makes the capacity formula work. Without it, autoscaling is guesswork.
      </p>
    </div>
  ),
  7: () => (
    <div>
      <SequenceDiagram />
      <h3 style={styles.h3}>End-to-End Flow</h3>
      <div style={styles.evolution}>
        <div style={styles.evoStep}>
          <span style={styles.evoLabel}>Phase 1 — Submit</span>
          <p style={styles.evoText}>User POSTs to API. API inserts row into MySQL with <code>scheduled_at</code> set. Returns task ID. Task is now in SCHEDULED state.</p>
        </div>
        <div style={styles.evoStep}>
          <span style={{ ...styles.evoLabel, color: "#facc15" }}>Phase 2 — Pick</span>
          <p style={styles.evoText}>Pickers poll MySQL every few seconds with the SKIP LOCKED query. Claim a batch of 100 tasks, set <code>picked_at</code>, enqueue to broker. Task is now PICKED.</p>
        </div>
        <div style={styles.evoStep}>
          <span style={{ ...styles.evoLabel, color: "#22c55e" }}>Phase 3 — Execute</span>
          <p style={styles.evoText}>Executor consumes from broker, updates <code>started_at</code>, runs the task function, then updates <code>completed_at</code>. SLA measured as <code>started_at - scheduled_at</code>.</p>
        </div>
      </div>
      <h3 style={styles.h3}>Summary of All Design Decisions</h3>
      <table style={styles.table}>
        <thead>
          <tr>
            <th style={styles.th}>Decision</th>
            <th style={styles.th}>Why</th>
          </tr>
        </thead>
        <tbody>
          {[
            ["MySQL over NoSQL", "Need ACID transactions for row-level locking during task claiming"],
            ["picked_at column", "Enables parallel picking — the claim marker other pickers check"],
            ["Separate pickers/executors", "Different resource profiles; independent scaling"],
            ["Message broker between them", "Decoupling; buffering; enables executor heterogeneity"],
            ["LIMIT 100", "Predictable query time → enables autoscaling math"],
            ["FOR UPDATE SKIP LOCKED", "Parallel pickers without contention — 3 pickers = 3x throughput"],
            ["-5s buffer on time window", "Account for clock skew + broker transit time"],
            ["ORDER BY scheduled_at", "Fairness — oldest due tasks get picked first"],
            ["Proactive autoscaling", "Tasks are pre-scheduled; look ahead and scale before load hits"],
          ].map(([d, w], i) => (
            <tr key={i}>
              <td style={styles.td}>{d}</td>
              <td style={styles.tdLight}>{w}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  ),
  8: () => (
    <div>
      <h3 style={styles.h3}>Go Implementation — Core Picker Loop</h3>
      <p style={styles.p}>A working Go demo showing the picker's core loop with <code>SKIP LOCKED</code>, batch claiming, and broker enqueuing.</p>
      <div style={{ ...styles.codeBlock, maxHeight: "none" }}>
        <pre style={{ margin: 0, color: "#e2daf5", fontSize: 11.5, lineHeight: 1.55, whiteSpace: "pre", overflowX: "auto" }}>
{`package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// ═══════════════════════════════════════════════
//  Domain Types — matches our evolved schema
// ═══════════════════════════════════════════════

type TaskStatus string

const (
	StatusScheduled TaskStatus = "scheduled"
	StatusPicked    TaskStatus = "picked"
	StatusRunning   TaskStatus = "running"
	StatusCompleted TaskStatus = "completed"
)

type Task struct {
	ID          string     \`json:"id"\`
	Function    string     \`json:"function"\`
	ScheduledAt time.Time  \`json:"scheduled_at"\`
	PickedAt    *time.Time \`json:"picked_at,omitempty"\`
	StartedAt   *time.Time \`json:"started_at,omitempty"\`
	CompletedAt *time.Time \`json:"completed_at,omitempty"\`
	Status      TaskStatus \`json:"status"\`
}

// ═══════════════════════════════════════════════
//  Broker Interface — decouples picker from
//  execution. Could be SQS, Kafka, Redis, etc.
// ═══════════════════════════════════════════════

type Broker interface {
	Enqueue(ctx context.Context, tasks []Task) error
	Dequeue(ctx context.Context) (*Task, error)
}

// ═══════════════════════════════════════════════
//  Picker — the core component from the lecture.
//  Lean machine: query DB → claim → enqueue.
// ═══════════════════════════════════════════════

type Picker struct {
	db         *sql.DB
	broker     Broker
	batchSize  int           // LIMIT N — key for predictability
	pollInterval time.Duration
	bufferSec  int           // the -5s early buffer
}

func NewPicker(db *sql.DB, broker Broker) *Picker {
	return &Picker{
		db:           db,
		broker:       broker,
		batchSize:    100,             // LIMIT 100
		pollInterval: 2 * time.Second, // poll every 2s
		bufferSec:    5,               // pick 5s early
	}
}

// PickBatch executes the magic query from the lecture:
//   SELECT ... WHERE time_window AND picked_at IS NULL
//   ORDER BY scheduled_at LIMIT 100
//   FOR UPDATE SKIP LOCKED
//
// Then in the SAME transaction: UPDATE picked_at = NOW()
func (p *Picker) PickBatch(ctx context.Context) ([]Task, error) {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	// ── Step 1: SELECT with SKIP LOCKED ──
	// The query evolved through the lecture:
	//   v1: just scheduled_at < now          (too simple)
	//   v2: + time window with SLA           (better)
	//   v3: + picked_at IS NULL              (no double-pick)
	//   v4: + LIMIT + ORDER BY + SKIP LOCKED (final)
	query := \`
		SELECT id, function, scheduled_at, status
		FROM tasks
		WHERE scheduled_at - INTERVAL ? SECOND < NOW()
		  AND NOW() < scheduled_at + INTERVAL 30 SECOND
		  AND picked_at IS NULL
		ORDER BY scheduled_at ASC
		LIMIT ?
		FOR UPDATE SKIP LOCKED
	\`

	rows, err := tx.QueryContext(ctx, query, p.bufferSec, p.batchSize)
	if err != nil {
		return nil, fmt.Errorf("select tasks: %w", err)
	}
	defer rows.Close()

	var tasks []Task
	var ids []interface{}
	for rows.Next() {
		var t Task
		if err := rows.Scan(&t.ID, &t.Function, &t.ScheduledAt, &t.Status); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		tasks = append(tasks, t)
		ids = append(ids, t.ID)
	}

	if len(tasks) == 0 {
		return nil, nil // nothing to pick
	}

	// ── Step 2: UPDATE picked_at in same transaction ──
	// This is the "claim" — other pickers will skip these rows
	placeholders := buildPlaceholders(len(ids))
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

	log.Printf("[picker] claimed %d tasks", len(tasks))
	return tasks, nil
}

// Run starts the picker's polling loop
func (p *Picker) Run(ctx context.Context) error {
	log.Println("[picker] starting poll loop")
	ticker := time.NewTicker(p.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			tasks, err := p.PickBatch(ctx)
			if err != nil {
				log.Printf("[picker] error: %v", err)
				continue
			}
			if len(tasks) > 0 {
				// Enqueue to broker for executors to consume
				if err := p.broker.Enqueue(ctx, tasks); err != nil {
					log.Printf("[picker] broker enqueue error: %v", err)
					// TODO: in production, mark tasks as unpicked
				}
			}
		}
	}
}

// ═══════════════════════════════════════════════
//  Executor — bulky machine that runs tasks.
//  Consumes from broker, updates started_at
//  and completed_at.
// ═══════════════════════════════════════════════

type Executor struct {
	db     *sql.DB
	broker Broker
}

func NewExecutor(db *sql.DB, broker Broker) *Executor {
	return &Executor{db: db, broker: broker}
}

func (e *Executor) Run(ctx context.Context) error {
	log.Println("[executor] waiting for tasks...")
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			task, err := e.broker.Dequeue(ctx)
			if err != nil || task == nil {
				time.Sleep(500 * time.Millisecond)
				continue
			}

			// Mark as running
			_, err = e.db.ExecContext(ctx,
				"UPDATE tasks SET started_at = NOW(), status = 'running' WHERE id = ?",
				task.ID,
			)
			if err != nil {
				log.Printf("[executor] failed to mark running: %v", err)
				continue
			}

			// ── Execute the actual task ──
			log.Printf("[executor] running task %s (fn=%s)", task.ID, task.Function)
			err = executeFunction(ctx, task.Function)

			// Mark as completed
			status := StatusCompleted
			if err != nil {
				log.Printf("[executor] task %s failed: %v", task.ID, err)
				// In production: retry logic, dead-letter queue, etc.
			}
			_, _ = e.db.ExecContext(ctx,
				"UPDATE tasks SET completed_at = NOW(), status = ? WHERE id = ?",
				status, task.ID,
			)
			log.Printf("[executor] task %s completed", task.ID)
		}
	}
}

func executeFunction(ctx context.Context, fn string) error {
	// Placeholder — in reality, this would invoke the
	// actual task: HTTP call, gRPC, shell command, etc.
	time.Sleep(100 * time.Millisecond)
	return nil
}

// ═══════════════════════════════════════════════
//  Schema setup — the evolved table from lecture
// ═══════════════════════════════════════════════

const createTableSQL = \`
CREATE TABLE IF NOT EXISTS tasks (
    id           VARCHAR(36) PRIMARY KEY,
    function     TEXT NOT NULL,
    scheduled_at DATETIME NOT NULL,
    picked_at    DATETIME NULL,
    started_at   DATETIME NULL,
    completed_at DATETIME NULL,
    status       ENUM('scheduled','picked','running','completed')
                 NOT NULL DEFAULT 'scheduled',

    -- Index for the picker query: time range + not picked
    INDEX idx_picker (scheduled_at, picked_at)
);
\`

// ═══════════════════════════════════════════════
//  Helpers
// ═══════════════════════════════════════════════

func buildPlaceholders(n int) string {
	s := ""
	for i := 0; i < n; i++ {
		if i > 0 { s += "," }
		s += "?"
	}
	return s
}

// ═══════════════════════════════════════════════
//  Simple in-memory broker (for demo only)
//  In production: SQS, Kafka, RabbitMQ, etc.
// ═══════════════════════════════════════════════

type InMemoryBroker struct {
	ch chan Task
}

func NewInMemoryBroker(size int) *InMemoryBroker {
	return &InMemoryBroker{ch: make(chan Task, size)}
}

func (b *InMemoryBroker) Enqueue(_ context.Context, tasks []Task) error {
	for _, t := range tasks {
		b.ch <- t
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

// ═══════════════════════════════════════════════
//  Main — wire everything together
// ═══════════════════════════════════════════════

func main() {
	db, err := sql.Open("mysql", "user:pass@tcp(localhost:3306)/scheduler")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(createTableSQL); err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	broker := NewInMemoryBroker(10000)

	// Start 3 pickers (lean machines — in prod, separate processes)
	for i := 0; i < 3; i++ {
		go NewPicker(db, broker).Run(ctx)
	}

	// Start 2 executors (bulky machines — in prod, separate hosts)
	for i := 0; i < 2; i++ {
		go NewExecutor(db, broker).Run(ctx)
	}

	log.Println("[main] scheduler running. Ctrl+C to stop.")
	select {} // block forever
}`}
        </pre>
      </div>

      <h3 style={styles.h3}>Key Design Patterns in Code</h3>
      <div style={styles.evolution}>
        <div style={styles.evoStep}>
          <span style={styles.evoLabel}>Broker interface</span>
          <p style={styles.evoText}>Decoupled via interface — swap InMemoryBroker for SQS/Kafka without changing picker or executor code.</p>
        </div>
        <div style={styles.evoStep}>
          <span style={styles.evoLabel}>Transaction boundary</span>
          <p style={styles.evoText}>SELECT + UPDATE in a single <code>tx</code>. If the commit fails, no tasks are claimed — atomicity prevents orphaned claims.</p>
        </div>
        <div style={styles.evoStep}>
          <span style={styles.evoLabel}>Composite index</span>
          <p style={styles.evoText}><code>idx_picker (scheduled_at, picked_at)</code> covers the WHERE clause of the picker query. Without this index, every poll would do a full table scan.</p>
        </div>
      </div>
    </div>
  ),
};

// ─── Styles ───

const styles = {
  h3: { color: "#c084fc", fontFamily: "'JetBrains Mono', monospace", fontSize: 16, marginTop: 24, marginBottom: 8, letterSpacing: "-0.02em" },
  p: { color: "#b8b0cc", fontFamily: "'JetBrains Mono', monospace", fontSize: 12.5, lineHeight: 1.7, marginBottom: 12 },
  hint: { background: "#162016", border: "1px solid #22c55e", borderRadius: 8, padding: "12px 16px", marginBottom: 16 },
  evolution: { display: "flex", flexDirection: "column", gap: 10, marginBottom: 16 },
  evoStep: { background: "#13111d", border: "1px solid #2d2545", borderRadius: 8, padding: "10px 14px" },
  evoLabel: { color: "#c084fc", fontFamily: "monospace", fontSize: 12, fontWeight: "bold", display: "block", marginBottom: 4 },
  evoText: { color: "#9b8abf", fontFamily: "monospace", fontSize: 11, lineHeight: 1.6, margin: "4px 0 0 0" },
  code: { display: "inline-block", background: "#1a1730", border: "1px solid #3b2d63", borderRadius: 4, padding: "2px 8px", color: "#e2daf5", fontFamily: "monospace", fontSize: 11, margin: "4px 0" },
  codeBlock: { background: "#0d0b14", border: "1px solid #2d2545", borderRadius: 8, padding: "14px 16px", overflowX: "auto", marginTop: 8 },
  table: { width: "100%", borderCollapse: "collapse", marginTop: 8, fontFamily: "monospace", fontSize: 11 },
  th: { textAlign: "left", padding: "8px 10px", borderBottom: "2px solid #3b2d63", color: "#c084fc", fontSize: 11 },
  td: { padding: "7px 10px", borderBottom: "1px solid #1e1b2e", color: "#e2daf5", fontSize: 11 },
  tdLight: { padding: "7px 10px", borderBottom: "1px solid #1e1b2e", color: "#9b8abf", fontSize: 11 },
};

// ─── Main App ───

export default function App() {
  const [phase, setPhase] = useState(0);

  return (
    <div style={{ minHeight: "100vh", background: "#0a0914", color: "#e2daf5", fontFamily: "'JetBrains Mono', monospace" }}>
      {/* Header */}
      <div style={{ borderBottom: "1px solid #1e1b2e", padding: "20px 24px" }}>
        <h1 style={{ fontSize: 22, color: "#c084fc", margin: 0, letterSpacing: "-0.03em" }}>
          Distributed Job Scheduler
        </h1>
        <p style={{ color: "#6b5e8a", fontSize: 12, margin: "4px 0 0" }}>
          Design evolution from lecture — Store → Pick → Execute
        </p>
      </div>

      {/* Phase nav */}
      <div style={{ display: "flex", gap: 0, overflowX: "auto", borderBottom: "1px solid #1e1b2e", background: "#0d0b14" }}>
        {phases.map((p) => (
          <button
            key={p.id}
            onClick={() => setPhase(p.id)}
            style={{
              background: phase === p.id ? "#1e1b2e" : "transparent",
              border: "none",
              borderBottom: phase === p.id ? "2px solid #c084fc" : "2px solid transparent",
              color: phase === p.id ? "#c084fc" : "#6b5e8a",
              padding: "10px 14px",
              fontFamily: "monospace",
              fontSize: 11,
              cursor: "pointer",
              whiteSpace: "nowrap",
              transition: "all 0.15s",
            }}
          >
            <span style={{ opacity: 0.5, marginRight: 4 }}>{p.id}.</span>
            {p.title}
          </button>
        ))}
      </div>

      {/* Phase header */}
      <div style={{ padding: "16px 24px 0", borderBottom: "1px solid #1e1b2e" }}>
        <div style={{ display: "flex", alignItems: "baseline", gap: 12 }}>
          <span style={{ color: "#7c3aed", fontSize: 28, fontWeight: "bold" }}>
            {String(phase).padStart(2, "0")}
          </span>
          <div>
            <h2 style={{ margin: 0, fontSize: 18, color: "#e2daf5", letterSpacing: "-0.02em" }}>
              {phases[phase].title}
            </h2>
            <p style={{ margin: "2px 0 12px", color: "#6b5e8a", fontSize: 12 }}>
              {phases[phase].subtitle}
            </p>
          </div>
        </div>
      </div>

      {/* Content */}
      <div style={{ padding: "16px 24px 40px", maxWidth: 800 }}>
        {content[phase]()}
      </div>

      {/* Nav buttons */}
      <div style={{ position: "fixed", bottom: 0, left: 0, right: 0, background: "#0a0914ee", backdropFilter: "blur(8px)", borderTop: "1px solid #1e1b2e", padding: "10px 24px", display: "flex", justifyContent: "space-between" }}>
        <button
          onClick={() => setPhase(Math.max(0, phase - 1))}
          disabled={phase === 0}
          style={{ background: "#1e1b2e", border: "1px solid #3b2d63", color: phase === 0 ? "#3b2d63" : "#c084fc", padding: "8px 20px", borderRadius: 6, fontFamily: "monospace", fontSize: 12, cursor: phase === 0 ? "default" : "pointer" }}
        >
          ← Previous
        </button>
        <span style={{ color: "#3b2d63", fontSize: 11, alignSelf: "center" }}>
          {phase + 1} / {phases.length}
        </span>
        <button
          onClick={() => setPhase(Math.min(phases.length - 1, phase + 1))}
          disabled={phase === phases.length - 1}
          style={{ background: phase === phases.length - 1 ? "#1e1b2e" : "#7c3aed", border: "none", color: phase === phases.length - 1 ? "#3b2d63" : "#fff", padding: "8px 20px", borderRadius: 6, fontFamily: "monospace", fontSize: 12, cursor: phase === phases.length - 1 ? "default" : "pointer" }}
        >
          Next →
        </button>
      </div>
    </div>
  );
}
