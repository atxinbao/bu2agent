// Timers client: browser/dev -> Mock Gateway AO-2
// timer surface (create + list + revoke).
//
// Same shape as the other domain clients: the gateway
// returns snake_case; the domain shape is camelCase, so
// the mapping lives here, not in the view. A timer is a
// fact row, not an Agent run: creating one only records
// when it should fire. The sweeper drive fires due rows
// by writing an event through AO-1's ingest path — a
// timer NEVER starts an Agent directly.
//
// One timer row (the gateway `AgentTimerView` shape,
// camel-cased): the timer's kind, its fire time, and
// its live status. The payload is a summary object,
// never raw text (red line 7).
export interface Timer {
  timerId: string;
  kind: string;
  payloadSummaryJson: Record<string, unknown>;
  sessionId: string | null;
  status: string;
  nextFireMs: number;
  recurrenceJson: { intervalMs: number } | null;
  createdMs: number;
}

// A timer create request: `kind` is `follow_up`
// (one-shot) or `schedule` (periodic). A schedule
// needs `recurrenceJson` = `{ intervalMs: N }` with
// N > 0; a follow-up must not carry one (AO2-J2).
// `nextFireMs` may be in the past — past means due
// on the first tick (B4).
export interface TimerCreateInput {
  kind: string;
  nextFireMs: number;
  recurrenceJson: { intervalMs: number } | null;
  payloadSummaryJson: Record<string, unknown> | null;
  sessionId: string | null;
}

// Transport-neutral surface used by the view. The
// HTTP client below and the invoke client both
// implement it, so the view never knows which
// transport it runs on.
export interface TimersGateway {
  createTimer(input: TimerCreateInput): Promise<Timer>;
  listTimers(): Promise<Timer[]>;
  deleteTimer(timerId: string): Promise<Timer>;
}

interface ServerTimer {
  timer_id: string;
  kind: string;
  payload_summary_json: Record<string, unknown>;
  session_id?: string | null;
  status: string;
  next_fire_ms: number;
  recurrence_json?: { interval_ms: number } | null;
  created_ms: number;
}

export function toTimer(server: ServerTimer): Timer {
  return {
    timerId: server.timer_id,
    kind: server.kind,
    payloadSummaryJson: server.payload_summary_json ?? {},
    sessionId: server.session_id ?? null,
    status: server.status,
    nextFireMs: server.next_fire_ms,
    recurrenceJson: server.recurrence_json
      ? { intervalMs: server.recurrence_json.interval_ms }
      : null,
    createdMs: server.created_ms,
  };
}

const ALLOWED_HOSTS = ["127.0.0.1", "localhost"];

function checkUrl(baseUrl: string): void {
  const stripped = baseUrl.replace(/^http:\/\//, "").replace(/\/$/, "");
  const host = stripped.split(":")[0];
  if (!baseUrl.startsWith("http://") || !ALLOWED_HOSTS.includes(host)) {
    throw new Error("timers gateway URL must be a localhost Mock");
  }
}

export class TimersClient implements TimersGateway {
  private readonly baseUrl: string;

  constructor(baseUrl: string) {
    checkUrl(baseUrl);
    this.baseUrl = baseUrl.replace(/\/$/, "");
  }

  private async requestJson<T>(method: string, path: string): Promise<T> {
    const response = await fetch(`${this.baseUrl}${path}`, { method });
    if (!response.ok) {
      const text = await response.text();
      throw new Error(`timers gateway ${method} ${path} failed: ${response.status} ${text}`);
    }
    return (await response.json()) as T;
  }

  private async postJson<T>(path: string, body: unknown): Promise<T> {
    const response = await fetch(`${this.baseUrl}${path}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!response.ok) {
      const text = await response.text();
      throw new Error(`timers gateway POST ${path} failed: ${response.status} ${text}`);
    }
    return (await response.json()) as T;
  }

  async createTimer(input: TimerCreateInput): Promise<Timer> {
    const server = await this.postJson<ServerTimer>("/agent/timers", {
      kind: input.kind,
      next_fire_ms: input.nextFireMs,
      recurrence_json: input.recurrenceJson,
      payload_summary_json: input.payloadSummaryJson,
      session_id: input.sessionId,
    });
    return toTimer(server);
  }

  async listTimers(): Promise<Timer[]> {
    const server = await this.requestJson<ServerTimer[]>("GET", "/agent/timers");
    return server.map(toTimer);
  }

  async deleteTimer(timerId: string): Promise<Timer> {
    const server = await this.requestJson<ServerTimer>("DELETE", `/agent/timers/${timerId}`);
    return toTimer(server);
  }
}
