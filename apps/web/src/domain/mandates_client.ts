// Mandates client for the TS Domain Store.
//
// Mandate rows only: list, get.
// FU-3C (Q-R0-01): the `/mandates` family had no production
// client; this wires it for the "授权范围" (authorization
// scope) sections in ExecutionDetail and TasksView. Rows live
// in SQLite only (P0-D); without a configured SQLite
// database the routes answer 5xx fail-closed.
//
// `allowedJson` is a JSON string, not an object; the view
// displays it as text (full in the expanded row detail).
// A row binds a task (`taskId`, `executionId` null) or one
// execution to a goal text, an allowed tool set and the policy
// version live at its creation; superseded rows are kept.

export interface MandateRow {
  mandateId: string;
  taskId: string | null;
  executionId: string | null;
  goal: string;
  allowedJson: string;
  policyVersionId: string;
  status: string;
  createdMs: number;
  originDeviceId: string;
}

interface ServerMandateRow {
  mandate_id: string;
  task_id: string | null;
  execution_id: string | null;
  goal: string;
  allowed_json: string;
  policy_version_id: string;
  status: string;
  created_ms: number;
  origin_device_id: string;
}

function toRow(server: ServerMandateRow): MandateRow {
  return {
    mandateId: server.mandate_id,
    taskId: server.task_id,
    executionId: server.execution_id,
    goal: server.goal,
    allowedJson: server.allowed_json,
    policyVersionId: server.policy_version_id,
    status: server.status,
    createdMs: server.created_ms,
    originDeviceId: server.origin_device_id,
  };
}

export interface MandateFilters {
  taskId?: string | null;
  executionId?: string | null;
}

// Transport-neutral surface used by the views. The HTTP client
// below and the invoke client both implement it, so the view
// never knows which transport it runs on.
export interface MandatesGateway {
  list(filters?: MandateFilters): Promise<MandateRow[]>;
  get(mandateId: string): Promise<MandateRow>;
}

export class MandatesClient implements MandatesGateway {
  private readonly baseUrl: string;

  constructor(baseUrl: string) {
    this.baseUrl = baseUrl.replace(/\/$/, "");
  }

  private async request<T>(path: string, init?: RequestInit): Promise<T> {
    const response = await fetch(`${this.baseUrl}${path}`, init);
    if (!response.ok) {
      throw new Error(`mock gateway returned HTTP ${response.status}`);
    }
    return (await response.json()) as T;
  }

  async list(filters?: MandateFilters): Promise<MandateRow[]> {
    const params = new URLSearchParams();
    if (filters?.taskId) {
      params.set("task_id", filters.taskId);
    }
    if (filters?.executionId) {
      params.set("execution_id", filters.executionId);
    }
    const query = params.toString();
    const rows = await this.request<ServerMandateRow[]>(
      query === "" ? "/mandates" : `/mandates?${query}`,
    );
    return rows.map(toRow);
  }

  async get(mandateId: string): Promise<MandateRow> {
    const row = await this.request<ServerMandateRow>(
      `/mandates/${encodeURIComponent(mandateId)}`,
    );
    return toRow(row);
  }
}
