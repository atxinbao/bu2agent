// Execution record client for the TS Domain Store.
//
// Execution records only: list, get, transition. The list/get
// routes return the P0-E read view: the stored record plus
// the derived recoverable / activeAssignmentId fields. The
// transition route returns the bare stored record, so the
// derived fields are null/false on that path. The client does
// not pre-judge the edge table: illegal edges are refused by
// the server with 409.

export interface ExecutionRecord {
  executionId: string;
  taskId: string | null;
  loopId: string | null;
  sessionId: string | null;
  workerSessionId: string | null;
  workspaceSnapshotId: string | null;
  status: string;
  originDeviceId: string;
  schemaVersion: number;
  // P0-E derived fields (list/get only): non-terminal status
  // with no active assignment driving it, and the driving
  // assignment id when one exists.
  recoverable: boolean;
  activeAssignmentId: string | null;
}

interface ServerExecutionRecord {
  execution_id: string;
  task_id: string | null;
  loop_id: string | null;
  session_id: string | null;
  worker_session_id: string | null;
  workspace_snapshot_id: string | null;
  status: string;
  origin_device_id: string;
  schema_version: number;
  recoverable?: boolean;
  active_assignment_id?: string | null;
}

function toRecord(server: ServerExecutionRecord): ExecutionRecord {
  return {
    executionId: server.execution_id,
    taskId: server.task_id,
    loopId: server.loop_id,
    sessionId: server.session_id,
    workerSessionId: server.worker_session_id,
    workspaceSnapshotId: server.workspace_snapshot_id,
    status: server.status,
    originDeviceId: server.origin_device_id,
    schemaVersion: server.schema_version,
    recoverable: server.recoverable ?? false,
    activeAssignmentId: server.active_assignment_id ?? null,
  };
}

// Transport-neutral surface used by the views. The HTTP client
// below and the invoke client both implement it, so the view
// never knows which transport it runs on.
export interface ExecutionsGateway {
  list(): Promise<ExecutionRecord[]>;
  get(executionId: string): Promise<ExecutionRecord>;
  transition(executionId: string, status: string): Promise<ExecutionRecord>;
}

export class ExecutionsClient implements ExecutionsGateway {
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

  async list(): Promise<ExecutionRecord[]> {
    const records = await this.request<ServerExecutionRecord[]>(
      "/executions/list",
    );
    return records.map(toRecord);
  }

  async get(executionId: string): Promise<ExecutionRecord> {
    const record = await this.request<ServerExecutionRecord>(
      `/executions/get?execution_id=${executionId}`,
    );
    return toRecord(record);
  }

  async transition(
    executionId: string,
    status: string,
  ): Promise<ExecutionRecord> {
    const record = await this.request<ServerExecutionRecord>(
      "/executions/transition",
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ execution_id: executionId, status }),
      },
    );
    return toRecord(record);
  }
}
