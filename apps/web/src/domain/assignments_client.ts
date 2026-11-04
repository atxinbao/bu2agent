// Assignment client for the TS Domain Store.
//
// Assignment rows only: list, get, takeover. Rows live in
// SQLite only (P0-E); reads serve SQLite and shadow modes,
// takeover requires SQLite mode. Takeover names no worker
// session: the gateway creates one for this process, and
// refuses with 409 while any driving assignment exists (no
// double-drive).

import type { ExecutionRecord } from "./executions_client";

export interface AssignmentRow {
  assignmentId: string;
  executionId: string | null;
  workerSessionId: string | null;
  originDeviceId: string;
  status: string;
  detail: string | null;
  leaseExpiresMs: number | null;
  heartbeatMs: number | null;
  schemaVersion: number;
}

export interface CheckpointRow {
  checkpointId: string;
  executionId: string | null;
  executionSeq: number;
  kind: string;
  payloadJson: string;
  originDeviceId: string;
  schemaVersion: number;
}

// Successful takeover outcome, verbatim from the gateway:
// the new driving assignment, the execution row after its
// worker update, the plan-position checkpoint written at
// takeover, and any old driving assignments superseded
// (normally empty: validation refuses when one exists).
export interface TakeoverOutcome {
  assignment: AssignmentRow;
  execution: ExecutionRecord;
  checkpoint: CheckpointRow;
  superseded: AssignmentRow[];
}

interface ServerAssignmentRow {
  assignment_id: string;
  execution_id: string | null;
  worker_session_id: string | null;
  origin_device_id: string;
  status: string;
  detail: string | null;
  lease_expires_ms: number | null;
  heartbeat_ms: number | null;
  schema_version: number;
}

interface ServerCheckpointRow {
  checkpoint_id: string;
  execution_id: string | null;
  execution_seq: number;
  kind: string;
  payload_json: string;
  origin_device_id: string;
  schema_version: number;
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

interface ServerTakeoverOutcome {
  assignment: ServerAssignmentRow;
  execution: ServerExecutionRecord;
  checkpoint: ServerCheckpointRow;
  superseded: ServerAssignmentRow[];
}

function toAssignment(server: ServerAssignmentRow): AssignmentRow {
  return {
    assignmentId: server.assignment_id,
    executionId: server.execution_id,
    workerSessionId: server.worker_session_id,
    originDeviceId: server.origin_device_id,
    status: server.status,
    detail: server.detail,
    leaseExpiresMs: server.lease_expires_ms,
    heartbeatMs: server.heartbeat_ms,
    schemaVersion: server.schema_version,
  };
}

function toCheckpoint(server: ServerCheckpointRow): CheckpointRow {
  return {
    checkpointId: server.checkpoint_id,
    executionId: server.execution_id,
    executionSeq: server.execution_seq,
    kind: server.kind,
    payloadJson: server.payload_json,
    originDeviceId: server.origin_device_id,
    schemaVersion: server.schema_version,
  };
}

function toExecution(server: ServerExecutionRecord): ExecutionRecord {
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

function toTakeover(server: ServerTakeoverOutcome): TakeoverOutcome {
  return {
    assignment: toAssignment(server.assignment),
    execution: toExecution(server.execution),
    checkpoint: toCheckpoint(server.checkpoint),
    superseded: server.superseded.map(toAssignment),
  };
}

// Transport-neutral surface used by the views. The HTTP client
// below and the invoke client both implement it, so the view
// never knows which transport it runs on.
export interface AssignmentsGateway {
  list(): Promise<AssignmentRow[]>;
  get(assignmentId: string): Promise<AssignmentRow>;
  takeover(executionId: string): Promise<TakeoverOutcome>;
}

export class AssignmentsClient implements AssignmentsGateway {
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

  async list(): Promise<AssignmentRow[]> {
    const rows = await this.request<ServerAssignmentRow[]>(
      "/assignments/list",
    );
    return rows.map(toAssignment);
  }

  async get(assignmentId: string): Promise<AssignmentRow> {
    const row = await this.request<ServerAssignmentRow>(
      `/assignments/get?assignment_id=${assignmentId}`,
    );
    return toAssignment(row);
  }

  async takeover(executionId: string): Promise<TakeoverOutcome> {
    const outcome = await this.request<ServerTakeoverOutcome>(
      "/assignments/takeover",
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          execution_id: executionId,
          worker_session_id: null,
        }),
      },
    );
    return toTakeover(outcome);
  }
}
