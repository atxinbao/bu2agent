import { invoke } from "@tauri-apps/api/core";
import type {
  AssignmentRow,
  AssignmentsGateway,
  CheckpointRow,
  TakeoverOutcome,
} from "./assignments_client";
import type { ExecutionRecord } from "./executions_client";

// The Rust assignment rows serialize snake_case; the domain
// shapes are camelCase, so the mapping lives here, not in
// the view.
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

// Assignment line over Tauri invoke, backed by the Rust
// AssignmentsClient. Invoke argument keys are snake_case to
// match the Rust command parameters exactly.
export class InvokeAssignmentsClient implements AssignmentsGateway {
  async list(): Promise<AssignmentRow[]> {
    const rows = await invoke<ServerAssignmentRow[]>("assignment_list");
    return rows.map(toAssignment);
  }

  async get(assignmentId: string): Promise<AssignmentRow> {
    const row = await invoke<ServerAssignmentRow>("assignment_get", {
      assignment_id: assignmentId,
    });
    return toAssignment(row);
  }

  async takeover(executionId: string): Promise<TakeoverOutcome> {
    const outcome = await invoke<ServerTakeoverOutcome>(
      "assignment_takeover",
      {
        execution_id: executionId,
      },
    );
    return toTakeover(outcome);
  }
}
