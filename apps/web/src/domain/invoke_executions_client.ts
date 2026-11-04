import { invoke } from "@tauri-apps/api/core";
import type {
  ExecutionRecord,
  ExecutionsGateway,
} from "./executions_client";

// The Rust execution view serializes snake_case; the domain
// shape is camelCase, so the mapping lives here, not in
// the view.
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

// Execution line over Tauri invoke, backed by the Rust
// ExecutionsClient. Invoke argument keys are snake_case to
// match the Rust command parameters exactly.
export class InvokeExecutionsClient implements ExecutionsGateway {
  async list(): Promise<ExecutionRecord[]> {
    const records = await invoke<ServerExecutionRecord[]>("execution_list");
    return records.map(toRecord);
  }

  async get(executionId: string): Promise<ExecutionRecord> {
    const record = await invoke<ServerExecutionRecord>("execution_get", {
      execution_id: executionId,
    });
    return toRecord(record);
  }

  async transition(
    executionId: string,
    status: string,
  ): Promise<ExecutionRecord> {
    const record = await invoke<ServerExecutionRecord>(
      "execution_transition",
      {
        execution_id: executionId,
        status,
      },
    );
    return toRecord(record);
  }
}
