import { invoke } from "@tauri-apps/api/core";
import type { TaskGateway, TaskRecord } from "./task_client";

// The Rust task record serializes snake_case; the domain shape
// is camelCase, so the mapping lives here, not in the view.
interface ServerTaskRecord {
  task_id: string;
  directory: string;
  artifact_path: string | null;
  status: string;
}

function toRecord(server: ServerTaskRecord): TaskRecord {
  return {
    taskId: server.task_id,
    directory: server.directory,
    artifactPath: server.artifact_path,
    status: server.status,
  };
}

// Task line over Tauri invoke, backed by the Rust TaskClient.
// Invoke argument keys are snake_case to match the Rust
// command parameters exactly.
export class InvokeTaskClient implements TaskGateway {
  async create(directory: string): Promise<TaskRecord> {
    const record = await invoke<ServerTaskRecord>("task_create", { directory });
    return toRecord(record);
  }

  async list(): Promise<TaskRecord[]> {
    const records = await invoke<ServerTaskRecord[]>("task_list");
    return records.map(toRecord);
  }

  async get(taskId: string): Promise<TaskRecord> {
    const record = await invoke<ServerTaskRecord>("task_get", {
      task_id: taskId,
    });
    return toRecord(record);
  }

  async update(
    taskId: string,
    status: string,
    artifactPath?: string | null,
  ): Promise<TaskRecord> {
    const record = await invoke<ServerTaskRecord>("task_update", {
      task_id: taskId,
      status,
      artifact_path: artifactPath ?? null,
    });
    return toRecord(record);
  }
}
