import { invoke } from "@tauri-apps/api/core";
import type { TaskDraft, TaskGateway, TaskRecord } from "./task_client";

// The Rust task record serializes snake_case; the domain shape
// is camelCase, so the mapping lives here, not in the view.
interface ServerTaskRecord {
  task_id: string;
  directory: string;
  artifact_path: string | null;
  status: string;
  error: string | null;
  approval_id: string | null;
  draft_sha256: string | null;
  draft_bytes: number | null;
}

interface ServerTaskDraft {
  task_id: string;
  draft: string;
  draft_sha256: string | null;
  draft_bytes: number | null;
}

function toRecord(server: ServerTaskRecord): TaskRecord {
  return {
    taskId: server.task_id,
    directory: server.directory,
    artifactPath: server.artifact_path,
    status: server.status,
    error: server.error,
    approvalId: server.approval_id,
    draftSha256: server.draft_sha256,
    draftBytes: server.draft_bytes,
  };
}

function toDraft(server: ServerTaskDraft): TaskDraft {
  return {
    taskId: server.task_id,
    draft: server.draft,
    draftSha256: server.draft_sha256,
    draftBytes: server.draft_bytes,
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

  private async postTaskId(command: string, taskId: string): Promise<TaskRecord> {
    const record = await invoke<ServerTaskRecord>(command, {
      task_id: taskId,
    });
    return toRecord(record);
  }

  async run(taskId: string): Promise<TaskRecord> {
    return this.postTaskId("task_run", taskId);
  }

  async draft(taskId: string): Promise<TaskDraft> {
    const draft = await invoke<ServerTaskDraft>("task_draft", {
      task_id: taskId,
    });
    return toDraft(draft);
  }

  async execute(taskId: string): Promise<TaskRecord> {
    return this.postTaskId("task_execute", taskId);
  }

  async reject(taskId: string): Promise<TaskRecord> {
    return this.postTaskId("task_reject", taskId);
  }

  async resume(taskId: string): Promise<TaskRecord> {
    return this.postTaskId("task_resume", taskId);
  }

  async cancel(taskId: string): Promise<TaskRecord> {
    return this.postTaskId("task_cancel", taskId);
  }
}
