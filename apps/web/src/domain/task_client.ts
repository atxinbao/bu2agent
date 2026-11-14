// Task record client for the TS Domain Store (task loop).
//
// Same shape as the gateway task endpoints, in the same order:
// create a task for a directory, list tasks, get one task,
// update a task's status (and, on completion, its artifact
// path). The gateway persists records to JSONL when
// MOCK_TASKS_PATH is set; otherwise they live in memory only.
//
// Task statuses, from the design doc: created -> draft_ready
// -> completed, plus rejected and failed. The client does not
// enforce the transitions; the view drives them step by step.

export interface TaskExecuteBinding {
  locationId: string;
  worktreeId: string;
  rootDigest: string;
}

export interface TaskRecord {
  taskId: string;
  directory: string;
  artifactPath: string | null;
  status: string;
  // Runner-only fields (Round 3b); manual tasks carry null.
  error: string | null;
  approvalId: string | null;
  draftSha256: string | null;
  draftBytes: number | null;
  // D1-1 link fields (FU-2, Q-R0-03); manual tasks carry null.
  executeBinding: TaskExecuteBinding | null;
  goalId: string | null;
  projectId: string | null;
  workspaceId: string | null;
}

// A gateway-run task's draft, fetched separately: the record
// never carries the body, only its digest and byte count.
export interface TaskDraft {
  taskId: string;
  draft: string;
  draftSha256: string | null;
  draftBytes: number | null;
}

interface ServerTaskExecuteBinding {
  location_id: string;
  worktree_id: string;
  root_digest: string;
}

interface ServerTaskRecord {
  task_id: string;
  directory: string;
  artifact_path: string | null;
  status: string;
  error: string | null;
  approval_id: string | null;
  draft_sha256: string | null;
  draft_bytes: number | null;
  execute_binding: ServerTaskExecuteBinding | null;
  goal_id: string | null;
  project_id: string | null;
  workspace_id: string | null;
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
    executeBinding: server.execute_binding
      ? {
          locationId: server.execute_binding.location_id,
          worktreeId: server.execute_binding.worktree_id,
          rootDigest: server.execute_binding.root_digest,
        }
      : null,
    goalId: server.goal_id,
    projectId: server.project_id,
    workspaceId: server.workspace_id,
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

// Transport-neutral surface used by the Tasks view. The HTTP
// client below and the invoke client both implement it, so the
// view never knows which transport it runs on.
export interface TaskGateway {
  create(directory: string): Promise<TaskRecord>;
  list(): Promise<TaskRecord[]>;
  get(taskId: string): Promise<TaskRecord>;
  update(
    taskId: string,
    status: string,
    artifactPath?: string | null,
  ): Promise<TaskRecord>;
  // Round 3b runner endpoints. run only starts the runner;
  // draft fetches the sidecar body; execute consumes the bound
  // approval and writes; reject decides it rejected; resume
  // restarts an interrupted task; cancel stops a running or
  // draft_ready task.
  run(taskId: string): Promise<TaskRecord>;
  draft(taskId: string): Promise<TaskDraft>;
  execute(taskId: string): Promise<TaskRecord>;
  reject(taskId: string): Promise<TaskRecord>;
  resume(taskId: string): Promise<TaskRecord>;
  cancel(taskId: string): Promise<TaskRecord>;
}

export class TaskClient implements TaskGateway {
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

  async create(directory: string): Promise<TaskRecord> {
    const record = await this.request<ServerTaskRecord>("/tasks/create", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ directory }),
    });
    return toRecord(record);
  }

  async list(): Promise<TaskRecord[]> {
    const records = await this.request<ServerTaskRecord[]>("/tasks/list");
    return records.map(toRecord);
  }

  async get(taskId: string): Promise<TaskRecord> {
    const record = await this.request<ServerTaskRecord>(
      `/tasks/get?task_id=${taskId}`,
    );
    return toRecord(record);
  }

  async update(
    taskId: string,
    status: string,
    artifactPath?: string | null,
  ): Promise<TaskRecord> {
    const record = await this.request<ServerTaskRecord>("/tasks/update", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        task_id: taskId,
        status,
        artifact_path: artifactPath ?? null,
      }),
    });
    return toRecord(record);
  }

  private async postTaskId(path: string, taskId: string): Promise<TaskRecord> {
    const record = await this.request<ServerTaskRecord>(path, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ task_id: taskId }),
    });
    return toRecord(record);
  }

  async run(taskId: string): Promise<TaskRecord> {
    return this.postTaskId("/tasks/run", taskId);
  }

  async draft(taskId: string): Promise<TaskDraft> {
    const draft = await this.request<ServerTaskDraft>(
      `/tasks/draft?task_id=${taskId}`,
    );
    return toDraft(draft);
  }

  async execute(taskId: string): Promise<TaskRecord> {
    return this.postTaskId("/tasks/execute", taskId);
  }

  async reject(taskId: string): Promise<TaskRecord> {
    return this.postTaskId("/tasks/reject", taskId);
  }

  async resume(taskId: string): Promise<TaskRecord> {
    return this.postTaskId("/tasks/resume", taskId);
  }

  async cancel(taskId: string): Promise<TaskRecord> {
    return this.postTaskId("/tasks/cancel", taskId);
  }
}
