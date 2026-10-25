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

export interface TaskRecord {
  taskId: string;
  directory: string;
  artifactPath: string | null;
  status: string;
}

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
}
