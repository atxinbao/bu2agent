// Goal client for the TS Domain Store (D1-1, minimal
// list/detail): create, list, get, archive.
//
// Same shape as the other domain clients: the gateway
// returns snake_case; the domain shape is camelCase, so
// the mapping lives here, not in the view.

export interface Goal {
  goalId: string;
  title: string;
  description: string;
  status: string;
  createdMs: number;
  updatedMs: number;
  originDeviceId: string;
  schemaVersion: number;
}

interface ServerGoal {
  goal_id: string;
  title: string;
  description: string;
  status: string;
  created_ms: number;
  updated_ms: number;
  origin_device_id: string;
  schema_version: number;
}

function toGoal(server: ServerGoal): Goal {
  return {
    goalId: server.goal_id,
    title: server.title,
    description: server.description,
    status: server.status,
    createdMs: server.created_ms,
    updatedMs: server.updated_ms,
    originDeviceId: server.origin_device_id,
    schemaVersion: server.schema_version,
  };
}

// Transport-neutral surface used by the Goals view. The HTTP
// client below and the invoke client both implement it, so the
// view never knows which transport it runs on.
export interface GoalGateway {
  listGoals(): Promise<Goal[]>;
  getGoal(goalId: string): Promise<Goal>;
  createGoal(title: string, description: string | undefined): Promise<Goal>;
  archiveGoal(goalId: string): Promise<Goal>;
}

const ALLOWED_HOSTS = ["127.0.0.1", "localhost"];

function checkUrl(baseUrl: string): void {
  const stripped = baseUrl.replace(/^http:\/\//, "").replace(/\/$/, "");
  const host = stripped.split(":")[0];
  if (!baseUrl.startsWith("http://") || !ALLOWED_HOSTS.includes(host)) {
    throw new Error("egress refused: destination is not on the Mock allowlist");
  }
}

export class GoalClient implements GoalGateway {
  private readonly baseUrl: string;

  constructor(baseUrl: string) {
    checkUrl(baseUrl);
    this.baseUrl = baseUrl.replace(/\/$/, "");
  }

  private async request<T>(method: string, path: string, body?: unknown): Promise<T> {
    const response = await fetch(`${this.baseUrl}${path}`, {
      method,
      headers: { "Content-Type": "application/json" },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    if (!response.ok) {
      throw new Error(`mock gateway returned HTTP ${response.status}`);
    }
    return (await response.json()) as T;
  }

  async listGoals(): Promise<Goal[]> {
    return (await this.request<ServerGoal[]>("GET", "/goals/list")).map(toGoal);
  }

  async getGoal(goalId: string): Promise<Goal> {
    return toGoal(await this.request<ServerGoal>("GET", `/goals/${goalId}`));
  }

  async createGoal(title: string, description: string | undefined): Promise<Goal> {
    return toGoal(
      await this.request<ServerGoal>("POST", "/goals/create", { title, description }),
    );
  }

  async archiveGoal(goalId: string): Promise<Goal> {
    return toGoal(await this.request<ServerGoal>("POST", `/goals/${goalId}/archive`, {}));
  }
}
