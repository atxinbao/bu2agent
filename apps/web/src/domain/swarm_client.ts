// Swarm client: browser/dev -> Mock Gateway ORC-5
// swarm surface (create + fanout + read).
//
// Same shape as the other domain clients: the gateway
// returns snake_case; the domain shape is camelCase, so
// the mapping lives here, not in the view. The client
// never requests or decides an approval: the step
// approvals are built gateway-side by the sweeper
// drive, and the user decides them through the
// existing approval surface.
//
// One step in the swarm view (the gateway
// `SwarmStepView` shape, camel-cased): the step's live
// run status plus its approval's live status. The
// SQLite-only content fields never leave the gateway,
// so they are not here.
export interface SwarmStepView {
  stepNo: number;
  workerId: string;
  worktreeId: string | null;
  runId: string | null;
  runStatus: string;
  approvalId: string | null;
  approvalStatus: string | null;
}

// One board row in the swarm view (the gateway
// `SwarmMessageView` shape, camel-cased): the board
// text is never stored — only its sha256.
export interface SwarmMessageView {
  messageId: string;
  senderWorkerId: string;
  kind: string;
  bodyHash: string;
  createdMs: number;
}

// The swarm view (the gateway `SwarmView` shape,
// camel-cased): the swarm row + step views + the
// latest board rows + worktree records still present
// after the swarm ended.
export interface Swarm {
  swarmId: string;
  taskId: string;
  planId: string;
  status: string;
  steps: SwarmStepView[];
  messages: SwarmMessageView[];
  leftoverWorktrees: string[];
  createdMs: number;
  updatedMs: number;
}

// A swarm fan-out request: the plan steps to fan out
// (1..=4, each `file_write` / `command`) and an
// optional priority for the copy add-runs.
export interface SwarmFanoutInput {
  stepNos: number[];
  priority: number | null;
}

// Transport-neutral surface used by the view. The
// HTTP client below and the invoke client both
// implement it, so the view never knows which
// transport it runs on.
export interface SwarmGateway {
  createSwarm(taskId: string): Promise<Swarm>;
  fanoutSwarm(swarmId: string, input: SwarmFanoutInput): Promise<Swarm>;
  swarm(swarmId: string): Promise<Swarm>;
}

interface ServerSwarmStepView {
  step_no: number;
  worker_id: string;
  worktree_id?: string | null;
  run_id?: string | null;
  run_status: string;
  approval_id?: string | null;
  approval_status?: string | null;
}

interface ServerSwarmMessageView {
  message_id: string;
  sender_worker_id: string;
  kind: string;
  body_hash: string;
  created_ms: number;
}

interface ServerSwarm {
  swarm_id: string;
  task_id: string;
  plan_id: string;
  status: string;
  steps: ServerSwarmStepView[];
  messages: ServerSwarmMessageView[];
  leftover_worktrees: string[];
  created_ms: number;
  updated_ms: number;
}

export function toSwarm(server: ServerSwarm): Swarm {
  return {
    swarmId: server.swarm_id,
    taskId: server.task_id,
    planId: server.plan_id,
    status: server.status,
    steps: server.steps.map((step) => ({
      stepNo: step.step_no,
      workerId: step.worker_id,
      worktreeId: step.worktree_id ?? null,
      runId: step.run_id ?? null,
      runStatus: step.run_status,
      approvalId: step.approval_id ?? null,
      approvalStatus: step.approval_status ?? null,
    })),
    messages: server.messages.map((message) => ({
      messageId: message.message_id,
      senderWorkerId: message.sender_worker_id,
      kind: message.kind,
      bodyHash: message.body_hash,
      createdMs: message.created_ms,
    })),
    leftoverWorktrees: server.leftover_worktrees,
    createdMs: server.created_ms,
    updatedMs: server.updated_ms,
  };
}

const ALLOWED_HOSTS = ["127.0.0.1", "localhost"];

function checkUrl(baseUrl: string): void {
  const stripped = baseUrl.replace(/^http:\/\//, "").replace(/\/$/, "");
  const host = stripped.split(":")[0];
  if (!baseUrl.startsWith("http://") || !ALLOWED_HOSTS.includes(host)) {
    throw new Error("swarm gateway URL must be a localhost Mock");
  }
}

export class SwarmClient implements SwarmGateway {
  private readonly baseUrl: string;

  constructor(baseUrl: string) {
    checkUrl(baseUrl);
    this.baseUrl = baseUrl.replace(/\/$/, "");
  }

  private async requestJson<T>(method: string, path: string): Promise<T> {
    const response = await fetch(`${this.baseUrl}${path}`, { method });
    if (!response.ok) {
      const text = await response.text();
      throw new Error(`swarm gateway ${method} ${path} failed: ${response.status} ${text}`);
    }
    return (await response.json()) as T;
  }

  private async postJson<T>(path: string, body: unknown): Promise<T> {
    const response = await fetch(`${this.baseUrl}${path}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!response.ok) {
      const text = await response.text();
      throw new Error(`swarm gateway POST ${path} failed: ${response.status} ${text}`);
    }
    return (await response.json()) as T;
  }

  async createSwarm(taskId: string): Promise<Swarm> {
    const server = await this.postJson<ServerSwarm>("/swarm/create", {
      task_id: taskId,
    });
    return toSwarm(server);
  }

  async fanoutSwarm(swarmId: string, input: SwarmFanoutInput): Promise<Swarm> {
    const server = await this.postJson<ServerSwarm>(`/swarm/${swarmId}/fanout`, {
      step_nos: input.stepNos,
      priority: input.priority,
    });
    return toSwarm(server);
  }

  async swarm(swarmId: string): Promise<Swarm> {
    const server = await this.requestJson<ServerSwarm>("GET", `/swarm/${swarmId}`);
    return toSwarm(server);
  }
}
