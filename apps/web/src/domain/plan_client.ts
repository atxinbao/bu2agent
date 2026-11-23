// Plan client: browser/dev -> Mock Gateway ORC-1 plan
// surface (draft + read).
//
// Same shape as the other domain clients: the gateway
// returns snake_case; the domain shape is camelCase, so
// the mapping lives here, not in the view. ORC-1 has no
// approval flow: drafting a plan is a plain SQLite
// write, not a write through the approval machinery.
// The digest is computed gateway-side; the draft input
// carries no digest field and no execution_id (a draft
// has no Execution yet).

// One blueprint step (the gateway `PlanStep` shape,
// camel-cased). The four-piece set — `path` / `symbol` /
// `change` / `verifyCmd` / `expected` — is required
// (Q-ORC-04); `kind` is one of `file_write` /
// `command` / `verify` / `review`.
export interface PlanStep {
  stepNo: number;
  kind: string;
  path: string;
  symbol: string;
  change: string;
  verifyCmd: string;
  expected: string;
}

// One Re-plan-if condition (the gateway `ReplanIf`
// shape, camel-cased). `signal` is free text; `action`
// has exactly one value, `stop_and_hand_back`
// (Q-ORC-05).
export interface PlanReplanIf {
  signal: string;
  action: string;
}

// One plan version (the gateway `PlanRecord` shape,
// camel-cased).
export interface Plan {
  planId: string;
  taskId: string;
  executionId: string | null;
  version: number;
  status: string;
  steps: PlanStep[];
  replanIf: PlanReplanIf[];
  plannerModel: string;
  digest: string;
  createdMs: number;
  updatedMs: number;
  originDeviceId: string;
}

// A plan draft request. No digest field, no
// execution_id: the gateway computes the digest
// server-side and a draft has no Execution yet.
export interface PlanDraftInput {
  taskId: string;
  steps: PlanStep[];
  replanIf: PlanReplanIf[];
  plannerModel: string | null;
}

// Transport-neutral surface used by the Tasks view.
// The HTTP client below and the invoke client both
// implement it, so the view never knows which transport
// it runs on.
export interface PlanGateway {
  draftPlan(input: PlanDraftInput): Promise<Plan>;
  currentPlan(taskId: string): Promise<Plan>;
  plan(planId: string): Promise<Plan>;
  plans(limit: number | null, offset: number | null): Promise<Plan[]>;
}

interface ServerPlanStep {
  step_no: number;
  kind: string;
  path: string;
  symbol: string;
  change: string;
  verify_cmd: string;
  expected: string;
}

interface ServerPlanReplanIf {
  signal: string;
  action: string;
}

interface ServerPlan {
  plan_id: string;
  task_id: string;
  execution_id: string | null;
  version: number;
  status: string;
  steps: ServerPlanStep[];
  replan_if: ServerPlanReplanIf[];
  planner_model: string;
  digest: string;
  created_ms: number;
  updated_ms: number;
  origin_device_id: string;
}

export function toPlan(server: ServerPlan): Plan {
  return {
    planId: server.plan_id,
    taskId: server.task_id,
    executionId: server.execution_id,
    version: server.version,
    status: server.status,
    steps: server.steps.map((step) => ({
      stepNo: step.step_no,
      kind: step.kind,
      path: step.path,
      symbol: step.symbol,
      change: step.change,
      verifyCmd: step.verify_cmd,
      expected: step.expected,
    })),
    replanIf: server.replan_if.map((condition) => ({
      signal: condition.signal,
      action: condition.action,
    })),
    plannerModel: server.planner_model,
    digest: server.digest,
    createdMs: server.created_ms,
    updatedMs: server.updated_ms,
    originDeviceId: server.origin_device_id,
  };
}

const ALLOWED_HOSTS = ["127.0.0.1", "localhost"];

function checkUrl(baseUrl: string): void {
  const stripped = baseUrl.replace(/^http:\/\//, "").replace(/\/$/, "");
  const host = stripped.split(":")[0];
  if (!baseUrl.startsWith("http://") || !ALLOWED_HOSTS.includes(host)) {
    throw new Error("plan gateway URL must be a localhost Mock");
  }
}

export class PlanClient implements PlanGateway {
  private readonly baseUrl: string;

  constructor(baseUrl: string) {
    checkUrl(baseUrl);
    this.baseUrl = baseUrl.replace(/\/$/, "");
  }

  private async requestJson<T>(method: string, path: string): Promise<T> {
    const response = await fetch(`${this.baseUrl}${path}`, { method });
    if (!response.ok) {
      const text = await response.text();
      throw new Error(`plan gateway ${method} ${path} failed: ${response.status} ${text}`);
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
      throw new Error(`plan gateway POST ${path} failed: ${response.status} ${text}`);
    }
    return (await response.json()) as T;
  }

  async draftPlan(input: PlanDraftInput): Promise<Plan> {
    const server = await this.postJson<ServerPlan>("/tasks/plan/draft", {
      task_id: input.taskId,
      steps: input.steps.map((step) => ({
        step_no: step.stepNo,
        kind: step.kind,
        path: step.path,
        symbol: step.symbol,
        change: step.change,
        verify_cmd: step.verifyCmd,
        expected: step.expected,
      })),
      replan_if: input.replanIf,
      planner_model: input.plannerModel,
    });
    return toPlan(server);
  }

  async currentPlan(taskId: string): Promise<Plan> {
    const server = await this.requestJson<ServerPlan>(
      "GET",
      `/tasks/plan?task_id=${encodeURIComponent(taskId)}`,
    );
    return toPlan(server);
  }

  async plan(planId: string): Promise<Plan> {
    const server = await this.requestJson<ServerPlan>(
      "GET",
      `/tasks/plan/${encodeURIComponent(planId)}`,
    );
    return toPlan(server);
  }

  async plans(limit: number | null, offset: number | null): Promise<Plan[]> {
    const query = new URLSearchParams();
    if (limit !== null) query.set("limit", String(limit));
    if (offset !== null) query.set("offset", String(offset));
    const suffix = query.toString() === "" ? "" : `?${query.toString()}`;
    const servers = await this.requestJson<ServerPlan[]>("GET", `/tasks/plan${suffix}`);
    return servers.map(toPlan);
  }
}
