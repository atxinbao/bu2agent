// Invoke plan client: packaged app -> Tauri shell
// plan_* commands (ORC-1 draft + read). Param names
// are snake_case to match the shell's rename_all; the
// invoke transport returns the Rust structs'
// snake_case fields, so the mapping lives here, not in
// the view.
//
// ORC-1 has no approval flow: drafting a plan is a
// plain SQLite write, so this client never requests or
// decides an approval.

import { invoke } from "@tauri-apps/api/core";
import type {
  Plan,
  PlanDraftInput,
  PlanGateway,
} from "./plan_client";
import { toPlan } from "./plan_client";

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
  replan_count: number;
}

export class InvokePlanClient implements PlanGateway {
  async draftPlan(input: PlanDraftInput): Promise<Plan> {
    const server = await invoke<ServerPlan>("plan_draft", {
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
    const server = await invoke<ServerPlan>("plan_current", {
      task_id: taskId,
    });
    return toPlan(server);
  }

  async plan(planId: string): Promise<Plan> {
    const server = await invoke<ServerPlan>("plan_get", {
      plan_id: planId,
    });
    return toPlan(server);
  }

  async replan(planId: string, plannerModel: string | null): Promise<Plan> {
    const server = await invoke<ServerPlan>("plan_replan", {
      plan_id: planId,
      planner_model: plannerModel,
    });
    return toPlan(server);
  }

  async plans(limit: number | null, offset: number | null): Promise<Plan[]> {
    // The shell has no list command: the gateway list
    // is HTTP-only, and the packaged app reads plans
    // per task (currentPlan) or per id (plan). A list
    // through invoke would need a new shell command;
    // until one exists this throws instead of silently
    // returning a partial list.
    void limit;
    void offset;
    throw new Error("plan list is not available through the shell transport");
  }
}
