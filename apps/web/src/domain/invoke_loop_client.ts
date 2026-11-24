import { invoke } from "@tauri-apps/api/core";
import type { ApprovalBinding, LoopGateway, LoopSnapshot } from "./loop_client";

export class InvokeLoopClient implements LoopGateway {
  start(prompt: string, maxCalls: number, budgetMs: number) {
    return invoke<LoopSnapshot>("loop_start", { prompt, max_calls: maxCalls, budget_ms: budgetMs });
  }
  get(loopId: string) { return invoke<LoopSnapshot>("loop_get", { loop_id: loopId }); }
  resume(loopId: string, approvalId?: string, binding?: ApprovalBinding) {
    return invoke<LoopSnapshot>("loop_resume", { loop_id: loopId, approval_id: approvalId ?? null, binding: binding ?? null });
  }
  cancel(loopId: string) { return invoke<LoopSnapshot>("loop_cancel", { loop_id: loopId }); }
  researchStart(taskId: string, worktreeId?: string, maxCalls?: number, budgetMs?: number) {
    return invoke<LoopSnapshot>("research_start", { task_id: taskId, worktree_id: worktreeId ?? null, max_calls: maxCalls ?? null, budget_ms: budgetMs ?? null });
  }
  researchGet(loopId: string) { return invoke<LoopSnapshot>("research_get", { loop_id: loopId }); }
  list() { return invoke<LoopSnapshot[]>("loop_list"); }
}
