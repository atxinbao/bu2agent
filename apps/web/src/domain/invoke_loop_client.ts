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
}
