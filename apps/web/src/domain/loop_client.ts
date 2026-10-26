// 循环快照与绑定保留网关 snake_case，HTTP 与 invoke 共用同一份类型。
export type LoopStatus = "created" | "running" | "awaiting_approval" | "completed" | "needs_review" | "failed" | "cancelled" | "interrupted";
export interface ContentDigest { sha256: string; bytes: number }
export interface ApprovalBinding { loop_id: string; call_id: number; tool: string; args_sha256: string }
export interface ResultSummary { status: string; error: string | null; body_digest: ContentDigest; truncated: boolean }
export interface LoopCall {
  call_id: number; tool: string; path: string | null; args_sha256: string;
  content_digest: ContentDigest | null; status: string; error: string | null;
  approval_id: string | null; binding: ApprovalBinding | null; submitted: boolean;
  duplicate_of: number | null; result: ResultSummary | null;
  late: { arrived_at_ms: number; result: ResultSummary } | null;
}
export interface LoopSnapshot {
  loop_id: string; prompt_digest: ContentDigest; model: string; status: LoopStatus;
  error: string | null; max_calls: number; proposal_count: number; model_attempts: number;
  budget_ms: number; used_ms: number; remaining_ms: number; created_at_ms: number; updated_at_ms: number;
  queue_position: number; calls: LoopCall[]; unknown_writes: number[]; unknown_reads: number[];
  late_model_arrivals_ms: number[]; final_digest: ContentDigest | null;
  in_flight: boolean; final_text: string | null;
  pending_write: { call_id: number; path: string; text: string; args_sha256: string } | null;
  approval_status: string | null;
}
export interface LoopGateway {
  start(prompt: string, maxCalls: number, budgetMs: number): Promise<LoopSnapshot>;
  get(loopId: string): Promise<LoopSnapshot>;
  resume(loopId: string, approvalId?: string, binding?: ApprovalBinding): Promise<LoopSnapshot>;
  cancel(loopId: string): Promise<LoopSnapshot>;
  list(): Promise<LoopSnapshot[]>;
}
export function isTerminal(status: LoopStatus): boolean {
  return !["created", "running", "awaiting_approval"].includes(status);
}
export class LoopClient implements LoopGateway {
  constructor(private readonly baseUrl: string) {}
  private async request(path: string, body?: unknown): Promise<LoopSnapshot> {
    // 不自动重试；请求中断后通过 get 同步，不能假定服务端步骤未执行。
    const response = await fetch(`${this.baseUrl.replace(/\/$/, "")}${path}`, {
      method: body === undefined ? "GET" : "POST",
      headers: body === undefined ? undefined : { "Content-Type": "application/json" },
      body: body === undefined ? undefined : JSON.stringify(body),
      signal: AbortSignal.timeout(70_000),
    });
    if (!response.ok) throw new Error(`mock gateway returned HTTP ${response.status}`);
    return await response.json() as LoopSnapshot;
  }
  start(prompt: string, maxCalls: number, budgetMs: number) {
    return this.request("/agent/loop/start", { prompt, max_calls: maxCalls, budget_ms: budgetMs });
  }
  get(loopId: string) { return this.request(`/agent/loop/get?loop_id=${encodeURIComponent(loopId)}`); }
  resume(loopId: string, approvalId?: string, binding?: ApprovalBinding) {
    return this.request("/agent/loop/resume", { loop_id: loopId, approval_id: approvalId ?? null, binding: binding ?? null });
  }
  cancel(loopId: string) { return this.request("/agent/loop/cancel", { loop_id: loopId }); }
  async list(): Promise<LoopSnapshot[]> {
    const response = await fetch(`${this.baseUrl.replace(/\/$/, "")}/agent/loop/list`, {
      signal: AbortSignal.timeout(70_000),
    });
    if (!response.ok) throw new Error(`mock gateway returned HTTP ${response.status}`);
    return await response.json() as LoopSnapshot[];
  }
}
