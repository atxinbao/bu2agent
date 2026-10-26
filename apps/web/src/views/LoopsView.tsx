import { useEffect, useRef, useState } from "react";
import type { AgentGateway } from "../domain/agent_client";
import { isTerminal, type LoopGateway, type LoopSnapshot, type LoopStatus } from "../domain/loop_client";

const LAST_ID = "bu2agent.lastLoopId";
const labels: Record<LoopStatus, string> = {
  created: "已创建", running: "进行中", awaiting_approval: "等待审批", completed: "已完成",
  needs_review: "需要核查", failed: "已失败", cancelled: "已取消", interrupted: "重启中断",
};
function savedId(): string { try { return localStorage.getItem(LAST_ID) ?? ""; } catch { return ""; } }
function errorText(e: unknown): string {
  const text = e instanceof Error ? e.message : String(e);
  if (text.includes("HTTP 503")) return "循环模型不可用：网关未配置模型凭据或使用了 --mock-chat。";
  if (text.includes("HTTP 409")) return "自动运行中，稍后查询即可；这不是失败。";
  if (text.includes("HTTP 403")) return "审批绑定未通过网关复核。请查询状态；不要另建审批绕过。";
  if (text.includes("HTTP 404")) return "没有找到该循环，请检查 ID 以及网关是否启用了持久化。";
  return `响应未确认：${text}。请先查询状态，不要重复发起或批准。`;
}

export function LoopsView({ loops, agent }: { loops: LoopGateway; agent: AgentGateway }) {
  const [prompt, setPrompt] = useState("");
  const [maxCalls, setMaxCalls] = useState(5);
  const [budgetSeconds, setBudgetSeconds] = useState(120);
  const [lookupId, setLookupId] = useState(savedId);
  const [current, setCurrent] = useState<LoopSnapshot | null>(null);
  const [busy, setBusy] = useState(false);
  const [cancelling, setCancelling] = useState(false);
  const [error, setError] = useState("");
  const snapshot = useRef(current);
  const generation = useRef(0);
  const mutation = useRef(false);
  const cancelPending = useRef(false);
  snapshot.current = current;

  function accept(next: LoopSnapshot, token: number) {
    if (generation.current !== token) return;
    const old = snapshot.current;
    if (old?.loop_id === next.loop_id && (old.updated_at_ms > next.updated_at_ms || (isTerminal(old.status) && !isTerminal(next.status)))) return;
    snapshot.current = next;
    setCurrent(next);
    if (!old || old.loop_id !== next.loop_id) setLookupId(next.loop_id);
    // 只保存定位 ID；提示、写入预览、结果和审批决定均不写浏览器存储。
    try { localStorage.setItem(LAST_ID, next.loop_id); } catch { /* 存储不可用不影响当前循环。 */ }
  }
  async function refresh(id = snapshot.current?.loop_id ?? lookupId, token = generation.current) {
    if (!id.trim()) return;
    try {
      const next = await loops.get(id.trim());
      if (!snapshot.current || snapshot.current.loop_id === id.trim()) accept(next, token);
    }
    catch (e) { if (generation.current === token) setError(errorText(e)); }
  }
  useEffect(() => {
    const token = generation.current;
    const id = savedId();
    if (id) void refresh(id, token);
    return () => { generation.current += 1; };
    // 本次挂载只查询上次 ID，不推进、不决定审批。
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [loops]);
  useEffect(() => {
    if (!current || (isTerminal(current.status) && !current.in_flight)) return;
    let stopped = false;
    let timer: ReturnType<typeof setTimeout>;
    const id = current.loop_id;
    const poll = async () => {
      if (stopped) return;
      await refresh(id, generation.current);
      if (!stopped) timer = setTimeout(() => void poll(), 1500);
    };
    timer = setTimeout(() => void poll(), 1500);
    return () => { stopped = true; clearTimeout(timer); };
    // 轮询只查状态；推进由网关驱动器自动进行，审批决定来自用户明确点击。
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [loops, current?.loop_id, current?.status, current?.in_flight]);

  async function run(action: () => Promise<LoopSnapshot>, newSelection = false) {
    if (mutation.current) return;
    mutation.current = true;
    const token = newSelection ? ++generation.current : generation.current;
    setBusy(true); setError("");
    try { accept(await action(), token); }
    catch (e) { if (generation.current === token) setError(errorText(e)); }
    finally { mutation.current = false; setBusy(false); }
  }
  function resume(view: LoopSnapshot) {
    const call = view.calls[view.queue_position];
    return loops.resume(view.loop_id,
      view.status === "awaiting_approval" ? call?.approval_id ?? undefined : undefined,
      view.status === "awaiting_approval" ? call?.binding ?? undefined : undefined);
  }
  async function decide(decision: "approve" | "reject") {
    const view = snapshot.current;
    const call = view?.calls[view.queue_position];
    if (!view || !call?.approval_id || !call.binding || view.approval_status !== "pending") return;
    await run(async () => {
      // 使用网关已有的绑定审批；绝不 requestApproval 创建一张无绑定记录。
      await agent.decideApproval(call.approval_id!, decision);
      return resume(view);
    });
  }
  async function cancel() {
    const view = snapshot.current;
    if (!view || cancelPending.current) return;
    cancelPending.current = true; setCancelling(true); setError("");
    const token = generation.current;
    try { accept(await loops.cancel(view.loop_id), token); }
    catch (e) { if (generation.current === token) setError(errorText(e)); }
    finally { cancelPending.current = false; setCancelling(false); }
  }
  const terminal = current ? isTerminal(current.status) : true;
  const waiting = current?.status === "awaiting_approval";
  const preview = current?.pending_write;
  const call = current?.calls[current.queue_position];
  const previewMatches = !!(preview && call?.binding && preview.call_id === call.call_id && preview.args_sha256 === call.binding.args_sha256);
  const decided = current?.approval_status === "approved" || current?.approval_status === "rejected";

  return <section className="view loop-view">
    <h2>模型工具循环</h2>
    <p>发起后网关自动运行到审批点或终态；审批后继续自动运行，无需一步步点。模型可以列目录、读文本；每次新写入都需你确认。最多 5 次工具提议，执行预算最多 120 秒。</p>
    <label htmlFor="loop-prompt">任务要求</label>
    <textarea id="loop-prompt" value={prompt} onChange={e => setPrompt(e.target.value)} placeholder="例如：读取 demo 下的材料，生成报告到 demo/report.md" disabled={busy || !terminal} />
    <div className="row loop-options">
      <label>提议上限 <input aria-label="提议上限" type="number" min={1} max={5} value={maxCalls} onChange={e => setMaxCalls(Number(e.target.value))} disabled={busy || !terminal} /></label>
      <label>执行预算（秒） <input aria-label="执行预算（秒）" type="number" min={1} max={120} value={budgetSeconds} onChange={e => setBudgetSeconds(Number(e.target.value))} disabled={busy || !terminal} /></label>
      <button disabled={busy || !terminal || !prompt.trim() || !Number.isInteger(maxCalls) || maxCalls < 1 || maxCalls > 5 || !Number.isInteger(budgetSeconds) || budgetSeconds < 1 || budgetSeconds > 120}
        onClick={() => void run(() => loops.start(prompt, maxCalls, budgetSeconds * 1000), true)}>发起循环</button>
    </div>
    <div className="row">
      <input aria-label="循环 ID" type="text" value={lookupId} onChange={e => setLookupId(e.target.value)} placeholder="loop-1" disabled={busy} />
      <button disabled={busy || !/^loop-\d+$/.test(lookupId.trim())} onClick={() => void run(() => loops.get(lookupId.trim()), true)}>按 ID 查询</button>
    </div>
    {error && <p className="error" role="alert">{error}</p>}
    {current && <>
      <h3>{current.loop_id} · {labels[current.status]}</h3>
      <p aria-live="polite">状态：{current.status}{current.in_flight ? " · 步骤在途" : ""} · 模型：{current.model}<br />
        工具提议 {current.proposal_count}/{current.max_calls} · 模型尝试 {current.model_attempts} · 已用 {(current.used_ms / 1000).toFixed(1)} 秒 · 剩余 {(current.remaining_ms / 1000).toFixed(1)} 秒
      </p>
      {waiting && <p>等待审批不消耗执行预算。审批状态：{current.approval_status ?? "尚未同步，请查询"}。</p>}
      <div className="row">
        {!terminal && !waiting && <button disabled={busy || current.in_flight || cancelling} onClick={() => void run(() => resume(current))}>运行到下一停止点</button>}
        {waiting && decided && <button disabled={busy || current.in_flight || cancelling} onClick={() => void run(() => resume(current))}>运行到下一停止点</button>}
        <button onClick={() => { setError(""); void refresh(); }}>查询状态</button>
        {!terminal && <button disabled={cancelling} onClick={() => void cancel()}>取消循环</button>}
      </div>
      {waiting && preview && <div className="loop-approval">
        <h3>待写入文件：{preview.path}</h3>
        <p>操作：覆盖整个文件 · {call?.content_digest?.bytes ?? 0} 字节</p>
        <details><summary>查看写入全文</summary><pre className="result">{preview.text}</pre></details>
        <p className="note">调用 #{preview.call_id} · 审批 {call?.approval_id}</p>
        <details><summary>核对绑定摘要</summary><pre className="result">{preview.args_sha256}</pre></details>
        {!previewMatches && <p className="error">预览与绑定不一致，请查询状态。</p>}
        {current.approval_status === "pending" && <div className="row">
          <button disabled={busy || cancelling || !previewMatches} onClick={() => void decide("approve")}>批准本次写入并执行</button>
          <button disabled={busy || cancelling} onClick={() => void decide("reject")}>拒绝本次写入</button>
        </div>}
      </div>}
      {!!current.unknown_writes.length && <p className="error">未确认写入：{current.unknown_writes.map(i => `#${i}`).join("、")}。不要重试相同写入；读取只提供观测，不消除未知状态。</p>}
      {!!current.unknown_reads.length && <p>未确认读取：{current.unknown_reads.map(i => `#${i}`).join("、")}。</p>}
      {current.status === "interrupted" && <p>网关重启已中断该循环。只能查看记录，请新建任务；旧审批不能复用。</p>}
      {current.status === "cancelled" && <p>已取消后续推进；已在途的操作可能仍有结果，请查询迟到记录。取消不撤回已经提交的写入。</p>}
      {current.error && <p className="error">结束原因：{current.error}</p>}
      {current.final_text && <pre className="result" aria-label="循环最终说明">{current.final_text}</pre>}
      {terminal && !current.final_text && <p>重启后仅保留审计摘要，最终回答正文不落盘。</p>}
      <h3>调用记录</h3>
      {current.calls.length === 0 && <p>暂无工具提议。</p>}
      <ol className="loop-calls">{current.calls.map(item => <li key={item.call_id}>
        <strong>#{item.call_id} {item.tool}</strong> · {item.path ?? "无有效路径"} · {item.status}
        {item.error && <span> · {item.error}</span>}
        {item.duplicate_of !== null && <span> · 复用 #{item.duplicate_of}（未再次执行）</span>}
        {item.result && <p>结果：{item.result.status} · 原正文 {item.result.body_digest.bytes} 字节{item.result.truncated ? " · 回传已截断" : ""}</p>}
        {item.late && <p>迟到结果：{item.late.result.status}（仅归档，不回传模型、不推进循环）{item.status === "unknown" && "；原调用仍为 unknown。"}</p>}
      </li>)}</ol>
      {!!current.late_model_arrivals_ms.length && <p>迟到模型响应 {current.late_model_arrivals_ms.length} 次，仅保留到达元数据，未触发工具。</p>}
    </>}
  </section>;
}
