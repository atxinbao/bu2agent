import { invoke } from "@tauri-apps/api/core";
import { useEffect, useState } from "react";
import type {
  Binding,
  EngineeringGateway,
  Location,
  Project,
  WorkerRun,
  Worktree,
} from "../domain/engineering_client";
import type { EffectRecord, EffectsGateway } from "../domain/effects_client";
import type { EvidenceGateway, EvidenceRow } from "../domain/evidence_client";
import type { LoopGateway } from "../domain/loop_client";
import { EffectBadge, latestEvidenceFor } from "./ExecutionDetail";

// P3-C 工程视图：绑定与记录的管理面 + Worker 运行（命令执行）。
// P3-A 的只读约束已解除：本页可以排队执行白名单命令
// （git-status、git-diff、git-worktree-add、git-commit、
// cargo-test），只在临时 Worktree 上写、只在临时分支上提交，
// 绝不推送到远端；每次执行都是一条 Worker run，有 Effect
// 与 Evidence，可在运行列表里看状态、尾部输出与 Effect。
// 选中的运行在详情里展示最新 Evidence 与验证按钮（复用
// ExecutionDetail 的 EffectBadge，与 PatchesView 同款）。
// Location 只是目录绑定，没有 git 语义；Worktree 的
// branch_label 只是展示字符串，不代表已连接 git 分支或
// 真实工作树——文案不得让用户误以为已连接 git 仓库。
// Project 在 P3-A 没有独立管理 UI（方案 §3.5）：只在
// Worktree 创建表单的 project 选择框与 Worktree 详情里出现。
// code.search 是 loop 工具，不走领域客户端：本页的搜索入口
// 发起一个循环（照 MailView“摘要为草稿”的 loops.start 写法），
// 搜索结果在 Loops 页的循环执行详情里展示（tool 调用记录），
// 不在本页内展示。

const LOOP_MAX_CALLS = 5;
const LOOP_BUDGET_MS = 120000;

// Worker run 的终态（与网关 worker.rs 的终态词一致）：
const RUN_TERMINAL = new Set([
  "succeeded",
  "failed",
  "timeout",
  "cancelled",
  "lost",
  "orphaned",
  "superseded",
]);

interface EngineeringViewProps {
  engineering: EngineeringGateway;
  loops: LoopGateway;
  isInvoke: boolean;
  onNavigate: (target: "loops") => void;
  // 与 PatchesView 同款：可选的 Effect/Evidence 客户端。
  // 未传时运行详情只显示 Effect ID 文本，不展示验证按钮。
  effects?: EffectsGateway | null;
  evidence?: EvidenceGateway | null;
}

function errText(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

export function EngineeringView({
  engineering,
  loops,
  isInvoke,
  onNavigate,
  effects: effectsClient = null,
  evidence: evidenceClient = null,
}: EngineeringViewProps) {
  const [bindings, setBindings] = useState<Binding[]>([]);
  const [projects, setProjects] = useState<Project[]>([]);
  const [locations, setLocations] = useState<Location[]>([]);
  const [worktrees, setWorktrees] = useState<Worktree[]>([]);
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);

  // 绑定表单：Invoke 环境用目录选择器，HTTP 环境用路径文本输入。
  const [bindingName, setBindingName] = useState("");
  const [bindingPath, setBindingPath] = useState("");
  // Location 表单。
  const [locationBindingId, setLocationBindingId] = useState("");
  const [locationName, setLocationName] = useState("");
  const [locationPath, setLocationPath] = useState("");
  // Worktree 表单。
  const [worktreeLocationId, setWorktreeLocationId] = useState("");
  const [worktreeDir, setWorktreeDir] = useState("");
  const [worktreeBranch, setWorktreeBranch] = useState("");
  const [worktreeProjectId, setWorktreeProjectId] = useState("");
  // code.search 入口。
  const [searchQuery, setSearchQuery] = useState("");
  const [searchWorktreeId, setSearchWorktreeId] = useState("");
  // P3-C：Worker 运行列表与详情。
  const [runs, setRuns] = useState<WorkerRun[]>([]);
  const [selectedRunId, setSelectedRunId] = useState("");
  const [selectedRun, setSelectedRun] = useState<WorkerRun | null>(null);
  // 选中运行的 Effect 记录与 Evidence（与 PatchesView 同款，
  // 供运行详情的 EffectBadge 展示最新验证与验证按钮）。
  const [effectRows, setEffectRows] = useState<EffectRecord[]>([]);
  const [effectEvidence, setEffectEvidence] = useState<EvidenceRow[]>([]);
  // P3-C：运行命令表单（UI 直接点击即授权，不另行审批）。
  const [runWorktreeId, setRunWorktreeId] = useState("");
  const [runCommand, setRunCommand] = useState("git-status");
  const [runPath, setRunPath] = useState("");
  const [runBranch, setRunBranch] = useState("");
  const [runMessage, setRunMessage] = useState("");
  const [runPackage, setRunPackage] = useState("");

  async function refreshEffectRows() {
    if (!effectsClient || !evidenceClient) return;
    try {
      const [allEffects, allEvidence] = await Promise.all([
        effectsClient.list(),
        evidenceClient.list(),
      ]);
      setEffectRows(allEffects);
      setEffectEvidence(allEvidence);
    } catch (e) {
      setError(`Effect 记录读取失败：${errText(e)}`);
    }
  }

  async function refreshRuns() {
    try {
      const nextRuns = await engineering.workerRunList();
      setRuns(nextRuns);
      if (selectedRunId !== "") {
        const found = nextRuns.find((run) => run.runId === selectedRunId);
        if (found) setSelectedRun(found);
      }
    } catch (e) {
      setError(`运行列表读取失败：${errText(e)}`);
    }
    // 运行终态时 Effect/Evidence 才落盘：随运行列表一起刷新，
    // 详情里的 EffectBadge 才不会停留在旧状态。
    await refreshEffectRows();
  }

  async function refreshAll() {
    try {
      const [nextBindings, nextProjects, nextLocations, nextWorktrees] =
        await Promise.all([
          engineering.bindingList(),
          engineering.projectList(),
          engineering.locationList(),
          engineering.worktreeList(),
        ]);
      setBindings(nextBindings);
      setProjects(nextProjects);
      setLocations(nextLocations);
      setWorktrees(nextWorktrees);
    } catch (e) {
      setError(`工程记录读取失败：${errText(e)}`);
    }
    await refreshRuns();
  }

  useEffect(() => {
    void refreshAll();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // 有运行中的 run 时每 2 秒刷新一次列表（与 Tasks 页同频），
  // 终态后停止轮询；无运行中的 run 时不轮询。
  useEffect(() => {
    if (!runs.some((run) => !RUN_TERMINAL.has(run.status))) return;
    const timer = window.setInterval(() => {
      void refreshRuns();
    }, 2000);
    return () => window.clearInterval(timer);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [runs]);

  function projectName(projectId: string | null): string {
    if (projectId === null) return "（未关联 Project）";
    const found = projects.find((p) => p.projectId === projectId);
    return found ? `${found.name}（${found.projectId}）` : projectId;
  }

  async function handleBind() {
    const displayName = bindingName.trim();
    if (displayName === "") {
      setError("绑定仓库需要先填显示名称。");
      return;
    }
    setError("");
    setMessage("");
    setBusy(true);
    try {
      let path: string;
      if (isInvoke) {
        // pick_directory 是 Rust 侧的壳命令：弹目录选择器，
        // 取消返回 null（与 tool_commands.rs 对过，命令已落地）。
        const picked = await invoke<string | null>("pick_directory");
        if (picked === null || picked === "") {
          setMessage("已取消选择目录，未绑定。");
          return;
        }
        path = picked;
      } else {
        // HTTP 预览没有 Tauri 对话框：直接输路径绑定，
        // 与选择器是等价入口（dev 模式）。
        path = bindingPath.trim();
        if (path === "") {
          setError("HTTP 预览没有目录选择器，请直接输入要绑定的目录路径。");
          return;
        }
      }
      const created = await engineering.bindingCreate(displayName, path);
      setBindingName("");
      setBindingPath("");
      await refreshAll();
      setMessage(`已绑定 ${created.displayName}（${created.workspaceId}）。`);
    } catch (e) {
      setError(`绑定失败：${errText(e)}`);
    } finally {
      setBusy(false);
    }
  }

  async function handleCreateLocation() {
    if (locationBindingId === "" || locationName.trim() === "" || locationPath.trim() === "") {
      setError("新建 Location 需要先选所属绑定，并填名字与路径。");
      return;
    }
    setError("");
    setMessage("");
    setBusy(true);
    try {
      const created = await engineering.locationCreate(
        locationBindingId,
        locationName.trim(),
        locationPath.trim(),
      );
      setLocationName("");
      setLocationPath("");
      await refreshAll();
      setMessage(`已新建 Location ${created.name}（${created.locationId}）。`);
    } catch (e) {
      setError(`新建 Location 失败：${errText(e)}`);
    } finally {
      setBusy(false);
    }
  }

  async function handleCreateWorktree() {
    if (worktreeLocationId === "") {
      setError("新建 Worktree 需要先选所属 Location。");
      return;
    }
    setError("");
    setMessage("");
    setBusy(true);
    try {
      const created = await engineering.worktreeCreate(
        worktreeLocationId,
        worktreeDir.trim(),
        worktreeBranch.trim(),
        worktreeProjectId === "" ? null : worktreeProjectId,
      );
      setWorktreeDir("");
      setWorktreeBranch("");
      setWorktreeProjectId("");
      await refreshAll();
      setMessage(`已新建 Worktree（${created.worktreeId}）。`);
    } catch (e) {
      setError(`新建 Worktree 失败：${errText(e)}`);
    } finally {
      setBusy(false);
    }
  }

  async function handleArchive(
    kind: "binding" | "location" | "worktree",
    id: string,
  ) {
    setError("");
    setMessage("");
    setBusy(true);
    try {
      if (kind === "binding") {
        await engineering.bindingArchive(id);
      } else if (kind === "location") {
        await engineering.locationArchive(id);
      } else {
        await engineering.worktreeArchive(id);
      }
      await refreshAll();
      setMessage(`已归档 ${id}。`);
    } catch (e) {
      // 归档守则的 409（如仍有 active 下级）由服务端报错；
      // 客户端不改写措辞，原样展示。
      setError(`归档失败：${errText(e)}`);
    } finally {
      setBusy(false);
    }
  }

  async function handleSearch() {
    const query = searchQuery.trim();
    if (query === "") {
      setError("搜索需要先填 query（不能为空）。");
      return;
    }
    if (searchWorktreeId === "") {
      setError("搜索需要先选一个 Worktree 作为作用域。");
      return;
    }
    setError("");
    setMessage("");
    setBusy(true);
    try {
      const worktree = worktrees.find((w) => w.worktreeId === searchWorktreeId);
      const scope =
        worktree === undefined
          ? searchWorktreeId
          : `${searchWorktreeId}（Location ${worktree.locationId} 下相对目录“${worktree.directoryRelative === "" ? "（根）" : worktree.directoryRelative}”）`;
      const prompt =
        `请在指定的 Worktree 中搜索代码。\n` +
        `\n` +
        `Worktree：${scope}\n` +
        `搜索 query：${query}\n` +
        `\n` +
        `步骤：\n` +
        `1. 用 code.search 工具搜索该 query，作用域就是上述 Worktree（传 worktree_id=${searchWorktreeId}）；只读，不写任何文件。\n` +
        `2. 列出命中文件与行号（1-based），每条命中附一行短上下文；超过上限被截断时注明 truncated。\n` +
        `3. 只汇报工具结果实际返回过的内容，不猜测未命中的文件。\n`;
      const snapshot = await loops.start(prompt, LOOP_MAX_CALLS, LOOP_BUDGET_MS);
      // Loops 页挂载时按 localStorage 里的上次循环 ID 恢复；
      // 把新循环 ID 存下，跳转后直接跟进该循环的审批。
      try {
        localStorage.setItem("bu2agent.lastLoopId", snapshot.loop_id);
      } catch {
        // 存储不可用不影响跳转，Loops 页可按 ID 手动查询。
      }
      onNavigate("loops");
    } catch (err) {
      setError(errText(err));
    } finally {
      setBusy(false);
    }
  }

  // P3-C：运行一条白名单命令。UI 直接点击即授权，不另行
  // 审批（与 /files/write 的直接点击一致）；循环提议的
  // worker.run 仍需审批（模型不能自授权）。写命令只在
  // 临时 Worktree 上写、只在临时分支上提交，绝不推送。
  async function handleRunCreate() {
    if (runWorktreeId === "") {
      setError("运行命令需要先选一个 Worktree。");
      return;
    }
    const params: Record<string, string> = {};
    if (runCommand === "git-diff" && runPath.trim() !== "") {
      params.path = runPath.trim();
    }
    if (runCommand === "git-worktree-add") {
      if (runPath.trim() === "" || runBranch.trim() === "") {
        setError("新建 Worktree 需要填相对目录与分支名。");
        return;
      }
      params.path = runPath.trim();
      params.branch = runBranch.trim();
    }
    if (runCommand === "git-commit") {
      if (runMessage.trim() === "") {
        setError("提交需要填提交信息。");
        return;
      }
      params.message = runMessage;
    }
    if (runCommand === "cargo-test") {
      if (runPackage.trim() === "") {
        setError("运行测试需要填包名。");
        return;
      }
      params.package = runPackage.trim();
    }
    setError("");
    setMessage("");
    setBusy(true);
    try {
      const run = await engineering.workerRunCreate(
        runWorktreeId,
        runCommand,
        params,
      );
      setSelectedRunId(run.runId);
      setSelectedRun(run);
      await refreshRuns();
      setMessage(`已排队运行 ${run.runId}（${runCommand}）。`);
    } catch (e) {
      setError(`运行命令失败：${errText(e)}`);
    } finally {
      setBusy(false);
    }
  }

  async function handleSelectRun(runId: string) {
    setSelectedRunId(runId);
    setError("");
    try {
      const run = await engineering.workerRunGet(runId);
      setSelectedRun(run);
    } catch (e) {
      setError(`运行详情读取失败：${errText(e)}`);
    }
  }

  async function handleCancelRun(runId: string) {
    setError("");
    setMessage("");
    setBusy(true);
    try {
      const run = await engineering.workerRunCancel(runId);
      setSelectedRun(run);
      await refreshRuns();
      setMessage(`已取消运行 ${run.runId}。`);
    } catch (e) {
      setError(`取消运行失败：${errText(e)}`);
    } finally {
      setBusy(false);
    }
  }

  return (
    <section>
      <h2>工程</h2>
      <p className="note">
        绑定与记录的管理面 + Worker 运行：绑定仓库、建 Location /
        Worktree、用 code.search 读代码，还可以排队执行白名单命令
        （git-status、git-diff、git-worktree-add、git-commit、
        cargo-test）。写命令只在临时 Worktree 上写、只在临时分支
        上提交，绝不推送到远端；每次执行都是一条 Worker run，有
        Effect 与 Evidence。Location 只是目录绑定，没有 git 语义；
        Worktree 的分支标签只是展示字符串，不代表已连接 git 分支
        或真实工作树。
      </p>
      <div className="row">
        <button type="button" onClick={() => void refreshAll()} disabled={busy}>
          刷新
        </button>
      </div>
      {error !== "" && (
        <p className="error" role="alert">
          {error}
        </p>
      )}
      {message !== "" && <p>{message}</p>}

      <h3>绑定仓库</h3>
      <p className="note">
        绑定只登记你选定的目录（网关会 canonicalize 后登记为允许根）；
        绑定本身不读、不写该目录下的任何文件。
      </p>
      <div className="row">
        <label>
          显示名称
          <input
            type="text"
            value={bindingName}
            onChange={(event) => setBindingName(event.target.value)}
            placeholder="如 bu2agent"
          />
        </label>
        {!isInvoke && (
          <label>
            目录路径
            <input
              type="text"
              value={bindingPath}
              onChange={(event) => setBindingPath(event.target.value)}
              placeholder="HTTP 预览：直接输入绝对路径"
            />
          </label>
        )}
        <button type="button" onClick={() => void handleBind()} disabled={busy}>
          {isInvoke ? "选择目录并绑定" : "绑定"}
        </button>
      </div>
      {bindings.length === 0 && <p className="empty">暂无绑定。</p>}
      <ul>
        {bindings.map((binding) => (
          <li key={binding.workspaceId}>
            <p>
              {binding.displayName} · {binding.workspaceId} · 允许根：
              {binding.allowedRoots.join("、") || "（无）"}
            </p>
            <p>
              policy_digest：{binding.policyDigest}
            </p>
            <button
              type="button"
              onClick={() => void handleArchive("binding", binding.workspaceId)}
              disabled={busy}
            >
              归档
            </button>
          </li>
        ))}
      </ul>

      <h3>Location</h3>
      <p className="note">
        Location 是绑定下的目录锚点：名字 + 一条已登记绑定内的路径。
        只是目录绑定，没有 git 语义；不创建目录。归档时若其下仍有
        active Worktree，服务端会 409 拒绝，须先归档下级。
      </p>
      <div className="row">
        <label>
          所属绑定
          <select
            value={locationBindingId}
            onChange={(event) => setLocationBindingId(event.target.value)}
          >
            <option value="">（请选择）</option>
            {bindings.map((binding) => (
              <option key={binding.workspaceId} value={binding.workspaceId}>
                {binding.displayName}（{binding.workspaceId}）
              </option>
            ))}
          </select>
        </label>
        <label>
          名字
          <input
            type="text"
            value={locationName}
            onChange={(event) => setLocationName(event.target.value)}
            placeholder="如 bu2agent 根目录"
          />
        </label>
        <label>
          路径
          <input
            type="text"
            value={locationPath}
            onChange={(event) => setLocationPath(event.target.value)}
            placeholder="须在所属绑定的允许根内"
          />
        </label>
        <button type="button" onClick={() => void handleCreateLocation()} disabled={busy}>
          新建 Location
        </button>
      </div>
      {locations.length === 0 && <p className="empty">暂无 Location。</p>}
      <ul>
        {locations.map((location) => (
          <li key={location.locationId}>
            <p>
              {location.name} · {location.locationId} · {location.status}
            </p>
            <p>
              所属绑定：{location.workspaceId} · 根：{location.canonicalRoot}
            </p>
            <p>root_sha256：{location.rootSha256}</p>
            {location.status === "active" && (
              <button
                type="button"
                onClick={() => void handleArchive("location", location.locationId)}
                disabled={busy}
              >
                归档
              </button>
            )}
          </li>
        ))}
      </ul>

      <h3>Worktree</h3>
      <p className="note">
        Worktree 是读代码的锚点：Location 下的一个相对目录（留空即
        Location 根本身）。P3-A 不执行真实 git worktree add，不建目录，
        不改分支；branch_label 只是展示字符串。
      </p>
      <div className="row">
        <label>
          所属 Location
          <select
            value={worktreeLocationId}
            onChange={(event) => setWorktreeLocationId(event.target.value)}
          >
            <option value="">（请选择）</option>
            {locations.map((location) => (
              <option key={location.locationId} value={location.locationId}>
                {location.name}（{location.locationId}）
              </option>
            ))}
          </select>
        </label>
        <label>
          相对目录
          <input
            type="text"
            value={worktreeDir}
            onChange={(event) => setWorktreeDir(event.target.value)}
            placeholder="留空即 Location 根目录"
          />
        </label>
        <label>
          分支标签（仅展示）
          <input
            type="text"
            value={worktreeBranch}
            onChange={(event) => setWorktreeBranch(event.target.value)}
            placeholder="如 main"
          />
        </label>
        <label>
          Project（可选）
          <select
            value={worktreeProjectId}
            onChange={(event) => setWorktreeProjectId(event.target.value)}
          >
            <option value="">（不关联）</option>
            {projects.map((project) => (
              <option key={project.projectId} value={project.projectId}>
                {project.name}（{project.projectId}）
              </option>
            ))}
          </select>
        </label>
        <button type="button" onClick={() => void handleCreateWorktree()} disabled={busy}>
          新建 Worktree
        </button>
      </div>
      {worktrees.length === 0 && <p className="empty">暂无 Worktree。</p>}
      <ul>
        {worktrees.map((worktree) => (
          <li key={worktree.worktreeId}>
            <p>
              {worktree.worktreeId} · {worktree.status} · 相对目录：
              {worktree.directoryRelative === "" ? "（Location 根目录）" : worktree.directoryRelative}
              {worktree.branchLabel !== "" ? ` · 分支标签：${worktree.branchLabel}` : ""}
            </p>
            <p>
              所属 Location：{worktree.locationId} · Project：
              {projectName(worktree.projectId)}
            </p>
            {worktree.status === "active" && (
              <button
                type="button"
                onClick={() => void handleArchive("worktree", worktree.worktreeId)}
                disabled={busy}
              >
                归档
              </button>
            )}
          </li>
        ))}
      </ul>

      <h3>搜索代码（code.search）</h3>
      <p className="note">
        只读搜索：循环用 code.search 在选定的 Worktree 里找 query，
        列出命中文件与行号。搜索结果在 Loops 页的循环执行详情里
        展示（tool 调用记录），不在本页内展示。
      </p>
      <div className="row">
        <label>
          Worktree
          <select
            value={searchWorktreeId}
            onChange={(event) => setSearchWorktreeId(event.target.value)}
          >
            <option value="">（请选择）</option>
            {worktrees
              .filter((worktree) => worktree.status === "active")
              .map((worktree) => (
                <option key={worktree.worktreeId} value={worktree.worktreeId}>
                  {worktree.worktreeId}
                  {worktree.directoryRelative === ""
                    ? "（Location 根目录）"
                    : `（${worktree.directoryRelative}）`}
                </option>
              ))}
          </select>
        </label>
        <label>
          query
          <input
            type="text"
            value={searchQuery}
            onChange={(event) => setSearchQuery(event.target.value)}
            placeholder="如 plan_loop_effect"
          />
        </label>
        <button type="button" onClick={() => void handleSearch()} disabled={busy}>
          搜索
        </button>
      </div>

      <h3>运行命令（Worker runs）</h3>
      <p className="note">
        排队执行一条白名单命令：git-status、git-diff 只读；git-worktree-add
        在 Location 根下新建目录与临时分支；git-commit 在选定 Worktree
        上提交（--no-verify）；cargo-test 离线跑指定包的测试。UI 直接
        点击即授权，不另行审批；循环提议的 worker.run 仍需审批。
        写命令只在临时 Worktree 上写、只在临时分支上提交，绝不推送。
      </p>
      <div className="row">
        <label>
          Worktree
          <select
            value={runWorktreeId}
            onChange={(event) => setRunWorktreeId(event.target.value)}
          >
            <option value="">（请选择）</option>
            {worktrees
              .filter((worktree) => worktree.status === "active")
              .map((worktree) => (
                <option key={worktree.worktreeId} value={worktree.worktreeId}>
                  {worktree.worktreeId}
                  {worktree.directoryRelative === ""
                    ? "（Location 根目录）"
                    : `（${worktree.directoryRelative}）`}
                </option>
              ))}
          </select>
        </label>
        <label>
          命令
          <select
            value={runCommand}
            onChange={(event) => setRunCommand(event.target.value)}
          >
            <option value="git-status">git-status</option>
            <option value="git-diff">git-diff</option>
            <option value="git-worktree-add">git-worktree-add</option>
            <option value="git-commit">git-commit</option>
            <option value="cargo-test">cargo-test</option>
          </select>
        </label>
        {(runCommand === "git-diff" || runCommand === "git-worktree-add") && (
          <label>
            相对目录 / 路径
            <input
              type="text"
              value={runPath}
              onChange={(event) => setRunPath(event.target.value)}
              placeholder={runCommand === "git-worktree-add" ? "如 scratch/feat-x" : "留空即整个 Worktree"}
            />
          </label>
        )}
        {runCommand === "git-worktree-add" && (
          <label>
            分支名
            <input
              type="text"
              value={runBranch}
              onChange={(event) => setRunBranch(event.target.value)}
              placeholder="如 scratch/feat-x"
            />
          </label>
        )}
        {runCommand === "git-commit" && (
          <label>
            提交信息
            <input
              type="text"
              value={runMessage}
              onChange={(event) => setRunMessage(event.target.value)}
              placeholder="如 WIP: scratch checkpoint"
            />
          </label>
        )}
        {runCommand === "cargo-test" && (
          <label>
            包名
            <input
              type="text"
              value={runPackage}
              onChange={(event) => setRunPackage(event.target.value)}
              placeholder="如 mock-gateway"
            />
          </label>
        )}
        <button type="button" onClick={() => void handleRunCreate()} disabled={busy}>
          排队运行
        </button>
      </div>
      {runs.length === 0 && <p className="empty">暂无运行。</p>}
      <ul>
        {runs.map((run) => (
          <li key={run.runId}>
            <p>
              {run.runId} · {run.command} · {run.status}
              {run.exitCode !== null ? ` · 退出码 ${run.exitCode}` : ""}
              {run.effectId !== null ? ` · Effect ${run.effectId}` : ""}
            </p>
            <p>
              Worktree：{run.bindingWorktreeId ?? "（未绑定）"} · 超时：
              {run.timeoutMs} ms
            </p>
            <button
              type="button"
              onClick={() => void handleSelectRun(run.runId)}
              disabled={busy}
            >
              详情
            </button>
            {!RUN_TERMINAL.has(run.status) && (
              <button
                type="button"
                onClick={() => void handleCancelRun(run.runId)}
                disabled={busy}
              >
                取消
              </button>
            )}
          </li>
        ))}
      </ul>
      {selectedRun !== null && (
        <div>
          <h4>运行详情 {selectedRun.runId}</h4>
          <p>
            {selectedRun.command} · {selectedRun.status}
            {selectedRun.exitCode !== null
              ? ` · 退出码 ${selectedRun.exitCode}`
              : ""}
            {selectedRun.effectId !== null && effectsClient === null
              ? ` · Effect ${selectedRun.effectId}`
              : ""}
          </p>
          {selectedRun.effectId !== null && effectsClient !== null && (
            <p>
              {(() => {
                const effect = effectRows.find(
                  (row) => row.effectId === selectedRun.effectId,
                );
                if (!effect) {
                  return `Effect ${selectedRun.effectId}（记录加载中）`;
                }
                return (
                  <EffectBadge
                    effect={effect}
                    latestEvidence={latestEvidenceFor(
                      effect.effectId,
                      effectEvidence,
                    )}
                    effectsClient={effectsClient}
                    onRefresh={() => void refreshRuns()}
                    onError={(message) => setError(message)}
                  />
                );
              })()}
            </p>
          )}
          <p>cwd：{selectedRun.cwd}</p>
          <p>argv：{selectedRun.argvJson}</p>
          {selectedRun.progressJson !== null && (
            <p>
              进度：
              {(() => {
                try {
                  const progress = JSON.parse(selectedRun.progressJson) as {
                    elapsed_ms?: unknown;
                    output_bytes?: unknown;
                    tail_lines?: unknown;
                  };
                  if (
                    typeof progress.elapsed_ms === "number" &&
                    typeof progress.output_bytes === "number" &&
                    typeof progress.tail_lines === "number"
                  ) {
                    return `已用 ${progress.elapsed_ms} ms · 输出 ${progress.output_bytes} 字节 · 尾行 ${progress.tail_lines} 行`;
                  }
                } catch {
                  // 落到原文展示。
                }
                return selectedRun.progressJson;
              })()}
            </p>
          )}
          <pre>{selectedRun.tail === "" ? "（无输出）" : selectedRun.tail}</pre>
          {selectedRun.truncated && <p className="note">输出已截断（仅保留最后 4096 字节）。</p>}
        </div>
      )}
    </section>
  );
}
