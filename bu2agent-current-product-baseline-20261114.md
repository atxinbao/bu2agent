# bu2agent 当前产品基线（2026-11-14 草稿）

> 日期：2026-11-14。基线：远端 main `4b12465`（本地 `2e5bd3a`，小尾巴）。本文刷新 2026-11-04 版（`bu2agent-current-product-baseline-20261104.md`，基线 `72d23a0`）；1104 版保留为历史，本文只写现状，不改写历史。2026-11-14 ~21:35 由初稿基线 `1209b83f` 刷新至 `4b12465`（E2-D 第二批 + 小尾巴已合入 main，见 §二）。
>
> 来源：R0 调查（`hidden_files/r0-investigation.md`，逐条 file:line 引用）。凡是本地定义皆已标明；凡是只凭路由数能说的，只报数，不算等价、不算百分比。
>
> 批复：2026-11-14 ~21:36，用户按 §七建议整批批复 Q-R0-01–Q-R0-06：Q-R0-01 16 条路由注明管理/测试入口、该补 UI 的另立小项、不一律删；Q-R0-02 history 改读 `approvals` 表，另立修复方案（涉 `supersedes_version` 链），不在 R0 内改；Q-R0-03 Task DTO 4 个字段在 R0 内补（桌面 + TS 加字段，网关不动）；Q-R0-04 新文件 `bu2agent-current-product-baseline-20261114.md` 落盘，1104 版保留；Q-R0-05 回归门先作本地 pre-push 检查单，不进 CI；Q-R0-06 PA §8 只加对照注，不重写正文。

## 一、基线范围

本版以远端 main `1209b83f` 为当前基线，核对了：

- `README.md`、`PRODUCT-STRATEGY.md`、`PA-ARCHITECTURE.md`、`ARCHITECTURE.md`、`TOOLS-INTEGRATION.md`；
- `apps/desktop`、`apps/shell`、`apps/web`、`services/mock-gateway`、`packages/contract`；
- `~/workspace/goals/bu2agent-muse/` 的全部关闭记录（GOAL.md、project-roadmap.md、D1 与 6b 方案、CI 日志）；
- 路由注册表（机械提取）、客户端字面量（机械提取）、shell 注册表（逐项核对）、SQLite DDL 与迁移步数、JSONL 接线。

## 二、当前状态清单

每行末尾的标签是事实来源：**源码事实**（代码里可查）、**窗口事实**（打包窗口验收过）、**历史记录**（已关闭工作的记录）、**设计意图**（批复的方向，未落地的不要当成已交付）。

### 产品定位

- bu2agent 是本地单用户、PC 优先的 Personal Agent 控制平面；Muse 只是参考基线，不是等价目标（2026-11-03 方向调整，批复）。
- 长期方向仍是 Local-first、多设备连续 Personal Agent；手机、手表、跨设备 handoff、主动式能力不是当前交付边界（C1 已由用户延期，先做完整 PC 本地）。
- 市场定位：本地 PC 上的可验证工作执行——能读真实状态、能请求确认、能留下产物、能在失败或重启后继续；不以“连接器数量”为竞争点。
- 首发用户：经常处理本地文件、提醒、邮件和小型代码仓库的个人知识工作者。

### 规模（源码事实）

- 网关路由 247 次注册 / 234 条去重路径；`server.rs` 20,637 行；网关模块 47 个 `.rs` 文件。
- 前端 22 个视图；63 个领域客户端文件（其中 31 个 `invoke_*`）；桌面客户端 33 个文件。
- Shell：243 个 `#[tauri::command]`，`invoke_handler` 注册 243 项，逐项有函数定义。
- SQLite schema v13；26 张表；13 步迁移；网关测试 518 项、工作区 567 项（最新全绿日志：网关 `tails-rebase2-gateway.log`、工作区 `tails-rebase2-workspace-rerun.log`）；CI ci #117 / `43596070737` 全绿 5/5。
- `packages/contract` 只有 3 个 JSON schema（bridge-contract、files-extended、gateway-contract），无 TS/Rust 共享 DTO 类型。

### 已关闭的工作（历史记录）

- Tasks 1–21、Task 22 批 1（真实邮件只读）；P0-A–E、P0-R、P1、P2、P3-A/B/C；E2 前置切片、E2-A、E2-B、E2-C、E2-D 第一批、**E2-D 第二批**（延迟启动 + 优先级，2026-11-14 全部关闭）；D1-1→D1-5、D1-6a、D1-6b-1→6b-4；**小尾巴**（reconnect 路由、MOCK_EVIDENCE_PATH 接线、Patches 再次应用，2026-11-14 全部关闭）。
- D1-6b 四批是最后一批个人数据平面写操作：邮件发送（v11）、日历创建（v12）、删信（不升版）、Notes 读 + 写（v13）。
- E2-D 第二批已全部关闭（2026-11-14 ~20:20；远端 `dce2ae3`，CI #116 重跑全绿；D2 在旧 TEXT 列库上重验通过）。Outbox 定时投递消费端尚未建（D1-3 的后续，需另立方案/批复）。

### 能力现状（源码事实 + 窗口事实）

- 统一执行内核：Execution/Effect 记录、审批绑定 Effect 与参数摘要、幂等键、SQLite 事务状态库、事件日志、Inbox/Outbox、Worker lease（心跳 5 秒、租约 = 心跳 + 30 秒、sweeper 10 秒 tick）、checkpoint、takeover、崩溃后核查、重启恢复。
- 真实读回：设备类、文件类（`hash_match`）、日历创建、日历/邮件/Notes 删除与 Notes 读回已交付；邮件发送仍 409 `effect_no_readback`（设计如此）。
- 真实邮件：AppleScript 只读、发送、删除（进废纸篓）；日历 EventKit 只读 + 创建；Notes AppleScript 读 + 写（指定文件夹、正文只存 hash、追加冲突 409）。OAuth 不做（Q-D1-61）。
- 记忆分层：session 工作记忆（SessionRecord 内）+ `project_memories` + `user_memories`，显式保存、scope 三元组、断开级联墓碑。
- Source Registry：`sources` 表 v8；断开只翻 flag；小尾巴已交付 `POST /sources/{id}/reconnect`（窗口得证：断开 → 重连恢复 active + 补发 `source.registered` 事件）；sqlite3 直改恢复仍不补发事件（已知项）。
- 长期记忆与 Source Registry 已形成；主动规则与跨设备 handoff 未形成。

### 仍开放的尾巴（历史记录）

- 无订阅文件变化（调查 + 方案已出，§12 待批复）、Task 22 批 2（B2-0 探针已做，B2-1–B2-4 待重批复）、GitHub App Workflows 权限疑点（下次推 workflow 文件前核实）、Patches 再次应用的深层性质（新文件上哈希不变 + 验证 verified 未在本轮窗口重证，小尾巴残留）。重连路由、MOCK_EVIDENCE_PATH 接线、6b-2 location 已由小尾巴解决：location 在同一 iCloud 日历上往返无丢失（S-T4 得证），6b-2 的 null 最可能是创建时重列读到了未同步副本。
- 开放审计行（按红线 9 留着不 ack）：inbox-7、inbox-14、inbox-38、inbox-59/60/61、inbox-64/65/66、inbox-72、inbox-90、effect-77。

## 三、DTO parity 检查结果

方法：网关路由表（机械提取）× 客户端路径字面量（机械提取）× shell 注册表（逐项核对）；字段级只抽查了 Task，全部字段比对未做（见 §七）。

- **路由级：客户端孤儿调用 0。** 桌面端 212 条、TS 端 187 条去重路径，逐条都在网关路由表里。
- **路由级：16 条网关路由无生产客户端调用方**（逐条 grep 过 web/desktop/shell）：`/approvals`、`/approvals/history`、`/approvals/{id}`、`/approvals/{id}/edit-message`、`/approvals/{id}/editable-message`、`/mandates`、`/mandates/{id}`、`/policies/versions`、`/outbox/get`、`/outbox/list`、`/outbox/{id}/deliver`、`/outbox/{id}/requeue`、`/effects/transition`、`/evidence/export`、`/files/raw`、`/devices/authorization/request`。全部仅有测试调用（`/mandates/{id}` 连测试调用都未找到）。处理方式待用户定（R0-Q1）：管理/测试入口的写入基线注明，无 UI 入口；该补 UI 的另立小项；不得一律删除（`/outbox/{id}/deliver|requeue` 是手动重投的唯一入口）。
- **字段级（抽查 Task，真实缺口）：** 网关 `TaskFileRecord` 11 个字段，桌面与 TS 的 `TaskRecord` 只有前 8 个，静默丢掉 `execute_binding`、`goal_id`、`project_id`、`workspace_id`（HTTP 响应里有，解码时丢，不报错）。UI 无法从任务 DTO 读到任务链接与冻结绑定。是否补，待用户定（R0-Q3）。
- **已核实，非缺陷：** 不存在 `/connectors/list` 路由（客户端正确走 `GET /connectors` 与 `POST /v1/custom-connectors/list`）；TS 孤儿 `/api/email/threads{id}` 是 `${suffix}` 模板的提取 artifact；shell 注册 243/243 全对。
- **两个审批读路由读 SQLite 内的两个不同源（真实缺口，已按主代理核对修正）：** `approvals_list` 读 `approvals` 表，`approvals_history` 在 SQLite 模式下读 `events` 表（只取 `aggregate_kind == "approval"`；JSONL 读只在非 SQLite 模式才走）。是否让 history 直接读表，待用户定（R0-Q2）。

## 四、SQLite / JSONL 领域边界

### SQLite 是事实源的域（26 张表，v13）

`db_meta`、`events`、`executions`、`effects`、`approvals`、`assignments`、`workspace_snapshots`、`worker_sessions`、`checkpoints`、`inbox`、`outbox`、`evidence`、`patches`、`patch_files`、`worker_runs`、`goals`、`policy_versions`、`mandates`、`event_cursors`、`sources`、`project_memories`、`user_memories`、`mail_messages`、`sent_messages`、`notes`、`calendar_events`。

### JSONL 仍是事实源的域

tasks、loops、sessions、actions、chat、feed、spaces、peers、nodes、stream、voice、bindings、projects、locations、worktrees、vault、model。Task 仍是 JSONL 记录，Goal 才是 SQLite 一等对象（D1-1 定论）。

### 双轨域（JSONL 读入 + SQLite 一次性折叠）

executions、effects、approvals：启动时读入内存 map，同时 `import_folded` 一次性折叠进 SQLite；内存 map 与 SQLite 表并存。policy/connectors：JSONL 一次性导入后即只读存档，表是事实源。evidence.jsonl 是派生审计导出，路由永远读 SQLite；shell 已接 `MOCK_EVIDENCE_PATH`（小尾巴，`gateway_home.rs`，窗口得证：文件在应用数据目录根下 97 行 == Evidence 表 97 行）。

### 迁移规则

WAL + synchronous FULL；`if version < N` 单事务步进；新列一律 PRAGMA 回填、不升版；快照表只插不改；历史行不删不改（删除 = 墓碑）；Policy/Mandate 旧版本永不删除。版本史：v1 初始表、v2 evidence、v3 patches + patch_files、v4 worker_runs + executions.deadline_ms、v5 goals、v6 policy_versions + mandates、v7 event_cursors、v8 sources、v9 记忆两表、v10 mail_messages、v11 sent_messages、v12 calendar_events、v13 notes。

## 五、`server.rs` 拆分计划（只列出，不重写）

现状：`server.rs` 20,637 行，路由 247 次注册。拆分只动 handler 归属，不动行为、不动 store（store 已按域分 47 个模块）。以下按注册次数列出，供后续分批抽取；每批抽取前须先过回归门（§六）。

| 拟拆模块 | 域（注册次数） | 备注 |
|---|---|---|
| `routes_permissions.rs` | /permissions (18) | 最大的一组；含 hitl/mitm/tproxy/sni/network-rules |
| `routes_files.rs` | /files (15) | 含 subscribe long-poll 与 raw |
| `routes_chat.rs` | /chat (14) + /api/chat (4) + /sessions (5) + /session (5) | 会话与聊天同一簇 |
| `routes_peers.rs` | /peers (14) + /api/nodes (5) | 对等节点与邀请 |
| `routes_connectors.rs` | /connectors (13) + /v1 (13) | 含 custom-connectors 与 credentials |
| `routes_mail.rs` | /api/email (13) | 含 sync/send/sent/synced/attachments |
| `routes_voice.rs` | /api/voice (11) | |
| `routes_tasks.rs` | /tasks (10) | Task 仍是 JSONL 域 |
| `routes_devices.rs` | /devices (9) | 含 authorization（见 §三无调用方路由） |
| `routes_feed.rs` | /api/feed (8) | |
| `routes_loop.rs` | /agent (6) | Loop 引擎入口 |
| `routes_notes.rs` | /api/notes (6) | |
| `routes_ledger.rs` | /effects (5) + /executions (3) + /assignments (3) + /worker (4) + /inbox (3) + /outbox (4) + /evidence (3) | 账本簇；transition 两条路由分属 executions/effects，抽取时别合错 |
| `routes_approvals.rs` | /approval (3) + /approvals (5) + /mandates (2) + /policies (1) | 含 F-R0-02 的双源问题，抽取不动该行为 |
| `routes_engineering.rs` | /projects (4) + /bindings (4) + /locations (4) + /worktrees (4) + /patches (4) | P3-A/B 工程域 |
| `routes_data.rs` | /memories (4) + /goals (4) + /sources (3) + /api/calendar (3) | D1 数据面 |
| `routes_misc.rs` | /model (2) + /health (1) + /config (1) + /spaces (3) | 杂项；/spaces 另有 v2 路由两条 |
| `server.rs`（保留） | AppState、路由组装、共享 helper | 不再新增 handler |

注：`/api/calendar` 的 POST 与 GET 共用同一路径（注册 3 次、去重 2 条）；`/api/email` 与 `/api/notes` 同理。拆分时按 handler 抽取，不按路径去重数。

## 六、编号对照

三套编号并存，本文一律用路线编号；引用旧文档的 P 编号时注明出处。

| 路线编号 | 含义 | 对应旧说法 |
|---|---|---|
| R0 | 基线收口（本文） | 无对应；旧基线无此阶段 |
| E1 | 可审查工程 Agent（原基线 P3；PA 的 P1） | 旧基线 P3、PA-ARCHITECTURE §8 的 P1 |
| E2 | Durable Worker 与统一执行内核 | 旧基线 P0-A–E（执行内核迁移子阶段）与 P0 剩余工作的合流；PA-ARCHITECTURE §8 的 P0 |
| D1 | Personal Data Plane | 旧基线 P2 的一部分（Mail/Notes 真实接入）+ P4 的一部分（Source Registry、记忆）；PA-ARCHITECTURE §8 的 P2 的一部分 |
| C1 | 多设备连续 | 旧基线 P5；PA 未单列（在 P2 的“多设备”一句里） |
| A1 | 主动性与长期自动化 | 旧基线 P4 的其余部分；PA-ARCHITECTURE §8 的 P2 的其余部分 |

PA-ARCHITECTURE.md §8 本身是否改写，待用户定（R0-Q6）。建议只加一段对照注，不重写 PA 正文。

## 七、固定本地回归门

新提交进下一阶段前必须全过；任一失败即停，不进下一阶段。

| 门 | 命令（VM 内） | 通过标准 |
|---|---|---|
| 网关测试 | `export PATH="$HOME/.cargo/bin:$PATH" && cargo test -p mock-gateway` | 0 失败；日志落 `~/workspace/bu2agent-vm-checks/` |
| 工作区测试 | `cargo test --workspace` | 0 失败；先 `rm -rf /tmp/bu2agent-*`（/tmp 是 512M tmpfs）；libdbus 软链丢了先重建（`sudo ln -sf /usr/lib/x86_64-linux-gnu/libdbus-1.so.3 /usr/lib/x86_64-linux-gnu/libdbus-1.so`） |
| Clippy | `cargo clippy --workspace --all-targets -- -D warnings`（CI 原样命令）+ 平行 `cargo clippy` | exit 0、0 警告；exit 码直接取，不经管道 |
| 前端 | `apps/web` 内 `npm run build`（`tsc --noEmit && vite build`） | exit 0 |
| 路由计数 | 多行感知正则 `\.route\(\s*"([^"]+)"` | 只报调用数/去重数，不算百分比、不算等价 |
| 代码卫生 | `git diff --check` | 干净 |
| Shell 与 macOS 专属体 | CI 对应 job | VM 编不过，以 CI 为准（shell job、macOS job） |
| 窗口验收 | `.codex/<phase>-window-validation-YYYYMMDD/report.md` + `full/` 证据 | 报告须亲手读 + 抽查证据；UI 步不得用 curl 替代；失败即停 |

## 八、R0 关闭还差什么

1. 本草稿须用户审阅；§七的开放问题（R0-Q1…Q-R0-06，见调查 §8）须逐条拍板。
2. 拍板后：基线文档以新文件 `bu2agent-current-product-baseline-20261114.md` 落盘进仓库（1104 版保留为历史），走正常推送流程（用户批批准卡、核对 blob、复核 CI）。
3. 回归门须跑一次完整 live run（网关 + 工作区 + Clippy + 前端 + 路由计数），以本次基线数为新基线。
4. 以下不属于 R0 关闭条件，另行安排：Task DTO 补字段（Q-R0-03 若批复）、`approvals_history` 改读 SQLite（Q-R0-02 若批复）、16 条路由的 UI/删除处置（Q-R0-01）、`server.rs` 实际拆分（§五只列出）、PA §8 改写（Q-R0-06）。
