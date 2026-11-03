# bu2agent 当前产品基线、市场定位与下一阶段计划

日期：2026-11-04  
执行者：Codex  
重校准：2026-11-04（小宝）——基线更新至远端 main `72d23a0`；原文方向、市场定位与 P1–P5 结构未动；§一、§三证据、§四、§六 P0、§八按新基线改写；原文以 `9dba3c7` 为基线的版本为 20261104 原稿。  
基线：远端 main `72d23a0`（P0-A–E 已交付，含 Windows 测试门控修复）；Mac 本地 main 检出仍为 `9dba3c7`（已落后远端；`9dba3c7` 是远端 main 的祖先，拉取原则上可 fast-forward）。

编号说明：本文 P0–P5 是产品阶段编号；PA-ARCHITECTURE 的 P0-A–E 是已交付的执行内核迁移子阶段，两套编号含义不同。本文 §六的 P0 指产品阶段 P0 在内核交付后的剩余工作，不是 P0-A–E 本身。

## 一、基线范围

本版以远端 main `72d23a0` 为当前基线，核对了：

- `README.md`、`PRODUCT-STRATEGY.md`、`PA-ARCHITECTURE.md`、`ARCHITECTURE.md`、`TOOLS-INTEGRATION.md`；
- `apps/desktop`、`apps/shell`、`apps/web`、`services/mock-gateway`、`packages/contract`；
- `.codex` 中的全景审查、窗口验收、测试和验证记录；
- P0-A–E 的契约与方案文档、执行/Effect/SQLite 存储、恢复与接管测试、CI 记录；
- 最近提交与远端 main 状态。

原稿以 Mac 本地目录（`9dba3c7`）为基线；本版重校准以远端 main 为准。文档类内容（README、PA 架构、产品策略）两边一致，代码基线以远端为准；原稿引用的 Task 9–22 验收证据仍然有效。Mac 本地工作树另有大量未提交的 .codex 验收和审查产物，不计入基线。

## 二、项目现在是什么

bu2agent 当前是一个本地单用户、PC 优先的 Personal Agent 控制平面：

```text
Tauri 2
  → Rust 客户端 / Invoke
  → 本地 Mock Gateway / Native Source
  → React + TypeScript 工作台
```

当前用户可见入口包括：

- Workbench
- Files
- Agent
- Connectors
- Tasks
- Devices
- Loops
- Settings
- Spaces
- Feed
- Voice
- Nodes / Peers
- Mail

长期方向仍是 Local-first、多设备连续 Personal Agent；手机、手表、跨设备 handoff、长期记忆和主动式能力不是当前交付边界。

## 三、当前已经完成的能力

### 已形成稳定基线的能力

1. 聊天历史、流事件重放、游标、取消订阅、会话搜索、未读/已读、归档、置顶、改名和重启恢复。
2. 文件列表、读取、分块读取、写入、删除、stat、rename、mkdir、search、raw、thumbnail、subscribe、export、library。
3. Agent 工具调用、审批、拒绝、编辑参数、服务端审批复核和一次性消费。
4. Mock Notes / Mail 连接器、账号 link/unlink、connect/disconnect、凭据、票据、Keychain、Egress 和重启持久化。
5. Task 草稿、审批写入、Artifact、任务状态和普通重启恢复。
6. Loop 的审批、取消、预算、unknown、重复未知写、迟到结果、重启恢复和提醒事项工具。
7. Fake/EventKit 提醒事项的读取和经审批的完成状态修改。
8. Devices / Nodes 的登记、下线、心跳、解配 tombstone 和重启恢复。
9. Peers 的邀请、接受、撤销、断开和收件拒绝。
10. Spaces 的引用集合、替换、删除、复活和重启恢复。
11. Feed 的读取、反应、已读、删除和重启恢复。
12. Voice 的 Mock 目录、过滤、自定义 voice 改名/删除、活动 voice 记录和重启恢复。
13. Mail 的真实 macOS AppleScript 只读能力：邮箱、线程、`Re:` 归组、完整正文和附件字节读取已经有本地修复后的证据；这仍然不是 OAuth、发信或跨平台邮件能力。
14. Tauri release app、DMG、sidecar 生命周期、macOS 窗口链路、前端构建和跨平台 CI 基线。
15. 统一执行内核（P0-A–E）：Execution 与 Effect 记录、审批绑定 Effect 与参数摘要、幂等键、SQLite 事务状态库、事件日志、Inbox/Outbox、崩溃后核查（设备类）、重启恢复与重新分配接管、创建时 Workspace 快照、计划检查点。

### 验证证据

当前记录显示：

- `cargo test --workspace`：328 项通过（其中网关 297 项）；
- `apps/web/npm run build`：TypeScript strict + Vite production build 通过；
- CI run `41690215376`（重跑后）的 Ubuntu、macOS、Windows、macOS shell tests 和前端构建均成功；macOS、Windows、Ubuntu 的 Clippy（warnings deny）步骤均成功；
- Task 18–21 窗口验收已闭合；Task 22 的 Mail 本地修复后已完成线程、正文、附件的真实读取验证。

## 四、当前还没有完成的核心事情

### 1. 产品模型已部分统一，仍有未完成处

`Goal / Project / Workspace` 目前主要是目标设计，不是一等运行对象。`Space` 只是引用集合，不能替代 `Workspace`。

`Task`、`Loop`、`Device Action`、`File Write`、`Connector Write` 的执行侧已经由 P0-A–E 统一到 Execution 与 Effect：统一 Execution 记录、Effect 生命周期、审批绑定 Effect 与参数摘要、幂等键、SQLite 状态库、事件日志、Inbox/Outbox、恢复与接管。以下仍未完成：

```text
Task → Execution → Effect → Evidence / Artifact
```

其中 Evidence / Artifact 一环尚未成为与 Effect 校验挂钩的一等对象（任务产物目前以文件形式存在，没有统一的 Evidence 记录）；Effect 状态词仍需对照：本文的 proposed / approved / dispatched / observed / verified / unknown 与 P0-C 落地的 planned / authorized / started / succeeded / failed / unknown / reconciling / reconciled 之间的映射关系尚未钉住，文件任务闭环实施前必须先定；Workbench、Tasks、Files、Inbox、接管的 UI 同记录绑定尚未做（服务端内核已统一，客户端绑定是 P0 之后的工作）。

### 2. 持久化和恢复已收敛到 SQLite 内核，读回覆盖仍不完整

P0-D/E 已交付：SQLite 事务状态库（WAL + synchronous FULL）、事件日志与 schema 版本迁移（db_meta v1、手写迁移）、Inbox / Outbox（首批四类 + 本机通知占位）、事件游标（聚合内 seq + 全局自增 id，绝不按 wall-clock 排序）、消费幂等（幂等键 + UNIQUE 兜底）、WorkerSession / Assignment / Checkpoint 表与创建时快照、崩溃后核查与重启恢复、SQLite 域的断电耐久。切换后 JSONL 转为生成的审计导出；tasks / loops / actions / sessions / policies 按 P0 范围决定仍留 JSONL。

仍未完成：

- 文件、任务、连接器写入的真实读回：P0-C 只给设备类做了可核查读回，其余类暂以 `effect_no_readback` 拒绝核查；
- Worker lease、heartbeat：P0 有意留给 P0 之后（本机没有持续写心跳的 Worker，超时判定无法兑现），租约列继续留空；
- 迟到结果与重复副作用的统一处理：已覆盖类（设备、文件/循环终态复用）已有规则，未覆盖类仍随读回扩展走；
- 跨设备 handoff 不在 P0 内，仍未形成。

### 3. 真实业务面仍有边界

P0-A–E 未改变上述边界（执行内核收敛不等于业务面补齐）：

- Mail 目前是 macOS AppleScript 只读路径，不是 OAuth 或发送能力；
- Connectors 主要是 Mock 账号和 Mock 数据；
- Git、Worktree、CodeChange、diff、patch、commit 和测试 Evidence 还不是完整产品链路；
- 长期记忆、Source Registry、主动规则和跨设备 handoff 尚未形成；
- LAN、外部鉴权、多用户、Noise、签名、公证、正式升级和完整安装生命周期尚未交付。

### 4. 技术债务

源码仍有遗留 Mock 骨架和 TODO，例如 `apps/desktop/src/transport.rs`、`gateway.rs`、`lib.rs` 中的部分路径。共享 Schema 也没有覆盖所有当前 HTTP/Invoke DTO，部分模块仍存在双轨契约。

## 五、市场定位修正

市场上的通用 Agent 已经集中在跨应用执行、浏览器操作、持续任务、日程/邮件和成品交付：

- [OpenAI ChatGPT Work](https://openai.com/index/chatgpt-for-your-most-ambitious-work/)
- [Google Gemini Agent](https://blog.google/products-and-platforms/products/gemini/gemini-3-gemini-app/)
- [Perplexity Computer](https://www.perplexity.ai/products/computer)
- [Manus Features](https://manus.im/features)
- [Lindy Product](https://www.lindy.ai/product)

因此 bu2agent 不应该以“连接器数量”作为首要竞争点，而应聚焦：

> **本地 PC 上的可验证工作执行：能读真实状态、能请求确认、能留下产物、能在失败或重启后继续。**

首发用户建议锁定为经常处理本地文件、提醒、邮件和小型代码仓库的个人知识工作者：产品经理、独立开发者、研究者、顾问和内容创作者。

## 六、下一阶段必须做什么

### P0 剩余：本地文件任务闭环（建在已交付内核上）

P0-A–E（执行内核迁移）已交付：Task / Execution / Effect 三对象、审批绑 Effect、幂等、SQLite / 事件 / Inbox / Outbox、恢复与接管。本节 P0 是在该内核上补齐文件任务闭环，不再重做内核。

文件任务必须完整经过：

```text
Workbench
→ Task
→ Execution
→ Local Workspace Snapshot
→ 文件读取
→ 草稿 / 计划
→ Approval
→ Effect
→ 写入
→ Readback
→ Artifact / Evidence
→ 重启恢复
```

其中以下环节已交付：Execution 创建时同事务建快照（内容取现有 store，files_root 取网关 home 属近似、memory_scope 记字面 `"default"` 占位、指令摘要取模板，见 P0-E 方案）；审批绑 Effect；写入经统一 Effect；重启恢复与接管。以下环节待补：

- 真实读回扩展：把核查能力从设备类扩展到文件、任务、连接器写入；
- Artifact / Evidence 对象：与 Effect 校验挂钩的一等记录，不只是文件产物；
- Effect 状态词对照：本文与 P0-C 词表的映射，实施前钉住；
- UI 同记录绑定：Workbench、Tasks、Files、Inbox、接管看到同一事实。

P0 退出标准（已交付 / 待补对照）：

- Workbench、Tasks、Files 看到的是同一条任务记录：内核已统一，UI 绑定待做（部分）；
- 写入、重命名、导出和创建目录都经过统一 Effect：已交付（P0-B）；
- Effect 状态：本文 proposed / approved / dispatched / observed / verified / unknown 与 P0-C 词表的映射待钉住；
- 重启后不依赖模型重新猜测进度：已交付（P0-E 恢复 + 检查点）；
- 重复请求不会重复写入：已交付（幂等键 + UNIQUE 兜底）；
- unknown 会先读回，再决定重试或人工处理：设备类已交付，文件/任务/连接器类随读回扩展做。

### P1：提醒事项

复用 P0 的 Execution/Effect：读取提醒、提出动作、审批、EventKit/Fake 执行、真实状态读回、超时和 unknown 处理。复用的内核已就绪（提醒事项的 Effect 与设备核查已在 P0-B/C 落地）。

### P2：Mail / Notes

先做真实读取、引用、摘要和草稿 Artifact；发送和外部写入单独建 Effect，不与只读能力混在一起。

### P3：工程 Agent

引入 Project、Location、Workspace、Worktree、Git status/diff、CodeChange、Patch、测试 Evidence 和审批后应用。

### P4：主动性和记忆

在执行内核稳定后，再做 Source Registry、定时任务、会议输入、长期记忆和主动规则。

### P5：多设备连续性

最后做手机审批、PC 继续执行、设备能力注册、离线队列、冲突解决和 Watch attention。

## 七、每条业务切片的统一验收标准

- UI、Invoke、Rust、Gateway/Native Source 全链路接通；
- 状态不只存在 UI 内存；
- 写入类动作有明确 Approval；
- 完成结果来自真实 Readback 或 Artifact；
- 覆盖拒绝、取消、超时、权限失败、目标切换、重启、重复调用和 unknown；
- Workbench、Tasks、Files、Devices、Connectors 显示同一事实；
- 失败时提供下一步动作；
- release app 退出后无残留进程、端口和临时测试数据。

## 八、最终判断

当前 bu2agent 已经不是“客户端骨架”，而是一个 PC-first 本地 Agent 控制平面，横向入口和专项验收已经比较丰富；P0-A–E 又把执行侧收敛到了统一内核。

下一阶段不应继续增加更多页面或孤立连接器，而应把已有文件、任务、循环、设备和连接器能力收敛到同一个可恢复执行模型。最优先的交付物仍是：

> **一个从用户目标到本地文件结果、经过审批、真实读回、产物交付并支持重启恢复的完整 PC 业务闭环。**

其中审批、产物写入、重启恢复的内核部分已由 P0-A–E 交付；待补的是真实读回扩展、Artifact/Evidence 对象与 UI 同记录绑定。

### 本轮验证限制（原稿）与重校准说明

原稿直接执行 `bash run-tests.command` 时，受限环境无法创建项目 `target/debug/.cargo-lock`，因此没有把这次命令失败当成产品测试失败；原稿的通过结果来自项目现有 `.codex/testing.md`、`.codex/verification.md`、窗口验收报告和 CI 记录。本版重校准的测试证据已更新为 CI run `41690215376` 与工作区 328 项（VM 验证），取代原稿的受限环境说明。
