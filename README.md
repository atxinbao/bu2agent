# 个人 Agent 平台（首期）——项目文档

[![CI](https://github.com/atxinbao/bu2agent/actions/workflows/ci.yml/badge.svg)](https://github.com/atxinbao/bu2agent/actions/workflows/ci.yml)

- 目标记录：`goal_a083c94c3d89`（bu2agent；2026-10-24 由旧目标 `goal_33244669e730` 重建，slug 统一为 bu2agent）
- 最终目标（2026-10-24 用户确认）：逆向 Muse app，做一个完全一样的个人 Agent 客户端，并为此构建底层架构；首期先按 Rust 客户端 + TS 前端 + Mock Gateway 跑通主线，底层架构按契约先行、可替换设计。
- 基线文档：《Muse macOS 8.0 逆向工程与重建规格》（2026-10-24）
- 详细规划：`~/workspace/goals/web-agent/files/phase1-foundation-architecture-and-plan.md`
- 本目录是 Mac 本地项目家目录：/Users/mac/Documents/bu2agent，先放项目文档与 Phase 0 契约骨架，代码实现按 Phase 0 → Phase 5 推进。
- 语言：本文新写内容使用 zh-CN。

## 1. 一句话目标

做一个首期可用的个人 Agent 平台：能聊天、能看 Agent 流式输出、能读写文件、每一次工具调用都可追踪、可审批。先让主线真正跑通，不追与原客户端完全等价。

## 2. 方案（2026-10-24 调整后）

**客户端从一开始就用 Rust 写，前端用 TypeScript 写，同一套 Rust 客户端方案覆盖 macOS / Linux / Windows。**

```text
TS 前端 UI → Domain Store → Rust↔TS 桥
  → Rust 客户端壳：窗口/WebView、Gateway 传输、会话目标、作用域文件、密钥抽象、权限预检
  → Rust Mock Gateway（首期）
```

| 层 | 选型 | 说明 |
|---|---|---|
| 客户端 | Rust，Tauri 2（2026-10-24 已锁定） | 窗口、WebView、IPC、打包由框架收敛；不直接拼 tao + wry |
| 前端语言 | TypeScript | 契约类型即文档；UI 框架只做渲染，Domain Store 为核心 |
| Mock Gateway | Rust，推荐 Axum | 与客户端同语言；内存 + fixture，不先上数据库 |
| 契约 | JSON Schema（语言无关） | Rust 用 serde 类型、TS 用对应类型，两侧由同一份 Schema 校验 |
| 测试 | 契约测试 + Mock 端到端 + 失败矩阵 + 三系统冒烟 | 失败矩阵是必选项，不是可选项 |

## 3. 首期做与不做

**做：** 聊天与 Agent 流式交互、文件浏览与读写、任务、审批；Rust 客户端壳、桥、会话目标、作用域文件、密钥存储抽象、权限预检与降级；Mock Gateway、契约测试、fixture、失败回放。

**暂缓（不计入首期完成）：** Swift macOS 单系统壳、系统数据同步（日历/联系人/邮件/备注/提醒）、Computer Use、真实 Noise 传输、私有鉴权与 CVM 恢复、真实连接器 OAuth、真实邮件/消息发送、共享 Agent 与 peer 的完整形态、签名与公证（先保证三系统可安装、可启动）。

## 4. 必须先定的契约

本目录 `packages/contract/schemas/` 已落 Phase 0 骨架：

- `gateway-contract.schema.json`：GatewayTargetKey、GatewayRequest、GatewayResponse、SubscriptionRecord、AgentInvocation、UnifiedError、PermissionSnapshot。
- `bridge-contract.schema.json`：桥命令（前端 → Rust，类型化 invoke）、桥事件（Rust → 前端）、桥版本。
- `services/mock-gateway/fixtures/`：聊天历史与流式事件示例，格式必须过 Schema 校验。

关键规则（详见详细规划文档）：

- 每个请求带 requestId、超时、目标 key、取消信号；目标切换或 generation 变化后，旧结果必须丢弃。
- 订阅按 sequence 去重，断线可用 resumeCursor 续传，续传失败则重新 hydrate，不得重复追加 delta。
- 审批状态机为 `pending -> approved | rejected | edited`；未审批的敏感请求在 Mock 侧也必须被拒绝，前端按钮不是防线。
- 取消要贯穿 UI → Store → 桥 → Transport → Mock，取消后不得再写入该 turn 的后续 delta。
- 401 刷新后只重试一次，且写操作不得因重试而重复写入。
- 脱敏清单：token、cookie、恢复 secret、完整用户输入、邮件正文、本地路径原文、附件正文，一律不得进普通日志、fixture 与 bug report。

## 5. 仓库规划

```text
bu2agent/
  README.md                 # 本项目文档
  apps/desktop/             # Rust 客户端壳（Tauri 2 推荐）：窗口、桥、传输、文件、会话、权限、可观测性
  apps/web/                 # TS 前端：Shell、Domain Store、Agent/Approval 模型、连接器模型、FeatureConfig、i18n
  packages/contract/        # 共享契约：JSON Schema + Rust serde 类型 + TS 类型（本阶段先落 Schema）
  services/mock-gateway/    # Rust Mock Gateway：路由矩阵、流式 fixture、权限 fixture、审批 fixture
  tests/
    contract/               # 契约测试
    integration/            # Mock 端到端
    failure-matrix/         # 失败与恢复矩阵
    cross-platform/         # 三系统构建与冒烟
```

当前状态：`apps/desktop/` 与 `apps/web/` 尚未开工；`packages/contract/schemas/` 与示例 fixture 已落骨架。不要把骨架当作已实现。

## 6. 开发阶段

- **Phase 0：契约与 Fixture**——本目录的 Schema 与示例即起点；退出标准是契约测试全过，且字段改名或改语义会让测试失败。
- **Phase 1：Rust 客户端壳 + TS 前端 + Mock Gateway + 聊天主线**——先保证 history / stream / cancel，断线重连不重复。
- **Phase 2：会话目标 + 文件**——目标切换取消旧请求；分块读写、base64、EOF、maxBytes、取消、重试。
- **Phase 3：Agent 调用 + 审批 + 工具结果**——全链路可追踪，审批历史可查，编辑后参数为准。
- **Phase 4：连接器、凭据与 Egress（Mock 级）**——scope 与 egress 显式、可审计，不接真实 OAuth。
- **Phase 5：可靠性、可恢复性与清理 + 三系统打包**——断线恢复、续传、迁移、退出登录隔离、三系统可安装可启动。

阶段之间是依赖关系：上一阶段退出标准不满足，不进入下一阶段的新能力。

## 7. 验收要点（摘要，完整版见详细规划文档第 4 节）

- 桥就绪、连接状态、致命错误上报可用；桥版本不匹配时启动即明确报错。
- 聊天、文件、审批三条主线在 macOS / Linux / Windows 上都跑通，只在某一系统上通过不算完成。
- 失败矩阵全覆盖：断线、取消、超时、401、权限拒绝、审批拒绝、旧 generation、目标切换。
- 反验收：只有前端动画、Mock 永远成功、取消只关 spinner、旧会话结果混入新会话、权限拒绝显示成功、日志或 fixture 含 secret、把 337 条静态路由当作已对接接口数，一律不算通过。

## 8. 证据边界与未知项

基线文档的结论分观察、推断、未知三档。337 条路由来自静态 route spec，请求体、响应模型、鉴权细节、服务端状态机、Noise 握手、真实 Tool 参数 schema 等仍属未知。没有受控运行时证据时，保持“未知”，不在代码注释或文档里写成已确认；有证据时，更新本文件与契约测试。

## 9. 待确认（不阻塞 Phase 0–1）

1. 前端框架的最终选型。
2. Tauri 与 tao + wry 的最终取舍。
3. 最终部署形态：本地客户端 + Mock 演示，还是内网可访问服务？
4. 真实鉴权提供方与多用户形态：首期 Mock 之后接哪一套？
5. 三系统打包的签名与公证策略。
6. 首期内是否加入任务与 artifacts 的最小闭环，还是只保聊天、文件、审批三条主线？建议先保三条主线。

## 10. 底层架构（已搭建骨架）

详见 `ARCHITECTURE.md`：Muse 模块映射、分层、核心接口、三条状态机、传输与文件、节点与工具、认证与存储、系统差异收敛、路由分批与等价验证策略。

骨架位置：`apps/desktop/src/`（Rust：gateway、transport、bridge、file_client、node、session、permission、approval、sync、tools、observability）、`apps/web/src/domain/`（TS：types、chat）。骨架是类型、trait 与状态机，未实现部分以 `todo!` / TODO 标记，不得当作已跑通。测试走混合回路（见第 13 节），不再以“VM 无 Rust”为由跳过编译校验。

## 11. 进展：契约测试与 Mock 聊天状态机在 VM 预筛和 Mac 本机都已通过

- 契约测试：`apps/desktop/tests/contract_tests.rs`，覆盖目标失效、桥白名单与版本、审批门、turn 终态、权限快照、401 最多重试一次、会话退出清理、Phase 1 工具注册、脱敏。
- Mock 聊天状态机：`services/mock-gateway/src/lib.rs`（纯逻辑，暂无 HTTP 服务），含种子历史、1..=4 序列的流式记录、去重、取消、审批门，另有内置测试。
- VM 预筛结果（2026-10-24，rustc/cargo 1.99.1，VM 临时镜像与 Mac 文件逐字节对齐）：`cargo test` 通过 17 项——契约测试 12 项 + Mock 内置测试 5 项。预筛发现并已修复一处真实 bug：`services/mock-gateway/Cargo.toml` 曾写 `agent_desktop = { path = ... }`，Cargo 找不到该包，已改为包名 `agent-desktop = { path = ... }`（lib 名仍为 `agent_desktop`，代码里的 `use agent_desktop::...` 不变）。
- Mac 最终验证已通过（2026-10-24，用户在 Mac 上跑 `run-tests.command`，终端输出 `PASS: all tests passed.`）：契约测试 12 项 + Mock 内置测试 5 项，与 VM 预筛结果一致，含已修复的 mock-gateway 依赖名 bug。至此这套测试在 VM 预筛和 Mac 本机两边都通过了。

## 12. 进展：Mock Gateway 已接 Axum 传输，VM 预筛和 Mac 本机都已通过

- 传输层只新增，不改规则：`services/mock-gateway/src/server.rs`（Axum 路由）+ `src/main.rs`（二进制，默认只绑 `127.0.0.1:3001`，可用 `MOCK_GATEWAY_ADDR` 改地址）把 `lib.rs` 的纯逻辑规则暴露为 HTTP；`lib.rs` 本身只加了一行 `pub mod server;`，种子历史、1..=4 序列、去重、取消、审批门逻辑未动。
- 端点：`GET /health`、`GET /chat/history`、`POST /chat/stream/start`、`POST /chat/stream/apply`、`POST /chat/stream/cancel`、`POST /approval/check`；未知流返回 404、未知审批状态返回 400，不编造结果。
- 依赖：`axum 0.8`、`tokio 1`、`serde`、`serde_json`（此前 std-only 的刻意限制至此结束，Phase 1 接线本就需要它们）。
- VM 预筛结果（2026-10-24）：`cargo test` 通过 19 项——契约 12 + Mock 内置 5 + HTTP 冒烟 2（`tests/http_smoke.rs`，在临时端口上用原始 HTTP 验健康、历史、启动/去重/取消、审批门与 404/400）；另用 `curl` 实测真实二进制：健康、历史、流启动、审批 pending 拒绝均与预期一致，测完服务已停、3001 端口已关。
- Mac 最终验证已通过（2026-10-24，用户在 Mac 上跑 `run-tests.command`，终端输出 `PASS: all tests passed.`）：19 项全过——契约 12 + Mock 内置 5 + HTTP 冒烟 2，与 VM 预筛一致；Mac 上首次拉取依赖共 60 个包（axum 0.8.9、tokio 1.54.1、serde 1.0.229 等，与 VM 锁定的版本一致），`Cargo.lock` 已随之更新。至此 Axum 传输在两边都通过了。

## 13. 进展：聊天主线已接通，VM 预筛和 Mac 本机都已通过

- Rust 侧：`apps/desktop/src/chat_client.rs`（std TcpStream HTTP 客户端，localhost-only、无 TLS，Mock 专用；错误体不进日志，只报状态）+ `src/bin/chat-cli.rs`（可跑的演示 CLI，非 UI、非 Tauri 壳）+ `tests/chat_client_test.rs`（对仿真服务器的端到端测试）。流程：history → stream start → stream apply（去重）→ 组装 assistant 文本；取消路径：start → cancel → apply，kept 为 0、last 为 0、文本为空。
- TS 侧：`apps/web/src/domain/gateway_client.ts`（fetch 客户端，服务端 snake_case 与 TS camelCase 双向映射，payload 字符串只解析一次）+ `chat.ts` 扩展（applyStreamRecord 真正折叠事件：turn_start 开 turn、text_append 追加 assistant 文本、message_done 完成；cancelTurn 标记取消流，其后续记录被丢弃；runTurn 串起 start/apply/折叠）。
- 预筛时发现并已修复一处真实编译错误：`ApplyResponse` 曾派生 Deserialize，但 `SubscriptionRecord` 未实现 Deserialize，改为只在传输 DTO 上派生，`ApplyResponse` 本身不再派生。
- VM 预筛结果（2026-10-24）：`cargo test` 通过 22 项——契约 12 + Mock 内置 5 + HTTP 冒烟 2 + 聊天客户端内置 2 + 聊天客户端端到端 1；TS 用 TypeScript 5.9 strict 模式 `--noEmit` 检查通过（types/chat/gateway_client 三文件，零错误）；另用真实二进制端到端实测：正常 turn 历史 2 条、启动 4 条、应用 4 条、assistant 文本与输入一致，取消 turn 应用 0 条、文本为空，测完服务已停。
- Mac 验证已完整（2026-10-24）：`run-tests.command` 22 项全过，`run-chat.command` 一键演示也已通过——正常 turn 历史 2 条、启动 4 条、应用 4 条、assistant 文本与输入一致；取消 turn 应用 0 条、last 为 0、文本为空，演示结束服务已停，与 VM 实测完全一致。至此聊天主线（Rust 数据流）在 VM 和 Mac 两边都验证通过；TS 类型检查为 VM 侧 TypeScript 5.9 strict 零错误，Mac 上尚未单独跑 tsc。
- 诚实边界：这一步接通的是聊天主线的数据流，不是带界面的客户端；Tauri 窗口/WebView 壳与前端渲染仍未做，是下一步之外的待确认项。

## 14. 进展：Phase 2 会话目标与文件已接通，VM 预筛和 Mac 本机都已通过

- 会话目标：`session.rs` 新增 `switch_target`（切目标后状态回 bootstrap、旧 generation 立即作废）与 `is_current_target`（迟到结果必须先验目标，否则丢弃）；`gateway.rs` 的 `GatewayTargetKey` 本已有的 generation/URL/VM 失效规则未动。
- 本地文件：`file_client.rs` 的 trait 骨架落成 `ScopedFileClient`——单根目录作用域（绝对路径与 `..` 一律拒绝）、写/删/改名/建目录默认关闭、只有显式审批后才放行、分块读（offset/len、EOF、maxBytes、文本或 base64）与 `read_all_chunked` 循环组装。
- 网关文件：Mock 新增文件端点（`server.rs`，根目录默认 `./mock-files`，可用 `MOCK_FILES_ROOT` 改，路径逃逸一律 400）——`GET /files/list`、`GET /files/read`（offset/len、base64、EOF、total_size）、`POST /files/write`（覆盖/追加）、`POST /files/delete`（拒目录）；Rust 侧新增 `http_client.rs`（聊天与文件共用的 HTTP  plumbing）与 `file_gateway_client.rs`（list/read/write/delete），TS 侧新增 `apps/web/src/domain/file_client.ts`（同规则：作用域检查、分块读、审批门）。
- 401 规则按 `file_client.rs` 的原话落地：读（list/read）401 后可刷新重试最多一次，写/删 401 后绝不盲重试（会重复写），未审批的写/删在发请求前就被客户端拒绝。Mock 本身从不返回 401，所以重试路径由桌面端仿真服务器测试覆盖（读：401 一次后 200、刷新恰好调用一次；写：401 只发一次、不重试），不是由真实 Mock 覆盖——这条限定写在这里，不藏起来。
- 演示：新增 `src/bin/file-cli.rs`（网关路径与 `--local-root` 本地路径两条线，命令在前、参数在后，如 `file-cli write --path demo.txt --text "hello" --approve`）与 `run-files.command`（一键：起服务、列目录、审批写、分块读、无审批写被拒、审批删、再列目录、停服务；首次需 `chmod +x run-files.command` 一次）；`.gitignore` 新增 `/mock-files`。
- 预筛时发现并已修复三处真实错误：Mock 追加分支的 Result 类型不匹配、文件冒烟测试漏写 `start_server(...).await`、冒烟断言拿 `serde_json::Value` 直接与 bool/数字/字符串比较（改为 `as_bool`/`as_u64`/`as_str`）。
- VM 预筛结果（2026-10-24）：`cargo test` 通过 26 项——契约 12 + Mock 内置 5 + HTTP 冒烟 2 + 文件冒烟 1 + 聊天客户端内置 2 + 聊天客户端端到端 1 + Phase 2 新增测试 3（会话切换、本地作用域文件、网关 401 规则）；TS 用 TypeScript 5.9 strict 模式 `--noEmit` 检查通过（types/chat/gateway_client/file_client 四文件，零错误）；另用真实二进制端到端实测：网关路径写 11 字节、分块读组装出原文、无审批写被拒且原文不变、审批删后目录为空、本地路径读写与逃逸拒绝均符合预期，测完服务已停、VM 临时镜像已清。
- Mac 验证已完整（2026-10-24）：`run-tests.command` 26 项全过（Mac 首次拉取新依赖 base64 0.22.1，`Cargo.lock` 随之更新），`run-chat.command` 回归也已通过（正常 turn 应用 4 条、取消 turn 应用 0 条，与之前一致，说明 Phase 2 的 HTTP 抽取没有破坏聊天主线），`run-files.command` 一键演示也已通过——事前列目录为空、审批写 11 字节、分块读出原文、无审批写被拒、审批删后列目录又为空、演示结束服务已停，与 VM 实测完全一致。至此 Phase 2（会话目标与文件）在 VM 和 Mac 两边都验证通过；TS 类型检查为 VM 侧 TypeScript 5.9 strict 零错误，Mac 上尚未单独跑 tsc。
- 诚实边界：这一步接通的是文件主线的数据流（网关文件 + 本地作用域文件），不是带界面的客户端；Tauri 窗口/WebView 壳与前端渲染仍未做；Mock 的符号链接逃逸未检查，真实客户端必须查 canonical 路径。

## 15. 进展：Phase 3 Agent 调用与审批已接通，VM 预筛和 Mac 本机都已通过

- 审批闭环（四步缺一不可）：客户端注册表门（未知工具在发请求前就被拒）→ `POST /approval/request`（审批工具开 pending 记录，非审批工具不需要记录）→ `POST /approval/decide`（approved/rejected/edited）→ `POST /agent/tool/call`（服务端用自己的记录复核：无记录、pending、被拒一律 403，客户端声称已审批不算数；客户端门只是纵深防御，服务端门才是执行点）。
- 工具集：注册表工具 `fs.list`/`fs.read`（无需审批）、`fs.write`/`fs.delete`（需审批），另加 Mock 专用演示工具 `echo`（无需审批，永远不是真实工具）；服务端审批标志以 `lib.rs` 的 `tool_requires_approval` 为准，与桌面注册表一致，客户端与服务端标志不一致时客户端直接报错，不猜。
- 服务端执行是真的：`fs.write`/`fs.delete`/`fs.read`/`fs.list` 落在文件根目录（与 Phase 2 同根），`echo` 回显文本；`fs.read` 只读 UTF-8 文本（非文本走文件端点）；缺参数、未知工具 400，审批问题 403。
- Rust 侧：`agent_client.rs`（request/decide/call/run_tool，`ApprovalPending` 带 tool_call_id 供调用方决策）+ `src/bin/agent-cli.rs`（`call --tool ... [--approve|--reject]`，无标志时在 pending 记录处停下并打印 id；`--reject` 时先记被拒再调用，调用必须被服务端拒绝）；TS 侧：`apps/web/src/domain/agent_client.ts`（同流程，`ApprovalPendingError` 带 id）。
- 预筛时发现并已修复一处真实编译错误：`AgentClient` 派生 Clone 但 `ToolRegistry` 未实现 Clone，已给注册表补 Clone。
- VM 预筛结果（2026-10-24）：`cargo test` 通过 29 项——契约 12 + Mock 内置 6（新增工具审批标志 1）+ HTTP 冒烟 2 + 文件冒烟 1 + Agent 冒烟 1 + 聊天客户端内置 2 + 聊天客户端端到端 1 + Phase 2 测试 3 + Phase 3 测试 1（仿真服务器上的完整审批闭环：直接跑、pending 停下、审批后跑、被拒、直调无记录被拒、未知工具零请求）；TS 用 TypeScript 5.9 strict 模式 `--noEmit` 检查通过（五文件，零错误）；另用真实二进制端到端实测：echo 直接跑、无审批写停在 approval-1、审批写 10 字节、读回原文、被拒写被服务端拒绝且再读内容不变，测完服务已停、VM 临时镜像已清。
- Mac 验证已完整（2026-10-24）：`run-tests.command` 29 项全过，`run-agent.command` 一键演示也已通过——echo 直接跑、无审批写停在 approval-1、审批写 10 字节、读回原文、被拒写被服务端拒绝且再读内容不变、演示结束服务已停，与 VM 实测完全一致。至此 Phase 3（Agent 调用与审批）在 VM 和 Mac 两边都验证通过；TS 类型检查为 VM 侧 TypeScript 5.9 strict 零错误，Mac 上尚未单独跑 tsc。
- 诚实边界：这一步接通的是 Agent 调用与审批的数据流，不是带界面的客户端；Tauri 窗口/WebView 壳与前端渲染仍未做；审批记录只在 Mock 内存里，重启即失，真实鉴权与持久化在 Phase 4 之后。

## 16. 进展：Phase 4 连接器/凭据/Egress 已接通，VM 预筛和 Mac 本机都已通过

- Mock 级边界（先说清楚，不藏）：连接器是 Mock 专用假数据（mock-notes、mock-mail，种子各 2 条），永远不是真实邮件/备忘录/日历，不碰真实 OAuth；凭据存储是内存专用（MemoryCredentialStore），不落盘、不进日志、只给 `****` + 后 4 位的脱敏显示，新进程即为空；真实密钥库（macOS Keychain 等）与真实鉴权/持久化仍在后续阶段。
- Egress 策略（`egress.rs`）：出站先验三关——目标只允许 loopback（127.0.0.1 / localhost，端口不限，Mock 默认 3001、测试用临时端口），其他主机与非 http 方案一律拒绝；Mock 邮件收件域只允许 `mock.local`，域外地址在发请求前就被客户端拒绝，服务端再复核一次（域外返回 403 `egress_denied`）；载荷含 `token:`、`Bearer `、`api_key=` 一律拒绝，防凭据被误贴进出站内容。
- 审批复用 Phase 3 闭环：`notes.create` 与 `mail.send` 需审批（服务端 `tool_requires_approval` 新增四项，桌面注册表新增 `ToolRegistry::phase4()`，AgentClient 改用它），`notes.list` 与 `mail.list` 无需审批；服务端把审批门抽成共用 `check_approval`，Agent 工具与连接器写共用同一道门，无记录/pending/被拒/工具不符一律 403。
- Rust 侧：`credential_store.rs`（内存存储 + 脱敏）+ `egress.rs`（三关策略）+ `connector_client.rs`（notes/mail 客户端，Egress→注册表→审批→载荷的顺序）+ `src/bin/connector-cli.rs`（notes list/create、mail list/send、cred store/show）；TS 侧：`apps/web/src/domain/connector_client.ts`（同规则，`ApprovalPendingError` 带 id）。
- 演示：新增 `run-connectors.command`（一键：起服务、列假笔记/假邮件、无审批新建笔记停在 pending、审批新建笔记、再列目录见新笔记、域外发邮件被 Egress 拒绝、域内审批发邮件、凭据存储与新进程为空的证明、停服务；首次需 `chmod +x run-connectors.command` 一次）。
- 预筛时发现并已修复两处真实问题：Egress 白名单起初只认 `:3001` 端口，导致仿真服务器（临时端口）被拒——修正为 loopback 主机即边界、端口不限；AgentClient 的注册表门起初只含 Phase 1 文件工具，导致 `notes.create`/`mail.send` 在发请求前就被当成未知工具——修正为注册表新增 `phase4()` 并由 AgentClient 使用，门没有被绕过而是被补齐。
- VM 预筛结果（2026-10-24）：`cargo test` 通过 33 项——契约 12 + Mock 内置 6 + HTTP 冒烟 2 + 文件冒烟 1 + Agent 冒烟 1 + 连接器冒烟 1 + 聊天客户端内置 2 + 聊天客户端端到端 1 + Phase 2 测试 3 + Phase 3 测试 1 + Phase 4 测试 3（凭据内存与脱敏、Egress 三关、仿真服务器上的连接器审批与域外拒绝）；TS 用 TypeScript 5.9 strict 模式 `--noEmit` 检查通过（六文件，零错误）；另用真实二进制端到端实测：假笔记/假邮件各 2 条、无审批新建停在 approval-1、审批新建得 note-3、再列见 3 条、域外发邮件被 Egress 拒绝、域内审批发邮件得 mail-3、凭据存后新进程仍为空，测完服务已停、VM 临时镜像已清。
- Mac 验证已完整（2026-10-24）：`run-tests.command` 33 项全过，`run-connectors.command` 一键演示也已通过——假笔记/假邮件各 2 条、无审批新建停在 approval-1、审批新建得 note-3、再列见 3 条、域外发邮件被 Egress 拒绝、域内审批发邮件得 mail-3、凭据存后新进程仍为空、演示结束服务已停，与 VM 实测完全一致。至此 Phase 4（连接器/凭据/Egress）在 VM 和 Mac 两边都验证通过；TS 类型检查为 VM 侧 TypeScript 5.9 strict 零错误，Mac 上尚未单独跑 tsc。
- 诚实边界：这一步接通的是连接器/凭据/Egress 的数据流与策略，不是真实连接；Tauri 窗口/WebView 壳与前端渲染仍未做；连接器数据只在 Mock 内存里，重启即失。

## 17. 进展：Phase 5 可靠性/清理/三系统打包已接通，VM 预筛和 Mac 本机都已通过

- 可靠性：新增 `retry.rs`（有界重试：只重试标记为 retryable 的错误，最多 3 次、线性延迟；不可重试错误一次即停，写操作不会因重试被重复执行；文件网关已有的 401 专属规则不动，不被它替代）+ `http_client.rs` 超时（连接/读/写默认 10 秒，静默服务器不再挂住 CLI，测试用 `with_timeout` 缩短到 500 毫秒验证）+ Mock 服务端 Ctrl-C 优雅停服（`server::serve` 接 `with_graceful_shutdown`，停服时打印一行，不半路截断响应）；TS 侧新增 `apps/web/src/domain/retry.ts`（同规则）。
- 清理（Clippy 门）：CI 要求 `cargo clippy --all-targets -- -D warnings` 全过；预筛修了 4 类警告——文件网关文档的懒续行（补空行）、file-cli 的 9 参数/8 参数函数（收进 `CliArgs` 结构体）、审批门的超大 Err 类型（`Response` 改装箱）、Phase 5 测试里多余的 `if let`（改 `flatten`）。修完后 Clippy 零警告，改动均有测试或实测覆盖，不是为过门而静默 lint。
- 三系统打包：新增 `.github/workflows/ci.yml`（macOS / Linux / Windows 三系统跑 `cargo test` + Clippy，另跑 TS 严格检查；YAML 已在 VM 侧解析验证，矩阵与任务无误）+ `packaging/README.md`（写明当前产物：四个演示 CLI + Mock 二进制；明确未交付：Tauri 壳、签名安装包、自动更新、真实 Noise 传输、真实密钥库、真实 OAuth）+ `run-package.command`（一键：`cargo build --release` 并列出 release 二进制、无参运行各 CLI 只验证不挂住；首次需 `chmod +x run-package.command` 一次）。
- VM 预筛结果（2026-10-24）：`cargo test` 通过 36 项——33 项旧的 + Phase 5 测试 3（重试两次失败后成功、不可重试一次即停、静默服务器 500 毫秒超时）；Clippy `-D warnings` 全过；TS 用 TypeScript 5.9 strict 模式 `--noEmit` 检查通过（七文件，零错误）；另用 release 二进制端到端实测：file-cli 重构后回归正常（审批写 11 字节、分块读出原文、审批删、目录复空）、Mock 服务 SIGINT 后打印停服行并退出、四个 CLI 无参运行均退出不挂住，测完服务已停、VM 临时镜像已清。
- Mac 验证已完整（2026-10-24）：`run-tests.command` 36 项全过，`run-package.command` 一键打包冒烟也已通过——release 构建完成、五个 release 二进制列出（agent-cli、chat-cli、connector-cli、file-cli、mock-gateway）、四个 CLI 无参运行均退出不挂住（退出码与 VM 实测一致：chat/file/agent 为 1、connector 为 2）。至此 Phase 5（可靠性/清理/三系统打包）在 VM 和 Mac 两边都验证通过；TS 类型检查为 VM 侧 TypeScript 5.9 strict 零错误，Mac 上尚未单独跑 tsc；Clippy 全过为 VM 侧验证，Mac 上尚未单独跑 clippy。
- 诚实边界：CI 工作流已入库但尚未在 GitHub 上跑过（项目还没进 GitHub），它是立好的门，不是已观测到的绿灯；Tauri 窗口/WebView 壳与前端渲染仍未做，是 Phase 5 之后的下一步，不是本阶段的产物。

## 18. 锁定决定（2026-10-24 用户确认）

- 前端框架：**React + Vite + TypeScript**，不引入 Web Components。域层（apps/web/src/domain）保持纯 TS、零框架依赖，框架只长在视图层。
- 壳：**Tauri 2**，不直接拼 tao + wry。理由：Tauri 2 等于 tao + wry 再加一套有人维护的能力权限模型、更新器、插件与脚手架；直接拼等于把壳也手写一遍。
- 长寿做法（与框架选择同等重要）：只用 React 本体，不用 Next.js 这类元框架；不引重型组件库；锁死 lockfile 与工具链版本；构建产物本地化，不依赖 CDN；CI 三系统定期跑。

## 19. 进展：Tauri 2 壳骨架已搭好，VM 预筛通过，Mac 最终验证待跑

- 壳：新增 `apps/shell/src-tauri/`（独立 Cargo 工程，不进 workspace，workspace 测试不受影响）：`tauri.conf.json`（devUrl http://localhost:5173、frontendDist ../../web/dist、主窗口 1100x750、identifier com.bu2ai.web-agent）、`capabilities/default.json`（main 窗口 + core:default）、`src/main.rs`（ping/app_info 两个命令，证明桥通）、`build.rs`（tauri_build）。打包图标尚未配（`icon: []`），那是出安装包时再补的事。
- 前端：新增 `apps/web/package.json`（React 19、@tauri-apps/api 2、Vite 6、TS 5.9）、`vite.config.ts`（5173 严格端口）、`index.html`、`src/main.tsx`、`src/App.tsx`、`src/styles.css`（手写简单样式，不引组件库）。App 用现有域层：`GatewayChatClient` + `runTurn` 跑聊天线，输入框刻意用非受控 input（绕开 React 受控输入与中文输入法组词的冲突），状态栏同时显示网关健康与壳桥接结果（Tauri 窗口里应为 shell bridge ok: ping=pong；纯浏览器预览时如实显示 unavailable，不静默失败）。
- 一键演示：新增 `run-shell.command`（首次需 `chmod +x` 一次；首次还需装一次 Tauri CLI：`cargo install tauri-cli --locked --version '^2'`；双击后起 Mock 网关 + Vite 开发服务器，再开 Tauri 窗口；Ctrl-C 退出时两后台服务一并停掉）。
- VM 预筛结果（2026-10-24）：前端 `npm install`（70 包）+ TypeScript 5.9 strict 零错误 + `vite build` 成功（dist/index.html，JS 229.98 KB / gzip 71.80 KB，含 React）；Tauri 壳的 `cargo check` 在 VM 上被系统库挡住（缺 gdk-3.0，VM 无 Linux 图形开发库），这是已知边界——VM 测不了 Tauri/WebView，壳的 Rust 编译与窗口实测由 Mac 承载；tauri.conf.json、capabilities、Cargo.toml 在 VM 上解析验证通过；VM 临时镜像已清。
- Mac 上 `run-tests.command` 已于同日通过 36 项（`PASS: all tests passed.`，与 VM Phase 5 计数一致），workspace 回归无异常；壳是独立工程，确实未影响 workspace 测试。
- Mac 首跑发现并已修复一处真实问题：`cargo tauri dev` 跑 `cargo metadata` 时报错，称 src-tauri“自以为在 workspace 里其实不在”——原因是项目根已有 workspace（members 为 apps/desktop 与 services/mock-gateway），而 src-tauri 嵌在根目录下却不是 member。修法是给 `apps/shell/src-tauri/Cargo.toml` 加空 `[workspace]` 表，把它标成自己的 workspace 根，壳继续不进主 workspace；VM 上用同结构做了对照验证：不加该表复现原报错，加上后 `cargo metadata` 通过（`--no-deps`，只验证 workspace 定位，不含依赖解析）。
- 窗口首跑发现并已修复第二处真实问题：窗口显示 gateway unreachable、桥接正常（ping=pong）——原因是 Mock 服务器没有 CORS 层：页面源 http://localhost:5173 跨域取 127.0.0.1:3001 时被浏览器/WebView 拦掉，而 Rust CLI 与 curl 不受 CORS 限制，所以之前所有测试都过了、唯独窗口露馅。修法是给 `services/mock-gateway/src/server.rs` 的 `app()` 加 `tower-http 0.6` 的 CorsLayer（`Cargo.toml` 新增依赖），只放行 http://localhost:5173 与 http://127.0.0.1:5173、只放 GET/POST 与 content-type 头，不用通配符；另给 `run-shell.command` 加硬：网关或 Vite 轮询后仍不健康时直接退出并指到日志，不再开一个只能报 unreachable 的窗口。VM 上用同配置的隔离 Axum 程序实测：放行源 GET 有 ACAO 回显、POST 预检 200 且 allow-methods/allow-headers 正确、域外源无 ACAO；VM 临时目录已清。Mac 窗口重跑已验证：server.rs 顺利编译、窗口显示 gateway ok 且种子历史正常出现，CORS 修复在真实链路上生效。
- 窗口发送路径已于同日验证：新发“我新发一条消息”后追加同文 assistant 回复，符合 Mock 回显设计（start_stream 的 text_append 即输入原文，不是模型回复）；`apps/web` 的 `npm run build` 也已于同日通过（全量 tsc --noEmit 严格检查 + Vite 构建），至此 Tauri 壳在 Mac 上完整验证通过。
- 诚实边界：壳里跑通的是聊天线 + 桥接证明，不是完整客户端；前端与 Mock 网关之间的聊天线已改走 Tauri invoke + Rust 客户端（见第 20 节，浏览器预览仍用 HTTP 兜底）；文件、Agent、连接器三条线已于第 22 节接进视图（VM 预筛通过，Mac 窗口复验待跑）。

## 20. 进展：聊天线改走 Tauri invoke + Rust 客户端，VM 预筛通过，Mac 最终验证待跑

- 壳侧：`apps/shell/src-tauri/` 新增 `src/gateway_commands.rs`（`gateway_health/history/stream_start/stream_apply/stream_cancel/approval_check` 六个命令，背后用 `apps/desktop` 的 `ChatClient`，`Cargo.toml` 新增 `agent-desktop = { path = "../../desktop" }`，`main.rs` 注册），命令为同步函数（Tauri 命令线程池执行，本地调用毫秒级、客户端自带 10 秒超时，本阶段不需要 spawn_blocking）；错误只报 `UnifiedError` 的 code 与脱敏 detail，不含响应体；DTO 沿用 HTTP 的 snake_case 形状（payload 仍为 JSON 字符串），前端映射代码两边一致；invoke 参数键刻意用 snake_case，与 Rust 参数名精确对应。
- 域层：`gateway_client.ts` 新增 `ChatGateway` 接口（health/history/start/apply/cancel/checkApproval），`runTurn` 改为依赖接口不再依赖具体类；新增 `domain/invoke_gateway_client.ts`（同接口的 invoke 实现）；`App.tsx` 启动时用 ping 探测：桥通则用 invoke 客户端并在状态栏显示 `transport: invoke (Rust client)`，否则回落 HTTP 并显示 `transport: http (browser preview)`，选择从不静默；发送按钮在 transport 未定前禁用（用 state 不用 ref，避免 ref 不触发渲染的坑）。
- VM 预筛结果（2026-10-24）：前端 `npm install`（70 包）+ TypeScript 5.9 strict 零错误 + `vite build` 成功（34 模块，JS 231.25 KB / gzip 72.02 KB）；壳的 Rust 编译仍由 Mac 承载（VM 缺图形库，cargo check 仍被 gdk-3.0 挡住）；VM 临时镜像已清。
- 本轮 workspace Rust 文件未动，`run-tests.command` 36 项不受影响，不必重跑。
- Mac 首编译发现并已修复一处真实错误：`cargo tauri dev` 编译 `web-agent-shell` 时报 `StreamRecordDto` / `ApplyResultDto` 私有——Tauri 的命令宏会生成跨模块可见的包装函数，命令签名里出现的类型必须至少和命令一样可见，而两个 DTO 起初是模块私有的。修法是把两个 DTO 改为 `pub`（字段仍私有，序列化由同模块的 derive 负责，不受影响），连带 3 条 private_interfaces 警告一并消除；path 依赖本身已证明可用（编译日志里 `Locking` 正常加入 agent-desktop 与 base64，agent-desktop 先编译通过）。
- Mac 验证已完整（2026-10-24）：`run-shell.command` 成功重建并运行 web-agent-shell，Mock 健康检查返回 `{"status":"ok"}`，Vite 5173 可访问，状态栏显示 `transport: invoke (Rust client)`，聊天历史、发送、回复均走 Tauri invoke 命令；`apps/web` 的 `npm run build` 通过（全量 tsc --noEmit 严格检查 + Vite 构建）；验证期间用户修正了 App.tsx 第 162 行的引号笔误；验证结束后 3001 与 5173 端口均已释放。至此聊天线 invoke 迁移在 VM 和 Mac 两边都验证通过。
- 诚实边界：迁移的只是聊天线；文件、Agent、连接器视图见第 22 节（VM 预筛通过，Mac 窗口复验待跑）；浏览器预览仍走 HTTP，那是兜底不是主线。

## 21. 测试混合回路（2026-10-24 用户确认）

- **Mac 是唯一代码家目录**：本目录即为准，VM 里不留代码永久副本，只留临时镜像，测完即清，避免两份漂移。
- **VM 是预筛**：VM 已装 Rust（rustup，stable 1.99.1），每次改代码后先把 Mac 文件拉到 VM 临时目录跑 `cargo test`，修通后再交 Mac 最终验证。VM 测不了 Tauri、WebView、密钥库等 Mac 专属部分。
- **Mac 是一键最终验证**：根目录有 `Cargo.toml`（workspace：`apps/desktop` + `services/mock-gateway`）和 `run-tests.command`（双击即在终端跑完整 workspace 测试并停留看结果）。首次使用前需在终端跑一次 `chmod +x run-tests.command` 赋可执行权限，之后双击即可。
- **长期**：项目进 GitHub 后上 CI（macOS / Linux / Windows 自动跑），届时不再依赖手动跑。

## 22. 进展：文件、Agent、连接器接进界面，VM 预筛通过，Mac 窗口复验待跑

- 壳侧：`apps/shell/src-tauri/` 新增 `src/tool_commands.rs`（12 个命令，`main.rs` 注册，`main.rs` 头注释里过时的“前端仍走 HTTP”说法已修正）：文件 `file_list/file_read_text/file_write/file_delete`（背后是 `FileGatewayClient`）、Agent `agent_request_approval/agent_decide_approval/agent_call_tool/agent_run_tool`（背后是 `AgentClient`）、连接器 `connector_notes_list/connector_mail_list/connector_notes_create/connector_mail_send`（背后是 `ConnectorClient`）；每命令每次调用新建客户端，与聊天线同模式；错误只报 code 与脱敏 detail；DTO 一开始就写成 `pub`，不再重犯第 20 节那个私有类型编译错误；Agent 循环的 pending 用 `approval_pending:<tool_call_id>` 一个标记串回传，TS 侧再转回 `ApprovalPendingError`。文件读出是 `read_all_chunked` 后按 UTF-8 lossy 转文本显示（视图层的显示取舍；Mock 里写进来的多为文本，非 UTF-8 字节会被替换符代替）。
- 域层：`file_client.ts` 新增 `FileGateway`、`agent_client.ts` 新增 `AgentGateway`（并导出 `ApprovalRequestResponse` 与 `toolRequiresApproval` 供视图判断）与 `connector_client.ts` 新增 `ConnectorGateway` 三个接口，HTTP 客户端各自实现接口；新增 `invoke_file_client.ts` / `invoke_agent_client.ts` / `invoke_connector_client.ts` 三个 invoke 实现，参数键一律 snake_case；文件列表的 snake_case（`is_dir` / `size_bytes`）到 camelCase（`isDir` / `sizeBytes`）映射放在 invoke 客户端里，不进视图。
- 视图：`App.tsx` 改成 Chat / Files / Agent / Connectors 四个标签页，四个客户端与 transport 一起选（窗口里全走 invoke，浏览器预览里全走 HTTP），视图只拿接口、不拿具体类。审批流统一为“先确认、后一次调用”：Agent 视图里 fs.write / fs.delete 与连接器视图里新建笔记 / 发邮件都先弹确认，点批准才以 `"approve"` 调一次，点拒绝则什么请求都不发；文件视图里写 / 删先过“Allow writes”开关这道客户端门（对应 Phase 2 的 `with_writes_allowed`），删还要再确认一次。诚实说明：Mock 文件端点本身不复核审批记录，文件线的门只在客户端；Agent 与连接器线的门服务端会复核自己的记录，这是两条线本来就不同的地方，视图没有把它们假装成一样。
- VM 预筛结果（2026-10-24）：Mac 文件原样拉到 VM 临时镜像，`npm install` + `npm run build`（全量 tsc --noEmit 严格检查零错误 + vite build 成功，43 模块，JS 247.77 KB / gzip 76.14 KB）；壳的 Rust 编译仍由 Mac 承载（VM 缺图形库）；VM 临时镜像已清。
- Mac 部分验证（2026-10-24，用户侧报告）：`run-shell.command` 成功重建并运行 web-agent-shell（含新增 12 个命令的壳在 Mac 上编译通过），Mock Gateway 与 Vite 正常运行、3001/5173 健康检查通过；代码核对确认四标签页（Chat / Files / Agent / Connectors）、状态栏 `transport: invoke (Rust client)`、文件/Agent/连接器 invoke 命令均已注册；但 CUA 原生应用清单未暴露 Tauri 窗口，无法代替点击标签页、读取窗口文字，因此各视图的窗口内交互（文件列/读/写/删、Agent 五工具与审批确认、连接器新建/发送与审批确认）尚未逐项观测，不得记为通过；进程已停；本轮 workspace Rust 未动，`run-tests.command` 36 项未重跑（与预期一致，不必补跑）。
- 不开窗口的壳命令测试（2026-10-24，用户选定验证方式）：新增 `apps/shell/src-tauri/src/shell_tests.rs`（3 个测试，覆盖全部 12 个命令：文件列/写/读/删往返、Agent 回显与审批 Ask 标记与批准执行与 request/decide/call 长流程、连接器新建/列表/发送与域外拒收；测试只在 `cfg(test)` 下编译，不进正式二进制）与一键脚本 `run-shell-tests.command`（首次需 `chmod +x`；双击后先查 3001 是否已有服务——有则拒绝，避免污染正在跑的 run-shell 会话——再起一次性 Mock（`MOCK_FILES_ROOT` 指向临时目录，退出时删除）、轮询健康、跑 `cargo test -- --test-threads=1`（单线程原因：文件测试断言根目录起始为空，测试共享一个服务端状态）、停服清场、打印 PASS/FAIL 并停留）。VM 预筛：四个 Rust 文件 rustfmt 解析通过（VM 首次装 rustfmt 组件）、脚本 `bash -n` 语法通过、grep 确认 12 个命令全被测试覆盖、JS invoke 参数键与 Rust 参数名静态对表 12 项全对；壳测试的真正编译与执行由 Mac 承载（VM 缺 gdk-3.0，编不了 tauri 依赖）；Mac 已于同日跑通 `run-shell-tests.command`：3 项全过（agent / connector / file 三个 roundtrip，0 失败），一次性 Mock 与临时文件根已清、3001 已释放，终端里那行 mock-gateway 的 Terminated 是退出时 trap 发 SIGTERM 停服，属于正常收尾不是失败。诚实边界：这套测试跑通后，命令本身的执行与错误映射就算在真实链路上验过，但前端 invoke 调用的键绑定（JS 键名对 Rust 参数名）仍只经过静态对表，没有窗口内的运行时证明——要彻底闭合仍需将来手动点一次标签页。

## 23. 进展：进 GitHub（进行中，用户定公开）

- 公开前审计（2026-10-24）：根目录与 `TOOLS-INTEGRATION.md`、`ARCHITECTURE.md`、`README.md`、各 `run-*.command` 均无凭证、无设备标识、无私人内容（只有通用的 `/Users/mac` 路径与应用名）；`.gitignore` 已覆盖 `/target`、`**/target`、`/mock-files`、`node_modules/`、`apps/web/dist`，本轮又加了 `.DS_Store`；首 commit 前又排除了 Tauri 生成的 `apps/shell/src-tauri/gen/`；`target/` 与 `mock-files/` 这类生成物不会进仓库；`Cargo.lock`（根与壳）会进仓库，供 CI 复现构建。
- CI 扩展（2026-10-24）：`ci.yml` 改为三任务——Rust workspace 测试 + Clippy（三系统）、前端全量构建（`apps/web` 里 `npm install && npm run build`，取代原来只列 7 个域文件的 tsc 调用，那份旧清单漏了 invoke 客户端、视图与 App.tsx）、macOS 壳测试任务（起一次性 Mock、跑 `cargo test -- --test-threads=1`、always 清场；壳是独立 workspace，workspace 任务覆盖不到它，所以单列）；新 YAML 已在 VM 用解析器验证通过（3 个任务、runner 与步数无误）。
- 建仓受阻（2026-10-24）：经 GitHub 连接器建公开仓 `atxinbao/bu2agent` 时 GitHub 返回 403 `Resource not accessible by integration`——审批卡已通过，但集成令牌本身没有建仓权限，不是授权问题；没有换路绕过。用户随后在 github.com 上建好公开仓 `atxinbao/bu2agent`（建仓时选了 GPL-3.0 协议，远端已有 1 个提交、仅含 LICENSE 文件，无 README、无 .gitignore）；推送前须先 `git pull --rebase origin main` 把 LICENSE 提交接进来（绝不用 force-push 盖掉它），再跑 `git add -A` 与 `git status --short` 贴回核对，确认文件清单干净后再 commit 推送；CI 首跑已于同日跑通（见顶部徽标）。

## 24. 命名统一（2026-10-24，用户定 bu2agent）

- 改名（6 个文件，8 处）：壳 `Cargo.toml` 包名 `web-agent-shell` → `bu2agent-shell`（描述同步）；`tauri.conf.json` 的 productName、窗口标题 → `bu2agent`，identifier `com.bu2ai.web-agent` → `com.bu2ai.bu2agent`；壳 `main.rs` 的 app_info 名与头注释 → `bu2agent`；前端 `package.json` 名 `web-agent-web` → `bu2agent-web`；`index.html` 标题与 `App.tsx` 的 h1 → `bu2agent`。
- 未改：`agent-desktop` 与 `mock-gateway` 是功能名、本身不含 web-agent 字样，改它们要动 lib 名与代码引用，不属于品牌统一；README 第 19、20 节里的 `web-agent-shell` 是当时实际跑过的二进制名，属于历史记录，不改；目标记录的 slug（见下）。
- VM 预筛：改名后 6 文件 TOML/JSON 解析通过、新名字样齐全、旧名字符串清零、`main.rs` 在补齐同目录模块后 rustfmt 解析通过；壳的真正编译仍由 Mac 承载（VM 缺 gdk-3.0）。
- Mac 待验：跑 `run-shell-tests.command`（新包名下壳编译 + 3 项测试）与 `run-tests.command`（workspace 未动，36 项回归）；改名后首次构建会刷新 `apps/shell/src-tauri/Cargo.lock` 里的包名，属预期变化，推送时一并提交。
- 目标 slug 未改：目标记录的 slug `web-agent` 建好后没有改名接口（见目标记录），Goals 标签页显示的是标题不是 slug，留着不影响使用；如坚持要改，只能重建目标（时间线会丢），待用户定。
