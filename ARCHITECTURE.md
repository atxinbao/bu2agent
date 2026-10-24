# 底层架构：逆向 Muse，做一个完全一样的个人 Agent 客户端

- 基线：《Muse macOS 8.0 逆向工程与重建规格》（2026-10-24，Bundle `com.meta.endo`，版本 8.0）
- 最终目标：逆向 Muse app，做一个完全一样的个人 Agent 客户端。
- 实现路线：Rust 客户端壳 + TypeScript 前端，同一套 Rust 代码覆盖 macOS / Linux / Windows。
- 本文件是底层架构文档，不是等价声明。基线中标为“未知”的部分（服务端 JSON schema、Noise 握手、私有鉴权、Tool 完整语义等）在这里只留接口与替换点，不写成已验证。
- 语言：本文新写内容使用 zh-CN。

## 1. 目标怎么理解“完全一样”

“完全一样”分三层，验收时要分开算，不能混成一句话：

| 层 | 目标 | 首期做到什么程度 |
|---|---|---|
| 客户端结构与流程 | 与 Muse 相同的模块划分、状态机、调用关系 | 架构与骨架先对齐，Mock 先跑通 |
| 客户端行为 | 聊天流式、文件、任务、审批、连接器、会话目标等行为可对齐 | 首期只保聊天、文件、审批三条主线 |
| 服务端等价 | 真实 Gateway、Noise、私有鉴权、Tool 完整语义与原服务端一致 | 不属于首期验收，需运行时证据逐项替换 |

判断标准：一个能力只有同时满足“接口对齐 + 行为对齐 + 失败路径对齐”才算对齐；只有 UI 长得像、Mock 永远成功，不算对齐。

## 2. 原 Muse 分层（基线摘要）与我们的映射

原分层（基线第 1、4、10 节）：

```text
Swift/AppKit/SwiftUI 壳
  ├─ WKWebView / HatchEmbeddedWebViewManager
  ├─ endoHatch WebKit message handler
  ├─ HatchClient / HatchFileClient / HatchAuth
  ├─ HatchNodeCore / HatchNodeProtocol
  ├─ HCHGatewayTransport + HCH Noise/WebSocket/duplex
  ├─ EndoAuth / Keychain / WebKit 登录会话
  ├─ EndoNodeDataSyncCoordinator + 系统数据同步源
  └─ HatchTools：文件、邮件、消息、备注、提醒、浏览器、屏幕、辅助功能、dictation
前端：hatch/index.html，Domain Store（chat/files/tasks/activity/identity/avatar/recording/search/deviceContext/audioPlayer/browserPanel/uiControl/connectors/approvals/cvm/voice/settings/spaces/artifacts）
```

映射表：左为 Muse 侧（基线证据），右为我们的实现，状态栏只写首期状态。

| Muse 侧 | 我们的模块 | 首期状态 |
|---|---|---|
| Swift 壳 + WKWebView | Rust 客户端壳（推荐 Tauri 2）+ 系统 WebView | 骨架，首期跑通三系统启动与主线 |
| `endoHatch` bridge | `apps/desktop/src/bridge.rs`：类型化 invoke + 事件，命令白名单，桥版本 | 契约已定，骨架 |
| `HatchClient` | `apps/desktop/src/gateway.rs`：连接、健康检查、重连、流式事件、认证状态 | 骨架，Mock 传输先行 |
| `HatchFileClient` | `apps/desktop/src/file_client.rs`：`/fs/*` 分块读写、上传、原始下载、缩略图、401 刷新 | 骨架，首期先 list/read/write/delete |
| `HatchNodeCore` / `HatchNodeProtocol` | `apps/desktop/src/node.rs`：节点注册、命令派发、历史、同步、权限门控、invoke result/ack | 骨架，首期只做命令派发与 ack 结构 |
| `HCHGatewayTransport` / Noise | `apps/desktop/src/transport.rs`：GatewayTransport trait；MockTransport 首期唯一实现，NoiseTransport 后期实现 | 接口先行，Noise 暂缓 |
| `EndoAuth` / Keychain / 登录会话 | `apps/desktop/src/session.rs` + 密钥存储抽象 | 骨架，首期 Mock 占位 token，不做真实恢复 |
| `EndoNodeDataSyncCoordinator` + 各同步源 | `apps/desktop/src/sync.rs`：SyncEnvelope 结构先行 | 只定结构，真实同步暂缓 |
| `HatchTools` | `apps/desktop/src/tools.rs`：工具注册表与审批前置点 | 骨架，首期只接文件类工具 |
| 前端 Domain Store | `apps/web/src/domain/`：chat、files、tasks、approvals 等域 | 骨架，首期 chat 域先行 |
| 权限（Apple Events、麦克风、摄像头、通讯录、日历、定位、照片、提醒、屏幕录制、辅助功能、文件系统、Keychain） | `apps/desktop/src/permission.rs`：PermissionSnapshot + 预检 + 降级 | 骨架，首期只做文件与通用降级 |
| 可观测性（request/turn/invocation ID、脱敏、bug report） | `apps/desktop/src/observability.rs` | 骨架，从一开始就带脱敏 |

不在首期映射范围内的原能力：系统数据真实同步、Browser lease 与 Computer Use、语音/dictation、共享 Agent 与 peer 完整形态。它们在架构里留位置，不在首期实现与验收。

## 3. 分层架构

```text
TS 前端 UI（只展示与触发）
  ↓
TS Domain Store（chat / files / tasks / activity / identity / approvals / settings）
  每个域：state / reducer / selectors / subscriptions / optimistic mutations
  ↓
Rust↔TS 桥（bridge.rs：白名单 invoke + 类型化事件 + 桥版本 + 错误映射）
  ↓
Rust 客户端壳 apps/desktop/src/
  ├─ shell：窗口、WebView 宿主、生命周期、深链接（hatch:// 等价路由）
  ├─ gateway.rs：HatchClient 等价物
  ├─ transport.rs：GatewayTransport trait（Mock / HTTPS / WebSocket / Noise）
  ├─ file_client.rs：HatchFileClient 等价物
  ├─ node.rs：HatchNodeCore / HatchNodeProtocol 等价物
  ├─ session.rs：EndoAuth 等价抽象 + GatewayTargetKey + generation
  ├─ permission.rs：权限快照、预检、降级
  ├─ tools.rs + approval.rs：工具注册、审批状态机、审批前置校验
  ├─ sync.rs：SyncEnvelope 结构（真实同步暂缓）
  ├─ platform/：系统差异适配层（WebView 引擎、密钥库、文件根、权限模型、打包）
  └─ observability.rs：脱敏日志、致命错误上报、bug report
  ↓
Mock Gateway（Rust，Axum 推荐：状态机 + 审批 + 失败注入 + fixture）
  ↓（后期替换，不改前端与 Store）
真实 Gateway 适配器（HTTPS / WebSocket / Noise，接口不变）
```

硬约束：

- 前端不直接调后端裸接口，不直接调系统能力；系统能力一律走 Rust 桥。
- Domain Store 是前端唯一事实来源；UI 状态不得绕过 Store。
- Transport 只负责发、收、超时、取消、重连，不负责业务语义。
- 业务逻辑、传输、审批前置、文件作用域校验不得按系统分叉；系统差异只允许在 `platform/` 适配层。
- Mock 与真实适配器实现同一契约，替换时前端与 Store 不改代码。

## 4. 核心接口（与 packages/contract/schemas/ 一致）

- `GatewayTargetKey = { vmName, gatewayUrl, vmId?, generation }`：目标切换或 generation 变化后，旧请求、旧 stream、旧 invocation 结果一律丢弃。
- `GatewayRequest = { requestId, method, path, service, noiseOnly, timeoutMs, targetKey, body? }`，其中 service 为 `gateway | daemon | authd | sentinel | vault | noise-only | media`。
- `GatewayResponse = { status, headers（脱敏后）, body, requestId, targetKey }`。
- `SubscriptionRecord = { streamId, sequence, eventType, payload, resumeCursor }`：按 sequence 去重，断线用 resumeCursor 续传，续传失败重新 hydrate。
- `AgentInvocation = { turnId, invocationId, tool, argsSummary（脱敏摘要）, approvalStatus, startedAt, endedAt, resultRef, error }`。
- `UnifiedError = { code, capability?, retryable, userAction, redactedDetail }`，code 枚举见契约 Schema；Rust 原始错误、堆栈、系统路径不得透传到前端或日志。
- `PermissionSnapshot = { capability, systemStatus, scope, source, updatedAt }`。
- 桥命令：`{ action, payload, requestId, bridgeVersion: "1" }`，只允许白名单；桥事件：`{ event, payload, bridgeVersion: "1", requestId?, streamId? }`；桥版本不匹配时启动即明确报错。

## 5. 状态机（必须先对齐的三条）

### 5.1 Gateway 连接（HatchClient 等价物）

```text
idle -> dialing -> transport_ready -> server_confirmed -> active
  ^          |                 |                |
  |          +-> failed -------+----------------+
  +---------------- reconnect(backoff/jitter) --+
```

失败分类（基线第 4.3 节）：`transport_failed | closed_before_confirm | keepalive_timeout | private_auth_unverified | backend_error | intentional_disconnect`。私有 gateway 未验证时拒绝传输，不降级到不可信传输。

### 5.2 Agent turn

```text
created -> submitted -> thinking -> tool_call_pending
  -> awaiting_approval -> tool_running -> streaming
  -> completed | cancelled | failed
```

流式事件（基线第 5.2 节）：`delta.message_start | delta.placeholder | delta.text_append | delta.turn_start | delta.agent_status | delta.message_done | message.assistant | stream/apply_event`。取消要贯穿 UI → Store → 桥 → Transport → Mock；取消后不得再写入该 turn 的后续 delta；断线重连不得重复消息。

### 5.3 审批与 Egress

```text
pending -> approved | rejected | edited
```

审批是服务端（Mock）状态，不是 UI 本地变量；未审批的敏感请求在 Mock 侧也必须被拒绝；连接器操作必须先形成显式 scope；审批历史可查，编辑后参数为准。

### 5.4 会话与 VM 目标（EndoAuth / VM lease 等价抽象）

登录与会话流程（基线第 7 节，按跨平台裁剪）：

```text
launch -> restore credential -> session bootstrap -> login options / account selection
  -> OAuth/OIDC or password/passkey/OTP -> token exchange -> VM admission / onboarding status
  -> gateway session bootstrap -> active
```

首期只实现该流程的抽象与 Mock 形态：恢复、bootstrap、目标选择、generation 管理。真实 OAuth、Keychain 恢复 secret、VM lease 响应、checkpoint、waitlist/geoblock 等仍属未知或暂缓，不伪造。

VM lease 生命周期（结构先行）：`requested -> assigned -> waking -> ready -> released`；切换期间暂停/取消旧 gateway 请求。

## 6. 关键子系统设计

### 6.1 传输：Mock 先行，Noise 后接

- `GatewayTransport` trait：`open(target, headers, privateAuthIntent, chipPolicy, timeout)`、`request(...)`、`subscribe(...) -> AsyncSequence<Record>`、`close(reason)`。
- 首期：`MockTransport`（HTTPS + fixture），必须真的执行状态机、审批、取消、失败、去重。
- 后期：`HttpsTransport`、`WebSocketTransport`、`NoiseTransport`（对应 HCH Noise 的握手、帧、双工 body、背压、健康状态），接口不变；私有鉴权失败时不降级。
- 订阅：`chat.subscribe`、`activity.subscribe`、`fs.subscribe`、`identity.subscribe`、`spaces.subscribe`、`tasks.subscribe`、`theme.subscribe`、`computer.subscribe`（首期只实现聊天、文件、任务相关订阅，其余留接口）。

### 6.2 文件：HatchFileClient 等价物

- 路由：`/fs/stats | list | stat | read | reads | write | delete | rename | mkdir | library | export | raw/{path} | thumbnail/{path} | search`，另加 `git.log | git.show_file`（首期后者暂缓）。
- 分块：`fs.read` 用 offset/len 循环，支持 text 与 base64；校验 base64 形状、返回长度、EOF、maxBytes；大文件不一次性塞进内存。
- 上传：slot、deadline、一次重试；401 触发 token 刷新后只重试一次，且写操作不得重复写入。
- 作用域：路径规范化、沙箱根目录、最大读写限制、目标 VM 校验、二进制流、取消语义，全部在 Rust 侧做，前端不直接拼路径。

### 6.3 节点与工具：HatchNodeCore 等价物

节点消息抽象（基线第 4.5 节，字段仍需运行时验证）：

```json
{
  "client_id": "string",
  "invoke_id": "string",
  "command": "string",
  "args": {},
  "card_json": {},
  "display_name": "string",
  "device_metadata": {},
  "capabilities": []
}
```

结果方向至少有 `nodes.invoke_result`、`nodes.invoke_ack`、`client.invoke.result`。工具注册表在 Rust 侧，工具调用必须过权限门控与审批前置；HITL 门控与 Permission 门控是两道关，不是一道。

### 6.4 认证、密钥与存储

- 密钥存储抽象：macOS Keychain、Windows Credential Manager、Linux Secret Service；首期先存 Mock 占位 token，真实恢复 secret（基线中 `hatch-cvm-credentials` / `keys` / `vesta-recovery-secret`）不实现真实流程。
- 存储分层：UI 偏好进前端普通 storage；会话绑定、token、generation 在 Rust 侧内存为主，确需持久化走系统密钥库并设过期；聊天/文件/活动缓存带 schema version 与迁移，按 account/session/VM 隔离；退出登录、账号切换、清除数据后上一账号状态不得残留。
- 脱敏：token、cookie、恢复 secret、完整用户输入、邮件正文、本地路径原文、附件正文不得进普通日志、fixture、bug report；日志按 requestId / turnId / invocationId 关联，分 transport、gateway、permission、tool、stream、UI 状态六类。

### 6.5 系统差异：只在 platform/ 适配层

| 差异 | macOS | Windows | Linux | 收敛 |
|---|---|---|---|---|
| WebView | WKWebView | WebView2 | WebKitGTK | Rust 壳统一封装 |
| 密钥库 | Keychain | Credential Manager | Secret Service | 统一抽象 |
| 文件根 | Desktop/Documents/Downloads 等 | 同名用户目录 | 同名用户目录 | 已授权根目录模型 |
| 权限 | TCC 等 | 系统设置与应用权限 | Portal / 系统权限 | PermissionSnapshot |
| 深链接 | `hatch://`、`endo-window://` 注册 | 同 scheme 注册 | 同 scheme 注册 | 统一路由到正确窗口 |
| 打包 | .app / dmg | 安装包 | AppImage / deb | 同一打包配置，签名与公证后期 |

## 7. 路由与能力覆盖策略

基线静态 route spec 为 337 条，直接 `useHatchRpc` / `sendRequest` 字面量调用为 80 个。首期不全量照搬，按域分批：

- 首期（Phase 0–3）：聊天（history、history_window、stream、stream_events、subscribe、cancel、search、message_get、delete_turn）、文件（见 6.2 首期子集）、任务、审批。
- Phase 4：连接器、凭据、egress 权限（Mock 级，不接真实 OAuth）。
- 暂缓：邮件/消息/备注/提醒/日历的真实同步、Browser lease 与 Computer Use、语音、共享 Agent 与 peer、feed/spaces/artifacts 完整形态。
- 每一批落地的路由都要有：契约 Schema、Mock 状态机、正常 fixture、至少一个失败 fixture、契约测试。没有这五件的路由不算落地。

## 8. 测试与等价验证策略

- 契约测试：字段、必填、可空、枚举、sequence 去重、generation 失效、审批前置、桥白名单、桥版本不匹配。
- Mock 端到端：聊天主线、文件主线、审批主线。
- 失败矩阵：断线、取消、超时、401、权限拒绝、审批拒绝、旧 generation、目标切换、keepalive 超时、服务端关闭。
- 跨平台冒烟：同一套 Rust 代码与同一套 TS 前端在 macOS / Linux / Windows 上跑通主线。
- 等价验证清单（基线第 12 节，逐项用受控证据替换“未知”）：启动 URL 与 Token 形态、chat.stream 与 invoke 的完整 schema、Tool 名称与参数 schema、/fs 路径根与分块上限、Browser lease 细节、同步窗口与 cursor、Keychain 与 session refresh、Noise 握手与 attestation、feature flag 覆盖规则、非主路径 UI 路由。
- 反验收：只有前端动画、Mock 永远成功、取消只关 spinner、旧会话结果混入新会话、权限拒绝显示成功、日志含 secret、把 337 条静态路由当作已对接接口数，一律不算通过。

## 9. 本次落地的骨架说明

本次在 Mac 项目里落的骨架（`apps/desktop/src/` 与 `apps/web/src/domain/`）是类型、trait、枚举与状态机骨架，不是完整实现：

- Rust 侧：`lib.rs`（模块清单）、`gateway.rs`、`transport.rs`、`bridge.rs`、`file_client.rs`、`node.rs`、`session.rs`、`permission.rs`、`approval.rs`、`observability.rs`，另有 `sync.rs`（SyncEnvelope 结构）与 `tools.rs`（工具注册表骨架）。
- TS 侧：`apps/web/src/domain/types.ts`（与契约 Schema 对齐的类型）与 `apps/web/src/domain/chat.ts`（chat 域骨架）。
- 骨架里的 `todo!` / `unimplemented!` / TODO 注释是诚实的未实现标记，下一步按 Phase 1 逐个替换为 Mock 实现与测试，不得把骨架说成已跑通。
- 下一步：先把契约测试立起来（Rust 与 TS 两侧），再实现 Mock Gateway 的聊天状态机，最后接通 Rust 壳 + TS 前端的聊天主线。
