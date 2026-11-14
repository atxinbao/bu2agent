# bu2agent Personal Agent 底层架构

> 本文是 `bu2agent` 的上位架构文档。它定义产品为什么存在、核心领域对象、长期执行语义和建设顺序。
> `ARCHITECTURE.md` 负责 Muse 客户端基线到代码模块的映射；README 负责当前状态和历史验证记录。

- 文档日期：2026-11-02
- 产品定位：本地单用户 Personal Agent 平台
- 当前实现：Tauri 2 + Rust 客户端/网关 + React/Vite/TypeScript 前端 + Mock Gateway
- 参考来源：Muse、Ambi、DSH-superagent 的逆向和源码研究

本文保留 `Goal`、`Project`、`Workspace`、`Task` 等通用产品词，但它们不是一棵严格的父子树。规范关系是：`Goal / Project → Task → Execution`；`Workspace` 是 Execution 使用的资源与权限边界；`Policy` 横向约束整个流程。

## 1. 结论先行

`bu2agent` 不应只是“带工具调用的聊天客户端”，而应成为：

> 在明确授权边界内，持续理解用户目标、安排可恢复执行、验证真实结果，并以最少用户注意力交付结果的本地 Agent 控制平面。

聊天、模型、连接器、设备和语音都是入口或适配器，不是系统的事实中心。系统的核心链路是：

```text
目标 / 外部事件
  → 受理与授权
  → 任务与计划
  → 执行分配
  → 工具与设备副作用
  → 结果验证
  → 产物与交付
  → 记忆与下一次触发
```

### 1.1 用户获得的是结果，不是对话

用户真正需要的是文件被整理、代码被修改并验证、会议被总结、任务被持续推进。对话是输入和解释界面，不是业务任务本身。

因此必须区分：

```text
Goal → Task → Execution → Effect → Result
```

`Session` 只承载某次 Agent 工作的上下文，不能代替 Goal、Task 或 Execution。

### 1.2 模型只能提出行动，不能授予自己权限

外部消息、引用文档、模型输出和工具返回值都属于输入，不能扩大权限。授权由宿主侧的 Policy、Mandate、Permission 和 Approval 强制执行。

### 1.3 外部世界不可靠

每个外部副作用都可能超时、迟到、重复、在客户端无响应时已经成功，或在重启时停在中间。因此 `unknown` 是一等结果：必须核验真实状态，不能把未知当失败后盲目重试。

### 1.4 用户注意力是稀缺资源

Agent 应安静工作，只在以下情况打扰用户：需要授权、需要补充信息、结果可用、结果需要审核、被阻塞、失败或需要处理未知状态。Workbench 是工作台，Inbox/Attention 是用户需要处理事项的事实投影。

## 2. 文档职责与证据边界

| 文档 | 职责 |
| --- | --- |
| `README.md` | 当前能力、运行方式、已验证结果和历史推进记录 |
| `PA-ARCHITECTURE.md` | PA 第一性原理、领域模型、长期运行时、缺口和路线图 |
| `ARCHITECTURE.md` | Muse 逆向基线、客户端分层、契约和代码模块映射 |
| `services/mock-gateway/LOOP.md` | 当前模型循环的具体协议、审批、unknown 和恢复边界 |
| `services/mock-gateway/DEVICES.md` | 当前设备动作和 EventKit/Fake 验收边界 |
| `TOOLS-INTEGRATION.md` | 本地系统能力、权限和工具接通记录 |
| `packaging/README.md` | 打包、sidecar、TCC 和发行验证状态 |

本文中的结论分为三类：

- **已验证**：当前仓库文档、代码或本机验收已经证明的行为；
- **逆向证据**：Muse、Ambi、DSH 中观察或静态分析得到的参考；
- **目标设计**：为未来 PA 提出的架构，不表示已经实现。

## 3. PA 核心领域模型

### 3.1 关系总图

```text
Goal ───────────────┐
                    ▼
Project? ────────► Task
                    │
                    │ 1..N attempts
                    ▼
                Execution
                    ├── Workspace Snapshot
                    ├── Session / Worker Session
                    ├── Loop Driver
                    ├── Worktree?
                    ├── Effects
                    ├── Artifacts
                    └── Evidence

Policy / Mandate / Approval
    └── 横向约束 Task、Workspace、Execution 和 Effect
```

这不是严格的父子树：Goal 和 Project 是 Task 的关联上下文，Workspace 是 Execution 的工作边界，Policy 是横向治理机制。

### 3.2 Goal

Goal 是用户希望达成的结果，回答“为什么做”。它可以产生多个 Task，也可以跨越多个 Project 和 Workspace。

```text
Goal：让 bu2agent 成为可恢复的个人 Agent 平台
```

Goal 不负责文件目录、工具调用、审批或模型会话。当前 README 中的目标记录是项目元数据，未来才需要成为可关联多个 Task 的领域对象。

### 3.3 Project

Project 是一个稳定的工作主题或工程对象，回答“关于什么”。例如 `bu2agent`、家庭旅行或个人财务。

Project 不是 Goal 的下级容器，也不是文件夹。Task 可以属于 Project，也可以没有 Project；一个 Goal 可以跨多个 Project。当前仓库本身可以作为第一个 Project，但不需要为了建模立即引入完整 Project 表。

### 3.4 Workspace

Workspace 是 Agent 工作时使用的资源、上下文和权限边界，回答“在哪里、基于什么工作”。它可以包含：

```text
Workspace
├── file roots
├── repository bindings
├── connector / device bindings
├── instructions
├── memory scope
└── policy bindings
```

当前 `MOCK_FILES_ROOT`、文件作用域、Connector Egress、Device 权限和部分 Policy Scope，都是 Workspace 的分散组成部分。Workspace 不是当前的 `Space`、文件目录、Git Worktree、Session 或 Loop。

一次 Execution 使用的是 Workspace 的不可变快照；这样 Workspace 后续变更不会改写历史执行的权限边界。

### 3.5 Task

Task 是可执行、可追踪、可验证的工作单元，回答“具体要完成什么”。它是 Goal、Project、Workspace 和 Execution 之间的连接点。

```text
Task
├── goal_id?
├── project_id?
├── workspace_id?
├── desired outcome
├── acceptance criteria
├── status
└── executions
```

Task 不等于一次执行。一个 Task 可以有多次 Execution：

```text
Task
  ├── Execution 1：失败
  ├── Execution 2：等待审批
  └── Execution 3：完成并通过验证
```

Task 建议使用较粗粒度的状态：

```text
created → active → blocked → completed | failed | cancelled
```

`draft_ready`、`awaiting_approval` 等更细状态应逐步移动到 Execution 或 Effect。

### 3.6 Execution、Session、Loop、Worktree

- **Execution**：一个 Task 的一次具体、可恢复的执行尝试，是运行时中心；
- **Session**：当前项目中的持久化聊天上下文，未来通过类型区分 conversation session 和 worker session；
- **Loop**：模型驱动工具循环，是一种 Execution Driver，不应永久作为与 Task 并列的业务对象；
- **Worktree**：代码类 Execution 使用的具体 Git 工作树、目录、容器或沙箱，不等于 Workspace。

目标关系：

```text
Task
  └── Execution
        ├── Workspace Snapshot
        ├── Session / Worker Session
        ├── Loop Driver
        ├── Worktree?
        ├── Effects
        ├── Checkpoints
        └── Reports
```

### 3.7 Effect、Artifact、Evidence

所有外部副作用都应成为 Effect，例如文件写入、提醒事项修改、连接器写入、Git 操作和设备控制。

```text
effect_id
execution_id
capability / operation / scope
input_digest
approval_id
idempotency_key
status
started_at / finished_at
outcome
reconciliation
```

结果对象分开表达：

- **Artifact**：生成的文件、报告或草稿；
- **CodeChange**：补丁、分支、提交和工作树差异；
- **Evidence**：测试结果、真实状态读取和验证摘要；
- **Report**：面向用户的结论和下一步。

### 3.8 Space、Workbench 和 Inbox

当前 `Space` 是保存 Session、File 等引用的命名集合，不是 Workspace：

```text
Space
  └── saved references
        ├── sessions
        ├── files
        ├── tasks
        └── artifacts
```

Workbench 是 Session、Task、Loop/Execution 的统一 UI 投影，不是真实领域实体。当前视图中的 `WorkItem` 也是 UI 联合类型，后续应避免与领域 Task 混淆。

Inbox 是用户真正需要处理的事项，而不是所有事件的转储：

```text
approval_required
input_required
review_required
result_ready
blocked
failed
unknown_requires_reconciliation
conflict
```

选择 Workbench 项目或 Inbox 项目不能隐式批准或恢复执行。

### 3.9 Policy、Mandate、Permission、Approval

授权至少分为：

1. Admission：请求是否进入系统；
2. Mandate：Task/Execution 允许达成什么目标；
3. Capability：可以使用哪些能力以及作用域；
4. Approval：具体 Effect 是否由用户批准；
5. System Permission：macOS TCC 等系统权限；
6. Credential Access：是否可以使用某个账号或密钥；
7. Delivery Permission：结果是否可以发给外部对象。

Policy 是可继承的长期规则，Mandate 是 Task/Execution 的授权快照，Approval 绑定某一个具体 Effect。当前文件、连接器和提醒事项已经验证了“客户端门 + 服务端复核 + 操作绑定”的方向。

## 4. 统一执行生命周期

### 4.1 总体流程

```text
Source/Event
  → Intake
  → Admission
  → Task + Mandate
  → Workspace selection
  → Execution / Assignment
  → Loop / Worker
  → Effect
  → Approval / Permission
  → Verification
  → Report / Artifact
  → Delivery / Attention
```

纯聊天可以停在 Conversation/Session，不必把每个只读问题都强行创建为 Task。只有当用户请求持续工作、外部副作用或可交付产物时，才创建 Task 和 Execution。

### 4.2 Effect 状态

```text
planned → authorized → started
  → succeeded
  → failed
  → unknown → reconciling → reconciled
```

规则：

- 每个副作用都有幂等键；
- 超时但可能已经执行时先记 `unknown`；
- `unknown` 必须先读取真实状态再决定重试；
- 重启时恢复 queued、running、unknown 和待交付记录；
- 迟到结果只能归档，不能修改已经结束的 Execution；
- 审批绑定到操作摘要、作用域和参数摘要，不绑定模糊的“任务已批准”。

### 4.3 恢复模型

长期执行所需的基础设施：

- 事务性状态和事件日志；
- Inbox 消费游标与去重键；
- Outbox 交付队列；
- Worker 租约、心跳和超时；
- Checkpoint；
- 外部副作用的 Reconciliation；
- schema version 和迁移；
- 重启后恢复而非重新猜测。

当前 JSONL 已经验证了普通停服重启场景，但不等同于断电耐久的事务事件日志。目标架构应逐步迁移到 SQLite 事务状态 + 事件/Outbox 表，保留脱敏审计视图。

### 4.4 当前实现到目标模型的映射

| 当前实现 | 目标含义 | 调整方向 |
| --- | --- | --- |
| `TaskFileRecord` / `TaskRunner` | Task 加上部分执行状态 | 保留 Task，逐步把 `draft_ready` 和执行字段移到 Execution |
| `LoopRecord` / `LoopEngine` | 一次 Execution 的模型循环 | 让 Loop 成为 Execution Driver，而不是长期并列的业务实体 |
| `CallRecord` | Effect | 增加统一 `execution_id`、幂等和 Reconciliation |
| `DeviceActionRecord` | Effect | 与文件、连接器写操作共享生命周期 |
| Connector 写操作 | Effect | 记录 Task/Execution、作用域和交付结果 |
| `ApprovalState` | Effect Approval | 保留参数摘要、操作摘要和消费语义 |
| `SessionRecord` | Conversation Session | 关联 Task/Execution，但不作为执行状态本身 |
| `Space` | Saved Collection | 保持独立，不改作 Workspace |
| `MOCK_FILES_ROOT` 等全局边界 | Workspace Resource Binding | 逐步收敛成 Workspace |

## 5. 运行时分层

```text
Tauri / React UI
  └── Projection + User Commands
        ↓
Local Agent Gateway
  ├── Domain Engine
  ├── Policy / Permission
  ├── Durable Event / Inbox / Outbox
  ├── Scheduler / Recovery
  ├── Worker Runtime
  ├── Source Registry
  └── Storage
        ↓
Tools / Connectors / Devices / Model Providers
```

职责边界：

- UI 只展示投影和提交命令；
- Model 只负责推理、规划和提出操作；
- Gateway/Engine 负责领域规则和状态转换；
- Worker 负责执行；
- Tool 负责连接外部世界；
- Store 负责事实；
- Event、Inbox 和 Outbox 负责恢复、去重和交付。

这延续了 Muse 的统一 Gateway、Ambi 的长生命周期 Native Runtime 和 DSH 的 Engine/Store/Effects/SourceRegistry 分层，但不把任何一个逆向产品的服务端 schema 当作已验证事实。

## 6. DSH 到 bu2agent 的映射规则与边界

DSH 的映射不是同名替换，也不是把 DSH 源码复制进来。只有满足以下规则才吸收一个 DSH 概念：

1. **责任相同**：两个对象解决的是同一个业务问题；
2. **边界相同**：状态、权限和数据的所有者一致；
3. **生命周期保留**：映射后仍保留等待、失败、unknown、迟到和恢复语义；
4. **授权不扩大**：外部消息和模型输出不能通过映射获得新权限；
5. **适配本地单用户**：团队身份、GitHub/飞书特定规则不能直接成为 PA 核心；
6. **可以独立验收**：每个吸收项必须有契约、失败路径和恢复验收。

### 6.1 概念映射

| DSH | `bu2agent` 规范对象 | 当前状态 |
| --- | --- | --- |
| Source | Connector、Device、File Watcher、Scheduler | 尚未统一 |
| Trigger | Event / Incoming Event | 尚未形成统一事件模型 |
| Eval | Admission / Intent Evaluation | 部分存在，未独立建模 |
| Ticket | Task | 已有，但当前主要是文件报告任务 |
| Mandate | Task/Execution 授权快照 | 尚未独立建模 |
| Assignment | Execution Assignment | 尚未统一 |
| Worker | Loop Worker、Task Runner、子进程 | 已有碎片 |
| Worker Session | Worker Session | 当前 Session 主要是聊天会话 |
| Report | Task Report、Loop Result、Device Result | 已有碎片 |
| Attention | Workbench 待审批/待复核/失败投影 | 有 UI 投影，尚无独立 Inbox |
| Channel | Workbench、通知、Connector Delivery | 尚未统一 |
| Policy | Permission Policy、Connector Policy、Egress | 已有部分 |
| Effects | CallRecord、DeviceAction、File/Connector Write | 多套实现，尚未统一 |
| Outbox | Durable Delivery/Effect Outbox | 缺口 |
| SourceRegistry | Connector/Device/Local Source Registry | Connector Registry 已有，未统一 |
| Store | 当前各组 JSONL Store | 有持久化，非统一事务 Store |
| Engine | Rust Gateway 内的协调器 | 已有碎片，未统一 |

### 6.2 现在吸收什么

当前吸收 DSH 的重点是四组规则：

```text
事件接入 → 是否受理 → 生成受约束任务 → 分配 Worker
→ 持久化执行 → 产生报告 → 交付结果 → 等待下一次事件
```

具体优先级：

- 现在：Task、Execution、Effect、Mandate、Recovery 的语义；
- 下一步：SQLite 事务状态、Inbox、Outbox、租约、心跳和 Reconciliation；
- 后续：Source、Trigger、Admission 和主动规则；
- 暂不引入：DSH Runtime、Cordis、DeepSeek Harness、GitHub/飞书连接器和团队身份模型。

DSH 设计的是团队/仓库 Agent；`bu2agent` 是本地单用户 PA。个人 PA 的 Source 将来可以包括用户输入、文件变化、Git、日历、提醒事项、邮件、消息、设备、语音和定时规则，但这些适配器不应反过来定义核心领域模型。

### 6.3 是否引入 DSH-superagent

当前不整体引入 DSH-superagent 代码。原因是 `bu2agent` 已经有自己的 Rust Gateway、Task Runner、Loop Engine、Device Action、Workbench 和审批体系；直接引入 DSH 会产生两套 Task/Ticket、Execution/Assignment、Session/Worker 和 Store/Engine。

正确策略是：

```text
吸收 DSH 的编排规则
  → 统一 bu2agent 已有的 Task/Loop/Device 执行模型
    → 建立 Durable Execution
      → 再引入 Source/Trigger/Proactive Agent
```

## 7. 当前实现与目标设计的关系

### 7.1 已有且可作为基础的能力

- Workbench 已统一展示 Session、Task、Loop；
- `Space` 已实现为带墓碑的保存引用集合，不应误认为 Workspace；
- Task 草稿、审批绑定、产物查看和普通重启恢复已验收；
- Loop 已覆盖审批、预算、取消、unknown、迟到结果和恢复；
- Device Action 已具备 unknown、重查、幂等和设备权限/Agent 审批分离语义；
- 文件工具和 Fake/EventKit 提醒事项已形成闭环；
- OS Keychain、客户端注册表门和服务端审批复核已有实现；
- Tauri + Rust + React/TypeScript + Mock Gateway 主路线已建立；
- sidecar Gateway 生命周期、权限和设备契约已有专项记录。

### 7.2 主要架构缺口

1. `Goal / Project / Workspace` 尚未成为一等对象；
2. Task、Loop、Device Action 尚未共享统一 `Execution / Effect`；
3. 当前 `Space` 仍是保存引用集合，不能承担 Workspace 职责；
4. Inbox、Outbox、事件游标和消费幂等尚未统一；
5. JSONL 尚不能承担生产级事务事件日志和断电恢复；
6. Approval 尚未升级为版本化 Policy + Mandate；
7. Worker 进程、租约、心跳和 Checkpoint 尚未完整建立；
8. Git、CodeChange、diff 和验证证据尚未成为一等模型；
9. Session 上下文与长期记忆尚未分层；
10. Connector、Device、Local Watcher 尚未统一为 Source Registry；
11. 用户注意力和通知策略尚未由 Inbox 统一承载。

## 8. 建设路线

### P0：统一执行内核

1. 定义 `Execution`，不改变现有 Goal/Workspace/Task 用户词；
2. 让 Task Runner 和 Loop Engine 创建 Execution；
3. 将 `CallRecord`、Device Action、Connector/File Write 统一为 Effect；
4. 为 Task/Execution 增加 Workspace 选择和不可变 Workspace Snapshot；
5. 统一 Policy、Mandate、Approval、Permission 契约；
6. 建立 SQLite 事务状态、事件日志、Inbox 和 Outbox；
7. 完成租约、幂等键、恢复和 Reconciliation 验收；
8. **不把 DSH Runtime、Cordis 或 Harness 作为 P0 依赖。**

### P1：可审查的工程 Agent

- Git Worktree、CodeChange、diff、patch、commit；
- 子进程 Worker 和检查点；
- 测试与验证 Evidence；
- 多次 Execution、预算、超时和失败后继续；
- 文件、提醒事项和连接器全部通过统一 Effect。

### P2：主动性与长期记忆

- Local Source Registry、定时器和例行任务；
- 项目记忆、事实记忆和用户偏好记忆；
- 真实 OAuth、邮件、日历和消息；
- 语音、媒体、多设备和跨项目调度；
- 基于明确规则的主动建议。

主动规则必须是用户可见、可撤销、可审计的，例如：

```text
会议结束且存在会议记录 → 生成摘要草稿
仅在用户批准后 → 发送给参与者
```

### 编号对照注（R0，2026-11-14）

本文 §8 的 P0/P1/P2 是 PA 自身的编号，与另外两套编号并存、字面冲突。本文不改写 §8 正文；凡引用旧文档的 P 编号须注明出处，路线文档一律用路线编号：

| 路线编号 | 含义 | 对应旧说法 |
|---|---|---|
| R0 | 基线收口 | 无对应；旧基线无此阶段 |
| E1 | 可审查工程 Agent | 旧基线 P3；本文件 §8 的 P1 |
| E2 | Durable Worker 与统一执行内核 | 旧基线 P0-A–E 与 P0 剩余工作的合流；本文件 §8 的 P0 |
| D1 | Personal Data Plane | 旧基线 P2 的一部分 + P4 的一部分；本文件 §8 的 P2 的一部分 |
| C1 | 多设备连续 | 旧基线 P5；本文件未单列（在 P2 的“多设备”一句里） |
| A1 | 主动性与长期自动化 | 旧基线 P4 的其余部分；本文件 §8 的 P2 的其余部分 |

注意：“P1”在 PA 指工程 Agent，在旧基线指提醒事项；“P2”在 PA 指主动性与长期记忆，在旧基线指 Mail/Notes。路线图里 P 编号已废弃。

## 9. 设计原则与反验收

必须坚持：

- UI 不是真实状态，Gateway/Store 才是真实状态；
- 模型输出不是授权；
- Approval 不等于 Task 完成；
- 超时不等于失败；
- 未知副作用不能盲目重试；
- 选择 Workbench 项目不能隐式批准或恢复；
- Mock 成功不代表真实连接器成功；
- 生成了文本不代表产生了可验证结果。

以下情况不算 PA 能力完成：

- 只有聊天界面或前端动画；
- 每次重启都靠模型重新猜测进度；
- 工具失败后自动重复写入；
- 日志或事件中包含凭据、正文或完整隐私数据；
- 连接器能调用但没有作用域、审批、幂等和交付状态；
- 任务只有 `done` 字段，没有产物、证据和恢复语义。
