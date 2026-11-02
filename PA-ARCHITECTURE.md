# bu2agent Personal Agent 底层架构

> 本文是 `bu2agent` 的上位架构文档。它定义产品为什么存在、核心领域对象、长期执行语义和建设顺序。
> `ARCHITECTURE.md` 负责 Muse 客户端基线到代码模块的映射；README 负责当前状态和历史验证记录。

- 文档日期：2026-11-02
- 产品定位：本地单用户 Personal Agent 平台
- 当前实现：Tauri 2 + Rust 客户端/网关 + React/Vite/TypeScript 前端 + Mock Gateway
- 参考来源：Muse、Ambi、DSH-superagent 的逆向和源码研究

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

### 3.1 Goal、Project、Workspace

```text
Goal
  └── Project
        └── Workspace
              ├── Worktree
              ├── Session
              ├── Files
              ├── Policy
              └── Tasks / Executions / Artifacts
```

- **Goal**：用户长期想达成的目标，可以生成多个 Task。
- **Project**：用户理解的长期工作对象，例如一个代码仓库或研究主题。
- **Workspace**：Agent 可以访问的资源、权限和上下文边界。
- **Worktree**：一次执行实际使用的目录、Git 分支、容器或沙箱。
- **Session**：某个 Agent 的上下文和交互历史。

`Project / Workspace / Worktree` 是当前 `bu2agent` 重要的结构缺口。没有这层，Agent 只能在“文件根 + Session”上工作，无法可靠表达工程边界、并行执行、分支隔离和代码变更审查。

### 3.2 Task / Ticket

Task 表达“要完成什么”，不表达某一次工具调用。一个 Task 可以有多次 Execution：

```text
Task
  ├── Execution 1：失败
  ├── Execution 2：需要用户补充信息
  └── Execution 3：完成并通过验证
```

建议状态：

```text
created → admitted → planned → queued → running
  → awaiting_input | awaiting_approval | awaiting_review
  → blocked | completed | failed | cancelled
```

拒绝某个操作不应自动删除 Task；Task 的完成必须由产物或验证证据确认，不能只由模型最后一句话确认。

### 3.3 Mandate / Policy / Permission / Approval

授权至少分为以下层次：

1. **Admission**：请求是否允许进入系统；
2. **Mandate**：该 Task 允许达成什么目标；
3. **Capability**：可使用哪些能力以及作用域；
4. **Approval**：某个具体副作用是否由用户批准；
5. **System Permission**：macOS TCC 等系统权限是否允许；
6. **Credential Access**：是否允许使用某个账号或密钥；
7. **Delivery Permission**：结果是否可以发给外部对象。

Mandate 不是 Prompt，而是可审计的授权快照：

```text
mandate_id
task_id
policy_version
allowed_capabilities
allowed_scopes
forbidden_actions
approval_mode
budget / deadline
actor
created_at / expires_at
```

当前文件写入、连接器写入和提醒事项写入已经验证了“客户端门 + 服务端复核 + 操作绑定”的方向。下一步应把它们统一为版本化 Policy 和 Mandate，而不是继续为每个功能单独实现审批。

### 3.4 Execution / Assignment / Worker Session

```text
Task
  └── Execution
        ├── Assignment
        ├── WorkerSession
        ├── Worktree
        ├── Effects
        ├── Checkpoints
        └── Reports
```

- **Execution**：一次可恢复的执行尝试；
- **Assignment**：将执行交给哪个 Worker；
- **WorkerSession**：模型、子进程或其他运行时的实际工作会话；
- **Checkpoint**：执行中可恢复的进度；
- **Report**：Worker 对结果、阻塞和下一步的结构化报告。

当前 Task Runner 和 Loop Engine 各自拥有状态机。目标架构要求两者最终都落到统一 Execution 上，避免任务、模型循环、设备动作和连接器调用分别发明不同的生命周期。

### 3.5 Effect / Artifact / CodeChange / Evidence

所有外部副作用都必须成为 Effect，例如文件写入、Git 操作、邮件发送、日历修改、提醒事项修改、设备控制和外部 API 调用。

Effect 至少需要记录：

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

### 3.6 Inbox / Attention / Delivery

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

每个 Inbox 项目应支持 approve、reject、edit、answer、retry、reconcile、snooze 和 dismiss。Workbench 只负责展示和触发命令；选择项目本身不能隐式批准或恢复执行。

## 4. 统一执行生命周期

### 4.1 总体流程

```text
Source/Event
  → Intake
  → Admission
  → Task + Mandate
  → Plan
  → Execution / Assignment
  → Effect
  → Approval / Permission
  → Worker / Tool
  → Verification
  → Report / Artifact
  → Delivery / Attention
```

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

## 6. DSH 到 bu2agent 的映射

| DSH | bu2agent 目标对象 |
| --- | --- |
| Source | Connector、Device、Local Watcher、Scheduler |
| Trigger | Durable Event / Incoming Event |
| Thread | Conversation、Project Context、External Thread |
| Eval | Admission / Intent Evaluation |
| Ticket | Task / Work Item |
| Mandate | Task Authority / Policy Snapshot |
| Assignment | Execution Assignment |
| Worker | Agent Worker / Process / Model Session |
| Report | Execution Report |
| Attention | Inbox Item |
| Channel | Delivery Target |
| Policy | Permission Policy |
| Effects | Durable External Effects |
| Outbox | Delivery and Effect Outbox |
| SourceRegistry | Connector and Device Registry |
| HealthBoard | Runtime Health and Dependency Status |

DSH 最值得迁移的是：

```text
事件接入 → 是否受理 → 生成受约束任务 → 分配 Worker
→ 持久化执行 → 产生报告 → 交付结果 → 等待下一次事件
```

个人 PA 不应固定围绕 GitHub 或飞书，而应允许用户输入、文件变化、Git、日历、提醒事项、邮件、消息、设备、语音和定时规则成为 Source。

## 7. 当前实现与目标设计的关系

### 7.1 已有且可作为基础的能力

- Workbench 已统一展示 Session、Task、Loop；
- Task 草稿、审批绑定、产物查看和普通重启恢复已验收；
- Loop 已覆盖审批、预算、取消、unknown、迟到结果和恢复；
- 文件工具和 Fake/EventKit 提醒事项已形成闭环；
- OS Keychain、客户端注册表门和服务端审批复核已有实现；
- Tauri + Rust + React/TypeScript + Mock Gateway 主路线已建立；
- sidecar Gateway 生命周期、权限和设备契约已有专项记录。

### 7.2 主要架构缺口

1. `Project / Workspace / Worktree` 尚未成为一等对象；
2. Task、Loop、Device Action 尚未共享统一 `Execution / Effect`；
3. Inbox、Outbox、事件游标和消费幂等尚未统一；
4. JSONL 尚不能承担生产级事务事件日志和断电恢复；
5. Approval 尚未升级为版本化 Policy + Mandate；
6. Worker 进程、租约、心跳和 Checkpoint 尚未完整建立；
7. Git、CodeChange、diff 和验证证据尚未成为一等模型；
8. Session 上下文与长期记忆尚未分层；
9. Connector、Device、Local Watcher 尚未统一为 Source Registry；
10. 用户注意力和通知策略尚未由 Inbox 统一承载。

## 8. 建设路线

### P0：统一执行内核

1. 增加 `Project / Workspace / Worktree`；
2. 定义 `Execution / Assignment / WorkerSession`；
3. 定义统一 `Effect` 和 `unknown` 语义；
4. 让 Task Runner、Loop Engine、Device Action 共享 Execution；
5. 建立 SQLite 事务状态、事件日志、Inbox 和 Outbox；
6. 统一 Policy、Mandate、Approval、Permission 契约；
7. 完成租约、幂等键、恢复和 Reconciliation 验收。

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
