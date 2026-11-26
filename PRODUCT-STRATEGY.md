# bu2agent 产品战略：跨设备连续的 Personal Agent

> 本文定义 `bu2agent` 长期要成为什么产品。当前交付状态见 `README.md`，技术领域模型和运行时设计见 `PA-ARCHITECTURE.md`。

- 初稿日期：2026-11-03。产品核心价值更新于 2026-11-26（Codex）。
- 当前交付形态：本地单用户、PC 优先
- 长期产品形态：Local-first、多设备、一个人的连续 Personal Agent

## 1. 产品定位

### 产品核心价值

**持续积累个人世界模型，并在需要时调用上下文、工具和执行能力完成事情。**

同一个 PA 服务于用户的生活、办公与专业工作。产品从 Muse 类可见工作台开始，让用户和 Agent 一起查看资料、规划任务、确认动作和检查结果。用户在共同工作中形成个人世界模型，PA 从数据库找出与当前事项有关的人、约定、目标和过往决定，结合日程、文件或任务状态提出下一步。经过反复验证的工作流程再逐步接入文字和语音入口，PA 调用工具执行，将实际结果与用户的纠正记下来，供下一次委托使用。

例如，用户说「把周五下午留给家人」。理想的 PA 能联系到家庭约定、当天的客户会议和已经承诺的交付时间，提出可行的调整。用户选定安排后，PA 更新日历并记录改期结果。下次询问项目进度时，它能引用新的约定。

长期目标是让这份个人上下文和任务进度随用户跨越 PC、手机、手表及其他可信设备，在合适的设备上继续工作。

当前工作树对记忆调用、任务规划与执行回写的支持，以及生活、办公和专业工作的验收场景，见 [个人助理产品设计与能力审计](PA-PRODUCT-DESIGN.md)。

产品阶段顺序是工作台共同参与、流程沉淀、文字与语音委托、主动跟进。语音和文字先服务于可见工作现场，成熟后才承担主要入口。

“本地单用户”描述的是当前产品的所有权、信任和部署边界，不表示最终只能有一台设备，也不表示所有能力都必须运行在 PC 上。

## 2. 为什么单一 PC Agent 不够

用户的生活和工作不是在一个设备上连续发生的：

- PC 适合代码、文件、长任务和高算力执行；
- 手机拥有移动位置、相机、通信、即时输入和主要通知入口；
- 手表适合提醒、快速确认、状态感知和短语音交互；
- 耳机、家庭设备、车辆和其他设备提供环境、声音和动作能力。

如果 Goal、Task、Memory、Approval 和执行进度只存在 PC 上，PA 无法真正理解“用户现在处于什么场景”，也无法在设备切换后继续工作。

因此产品的基本单位不是一台机器，而是：

```text
一个人
  └── 一个逻辑 Personal Agent
        ├── 多个可信设备
        ├── 一个连续的个人状态
        └── 多个可迁移的执行位置
```

## 3. 四种连续性

### 3.1 数据连续性

用户的 Goal、Project、Task、Conversation、Memory、Policy 和 Effect 历史，不应因为换设备而丢失。

### 3.2 场景连续性

PA 应知道当前工作场景的合理投影：

```text
当前设备 + 用户关注点 + 最近对话 + 时间 + 位置/环境信号 + 可用注意力
```

场景是动态上下文，不是新的事实源，也不能自行扩大权限。

### 3.3 执行连续性

任务可以从手机发起，在 PC 上执行，结果再回到手机或手表通知用户。任务的逻辑 ID 不随设备变化。

```text
Task
  └── Execution
        ├── Assignment on phone
        ├── Worker on PC
        └── Result delivered to watch
```

### 3.4 注意力连续性

PA 应根据用户当前可用设备选择交付位置：

- 手表：紧急提醒、快速确认；
- 手机：审批、简短回答、即时结果；
- PC：长文档、代码审查、复杂决策。

## 4. 产品架构：个人数据平面与执行平面分离

最终架构应分为两个平面。

```text
Personal Data Plane
  ├── Identity
  ├── Goals / Projects / Tasks
  ├── Conversations / Memory
  ├── Policies / Mandates / Approvals
  ├── Execution Ledger
  ├── Artifacts / Evidence references
  └── Device / Capability Registry

Execution Plane
  ├── PC Worker
  ├── Phone Worker
  ├── Watch Capability Node
  ├── Local Tools
  ├── Connectors
  ├── Device Actions
  └── Model Runtimes
```

个人数据平面回答“用户是谁、想做什么、已经发生了什么”；执行平面回答“这次由哪台设备、通过什么能力完成”。

PC 可以是当前阶段的主要 Data Plane 和 Worker，但产品模型不能把它写死为唯一节点。

## 5. 数据如何跟着用户

“数据跟着用户”不等于所有原始数据无差别复制到所有设备。数据应分层处理：

| 数据类型 | 默认策略 |
| --- | --- |
| Goal / Project / Task 状态 | 加密同步，设备可恢复 |
| Conversation 摘要和索引 | 按用户策略同步 |
| Memory | 分 scope 同步，可只同步摘要或引用 |
| Policy / Mandate / Approval | 加密同步，带版本和设备范围 |
| Effect / Execution 状态 | 同步事件和结果，保留设备来源 |
| 原始文件和媒体 | 默认留在来源设备，按需获取或加密复制 |
| 凭据和私钥 | 不作为普通数据同步，使用设备密钥和授权转发 |
| Worker 临时上下文 | 不直接同步，只同步 Checkpoint 和摘要 |

因此需要：

- 用户级 Identity，而不是设备级身份；
- 设备注册、密钥、撤销和可信状态；
- 加密同步和事件复制；
- 离线队列与冲突解决；
- 数据来源、版本和 provenance；
- 设备能力和当前可用性登记。

## 6. 设备角色

设备不是多个独立 PA，而是同一个 PA 的不同能力节点。

| 设备 | 主要角色 |
| --- | --- |
| PC/Mac | 深度工作、代码、文件、长时间 Worker、本地数据锚点 |
| 手机 | 移动上下文、通信、相机、位置、即时输入和审批 |
| 手表 | 提醒、快速确认、状态查看、短语音和传感器 |
| 耳机 | 语音输入、音频输出、环境交互 |
| 家庭/车辆设备 | 环境状态、设备控制和自动化动作 |

同一个 Task 可以在不同设备上被查看、批准、执行或交付，但每个 Effect 必须记录实际执行设备。

## 7. 对核心模型的影响

### Goal / Project / Task

这些对象必须是设备无关的。它们的 ID、状态和关联不能依赖某个 PC 的本地路径。

### Workspace

Workspace 需要拆成两层：

```text
Workspace
  └── Workspace Snapshot / Device Binding
        ├── resources
        ├── permissions
        ├── available capabilities
        └── preferred execution devices
```

Workspace 是逻辑工作边界，设备绑定是当前实际可用环境。

### Execution

Execution 必须拥有稳定的逻辑 ID，但可以拥有多个设备 Assignment：

```text
Execution
  ├── Assignment 1：手机创建
  ├── Assignment 2：PC 执行
  └── Delivery：手机和手表接收
```

执行迁移时不能重复执行已确认的 Effect；必须依靠 Effect ID、幂等键和 Checkpoint 恢复。

### Session

用户 Conversation 可以跨设备继续；Worker Session 则是设备本地运行时，只同步摘要、消息游标和 Checkpoint。

### Approval

审批请求应被路由到用户当前最合适的注意力设备，但审批事实必须进入个人数据平面，不能只存在手表或某台 PC 的内存里。

## 8. 未来产品形态

用户不应感知自己在使用几个独立应用，而应感知：

```text
我有一个 Personal Agent
  ├── 在 PC 上帮我工作
  ├── 在手机上理解我的即时场景
  ├── 在手表上提醒和请求确认
  └── 在其他设备上调用能力
```

产品入口可以不同，但 Goal、Task、Memory、Policy、Execution 和 Attention 必须连续。

## 9. 战略路线

### S0：PC-first 基础内核

当前阶段完成：

- 单用户身份；
- Goal/Project/Workspace/Task 的设备无关模型；
- Task → Execution → Effect；
- PC 本地 Worker 和文件/设备/连接器能力；
- 可恢复状态和审计。

### S1：个人数据平面

- 用户级 Identity；
- SQLite 事务状态和事件日志；
- 加密数据同步协议；
- Device Registry 和 Capability Registry；
- Session、Task、Approval、Effect 的跨设备恢复。

### S2：多设备连续性

- 手机作为上下文和审批设备；
- PC 与手机之间的 Task/Execution handoff；
- Watch Attention surface；
- 离线操作、冲突解决和设备撤销。

### S3：个人主动 Agent

- 文件、日程、消息和设备 Source；
- 用户可见、可撤销的主动规则；
- 跨设备场景推理；
- 在隐私边界内选择最合适的执行节点。

## 10. 产品成功标准

未来 PA 是否成立，不以“PC 上能调用多少工具”为主要标准，而看：

1. 用户换设备后，目标、任务和上下文是否仍然连续；
2. 任务是否可以在合适设备上继续，而不是重新开始；
3. 审批和注意力是否能到达用户当前可用设备；
4. 原始数据是否遵守来源设备和用户设置的隐私边界；
5. 设备离线、重启、丢失或撤销后，系统是否能安全恢复；
6. 用户是否感受到一个持续存在的 PA，而不是多个互不相识的工具。
