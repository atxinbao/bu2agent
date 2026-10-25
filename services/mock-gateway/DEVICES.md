# 设备闭环：提醒事项

日期：2026-10-25；整理：Codex。B 层提交基线：ec42561。

## 实现边界

网关负责设备动作与审批复核，桌面与壳负责传输，Devices 视图展示服务端事实。默认 Fake 后端种子为 reminder-demo，提醒本体存在内存；启用动作 JSONL 仅持久化动作记录，不会把 Fake 提醒变成持久化数据。

macOS 上显式设置 MOCK_DEVICE_BACKEND=eventkit 才使用 EventKit；通过 MOCK_DEVICE_REMINDER_ID 固定单条专用提醒。可写动作为 reminders.set_completed，使用显式 true/false。不得将某次成功读取或 inventory 标签当成永久 OS 授权保证。

系统授权回答能否访问提醒数据库，bu2agent 审批回答某一次写入是否可以执行，两者独立。授权未决定时等待授权，不提前写入；每次真实写入仍需批准且由服务端复核。该载体验证的是本机工具闭环，没有验证 LAN IoT，也没有开放网关到局域网。

## 生命周期与恢复

动作记录包括 requested、awaiting_authorization、completed、rejected、failed、unknown 等状态。工具超时可能仍在执行，因此先记 unknown，不盲重试；重查失败或仍为旧状态不能证明写失败。设备后端在保存后重查，读取到目标状态才确认成功。此规则属于设备流程，不是模型循环的未知解析规则：循环第一轮的普通 fs.read 永远不消除 unknown。

动作 JSONL 用 MOCK_DEVICE_ACTIONS_PATH，位于文件根之外；重启恢复记录并续接 ID。正常停服恢复已经验证，不声明断电耐久。EventKit 的恢复未完成修正会清空旧 completion_date，再保存显式状态。

## 已完成验收

- Fake：生命周期、授权、错误/超时、迟到、重查、幂等、网关审批与 pin、动作持久化；桌面、壳、Devices 接线与窗口操作。
- EventKit：真实网关编译、只读授权状态与列表读取；真实专用提醒经批准设完成、恢复未完成、double-complete、拒绝、收尾恢复未完成。
- 有效真实写入轮为 action-6 至 action-11 / approval-9 至 approval-14。普通重启后六条动作记录与真实状态一致，排序 JSON 与两份 JSONL 哈希一致。旧动作 1–5 不作为该轮验收证据。
- double-complete 只证明完成态不变、无报错，不证明所有内部元数据不变。提醒保留未删除。
- 早期外部工具的“0 条”是未完成提醒口径；EventKit 本实现列表同时返回已完成与未完成，不能由“0 条未完成”推断数据库为空。

历史设计稿 goals/bu2agent/files/device-loop-design.md 不在本次 Mac 仓库中，未假装回填该外部文件；本页是仓库内现行契约与验收结论。

## 打包事项（归属已闭合，发行项仍待核验）

独立实验网关包的 TCC 归属已由端到端直接验证闭合（结论 CONFIRMED_FOR_THIS_EXPERIMENTAL_BUNDLE）：Finder 发现后 bundle ID 可解析、not_determined→authorized、权限列表独立 R2 条目离开重进仍在、TCC 日志 subject/responsible 同为新 ID。弹窗因工具限制未直接读取，通过依据是独立条目，不补猜名称；R1 历史 NOT_CONFIRMED 不追改，不写成已完成根因唯一归因。实验包已清理，R2 权限条目保留未撤销。正式发行前仍需各自完成：稳定发行签名身份、版本升级后授权延续、Swift 运行库分发、安装与生命周期管理、公证、正式发布包自身验证。参见 [打包验证](../../packaging/README.md)。

macOS 14+ 使用 NSRemindersFullAccessUsageDescription；兼容 macOS 13 时保留 NSRemindersUsageDescription。Swift 桥当前使用工具链 rpath，开发机可运行不代表目标用户机器已具备运行库。
