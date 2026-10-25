# 打包现状与权限归属验证

日期：2026-10-25；更新：Codex。

## 当前交付

- Tauri 2 窗口与七个视图已实现。run-build.command 先构建 apps/web，再生成 Tauri release 包；本机开发包可用 cargo tauri build --debug --bundles app 构建。
- run-package.command 仅构建 workspace 的 CLI 与 mock-gateway，不等于完整桌面安装方案。
- 窗口与网关仍分开启动；run-shell.command 启动本地网关、Vite 与开发窗口。现有 .app 本身不负责安装或管理独立网关。
- OS Keychain、EventKit 提醒事项已有本机验证。签名发行、公证、自动更新、跨机器 Swift 运行库交付未据此宣告完成；独立网关权限归属已由独立实验包端到端验证闭合（见下节），但正式发行包自身验证仍待完成。

## 已有验证与限制

b8e61ac 的 GitHub Actions 五个任务成功（用户报告全绿）：三平台 workspace 测试与 Clippy、前端构建、macOS 壳 10/10。这证明该提交的构建/测试结果，不证明其它提交或所有平台的安装包都可用。

当前网关通过 build.rs 给开发产物加入活动 Swift 工具链 runtimeLibraryPaths 的 rpath，解决开发机加载问题。该路径可能只在构建机存在；发布前需要单独验证系统运行库或捆绑运行库方案，不能把本机启动当成无 Xcode 用户机器通过。

## 独立网关归属验证（已闭合，实验包范围）

当前结论为 **CONFIRMED_FOR_THIS_EXPERIMENTAL_BUNDLE**。端到端直接验证：Finder 发现后 bundle ID 可解析、not_determined→authorized、权限列表独立 R2 条目离开重进仍在、TCC 日志 subject/responsible 同为新 ID；实验包和临时文件已清理，R2 权限条目保留未撤销。弹窗因工具限制未直接读取，通过依据是独立条目，不补猜名称；签名 ID 与 bundle ID 一致，ad-hoc 验签通过并绑定 Info.plist。

R1 历史记录：Launch Services 历史日志有注册成功记录，系统隐私界面随后却报按该 bundle ID 查找应用失败。两者同时存在；直接验证闭合的是独立实验包的归属项，不单独证明 R1 异常的唯一原因，R1 历史 NOT_CONFIRMED 不追改。后续任何新身份授权实验仍须用户事前明确批准；不得重置 TCC、切换既有权限开关或修改提醒来制造通过。

R1 实验方法（历史记录；直接验证按端到端方案执行，见 .codex/bundle-permission-validation/direct-run/report.md），不提前决定正式安装路径或后台常驻机制：

1. 将当前网关 Mach-O 放入独立 .app/Contents/MacOS，使用独立 CFBundleIdentifier、CFBundleExecutable 与显示名；补充提醒事项用途字符串。为实验副本做本地 ad-hoc 签名并核验 Info.plist 绑定，不替换原始二进制，不申请发行证书。
2. 经 Launch Services 启动该 .app，而非在终端直接执行内部二进制。独立设置回环地址、临时文件根与日志路径；不连接真实模型、不读取真实模型凭据。
3. 先只读 GET /devices/authorization；需要触发系统授权时，明确交给用户选择，并记录提示中的准确应用名称。只读取提醒数量，不保存标题、ID 或内容。
4. 将 bundle 身份与签名、运行二进制路径、授权前后状态、提示名称及系统权限列表交叉核对。父进程链只作旁证；已经 authorized 或没有提示不单独证明权限属于本 bundle。
5. 同一 bundle 普通退出重开，再查状态及数量；不重置 TCC、不改已有终端/执行器的授权，不创建、完成或删除提醒。收尾停止实验实例，清理临时文件；系统留下的授权项如需撤销由用户决定。

macOS 14+ 用 NSRemindersFullAccessUsageDescription；需要兼容 macOS 13 时使用旧 NSRemindersUsageDescription。OS 授权不是 bu2agent 的逐次写入审批，独立打包也不取消既有审批与 pin。

LSEnvironment 可用于本地实验的固定启动配置，它只在 Launch Services 启动时生效；不能将凭据放进 Info.plist。若实验无法证明独立归属，结论保持未确认，不通过改变现有应用权限制造通过结果。

## 依据

- [Apple：Launch Services 配置键](https://developer.apple.com/library/archive/documentation/General/Reference/InfoPlistKeyReference/Articles/LaunchServicesKeys.html)（LSEnvironment、后台应用属性）。
- [Apple：访问 Event Store](https://developer.apple.com/documentation/eventkit/accessing-the-event-store)（用途描述与访问授权）。
- [eventkit crate 文档](https://docs.rs/eventkit/0.4.0/eventkit/)（命令行工具与负责进程权限的边界）。
- [B 层契约与已验收范围](../services/mock-gateway/DEVICES.md)。
