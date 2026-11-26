# 本地验证

2026-11-26 · Codex · 1237ff7

| 验证 | 命令与结果 |
|---|---|
| 普通会话、模型请求、跨会话记忆 | `cargo build -p mock-gateway --bin mock-gateway` 和 `python3 .codex/pa-value-audit-20261126/session_probe.py` 成功。三个模型请求在回环接口采集。已保存偏好可从记忆接口查到，模型输入中均无该标记。同会话出现前一轮标记，新会话只有新问题。[输出](probe.json) |
| 工作区本地单元、集成和冒烟 | 首次 `cargo test --workspace` 退出 101。进程组超时测试读取 grandchild.pid 时文件缺失。按同一命令完整重跑，729 项通过、0 失败，该用例亦通过。首次失败的根因未确认。[首次](workspace.log) · [复跑](workspace-rerun.log) |
| 前端 | apps/web 下的 `npm run build` 退出 0，TypeScript 和 Vite 通过，主 JS 产物 564.67 kB，触发构建体积提示。[输出](web.log) |

普通聊天实验使用临时数据库与模型替身，仅观测送入模型的消息。独立 Tauri 原生窗口、真实系统资料和用户试点未在本轮复验。
