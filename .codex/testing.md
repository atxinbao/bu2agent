# 测试记录

日期：2026-10-24；执行者：Codex。

- 单元、集成及 HTTP 冒烟测试：`cargo test --workspace`，通过。
- 服务冒烟测试：`curl --fail --silent --show-error http://127.0.0.1:3001/health`，返回 `{"status":"ok"}`。
- 文件一键演示：`chmod +x run-files.command && ./run-files.command`，通过；无审批写入被拒，审批写入/删除和分块读取成功，结束后 3001 已停止。
- Agent 一键演示：`chmod +x run-agent.command && ./run-agent.command`，通过；echo 成功，无审批写停在 pending，审批写入和读回成功，拒绝写被服务端拒绝，后续读取仍为 `from agent`，结束后 3001 已停止。
- 连接器一键演示：`chmod +x run-connectors.command && ./run-connectors.command`，通过；假笔记/邮件列表正常，未审批新建停在 pending，审批后出现新笔记，域外发信被拒，域内发信成功，凭据新进程不可见，结束后 3001 已停止。
- Release 打包演示：`chmod +x run-package.command && ./run-package.command`，通过；`cargo build --release -p agent-desktop -p mock-gateway` 成功，生成 `agent-cli`、`chat-cli`、`connector-cli`、`file-cli`、`mock-gateway`，4 个 CLI 无参数运行均退出不挂起。
- Tauri CLI：`~/.local/bin/cargo tauri --version` 返回 `tauri-cli 2.12.4`。
- Web 构建：`npm run build` 通过（TypeScript 检查与 Vite production build）。
- Tauri shell：`run-shell.command` 修复后成功进入 `Running target/debug/web-agent-shell`；并行验证 `GET /health` 返回 `{"status":"ok"}`、Vite 5173 可访问，结束后端口均已释放。
- 独立壳检查：`cargo check --manifest-path apps/shell/src-tauri/Cargo.toml` 通过。
- Mac 聊天线复核：`run-shell.command` 成功运行 `target/debug/web-agent-shell`；`curl http://127.0.0.1:3001/health` 返回 ok，Vite 5173 可访问。
- 前端严格检查：`apps/web/npm run build` 通过，包含 `tsc --noEmit` 与 Vite production build。
- Mac 交互轮：`run-shell.command` 成功运行 `target/debug/web-agent-shell`；源码确认 Chat/Files/Agent/Connectors 四标签及 invoke clients/commands 已注册，网关与 Vite 可访问。原生窗口未出现在 CUA 可访问应用清单，因此未伪造逐项点击结果。
- Shell command 测试：`chmod +x run-shell-tests.command && ./run-shell-tests.command`，PASS；`agent_commands_roundtrip`、`connector_commands_roundtrip`、`file_commands_roundtrip` 共 3 项通过，0 失败。运行时 JS 键名到 Rust 参数绑定仍属于静态检查边界。
