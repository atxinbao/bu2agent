# 本地操作记录

2026-11-26 · Codex

1. 通过 git、rg 和 graphify query 定位当前工作树、会话模型入口、长期记忆的读写调用点。核对 SQLite 数据结构、工具契约及同版 W4b 复验。
2. 通过 cargo build 构建当前网关，使用临时 SQLite 和本机 HTTP 模型替身执行 session_probe.py。观测保存、读回、同会话续聊及新会话四个操作。
3. 使用 cargo test --workspace、npm run build 验证本机入口。保留首次运行与相同命令复跑日志。
4. 用 apply_patch 写入根目录设计文档并从产品战略链接。用脚本核对引用、JSON 和测试统计。
5. 根据用户对产品阶段的修正，将 Muse 类可见工作台、人和 Agent 共同参与、流程沉淀、文字／语音委托的演进顺序同步到 README、PRODUCT-STRATEGY、PA-ARCHITECTURE 和 PA-PRODUCT-DESIGN。
