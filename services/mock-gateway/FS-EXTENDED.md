# 文件扩展端点（本地解释）

日期：2026-10-28；执行者：Codex。

`/files/reads`、`/files/stats`、`/files/search`、`/files/raw`、`/files/subscribe`、`/files/thumbnail`、`/files/export` 与 `/files/library` 是对 Muse 路由形态的本地解释，不宣称与原产品 schema 等价。所有路径仍受单一 Mock 文件根约束。

- `search` 只搜索相对路径和文件名，不做全文搜索。
- `subscribe` 使用递归清单摘要游标长轮询，最长等待 25 秒。
- `thumbnail` 只处理 png/jpeg/gif/webp，返回最大边不超过 512 的 JPEG；视频不处理。
- `export` 是唯一写扩展，目标必须是文件根外绝对路径，走 `fs.export` 绑定审批，目标存在时先返回 409 且不消费审批。
- `library` 在单一 Mock 根内按 image/video/audio/document 聚合，真实 Muse 的多授权根语义留待运行时证据修订。

HTTP 面提供 raw 原始字节；invoke 面继续使用 JSON/base64 协议，缩略图提供 JSON 变体。
