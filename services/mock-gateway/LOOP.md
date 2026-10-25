# 模型工具循环：网关与 DeepSeek 协议

日期：2026-10-25；执行者：Codex。基线：`ec42561`。

提供状态机、审计存储、四个 HTTP 端点、绑定审批及 DeepSeek 非流式工具协议。
正常启动从现有 env/Keychain 路径解析一次配置，聊天与循环共用配置。
有 key 时接入 DeepSeek 循环模型；无 key 或使用 `--mock-chat` 时，循环
`start` 返回 503 `loop_model_not_configured`，普通聊天仍回显。
普通聊天的请求、失败 502 和 stream 协议不变；不会把回显当作循环模型。
测试通过 `AppState::with_loop_backends` 注入模型与内存工具。桌面/壳/Loops
视图通过同一份快照接线；不开放删除或真实提醒。

## DeepSeek 适配

- 仍用 `POST {base_url}/chat/completions`，默认 `deepseek-flash`；复用
  `DEEPSEEK_BASE_URL` / `DEEPSEEK_MODEL` 配置，不新增凭据来源。
- 固定系统指令只约束工具使用；不能由用户请求编辑。请求含三工具 JSON
  Schema、`tool_choice=auto`、`stream=false`、`thinking.type=disabled`。
  提供方函数名不允许点号，因此对外用 `fs_list` / `fs_read` / `fs_write`，
  只映射这三个名字到内部 `fs.list` / `fs.read` / `fs.write`。
- assistant 的调用 ID、原始 arguments 字符串和伴随文本只保存在内存历史。
  按提议顺序为每个结果生成 `role=tool`，原样使用该次调用的 `tool_call_id`，
  包括非法、超额、重复、拒绝结果。相同写入的不同提议仍有各自的提供方 ID。
- arguments 缺失/非字符串/不是有效 JSON、JSON 参数不符合模式、额外参数或
  未知工具：作为非法提议计数并回传错误，不执行、不申请审批。
  调用可含可选非负整数 `index`，只忽略该元数据，不用来排序或授权；其它
  调用/function 多余字段同样使该提议非法。无法配对的缺失/重复 ID、
  缺少函数名、畸形响应结构、空回答和截断响应则是
  整次模型协议失败，不执行响应里的任何工具。单次响应要求一个 choice。
  缺失/非字符串 arguments 在回传历史中用空串表示（保留 ID），而执行参数
  明确标为非法；任何原本为字符串的 arguments 都逐字保留，包括坏 JSON。
- 工具结果 content 是 JSON 字符串，含 status、error、body、truncated、
  original_bytes、duplicate_of。4 KiB 限制作用于 body，元数据另计。
- 状态机把剩余步骤预算传给适配器；HTTP 请求超时取该值与 30 秒较小值，
  连接等待最多 10 秒且仍受总请求超时约束。关闭请求重试与重定向；HTTP
  错误/解析错误归 model_failed，超时归 model_timeout，预算耗尽优先。
  不保存或回传提供方错误响应体。迟到响应不会产生新的工具调用。

协议依据（2026-10-25 核验）：
[Chat Completions API](https://api-docs.deepseek.com/api/create-chat-completion/) 与
[Tool Calls](https://api-docs.deepseek.com/guides/tool_calls)。

## 接口

- `POST /agent/loop/start`：`{"prompt":"…","max_calls":5,"budget_ms":120000}`。
  上限与预算可省略，只能下调；返回 `loop_id` 和初始快照。
- `GET /agent/loop/get?loop_id=loop-1`：返回审计字段、在途标志、剩余预算，
  以及当前进程内可用的最终回答。待审批时额外返回 `pending_write`
  （call_id、规范化 path、text 全文、args_sha256）与 `approval_status`，
  用于用户核对写入和恢复已决定但尚未推进的审批。其它工具参数与消息
  历史不返回；该全文预览只留内存，不属于审计记录。
- `POST /agent/loop/resume`：普通推进传 `{"loop_id":"loop-1"}`。
  每次推进最多一次模型调用或队首工具处理；等待审批立即返回。
- `POST /agent/loop/cancel`：`{"loop_id":"loop-1"}`。终态不再推进。

等待审批的调用返回 `approval_id` 和 `binding`，后者包含循环 ID、调用序号、
工具和规范化参数 SHA-256。先向现有 `/approval/decide` 发送
`{"tool_call_id":"approval-N","status":"approved"}` 或 `rejected`，
再续跑时原样带上 `approval_id`、`binding`。错绑返回 403，待审批状态不变；
未决定返回 409。循环审批不允许 edited、更改已决定结果或重复消费。

循环复用 `/agent/tool/call` 的服务端工具分派函数，不调用裸 `/files/write`，
也不向自己的监听端口发 HTTP。内部执行前复核绑定与消费状态；公共工具
端点拒绝使用循环审批，避免从该入口绕过队列、参数绑定和去重。
已有人工工具审批不受影响。文件写入沿用现有覆盖模式，不新增 append。

## 执行规则

- 所有提议计数，最多前五个进入正常处理。批量超额逐条记
  `failed / cap_exceeded` 并回传结果；队列清空后最多一次收尾模型调用。
  收尾再次提工具就失败结束。`rejected` 只表示用户拒绝。
- 参数模式拒绝额外字段；提示最多 64 KiB，写内容最多 1 MiB，路径最多
  4096 字节。参数规范化后以 SHA-256 绑定；文本逐字节区分。
- 同一循环同参写一旦提交，不重复执行，也不重新审批；原结果未知则
  重复仍回未知。已拒绝同参提议同样复用结果。普通读不消除写的未知状态。
- 单步预留是启动的线性化点；预留、取消共用循环锁。已有步骤在途时
  续跑返回 409；HTTP 客户端断连不取消后台收尾。取消先发生则不能预留。
- 模型等待最多 30 秒，工具等待最多 10 秒，并受剩余 120 秒预算限制。
  计入本轮等待模型和执行工具的时间，包含失败与超时；审批和客户端闲置
  不计入。后台迟到工作不获得新执行名额，也不能推进循环。
- 非终态工具超时记 unknown；迟到结果在独立 `late` 中保存结构化摘要，
  不回写已经回传模型的 unknown、不重新投递。迟到模型只记到达时间。
- 仅正常收到最终回答时，按未确认写入选择 completed/needs_review。
  cancelled/interrupted/failed 保持原终态，同时列出未知项；未知读也列出，
  但不单独触发 needs_review。
- 真实文件分派在临时目录测试中覆盖保存后重读；不将保存响应当作落地
  证明。路径符号链接拒绝。第一轮不提供 unknown 解析或自动重试。

## 持久化与重启

`MOCK_LOOPS_PATH` 可选；未设置使用内存。设置时必须在文件根之外，
脚本使用文件根的兄弟位置。JSONL 只存结构化字段、哈希和长度：不存提示
正文、原始消息、写内容、读正文、最终回答或正文摘录。4 KiB 截断发生在
回传模型的工具正文上，按 UTF-8 边界截断并提供原始长度、截断标记。

重放按 ID 取最后一行，ID 计数续接。重启时所有非终态改为 interrupted；
在途工具改为 unknown（包括已取消但未返回的工具），不续跑、不自动重发。
旧审批带循环/调用绑定，且不能在公共工具端点复用。

写操作前的审计失败会阻止该步骤执行；损坏的循环审计文件拒绝加载，
避免按旧行恢复出错误状态。普通人工审批仍保持原 best-effort 行为。
本轮不承诺断电耐久性。取消不代表在途写被撤回。

## 验证

`cargo test -p mock-gateway --test loop_smoke` 覆盖冻结设计的十二类场景，
包括 HTTP 并发、断连、取消、迟到、预算、批量上限、审批绑定、重启、
UTF-8 截断与无正文审计。大部分工具使用内存假实现；共用文件分派测试
仅写入测试自建临时目录，不触碰真实用户文件或提醒。

完整门：`run-tests.command`、workspace Clippy、`run-shell-tests.command`。
测试不依赖真实模型凭据、Keychain 或 EventKit 写授权。

`cargo test -p mock-gateway --test deepseek_loop_smoke` 使用本地假 DeepSeek HTTP
服务和内存工具，覆盖 ID/顺序与历史回传、审批绑定、非法提议、超额收尾、
重复未知/拒绝写入、UTF-8 截断、审计无正文、HTTP/畸形响应/超时/取消，
以及生产配置装配与普通聊天兼容。测试只写临时审计文件；工具文件在内存。
真实模型与窗口验收已独立完成，范围见下节；假服务测试不替代真实模型证据。

## 已完成窗口验收（2026-10-25，Codex 整理）

- 真实 DeepSeek：专用临时目录中读、审批写入并确认、正常收尾；拒绝不落盘；待审批取消不落盘；真实网关重启后 interrupted、预览与续跑入口消失，重开窗口只按 ID 查询。
- 假模型与真实文件分派：写延迟超时、保存后读回失败、重复未知写均走现有状态机。正常收尾为 needs_review；普通读取、迟到成功不消除原 unknown；同参重复不重执行、不新开审批。
- 工具执行中取消：8000ms 写延迟小于 10000ms 工具超时，120000ms 总预算；约 4.2 秒取消时写 running/submitted=true 且目标文件不存在，约 8 秒迟到写入成功。循环始终 cancelled，结果仅归档 late，没有后续模型或工具调用，审批消费一次。前三次未进入该时序的准备轮不计通过。
- 窗口明确说明取消不撤回已提交写入。迟到说明统一为“仅归档，不回传模型、不推进循环”；原调用 unknown 时另行标明保留 unknown。取消在执行中、结果尚未投递模型时，调用可以记录实际 succeeded 与 late；循环终态仍为 cancelled。
- 审计检查未发现提示、读写正文、最终回答或原始模型工具 ID。以上均不证明断电耐久，不定义 unknown 自动解析，也不将假模型故障称为真实 DeepSeek 故障。

本地验证证据保存在 .codex，按约定不纳入项目提交。Loops 的 HTTP 浏览器视图仍未单独视觉验收；历史真实模型轮首次打开窗口时的页头 stale gateway 状态也未在本轮修复。

## 测试专用故障装饰器（默认关闭）

`MOCK_LOOP_FAULT` 未设置时完全关闭；配置仅在该网关启动时读取一次。
仅包装循环默认文件分派，位于服务端复核已消费审批之后。普通聊天、裸
文件端点、Agent 公共工具路径，以及注入的内存 LoopTools 不经过装饰器。
不改变正常分派、状态机、审批绑定、结果截断或终态优先级。

启用前须手动创建专用临时文件根并设置 `MOCK_FILES_ROOT`。配置要求根的
真实路径是操作系统临时目录的子目录（不能是临时目录本身）。仅使用测试
文件；绝不能指向用户资料。无效模式／时长／文件根会拒绝启动。

| 变量 | 语义 |
| --- | --- |
| `MOCK_LOOP_FAULT=write_delay` | 仅循环写入在原分派前等待，必须同时设置 `MOCK_LOOP_WRITE_DELAY_MS`，范围 1–60000 ms。 |
| `MOCK_LOOP_FAULT=readback_missing` | 仅在原写分派成功后删除本次目标，随后照常执行原有保存后重读；不会直接合成 unknown。 |
| `MOCK_LOOP_TOOL_TIMEOUT_MS` | 仅故障开启时可用，范围 1–10000 ms；只缩短现有工具等待，不改变模型等待或总预算。省略保留 10 秒。 |

两个故障模式互斥。无 `MOCK_LOOP_FAULT` 时辅助变量也不生效。写延迟通过
现有 timeout 产生 unknown；后台写可迟到成功，仍只归档 late，不消除 unknown。
读回失败通过现有字节内容确认逻辑产生 unknown。删除不成功时不伪造故障，
仅记录不含正文的警告；验收必须核对实际 unknown。

窗口建议用 5 秒总预算、250 ms 工具等待、700 ms 写延迟，再走读观测与
模型收尾。不能只把总预算当工具超时：总预算耗尽会优先 failed，而非
needs_review。配置不会延长任何预算；人工审批等待仍不计入执行预算。

验收使用本地假 DeepSeek HTTP 服务与假凭据，不读 Keychain、不调用真实
模型。每次写仍须窗口明确批准。相同未知写通过原有去重返回 duplicate_of，
不重执行、不新开审批；普通 fs.read 仅提供观测，不改变原调用 unknown。
清理前必须等待迟到工作结束，再停止网关并删除专用临时目录。

## 桌面、壳与窗口

- Loops 标签页支持发起、按 ID 查询、单步推进、查询同步和在途取消。
  不自动推进，不在前端推断终态；每次推进的决策仍由网关状态机负责。
- 桌面 LoopClient 保留 70 秒等待，四个 Tauri 命令均用 spawn_blocking，
  不阻塞窗口或取消命令；IPC 多词参数统一 snake_case。
- 待审批窗口展示目标路径、覆盖语义、字节数、全文预览及绑定摘要。
  只 decide 网关已创建的绑定审批，不另开普通工具审批。若 decide 成功但
  续跑断连，查询可显示 approved/rejected，再用同一绑定继续；不重复决定。
- 浏览器 localStorage 只保存最后一个 loop_id。返回 Loops 时仅查询记录，
  不自动批准/推进。重启后的 interrupted 不允许续跑，预览和最终正文不恢复。
- 展示提议计数、模型尝试、执行预算、每次调用的结果摘要、duplicate_of、
  未确认读/写、迟到标记及最终说明。普通读观测不提供消除 unknown 的入口。
- HTTP 与 invoke 使用相同 DTO。客户端错误后提示先查询，不自动重发；
  403/409/503 分别说明绑定拒绝、状态冲突、模型不可用。
