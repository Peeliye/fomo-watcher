# 后续审计修复实施 Prompt

逐项进度和验收状态以 [真实交易能力待办清单](LIVE_EXECUTION_CHECKLIST.md) 为准；本文件保留实施要求和历史边界，不以本文的进度描述替代清单勾选。

你是本仓库的高级量化交易系统工程师。继续完成尚未实现的能力，不得仅凭配置开关、假 signer 或接口桩宣称“实盘就绪”。先检查 `git status`，保留其他人的改动；不得读取、修改或输出 `.env` 中的秘密，不得写入真实私钥，不得启动实盘。任何已有 SQLite 的 schema/data 迁移必须先用 SQLite backup API 备份，验证备份完整性，提供回滚说明。`live_armed` 必须保持 false。

按以下顺序实施，每项均提供可复现的测试和失败场景：

1. **钱包来源持久化**：把 RPC provider 从只读契约落实为分别维护的 EVM/Solana 实现；按配置订阅 watch wallet，使用 receipt/log/instruction 与余额差确认 swap。checkpoint 与事件 outbox 原子提交，消费端 ack 后才移除待交付状态；重启、重复事件和 reorg reversal 可重放，不启动历史全量扫描。区分 pending/processed 与可执行确认级别，校验配置的每钱包过滤及金额规则。
2. **执行投递安全**：在网络广播前，从最终签名字节计算链上交易 ID，并在 journal 原子持久化 intent、ID、序列化哈希、nonce/blockhash。重启后只查 receipt 或人工判定，不自动重复广播。为失败广播、replacement、过期和 reorg 编写状态转换测试；replacement 需要独立的旧/新交易哈希历史，不能覆盖证据。
3. **真实 capability adapters**：逐链实现 quote、独立 sanity price、构建、最终字节反向解析、模拟、系统凭据库 signer、广播、nonce/blockhash 与 receipt 跟踪。每个 adapter 必须自检真实可用性；scope、资金预留、报价时效、route allowlist、退出路径和熔断必须在广播前共同通过。ARC 没有可信价格或退出 route 时始终 fail-closed。完成适配器不代表自动解锁 live。
4. **统一极速路径**：Node 与 Python 使用同一版本化 schema/golden vectors；持久化队列由同一执行服务消费，不以 1 秒文件轮询作为实盘路径。记录 upstream、queue、skew、decision latency 并测试迟到默认 dropped_late。
5. **账本与运维**：以最终 receipt 的部分成交、费用和 token/native delta 更新事实仓及分配子账；replacement/reorg 能反向冲销。完善全套运行文件备份、恢复演练、RPC WSS heartbeat/订阅新鲜度/模拟/广播能力和实时 failover。保护 sidecar 单写者锁及系统 secret store 回退权限。

每轮交付：列出实际完成与仍缺能力；运行 pytest、ruff、pyright、Node tests、`git diff --check`，必要时运行 100k fills + 100k market observations 基准；确认 `portfolio_maintenance check` 为 `ok=true`，明确声明 live 仍关闭。不要把“契约存在”写成“真实适配器已实现”。

## 最新边界与下轮聚焦

已加入 EVM/Solana HTTP JSON-RPC provider、0x/Jupiter quote、系统凭据库 signer、EVM V2 严格构建/解析、Solana legacy parser、模拟、广播、receipt、nonce/blockhash journal、共享 SQLite 投递队列。默认服务不注册广播器，数据库仍强制 `live_armed=0`。0x Settler/Jupiter versioned transaction 尚无法被最终字节严格 scope parser 完整验证，因此 capability/readiness 不得标为端到端实盘 ready。

下一步优先：实现并审计 0x/Jupiter 实际返回交易格式的最终字节 parser 与安全构建；实现独立行情、余额/gas 快照及完整 exposure/exit-path 证据提供器；为钱包 reorg 建立队列取消与已提交交易/账本反向冲销流程；实现 replacement 哈希历史和最终 receipt token/native delta 账本归集；再做 WSS heartbeat/订阅新鲜度和链级 failover 演练。任何一步都不得移除 live 硬锁或把假测试能力注册成生产能力。

追加进度：执行 journal v7 对“未提交且未绑定签名交易”的失败 nonce 租约做同事务释放并留审计；已绑定 hash、预提交或可能广播的 nonce 不自动释放。钱包来源 reorg 写入持久化 fence，在预提交和广播尝试前阻断，并使未领取队列信号失效；已上链交易及账本逆向冲销仍需实现。RPC transport 增加 EVM chain ID / Solana mainnet genesis 校验、短 TTL 链头缓存、主备高度及哈希一致性检查、pending nonce 回退防护、一组 receipt/监听读取的端点 pin，以及广播不重试。只读余额与 RPC 健康探针改走同一 transport；面板将身份/链头健康与执行就绪明确区分。生产装配结构已存在，但 0x Settler/Jupiter versioned scope 与完整风险证据仍缺，故所有链继续未就绪。

## RPC 与执行适配器基础架构审计追加任务

本节与上面的待办一并修复，不得把接口、单元测试通过或 RPC 配置存在误报为实盘可用。先复核以下审计发现及其当前代码状态；若已有其他改动，保留并在交付中说明。全程保持 `live_armed=false`，不使用真实资金、不触发广播、不输出 RPC 密钥或钱包秘密。

1. **修复 EVM nonce 租约生命周期（优先）**：`EvmNonceManager.acquire()` 当前以所有历史租约的最大 nonce 递增，而 `ExecutionJournal.fail_unsubmitted_job()` 只释放资金预留、不释放未广播 nonce。设计可审计的租约状态或安全回收机制：仅对已证明未提交的失败意图回收；已预提交、广播不确定、replacement 或可能上链的 nonce 绝不能盲目复用。覆盖“取得 nonce 后构建/模拟/签名失败，下一笔从链上 pending nonce 正常继续”、并发预留、重启恢复、外部钱包占用 nonce、重复请求和 SQLite 回滚测试。迁移现有数据库前先用 SQLite backup API 备份并验证。
2. **校验每个 RPC 端点的真实链身份与新鲜度**：`FailoverJsonRpc` 目前按配置链 ID 过滤，却未在切换端点时校验该端点实际链 ID。EVM 校验 `eth_chainId`，Solana 使用可信 genesis hash 等链身份依据；建立带 TTL 的端点身份/健康缓存，切换或缓存过期时重新验证。错误链、落后节点、异常响应与 DNS/连接失败必须 fail-closed，并返回不含 URL/密钥的诊断码。为 nonce、回执、监听读取和广播分别测试主备切换，避免把不同节点的区块高度或回执状态当成同一视图。
3. **补齐执行装配，但不得自动启用 live**：明确 `scripts/execution_service.py` 当前只组装队列与 journal，未注册 `ExecutionCoordinator` 及真实广播链路。实现可注入、逐链可审计的生产装配和启动自检；只有报价、独立价格、构建、最终字节 scope parser、模拟、签名、nonce/blockhash、广播、receipt、风控证据、退出路径均真实就绪时才可报告该链 ready。Solana legacy parser/构建器当前明确返回未就绪，不能通过配置或假 capability 绕过；0x/Jupiter 未支持的交易格式同样保持阻断。装配完成仍不改变默认服务的 live 硬锁，启用实盘需另行人工授权与独立验收。
4. **统一 RPC 访问安全边界**：检查 `execution.readiness._native_balance()` 等绕开 `FailoverJsonRpc`、直接向配置 URL 发请求的路径；统一 URL 安全校验、DNS pinning、超时、禁止重定向、错误脱敏和链身份验证。区分“RPC 已配置”“探针可达”“执行所需方法可用”“端到端执行就绪”，面板/API 不得混用这些状态。

验收：增加上述失败注入与回归测试，特别是未提交 nonce 空洞、错链备用节点、主备读数不一致、服务未装配却误报 ready；运行相关 pytest、ruff、pyright、Node tests 与 `git diff --check`。交付时列明实测命令和结果、未解决阻断项，并再次确认 `live_armed=false`、无真实交易广播。
