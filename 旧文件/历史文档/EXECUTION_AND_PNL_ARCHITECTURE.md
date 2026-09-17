# 多链执行钱包与 PnL 架构

当前实现已经推进到“填写公开执行地址并配置 RPC”的边界。程序仍然不读取私钥、不生成交易、不签名、不广播；这条边界只有在报价、模拟、熔断和小额 Canary 验收完成后才会继续向前移动。

## 1. 一个逻辑钱包，多种链地址

`execution-wallet.json` 表示一个逻辑执行钱包 `primary-multichain`：

- EVM 账户只有一个公开地址，可复用于 Ethereum、BNB Chain、Robinhood Chain 和 Base。这样 EVM 私钥不需要按链重复导入。
- Solana 使用独立公开地址。EVM 与 Solana 的密钥曲线和地址格式不同，不能安全地用同一个链上地址表示；它们仍归属于同一个逻辑钱包和同一套限额。
- 当前 `signer.backend` 固定为 `disabled`。配置文件只登记公开地址和未来签名器的引用，不允许保存私钥或助记词。

Dashboard 的“持仓 / PnL”会显示四个就绪阶段：`等待地址 → 等待 RPC → 等待签名器 → 影子执行就绪`。RPC URL 只通过 `.env` 中的变量提供，API 只返回是否已配置，不回显 URL。

| 链 | Chain ID | RPC 变量 | 地址账户 |
|---|---:|---|---|
| Ethereum | 1 | `RPC_ETHEREUM_URL` | primary-evm |
| BNB Chain | 56 | `RPC_BSC_URL` | primary-evm |
| Robinhood Chain | 4663 | `RPC_ROBINHOOD_URL` | primary-evm |
| Base | 8453 | `RPC_BASE_URL` | primary-evm |
| Solana | 1399811149 | `RPC_SOLANA_URL` | primary-solana |

下一步只需要提供测试用 EVM 地址、Solana 地址，并在本机 `.env` 配置计划测试链的 RPC。尚未测试的链可以不配置，且不能进入该链的执行就绪状态。

## 2. PnL 口径

`data/portfolio.sqlite3` 是长期账本，使用 SQLite WAL。金额以整数美元微单位存储，代币数量用十进制定点字符串存储，避免浮点累计误差。

- 购入时间：同一 `账户 + KOL + 链 + CA` 当前持仓周期的首次模拟成交时间。
- 成本：该持仓所有模拟买入的累计成交金额。
- 当前价值：数量 × 最近观察到的价格。
- 未实现盈亏：当前价值 − 当前持仓成本。
- 已实现盈亏：卖出所得 − 被关闭持仓成本 − 费用。
- 总盈亏：全历史已实现盈亏 + 当前未实现盈亏。
- 日统计：按 Asia/Shanghai 自然日记录买入、卖出、已实现盈亏、当时未实现盈亏快照、期末价值、费用和成交笔数。

当前 Fomo 卖出事件没有提供目标仓位的可靠卖出比例，因此采用明确且可审计的保守规则：同一 KOL、同一链、同一 CA 的第一次 `sell/clear` 信号关闭该 KOL 的整笔模拟仓位。它不会和其他 KOL 对同一代币的仓位混算。

在尚未接入 RPC 时，成交价和最新价来自 Fomo 事件。超过 `portfolio.mark_stale_seconds` 未更新会在页面标为“行情过期”，避免把陈旧估值误认为实时 PnL。接入 RPC 后，模拟成交价将由独立报价和滑点模型产生，真实 Canary 则以链上交易回执的实际数量、费用和成交价为准。

## 3. 持久性与性能

- `portfolio_events.event_id` 和 `portfolio_fills.fill_id` 唯一，重放同一事件不会重复记账。
- 持仓按 `account + KOL + chain + token` 建索引；成交时间、状态和日统计都有查询索引。
- WAL 允许监控程序写入时 Dashboard 并发只读；API 明细有上限，组合汇总始终扫描完整持仓集合，不受页面 limit 影响。
- 账本写入失败不会阻断原有告警，但会记录错误；后续应把该错误接入健康告警和熔断。
- SQLite online backup 可在 watcher 运行时生成一致备份，不复制可能未合并的 WAL 文件。
- watcher 持续运行时每天按 Asia/Shanghai 自动生成一次在线一致性备份；同一天重启不会覆盖已有日备份。

维护命令：

```powershell
.\.venv\Scripts\python.exe portfolio_maintenance.py check
.\.venv\Scripts\python.exe portfolio_maintenance.py backup
```

建议每日备份到另一块磁盘，并定期执行 `check`。备份脚本不会自动删除旧文件。

## 4. 到 RPC 之后的执行闸门

提供 RPC 并不等于允许买入。后续顺序固定为：

1. RPC 健康与延迟采集：最新区块高度、节点落后量、请求错误率、P50/P95。
2. 只读资产安全检查：代币合约权限、冻结/增发、税费、可卖性、池子深度和持币集中度。
3. 至少两个相互独立的报价源，计算预期滑点、价格冲击和最小到账量。
4. EVM `eth_call`/估算 gas 或 Solana 模拟交易；失败即关闭。
5. 把待确认交易计入敞口，执行组合、单链、单币、单 KOL 与日限额。
6. 独立签名器仅接收结构化交易意图；私钥不进入 watcher、配置文件或 Dashboard。
7. 只开放单链、单地址、单笔小额 Canary；实际回执回写当前账本后才扩大范围。

在上述闸门完成前，系统的最高状态仍是 `approved_for_shadow`，不会产生真实交易。

### 最快安全路由

真实买入不走临时跨链。资金和 gas 必须预先分布在目标链，信号到达后只做同链交换：

- EVM：0x Swap API v2 firm quote 与链上直池适配器并行竞速。0x 当前支持 Ethereum、BNB Chain、Base 和 Robinhood Chain。
- Solana：Jupiter Swap V2 Meta-Aggregator 与直池适配器并行；不采用已停止主动维护的旧 Ultra/Metis API。
- 后台持续维持 indicative 参考价、连接池、RPC 健康、EVM nonce/费用缓存和 Solana blockhash 缓存。信号到达后发起 firm quote，而不是从零发现路由。
- 默认 EVM 报价截止 200ms、Solana 160ms；必须至少两个独立路由、报价不超过 500ms、价格冲击不超过 100bps并通过交易模拟。
- 不是盲选最先返回者：只在相对最佳/预热参考价损失不超过 30bps 的候选中选预计“报价 + 提交 P95”最快者。超过截止时间或缺少第二路由即阻断。

上述参数是 Shadow 初始值，必须用目标地区服务器和付费 RPC 的实测 P50/P95 再调整，不能把供应商宣传延迟当成本机真实延迟。

## 5. 执行意图与持久熔断

`data/execution.sqlite3` 保存交易意图、状态转换和全局执行控制状态。风险决策会先落入该库，再进入后续阶段；相同 `signalId` 只能生成一个意图。

当前只存在 `blocked` 与 `shadow_ready` 两种可达状态。数据库约束强制 `live_armed=0`，初始熔断原因是 `startup_read_only`。也就是说，即使错误地把某个信号评估为 Shadow 就绪，它仍然没有构建、签名或广播路径。等 RPC 阶段的健康、报价、模拟和回执写入完成后，才会另行设计需要人工确认的 Canary 解锁流程。
