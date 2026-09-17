# Fomo Watcher 真实交易风控设计（草案 v1）

## 目标与边界

这套风控用于把 Fomo、EVM pending transaction、Solana processed/preprocessed
transaction 统一为同一种候选信号，再决定拒绝、进入报价、影子成交或真实交易。

默认状态必须满足以下不变量：

- `mode` 默认为 `shadow`，任何新环境都不能自动进入真实交易。
- 真实交易必须同时满足配置允许、启动时显式解锁、签名钱包在允许名单中。
- 只提取目标钱包“买了什么”，绝不复制和执行目标钱包的原始 calldata/instructions。
- 仅通过允许名单内的链、DEX Router/Program、报价器和发送器构建自己的交易。
- 没有完整退出计划、可靠报价或模拟结果时，买入必须失败关闭（fail closed）。

## 统一信号模型

```json
{
  "signalId": "source:chain:tx-or-event-id",
  "source": "fomo|evm_pending|solana_preprocessed|solana_processed",
  "observedAt": "ISO-8601",
  "sourceTimestamp": "ISO-8601|null",
  "chainId": "1|56|8453|1399811149",
  "kolId": "stable-internal-id",
  "wallet": "0x...|base58",
  "walletConfidence": 0.0,
  "originalTx": "hash-or-signature|null",
  "side": "buy|sell|unknown",
  "tokenIn": "address",
  "tokenOut": "address",
  "estimatedUsd": 0,
  "decoder": "name@version",
  "rawPayloadHash": "sha256"
}
```

`signalId`、原交易标识、钱包、token、side 共同参与幂等去重。原始消息只保存哈希及必要审计字段，不能把私钥、Privy token 或 RPC key 写入日志。

## 风控流水线

每一步只允许产生 `pass`、`reject` 或 `defer`，任何异常等同 `reject`。

1. **来源验证**：来源允许、时间戳单调、系统时钟健康、消息可解析。
2. **身份验证**：目标钱包必须属于关注 KOL，映射未过期且达到最低可信度。
3. **事件验证**：只处理明确的买入/卖出；未知 Router、Program 或 selector 拒绝。
4. **资产安全**：验证 token、mint、池子、权限、集中度和可卖出性。
5. **市场质量**：检查流动性、报价差异、价格冲击、滑点和价格新鲜度。
6. **敞口控制**：计算单笔、单币、单 KOL、单链和全局额度。
7. **执行健康**：检查 RPC 高度、nonce/blockhash、gas reserve、发送器和失败率。
8. **退出保障**：买入前必须登记止损、最大持仓时间及紧急退出路径。
9. **交易构建**：由本地允许名单模板重新构建；模拟通过后才可签名。
10. **发送与核对**：记录提交、包含、成交和最终确认；状态不确定时禁止重复买入。

## 钱包身份可信度

每个 `kolId -> wallet` 映射保存来源、证据、首次/最后验证时间和有效期。

建议评分：

- 钱包签名证明或 KOL 官方渠道明确公布：`1.00`
- 多个独立公开来源一致，且交易行为与 KOL 发言长期吻合：最高 `0.90`
- 单一第三方页面、群聊转述或一次资金关联：最高 `0.60`
- 聚类、共同入金、相似交易时间等推断：最高 `0.40`

真实交易最低值建议 `0.90`；低于阈值只进入影子观察。地址 30 天未重新验证自动降级，不允许仅凭一次资金转账把收款地址认定为 KOL 钱包。

## 链级检查

### EVM

- pending 交易的 `from` 必须与关注钱包完全匹配。
- 仅允许已审核 Router、Universal Router command、聚合器 target 和函数 selector。
- 独立解析最终 `tokenOut`，禁止 delegatecall、任意 target、多调用中未知子调用。
- 使用最新状态执行自己的 `eth_call`/simulation，而不是假设 KOL 交易会成功。
- 检查买卖税、黑名单/暂停/增发权限、代理合约实现变化及卖出路径。
- nonce 由单独管理器串行分配；出现 nonce gap、replacement 或 RPC 分歧立即熔断。

### Solana

- watched account 必须出现在签名者/账户键中，并验证它是实际资金所有者。
- 仅允许审核过的 AMM/聚合器 Program ID；拒绝未知 CPI 和异常 Address Lookup Table。
- 检查 mint/freeze authority、Token-2022 transfer hook/fee、池子流动性与账户所有者。
- preprocessed 信号没有执行结果，只能当作高风险候选；必须独立报价和构建。
- recent blockhash、ATA 和路由尽量预取，但发送前仍需做过期与余额检查。

## 默认额度与熔断

配套的 `risk-policy.example.json` 是保守默认值。关键原则：

- 初始真实单笔不超过 10 USD。
- 单 token 累计敞口不超过 20 USD。
- 单 KOL 每日不超过 50 USD；全局每日不超过 100 USD。
- 同时持仓不超过 5 个，单链敞口不超过总额度的 50%。
- 报价价格冲击不超过 1%，允许滑点不超过 1.5%。
- 两个报价源偏差超过 1% 或报价超过 750ms，拒绝交易。
- 连续 3 次执行失败、10 分钟失败率超过 20%、日内已实现亏损达到 20 USD，自动熔断。
- 系统时钟偏差超过 100ms、RPC 落后、WebSocket 断开、白名单过期，自动熔断。

熔断只能人工重新解锁，进程重启不能自动清除。

## 退出策略

第一版真实买入必须同时具备：

- 硬止损：相对实际成交价 `-8%`。
- 最大持仓时间：默认 30 分钟；到期进入减仓流程。
- KOL 明确卖出：按持仓比例跟随，但仍使用自己的允许名单路由。
- 流动性骤降、卖出模拟失败或 token 权限发生变化：停止加仓并告警。
- 无法自动卖出时立即触发人工告警，禁止继续买入该 token。

止盈不应在没有回测前写死；先记录 `+10%/+20%/+50%` 路径的影子结果，再选择分批退出参数。

## 状态机

```text
observed
  -> identity_checked
  -> decoded
  -> asset_checked
  -> quoted
  -> risk_approved
  -> shadow_executed
  -> live_armed
  -> signed
  -> submitted
  -> included
  -> confirmed
  -> exit_pending
  -> closed
```

任意状态可进入 `rejected`；从 `signed` 开始必须记录不可变审计日志。`submitted` 后结果未知时进入 `reconciliation_required`，核对链上状态之前不得重发。

## 上线门槛

在满足以下条件前保持 `shadow`：

- 连续运行至少 7 天，覆盖不少于 500 个候选信号。
- 钱包监听相对 Fomo 的领先时间 P50/P95 已量化。
- 每条链分别统计模拟成交率、滑点、费用、失败和重组/分叉影响。
- 影子策略扣除 gas、priority fee、Jito/发送器 tip 后仍为正收益。
- 买入、跟随卖出、止损、超时退出和紧急熔断均通过故障注入测试。
- 私钥不进入应用日志、前端、`.env` 明文备份或远程监控系统。

首次实盘采用 canary：单链、单钱包、单 KOL、10 USD 上限，至少观察 50 笔后才允许扩大范围。

