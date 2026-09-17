# risk-policy.example.json 字段参考

本文解释 `risk-policy.example.json` 中每个配置块的含义。示例文件不是实盘授权文件，加载后仍应保持 shadow 模式。

## 顶层字段

| 字段 | 默认值 | 说明 |
|---|---:|---|
| `version` | 1 | 策略结构版本；修改字段语义必须升级版本 |
| `mode` | shadow | 当前执行模式，允许值应限制为 shadow、paper、live |

## liveTrading

| 字段 | 默认值 | 触发效果 |
|---|---:|---|
| `enabled` | false | false 时任何路径都不能调用签名器 |
| `requireStartupArmFlag` | true | 要求本次进程显式解锁，防止重启后自动实盘 |
| `requireSignerAllowlist` | true | 签名钱包不在允许名单时拒绝启动 live |
| `requireExitPlan` | true | 没有退出执行器和退出参数时禁止买入 |

## identity

| 字段 | 默认值 | 说明 |
|---|---:|---|
| `minimumWalletConfidenceForShadow` | 0.6 | 进入钱包观察列表的最低可信度 |
| `minimumWalletConfidenceForLive` | 0.9 | 能触发真实交易的最低可信度 |
| `mappingMaxAgeDays` | 30 | 钱包归属超过该时间未复核则过期 |
| `allowInferredWalletsForLive` | false | 禁止仅靠行为聚类推断的钱包触发实盘 |

## signals

### allowedSources

- `fomo`：Fomo 的 WebSocket/API 信号。
- `evm_pending`：EVM 公开 pending transaction。
- `solana_preprocessed`：从 Shred 解码但尚无执行结果的交易。
- `solana_processed`：已被节点处理的 Solana 交易。

### maximumAgeMs

分别限制每类信号从来源时间到本机决策时间的最大年龄。超过阈值时记录为 stale，不进入报价。

| 来源 | 默认值 |
|---|---:|
| Fomo | 2000ms |
| EVM pending | 750ms |
| Solana preprocessed | 250ms |
| Solana processed | 800ms |

### 其他字段

| 字段 | 默认值 | 说明 |
|---|---:|---|
| `dedupeWindowSeconds` | 900 | 相同交易意图的去重窗口 |
| `rejectUnknownSide` | true | 无法确定买卖方向时拒绝 |
| `rejectUnknownDecoder` | true | 没有已审核解析器时拒绝 |

## execution

| 字段 | 默认值 | 说明 |
|---|---:|---|
| `paperBuyUsd` | 10 | 模拟买入金额 |
| `maximumLiveBuyUsd` | 10 | 初始实盘单笔硬上限 |
| `maximumPriceImpactBps` | 100 | 最大价格冲击，100 bps = 1% |
| `maximumSlippageBps` | 150 | 最大滑点容忍，150 bps = 1.5% |
| `maximumQuoteAgeMs` | 750 | 报价超过该年龄视为无效 |
| `maximumQuoteDivergenceBps` | 100 | 两个报价源允许的最大偏差 |
| `minimumIndependentQuotes` | 2 | 最少独立报价源数量 |
| `requireSimulation` | true | 签名前必须模拟自己的交易 |
| `copyOriginalCalldata` | false | 明确禁止复制目标钱包 calldata |
| `allowSkipPreflight` | false | 初期不允许跳过预执行检查 |
| `parallelBroadcastProviders` | 2 | 允许双提供商广播，但必须共享幂等状态 |

双广播不是发送两笔不同交易。EVM 应使用同一个签名交易哈希；Solana 应发送同一个签名交易，防止重复成交。

## asset

| 字段 | 默认值 | 说明 |
|---|---:|---|
| `minimumLiquidityUsd` | 100000 | 目标交易池最低有效流动性 |
| `minimumMarketCapUsd` | 100000 | 最低市值辅助阈值，不能代替流动性检查 |
| `maximumBuyTaxBps` | 500 | 最大买入税 5% |
| `maximumSellTaxBps` | 500 | 最大卖出税 5% |
| `maximumTopHolderPercent` | 20 | 单一最大持有人占比阈值 |
| `requireSellSimulation` | true | 买入前必须验证存在可用卖出路径 |
| `rejectMutableOrUnknownPrivileges` | true | 权限不明或危险可变权限存在时拒绝 |
| `rejectEvmDelegateCall` | true | 候选路径含未知 delegatecall 时拒绝 |
| `rejectUnknownSolanaCpi` | true | Solana 出现未知 CPI 时拒绝 |
| `rejectToken2022TransferHooksByDefault` | true | 未审核 Token-2022 hook 默认拒绝 |

## exposure

| 字段 | 默认值 | 说明 |
|---|---:|---|
| `maximumPerTokenUsd` | 20 | 同一 token 已成交加待确认总敞口 |
| `maximumPerKolDailyUsd` | 50 | 单个 KOL 每日累计投入 |
| `maximumGlobalDailyUsd` | 100 | 全系统每日累计投入 |
| `maximumOpenPositions` | 5 | 最大未关闭仓位数 |
| `maximumChainExposurePercent` | 50 | 单链不能占用超过一半总额度 |
| `minimumNativeGasReserveUsd` | 25 | 保留原生币用于 gas/priority fee/退出 |

## exit

| 字段 | 默认值 | 说明 |
|---|---:|---|
| `hardStopLossPercent` | 8 | 相对实际成交价的硬止损触发点 |
| `maximumHoldingMinutes` | 30 | 到期后必须进入退出流程 |
| `mirrorKolSells` | true | 检测到 KOL 卖出时按本地规则减仓 |
| `stopAddingWhenExitUnavailable` | true | 卖出路径异常时禁止继续加仓 |
| `takeProfit.enabled` | false | 没有回测前不启用自动止盈 |
| `takeProfit.shadowLevelsPercent` | 10/20/50 | 只记录这些涨幅处的假设退出结果 |

## circuitBreakers

| 字段 | 默认值 | 触发条件 |
|---|---:|---|
| `maximumClockOffsetMs` | 100 | 系统时钟不可信 |
| `maximumConsecutiveExecutionFailures` | 3 | 连续执行故障 |
| `failureRateWindowMinutes` | 10 | 失败率统计窗口 |
| `maximumFailureRatePercent` | 20 | 窗口内失败率上限 |
| `maximumDailyRealizedLossUsd` | 20 | 日内实际亏损硬上限 |
| `maximumEvmRpcBlockLag` | 1 | EVM RPC 落后主链高度 |
| `maximumSolanaRpcSlotLag` | 2 | Solana RPC 落后 slot |
| `tripOnWhitelistExpiry` | true | 关注/钱包白名单过期即熔断 |
| `tripOnPrimaryStreamDisconnect` | true | 主信号流断开即停止新买入 |
| `manualResetRequired` | true | 熔断后必须人工确认恢复 |

## audit

| 字段 | 默认值 | 说明 |
|---|---:|---|
| `appendOnly` | true | 审计事件只追加，不能就地修改历史 |
| `redactSecrets` | true | 清除私钥、token、RPC key 等敏感内容 |
| `hashRawPayload` | true | 用哈希证明原始载荷一致性 |
| `recordRejectedSignals` | true | 被拒绝的信号同样记录原因 |
| `recordQuoteAndSimulation` | true | 保存报价来源和模拟摘要 |
| `recordTransactionLifecycle` | true | 保存签名、提交、包含、确认和退出状态 |

## 配置变更规则

- 实盘运行时降低风控阈值必须经过人工审核。
- 每次配置加载都记录版本、文件哈希和加载时间。
- 无法识别的新字段应阻止启动，避免拼写错误被静默忽略。
- 金额字段统一使用十进制定点数或最小单位整数，不能用二进制浮点数记账。
- 策略热更新不能自动把 shadow 切换为 live。

