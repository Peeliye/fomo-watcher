# Fomo Watcher 风控文档入口

当前文档版本：v1（只读阶段已实现）  
当前执行状态：Shadow + 只读风险决策，真实交易关闭

## 已实现组件

- `wallet-registry.json`：钱包身份登记表；按 `kolId + chainId` 解析，无法唯一匹配时不推断归属。
- `fomo/risk/engine.py`：统一信号模型与失败即关闭的只读风险决策器（根目录保留兼容入口）。
- `risk_pipeline.py`：把 Fomo 买入、卖出和清仓事件转为统一信号，并把结果追加到 `data/risk-decisions.ndjson`。
- `/api/risk-decisions`：本机 Dashboard 的只读聚合 API。
- Dashboard“钱包风控”：展示身份缺口、统一信号、阻断原因和决策耗时。
- `wallet_registry_cli.py`：校验、原子写入、自动备份和撤销公开钱包映射；不接受私钥或签名凭据。
- `fomo/portfolio/ledger.py`：SQLite WAL 持久化模拟成交、持仓、精确盈亏和日统计（根目录保留兼容入口）。
- `execution-wallet.json` / `execution_readiness.py`：一个逻辑钱包管理 EVM 与 Solana 公开执行地址，并显示到 RPC、签名器的逐级就绪状态。
- Dashboard“持仓 / PnL”：展示首次购入时间、盈亏差额、行情陈旧状态、日统计和执行准备缺口。
- `execution_journal.py`：持久化交易意图、状态转换与全局执行锁；数据库约束保持 `live_armed=0`。

未登记或证据不足的用户会显示 `needs_identity / wallet_not_registered`。这表示接线正常但身份尚未达到交易风控要求，不是程序故障。喊单事件只继续通知，不进入交易型风险决策。

## 钱包登记操作

先在 Dashboard 的“钱包风控”复制待登记对象的 KOL ID，再录入经过核实的公开地址：

```powershell
.\.venv\Scripts\python.exe wallet_registry_cli.py add `
  --kol-id "FOMO_USER_ID" --handle "HANDLE" `
  --chain-id 1 --chain-id 8453 `
  --address "0xPUBLIC_ADDRESS" --confidence 0.95 `
  --evidence-type "signed-message" --evidence-ref "proof reference"
```

查看与撤销：

```powershell
.\.venv\Scripts\python.exe wallet_registry_cli.py list
.\.venv\Scripts\python.exe wallet_registry_cli.py revoke --kol-id "FOMO_USER_ID" --chain-id 1 --address "0xPUBLIC_ADDRESS"
```

每次变更都会先校验整个登记表、递增版本，并将变更前文件备份为 `wallet-registry.json.bak`。这里只能保存公开地址和证据引用，禁止写入私钥、助记词、RPC 密钥或会话 token。
watcher 会检测登记表和策略文件的修改时间并热加载；有效变更不需要重启。无效 JSON 或不合法地址不会替换内存中上一份已验证版本。

公开检索得到但尚不能排除“Fomo 托管/生成地址”的结果保存在 `wallet-candidates.json`。该文件不会被风控流水线读取，不能触发 Shadow 判定或交易。只有用户确认、签名消息或其他强证据完成复核后，才可通过 CLI 晋升到 `wallet-registry.json`。

## 建议阅读顺序

1. [RISK_CONTROL_GUIDE.md](RISK_CONTROL_GUIDE.md)  
   面向产品和操作人员，用通俗语言解释每道风控为什么存在、通过或拒绝后会发生什么。

2. [RISK_CONTROL_DESIGN.md](RISK_CONTROL_DESIGN.md)  
   面向开发实现，定义统一信号、风控流水线、链级检查、状态机、熔断器和上线门槛。

3. [RISK_POLICY_REFERENCE.md](RISK_POLICY_REFERENCE.md)  
   逐项解释策略 JSON 中每个字段的含义和默认值。

4. [risk-policy.example.json](risk-policy.example.json)  
   机器可读的保守默认策略；它不能开启真实交易。

5. [RISK_CONTROL_TEST_PLAN.md](RISK_CONTROL_TEST_PLAN.md)  
   规定单元测试、历史回放、实时 Shadow、Paper、故障注入和 Canary Live 的验收标准。

6. [EXECUTION_AND_PNL_ARCHITECTURE.md](EXECUTION_AND_PNL_ARCHITECTURE.md)  
   解释一个逻辑多链钱包、RPC 门槛、PnL 口径、SQLite 持久化与后续执行闸门。

## 已确定原则

- 不复制目标钱包原始 calldata 或 Solana instructions。
- 低可信度钱包只能观察，不能触发真实交易。
- 两个独立报价、自己的交易模拟和可用退出路径是买入前置条件。
- 已提交但未确认的交易计入额度。
- 熔断必须落盘且只能人工解除。
- 新环境、进程重启和策略热更新都不能自动开启实盘。
- 先完成 Shadow/Paper 数据验证，再进行小额 Canary。

## 下一步开发前需要确认

以下选择会影响具体实现，目前文档采用保守建议值：

| 决策 | 当前建议 |
|---|---|
| 第一条实盘链 | 暂不决定；先同时采集 EVM 与 Solana shadow 数据 |
| EVM 支持链 | Ethereum、Base、BNB Chain 分链验证 |
| Solana 数据等级 | 先 processed，之后对比 preprocessed |
| 钱包最低实盘可信度 | 0.90；canary 用户建议 1.00 |
| 单笔金额 | 10 USD |
| 单日总投入 | 100 USD |
| 最大价格冲击 | 1% |
| 最大滑点 | 1.5% |
| 止损 | 8% |
| 最大持仓时间 | 30 分钟 |
| 报价源数量 | 至少两个独立来源 |
| 熔断恢复 | 必须人工确认 |

## 推荐的下一项实现

逐个录入有充分证据的钱包，并接入链上资产安全快照、两个独立报价、敞口、RPC 健康、退出计划和交易模拟。上述数据全部齐备前，决策最多只能到 `needs_data`；即使全部通过也只会返回 `approved_for_shadow`。
