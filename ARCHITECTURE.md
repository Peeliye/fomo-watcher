# 项目架构

## 目录职责

```text
fomo-watcher/
├─ fomo/                       # 可复用业务包
│  ├─ app.py                   # 监控应用主体
│  ├─ risk/                    # 钱包身份解析、统一信号和只读风险决策
│  ├─ intelligence/            # Fomo 行为样本、证据充分度与地址画像（不伪造 PnL）
│  ├─ execution/               # 执行意图、就绪检查、路由与 RPC 节点池
│  ├─ portfolio/               # SQLite WAL 持仓、成交、PnL 与日统计
│  └─ web/                     # 本地 HTTP API 与 static/index.html
├─ sidecar/                    # 无窗口 Privy 和 Fomo WebSocket 边车
├─ scripts/                    # 回填、维护、RPC 基准和钱包登记命令
├─ tests/                      # 单元与回归测试
├─ data/                       # 运行状态、审计流水和 SQLite 数据库（不入库）
├─ 旧文件/                     # 可恢复的历史兼容层、旧文档和旧样例
└─ run.py                      # 唯一监控启动入口
```

## 依赖方向

`risk -> execution/portfolio -> web -> app -> run.py`。业务包不会反向依赖根入口，
Privy/WebSocket 边车也不持有交易签名职责。交易秘密只允许由未来独立 signer
读取；RPC URL 只存在 `.env`，健康数据库和 API 仅保存供应商代号、延迟、成功率及高度差。
助记词可由 `scripts.wallet_vault` 写入当前操作系统用户的凭据管理器，项目文件和日志不得保存或显示它；
存入凭据不代表 signer 已启用。

## 持久化边界

- `state.sqlite3`：监控去重状态。
- `portfolio.sqlite3`：持仓、成交、估值和日统计。
- `execution.sqlite3`：执行意图状态机与只读锁。
- `rpc-health.sqlite3`：RPC 样本与自动选择依据，不保存 URL。
- `wallet-intelligence.sqlite3`：去重后的观察事件和行为画像输入，不保存密钥。
- `verified-performance.sqlite3`：交易哈希证明的标准化成交、历史行情、社交身份及真实钱包业绩。
- `leaderboard.sqlite3`：每小时 24H 排行快照和北京时间自然日合并名单。
- `*.ndjson`：便于人工审计和回放的追加日志。

不同数据库避免高频监控、行情写入和面板查询互相锁住。SQLite 均使用 WAL；上线服务器后，
再根据实测写入量决定是否将遥测迁移到时序数据库，无需先引入额外运维复杂度。

## 聪明钱画像边界

画像层位于事件采集和风险决策之间。第一阶段只根据 Fomo 可证明的买卖金额、入场市值、
频率、代币分散度和活跃天数描述行为，并始终标记 `pnl_unverified`。只有未来链上索引器
完成来源钱包的买卖闭环、历史价格和资金流关联后，才允许生成胜率、已实现 PnL、回撤和
“早期 Alpha”评分。画像默认 `observe_only`，不能直接授权真实交易。

链上业绩层与 Feed 行为层物理隔离。只有具备交易哈希、成交数量、美元估值且来源置信度不低于
0.8 的记录才能进入 `verified-performance.sqlite3`。系统按 FIFO 重建每个代币仓位，只有至少三个
已闭环代币才将钱包标记为 `performanceVerified`；否则胜率保持为空。峰值倍率、早期入场、峰值后
暴跌和归零反弹均来自时间有序的历史市值观测，不从当前市值反推。

## RPC 上线前流程

1. 在 `.env` 为目标链填写主、备 HTTP/WSS 地址，不要写进 YAML。
2. 运行 `python -m scripts.rpc_benchmark --samples 10`，样本写入 `data/rpc-health.sqlite3`。
3. Web 面板“持仓 / PnL”中的 RPC 节点池会显示配置数和健康数。
4. 只有每条启用链至少一个 RPC 就绪后，执行阶段才会越过 `rpc`；签名与广播仍被独立执行锁阻止。
