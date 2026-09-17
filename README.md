# Fomo Watcher

一个针对 fomo.family 关注用户的实时监控 bot。它会建立本地基线，随后在检测到买入、减仓、清仓或新喊单时，把统一格式消息推送到 Telegram、飞书、企业微信、QQ OneBot 或自建 webhook。

## 已实现

- 多目标用户（`userHandle`）监控，首次启动只建立基线，避免把历史记录全部当成新消息。
- 直连 Fomo `trading_activity` WebSocket；收到事件后约 1 秒内消费，无浏览器窗口。
- 每 5 分钟同步当前账号的关注 ID 白名单；推荐或热门陌生用户事件不会推送。
- 仓位数量差分：买入、减仓、清仓；消息含本地时间、代币、估算美元金额、当前市值、数量变化及 CA。
- 喊单监控：中文原样发送；英文通过可配置的翻译接口翻译，并同时保留原文。
- CA 为消息中的独立纯文本，长按即可复制；Fomo、GMGN、DEBOT 链接模板可自行调整。
- SQLite 去重，且仅在通知平台发送成功后才落去重记录。
- 无窗口 Chromium 运行 Fomo 自带的 Privy SDK，独占完成一次性 refresh token 的轮换；Python 不直接刷新凭据。
- WebSocket 断线或 90 秒无活动会自动重连；REST Feed 每 30 秒兜底补漏。
- 随监控进程长期运行的本地 Web 面板，实时查看模拟买入、过滤原因、链分布和服务状态。
- RPC 健康度采用当前配置与五分钟新鲜度双重校验；未配置或遥测过期的历史节点不会计为可达。
- 实盘采用 fail-closed 门禁：每条链必须具备 RPC、至少两条已部署报价/模拟路由，并同时配置 signer、广播总开关与显式 live 模式。
- 执行回执账本支持交易哈希幂等、链一致性校验、成功/失败状态迁移及未对账统计；面板接口为 `/api/execution-reconciliation`。
- 同一平台 handle 的历史合成 ID 会在唯一真实 KOL ID 到达后自动归并，避免画像重复；存在多个真实 ID 时保持隔离。
- 钱包注册仅在证据字段完整、置信度不低于 0.8、且未过期/撤销时进入可信映射。
- 链上闭环业绩包含胜率 95% 置信区间、总 ROI、最大回撤与数据新鲜度；过期历史不会被当作当前可执行策略。
- 面板明确区分 Paper、Shadow、Live，并对服务状态、RPC 交易级健康和持仓行情过期进行醒目告警。
- 常驻进程每 3 分钟自动探测已配置 RPC，并每 60 秒用 DexScreener 公共行情刷新可匹配的模拟持仓；不再依赖手工测速或新交易信号更新估值。

### 实盘门禁说明

默认配置始终保持 Shadow/只读。仅填写环境变量不会自动进入实盘；适配器部署完成后还需显式启用对应 `ROUTE_*_ENABLED`，配置隔离 signer，将钱包与路由模式切换为 `live`，并最后开启 `LIVE_BROADCAST_ENABLED`。任何一项缺失都会在 `/api/execution-readiness` 中形成阻断项。

> 金额由“仓位数量变化 × 当前价格”估算。行情和市值来自 Fomo 返回数据，可能延迟。

## 安装

需要 Python 3.11+。

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
Copy-Item config.example.yaml config.yaml
```

编辑 `config.yaml`。`account_feed: true` 会监控当前账号所关注用户的个性化 Feed；`targets` 可额外指定需要做完整仓位差分的用户。程序会自动读取同目录下的 `.env`，该文件已被 `.gitignore` 排除。

实时模式需要三个无窗口常驻进程：Privy 会话保活、WebSocket 接收和通知消费：

```powershell
cd sidecar
npm run start:privy
npm start
cd ..
python run.py --config config.yaml
```

`start:privy` 使用独立持久化浏览器配置 `data/fomo-browser-profile`，不会显示 Chrome 窗口。首次种入一组有效 token 后，后续轮换由页面内 Privy SDK 完成，结果同步到 `data/.fomo-session.env`。

## 模拟跟单

`config.yaml` 的 `copy_trading.mode: paper` 只记录决策，不读取钱包私钥，也不会签名或广播交易。当前启用 Ethereum（1）、BNB Chain（56）、Robinhood Chain（4663）、ARC（5042）、Base（8453）和 Solana（1399811149）；只接受关注用户的新鲜买入信号：目标成交至少 $100、信号不超过 5 秒、市值至少 $100K；模拟固定买入 $10，单币上限 $20、每日上限 $100、最大滑点参数 2%。

接受与拒绝的模拟决策都会写入 `data/paper-orders.ndjson`；接受的决策还会在原飞书卡片中显示 `🧪 模拟买入`。成交、持仓、首次购入时间、已实现/未实现 PnL 和日统计持久化到 WAL 模式的 `data/portfolio.sqlite3`。在没有完成链上报价、流动性检查、交易模拟和独立热钱包配置前，不应把模式切换成真实交易。

监控启动后，在浏览器打开 `http://127.0.0.1:8765` 即可查看长期可视化面板。“持仓 / PnL”包含购入时间、盈亏差额、日统计、行情陈旧提示，以及 EVM + Solana 逻辑执行钱包到 RPC 阶段的配置状态。“钱包管理”用于维护 KOL 公开地址、证据与有效期，并维护独立观察钱包的标签、通知事件和金额/市值过滤条件；支持 CSV/JSON 批量导入。自定义观察规则保存在 `watch-wallets.json`，变更审计保存在 `data/wallet-management-audit.ndjson`。该页面拒绝私钥、助记词、种子词和 Keystore 字段。页面每 3 秒读取最新记录；默认仅监听本机，不会向局域网或公网暴露数据。详细结构见 [ARCHITECTURE.md](ARCHITECTURE.md)。

RPC 使用每链主备节点池，URL 和密钥只放在 `.env`，面板与 `data/rpc-health.sqlite3` 不保存秘密。填写 `.env.example` 中目标链变量后运行：

```powershell
.\.venv\Scripts\python.exe -m scripts.rpc_benchmark --samples 10
```

### 本机保存钱包助记词

真实资金钱包的助记词不要写入 `.env`、YAML 或 JSON。项目提供一个只负责保存秘密的命令，
它通过 Python `keyring` 写入当前 Windows 用户的凭据管理器；导入过程使用隐藏输入，且不会显示或导出助记词：

```powershell
.\.venv\Scripts\python.exe -m scripts.wallet_vault --name live-wallet init
.\.venv\Scripts\python.exe -m scripts.wallet_vault --name live-wallet verify
.\.venv\Scripts\python.exe -m scripts.wallet_vault --name live-wallet derive --apply-profile
```

如果钱包创建时额外设置过 BIP-39 passphrase，导入时增加 `--with-bip39-passphrase`。不要把钱包应用的
解锁密码误当作 BIP-39 passphrase。该命令只保存和验证凭据，**不会启用签名、实盘或交易广播**；
当前 `execution-wallet.json` 仍保持 `signer.backend: disabled`。
执行 `derive --apply-profile` 后会把标准账户 0 的公开地址写入钱包配置，面板随后通过已配置的 RPC
显示各链原生币余额。请先对照原钱包确认 EVM 与 Solana 地址；代币余额暂不计入这个只读概览。

命令会并行比较不同供应商，记录成功率、P95 延迟和区块/slot 高度差；正式结构与依赖边界见 [ARCHITECTURE.md](ARCHITECTURE.md)。

账本完整性检查与在线备份：

```powershell
.\.venv\Scripts\python.exe -m scripts.portfolio_maintenance check
.\.venv\Scripts\python.exe -m scripts.portfolio_maintenance backup
```

模拟下单额度现在同时由持久化账本检查：单币、单 KOL 每日、全局每日和最大未平仓数不会因 watcher 重启而清零。每天按 Asia/Shanghai 自动生成一次 SQLite 在线备份。

每个交易型风险结果还会写入 `data/execution.sqlite3`，形成幂等执行意图与状态转换审计。当前数据库级执行锁固定为只读、熔断状态为 `startup_read_only`，不存在签名或广播路径。

## 链上真实业绩回填

Fomo Feed 行为与链上真实业绩使用两个独立数据库。标准化索引器导出的成交必须包含交易哈希、
KOL/钱包、链、代币、买卖方向、成交数量、美元金额、时间和来源置信度；历史行情包含同链代币的
时间、价格和市值。导入命令为：

```powershell
.\.venv\Scripts\python.exe -m scripts.performance_backfill fills data/normalized-fills.ndjson
.\.venv\Scripts\python.exe -m scripts.performance_backfill market data/token-market-history.ndjson
.\.venv\Scripts\python.exe -m scripts.performance_backfill social data/social-identities.json
```

结果写入 `data/verified-performance.sqlite3`，并通过 `/api/wallet-performance` 提供 FIFO 已实现/未实现
PnL、闭环胜率、入场到峰值倍率、早期入场、峰值后暴跌、归零反弹及验证型画像。没有至少三个
闭环代币时不会输出“已验证聪明钱”。分型策略目前仍只输出只读建议，不会签名或广播。

WebSocket 收包后还会在 Node 进程内立即执行一遍只读“影子风控”，绕过 Python 轮询，并把信号年龄和决策耗时写入 `data/shadow-executions.ndjson`。该阶段只判断能否进入报价流程，不读取钱包、不签名、不请求真实成交。

```powershell
python run.py --save-creds
```

也可以通过环境变量临时注入：

```powershell
$env:FOMO_ACCESS_TOKEN='你的 access token'
$env:FOMO_REFRESH_TOKEN='你的 refresh token'
python run.py --config config.yaml --once
python run.py --config config.yaml
```

需要清除已保存的凭据时：

```powershell
python run.py --delete-creds
```

第一次 `--once` 应无消息，只写入 `data/state.sqlite3`。第二次开始只推送新变化。

## 推送平台

在 `config.yaml` 的 `notifications` 中将对应项设为 `true`：

- Telegram：`TG_BOT_TOKEN`、`TG_CHAT_ID`
- 飞书群机器人：`FEISHU_WEBHOOK_URL`
- 企业微信群机器人：`WECHAT_WORK_WEBHOOK_URL`
- QQ：OneBot v11 的 `ONEBOT_API_BASE`、`ONEBOT_TARGET_TYPE=group|private`、`ONEBOT_TARGET_ID`，可选 `ONEBOT_ACCESS_TOKEN`
- 个人微信：官方没有等价的群机器人 webhook。建议把 `generic_webhook` 指向你已有、合规的微信桥接服务；本项目不内置绕过个人微信风控的协议机器人。

所有平台均关闭时，消息打印到控制台，便于测试。

## 英文翻译

设置一个兼容 Chat Completions 的翻译服务：

```text
TRANSLATION_API_URL=https://你的服务/v1/chat/completions
TRANSLATION_API_KEY=...
TRANSLATION_MODEL=...
```

如果未配置翻译服务，英文喊单仍会保留原文，但不会生成中文译文；正式运行前应配置该服务以满足双语要求。

## 链接模板

第三方交易页 URL 可能调整，因此在 `config.yaml` 中配置。支持变量：`{ca}`、`{chain}`、`{network_id}`、`{trade_id}`。建议用一个真实 CA 点开验证后再长期运行。链接只是跳转，不会自动下单。

## 自检

```powershell
python -m unittest -v
```

真实连通性测试需要你自己的 Fomo 凭据。不要把 token 发进聊天、代码仓库或群消息中；只在本机环境变量中设置。
