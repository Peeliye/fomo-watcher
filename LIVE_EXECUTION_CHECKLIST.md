# 真实交易能力待办清单

本文件是后续审计修复的进度源。`[x]` 仅表示对应范围已有实现并通过本地测试，**不代表实盘就绪**；`[ ]` 表示尚未满足下方验收条件。每完成一项，需在同一提交中更新勾选状态、写明测试证据和仍存限制。不得仅凭接口、配置开关、假 signer 或 mock 测试勾选。

始终保持 `live_armed=false`；不得使用真实资金、广播真实交易、写入真实私钥或读取、修改、输出 `.env` 秘密。任何现有 SQLite schema/data 迁移前，先用 SQLite backup API 备份并验证完整性，保留回滚说明。

## 路线变更草案（待用户确认；此阶段只改清单，不改代码）

先做 **EVM Uniswap V2 → V3 → V4 AMM 直连**，每个协议分别通过 RPC 找池、读池、按本协议公式计算 quote-token 计价的 spot/amountOut/amountIn、本地构建并模拟，逐项独立验收；V3/V4 不能套用 V2 恒定乘积公式。**Solana mainnet / Raydium AMM v4 最后做**，同样只用经链身份校验的 RPC 读 pool/vault 状态并本地构造交易。第 0 层不依赖预言机、HTTP 聚合报价、多跳搜索或 WS/gRPC 行情订阅，不把 quote-token 价格写成 USD。Raydium CPMM、Pump.fun 等未被选中，均不在本轮范围。

这只变更交易路线，不删除现有 Fomo/Wallet、持久化队列、账本、风控或审计能力；已有 0x/Jupiter/Pyth 代码暂作为历史组件隔离，不能因为存在就算新直连路线完成，也不能让其进入新路线的运行装配。原 prompt 中“全仓库删除既有依赖/数据库/安全检查、`.env` 私钥签名、直接主网广播”的做法不适用于本仓库。模拟可使用隔离测试凭据；生产签名仍只允许系统凭据库，真实密钥不写入文件。一个 RPC 可用于第 0 层功能验证，但**不满足** P10 的生产容错验收；热路径能缓存但不得绕过广播前 fail-closed 门禁。

实施顺序：先 P01-V2 → P01-V3 → P01-V4，并逐版本完成适用的 P04/P05/P08/P10 安全门禁；**Solana 放在 EVM 路线之后**，再做 P03（Raydium 池发现、储备与公式）→ P02（本地指令、最终字节解析与模拟）及相同的广播前门禁。P06–P12 的跨来源、账本、容错与最终验收仍保留，不因路线顺序而豁免。任何链的只读和模拟验收都不授权实盘。以下 `[ ]` 均为待做，不能凭旧路线测试勾选。

## 已完成的基础能力（不等于端到端实盘能力）

- [x] B01 — Fomo 与钱包信号分离，共用标准化信号和执行意图；有跨语言 schema/golden vectors 与默认迟到丢弃规则。
- [x] B02 — 共享 SQLite 持久化信号队列和单一消费服务骨架；默认服务不注册广播链路。
- [x] B03 — EVM/Solana HTTP JSON-RPC 基础 provider、旧 0x/Jupiter 报价接入、模拟/广播/回执及系统凭据库 signer 接口与实现；这些是既有组件，**不是** Raydium AMM v4 / Uniswap V2 直连路线的完成证据。
- [x] B04 — EVM 未提交 nonce 租约安全回收、可能上链 nonce 保留；具备重启、重复请求和回滚回归测试。
- [x] B05 — RPC 端点链身份与链头新鲜度检查、受约束的主备切换和脱敏诊断；只读余额/健康探针统一走该传输层。
- [x] B06 — `readiness` 将 RPC 健康、适配器自检与实盘就绪分开；已知不支持的交易格式和风险证据会阻断就绪，`live_armed` 默认关闭。

## 待完成的实盘阻断项（编号保留；按上方新顺序实施）

- [ ] P01 — **优先实施 EVM Uniswap V2/V3/V4 AMM 直连路线**：每个版本独立发现池、读取状态、计算输出、构建交易、反向解析最终签名字节、模拟和验收；只做已明确选定的单池直连，不做跨版本最优路径搜索。原 0x Settler 结构解析已有局部测试，但不计入此项，不在本轮接入新路线。
  - [ ] P01-V2 — Pair/Router02：链上储备与恒定乘积公式、本地 calldata、独立 allowance 检查与 approve；最终字节核对 router、pair、token、金额、recipient、deadline、minOut 和授权范围，未知 selector fail-closed。
    - [x] 单池 V2 整数 `amountOut`/`amountIn` 与滑点下限纯函数，基于官方 V2 997/1000 公式；只读 Factory `getPair`、token0/token1、同区块 reserves/decimals、allowance 与短 TTL 快照；本地编码双 token `swapExactTokensForTokens` 和**独立** approve calldata，回读校验。见 `fomo/execution/direct_v2.py`、`tests/test_direct_v2.py`。此子项仅表示本地实现和测试，不代表真实 RPC 探针、签名交易构建或模拟通过。
    - [x] 本地 EIP-1559 未签名 swap 字节构建与测试专用签名字节反向解析，限制 canonical Router02、chain 1、双 token path、recipient 与 fee/gas 上限，并解析 nonce、deadline、minOut；篡改目标及错链测试拒绝。只读模拟入口仅调用 `eth_call`/`eth_estimateGas`，测试覆盖成功与 revert；尚未跑真实 RPC 模拟、接生产 signer 或交易 journal，也未与执行意图逐字段比对。
    - [ ] 已提供只读、脱敏的 `scripts.direct_v2_probe` 命令，但尚未使用真实主网 RPC 与公开 Pair 核对 Factory、区块、储备、精度、allowance 和延迟；还需将构建/解析接入执行意图、余额/资金预留、模拟和 journal，覆盖失败注入。P01-V2 总项保持未完成。
  - [ ] P01-V3 — 集中流动性池：按 token pair、fee tier 与 factory 核对池；从 `slot0`、当前流动性、tick/bitmap 和必要的 tick 跨越计算报价，不把全池 TVL 当可用储备；本地构造单池 swap，校验 fee tier、price limit、recipient、amountIn/minOut 及最终字节范围。未知 tick 状态或无法覆盖目标成交量时 fail-closed。
  - [ ] P01-V4 — PoolManager 单例与 pool key：核对 currency 顺序、fee、tick spacing、hooks 地址与池身份；按官方状态/数学与已审核的 hook 行为计算输出并构造单池交易。未知或可改变收费/结算/价格的 hook、动态费率、unlock/callback 路径不能解析时 fail-closed；从最终签名字节和模拟结果核对实际资产结算、minOut、调用者及权限。不得复用 V2/V3 解析器冒充支持。
- [ ] P02 — **Solana Raydium AMM v4 本地交易路线**：使用经核验的 AMM v4 program ID、官方账户布局/指令编码和 P03 选中的池，构造 buy/sell swap 指令；明确账户顺序、vault、用户 token accounts、ATA、WSOL 处理、compute budget、priority fee、recent blockhash 与有效期。先模拟，不广播。最终序列化/签名字节反向解析并验证 program、账户、mint、amountIn、minOut、签名者及授权范围；未知版本、lookup table 或 inner instruction fail-closed。覆盖模拟失败、篡改、过期、部分/失败执行与范围不符测试。
- [ ] P03 — **Solana Raydium AMM v4 链上池价与数量计算**：单一经链身份核验的 RPC 是第 0 层数据源；价格单位为指定 quote mint，不依赖 USD 或预言机。第一版不接行情 WS/gRPC，采用可复用且有 TTL/slot 的 RPC 快照；这不豁免 P10 的后续容错要求。
  - [ ] 用经审核的 AMM v4 program ID、base/quote mint 与官方布局发现主池；允许首次受限 `getProgramAccounts`，验证池状态、vault 所属与 mint、token program、池身份和候选排序依据；缓存 pool 地址到内存/可恢复本地文件，热路径不重复全量扫描。信息不足时先明确池发现规则，不擅自改用聚合器。
  - [ ] 同一 slot/context 的 `getMultipleAccounts` 读取池与两个 vault，解析 raw reserves/decimals 与池费率；证明余额、pool 状态和 RPC 链视图一致，过期、错 mint、错 owner、已关闭池或回滚 fail-closed。`spot=(reserveQuote/10^decQ)/(reserveBase/10^decB)`，明确是 quote/base，不是 USD。
  - [ ] 按 AMM v4 经确认的协议曲线与费用纯函数计算 buy/sell amountOut、amountIn、最小输出；与池储备/费率测试向量对齐，覆盖极小金额、精度、溢出、零流动性和高滑点。只读 `price <MINT>` 在已缓存池的条件下目标 1 秒内返回 spot 与 reserves；记录实测 RPC 延迟，不把 mock 基准称作主网结果。
  - [ ] 对模拟/广播前的 native gas 与支付资产余额读取保留可追溯 slot、钱包、时效和费用证据；quote-token spot 不等同于 USD 敞口估值。若全局敞口仍以 USD 记账，须另行定义可信换算或保持相应 live 门禁关闭，不得将 WSOL/USDC 数值直接当 USD。
  - [ ] 用隔离只读 RPC 和公开账户核对真实池布局、储备、slot/链身份、手续费与故障场景并保存脱敏结果；不能从 `.env` 是否有 URL 推断已验证。旧 Pyth、0x/Jupiter 报价、EVM WETH/USDC `Sync` 缓存以及对应测试属于历史组件，不计入本项完成率，也不要求为新路线配置 Pyth。
- [ ] P04 — **直连路线广播前风控**：沿用现有原子资金/仓位预留、全局/链/策略/Token 敞口、源时效、支付资产余额、gas、池储备与费率新鲜度、price impact、slippage、minOut、池/program allowlist、模拟、熔断和可执行退出路径。热路径避免无关 HTTP/全量扫描，但不得跳过会阻止重复花费或错误交易的门禁。quote 数量与 USD 风险账本须明确换算，未知资产及无可信退出路径的 ARC 一律阻断。
- [ ] P05 — **直连路线生产装配**：`scripts/execution_service.py` 仅在对应链/协议版本的真实 RPC 池发现/状态、正确公式、指令构建、最终字节 scope parser、模拟、系统凭据库 signer、nonce/blockhash、广播、回执和 P04 证据均自检通过时注册并报告 ready。Raydium v4 不走旧 Jupiter/HTTP 报价，后续 Uniswap V2/V3/V4 均不走旧 0x/HTTP 报价；V2 通过不代表 V3/V4 ready，旧适配器不得凭配置开关或假 capability 混入。模拟模式不得触发广播；即使装配完成也不自动解除 live 硬锁。
- [ ] P06 — 钱包实时来源闭环：EVM/Solana 分别按 watchlist 订阅，receipt/log/instruction + 余额差确认 swap；checkpoint 与 outbox 原子提交、消费 ack、断线续传、重复去重及每钱包过滤；不启动历史全量扫描。
- [ ] P07 — 钱包 reorg 全生命周期：回滚时取消未执行队列项；已提交交易进入人工/receipt 判定流程，最终回执和账本支持 reversal；重启后可重放且不会重复广播。
- [ ] P08 — 广播与 replacement 证据链：广播前从最终签名字节计算并持久化链上交易 ID、序列化哈希及 nonce/blockhash；不确定广播只查回执/人工判定；replacement 保留旧、新哈希及各自状态，不覆盖历史。覆盖失败、过期、replacement、reorg 测试。
- [ ] P09 — 最终回执账本：按真实 token/native delta、gas/DEX fee 记录部分成交、部分卖出及失败；维护事实钱包仓位与策略分配子账，并对 replacement/reorg 反向冲销。已实现 PnL 不依赖行情新鲜度，未实现 PnL 需要新鲜可追溯行情。
- [ ] P10 — RPC 实时运行能力：第 0 层可先以单一 RPC 做只读/模拟功能验证，但生产仍需链级主备身份、新鲜度、同 slot/区块视图、模拟与广播能力探针及实时 failover。钱包订阅另需 WSS heartbeat 和 subscription freshness；第一版池价不因此强制使用 WS 行情。错链、落后节点、主备读数不一致、DNS/连接失败均 fail-closed，不泄露 URL/密钥。
- [ ] P11 — 备份与恢复演练：覆盖 state、portfolio、execution、performance、registry、watchlist、策略及所需运行配置；SQLite 使用 backup API，校验完整性和哈希，并演练恢复/回滚。不得备份或输出 `.env` 秘密。
- [ ] P12 — 端到端验收：先证明新直连路线的 `price <MINT>` 只读 RPC、返回 quote/base spot 与 reserves，缓存命中目标 1 秒；`SIMULATE=true` 的 buy/sell 只模拟、绝不发送；直接交易模块不调用聚合报价、预言机或安全/行情 HTTP API，依赖清单只增加经批准的链客户端和必要编码/布局包（不要求删除仓库旧路线）。再覆盖 Fomo/Wallet 契约、跨语言 golden、断线恢复、并发资金预留、部分卖出、失败广播、replacement、回执、reorg/PnL 回滚、伪 readiness、反向代理未授权访问等失败注入。运行 `pytest`、`ruff`、`pyright`、Node tests、`git diff --check`、`portfolio_maintenance check`（`ok=true`）及至少 100k fills + 100k market observations 基准。最后仍保持 live 关闭，另行人工授权才可考虑上线。

## 发布同步（不等于启用实盘）

- [ ] D01 — 将已推送的代码同步到服务器。待确认 SSH 主机/登录用户及部署目录；同步前备份并验证服务器运行数据，不擅自重启服务、迁移现有库或解除 live 硬锁。

## 最近验收基线

2026-09-21 本地：`pytest` 202 passed + 8 subtests；Node 16 passed；`ruff` 与 `pyright` 通过；`git diff --cached --check` 通过；`portfolio_maintenance check` 返回 `ok=true`；本地执行库 `live_armed=0`、熔断开启。这些结果只证明当前基础实现的回归状态，不满足 P01–P12 的完成条件。

2026-09-21 P03 局部验收：`pytest` 210 passed + 18 subtests；Node 21 passed；`ruff` 与 `pyright` 通过；`portfolio_maintenance check` 为 `ok=true`；执行库 `live_armed=0`、熔断开启。`git diff --check` 全量被既存的 `sidecar/privy-keeper.bundle.js` 尾随空白阻断；排除该无关文件后通过。P03 总项仍未完成。

2026-09-22 P03 后续：增加支付资产链上余额、EVM 最终签名字节 gas fee cap 证据与跨快照一致性校验；修正 `portfolio_maintenance` 将已接受的 `swap_sell` 误当买入的检查口径，仅改检查器和测试，未迁移或改写账本。`pytest` 215 passed + 24 subtests，Node 21 passed，`ruff`/`pyright` 通过；`portfolio_maintenance check` 恢复 `ok=true`，执行库 `live_armed=0`。全量 `git diff --check` 仍被既存 bundle 尾随空白阻断，排除该文件后通过。P03 总项仍未完成。

2026-09-22 P03 闭环代码：EVM/Solana 市场证据提供器与最终广播前门禁已接线，增加 Solana RPC 精确 message 费率探针、只读真实端点验收入口和错链/断线失败注入。`pytest` 220 passed + 29 subtests，Node 21 passed，`ruff`/`pyright` 通过，`portfolio_maintenance check` 为 `ok=true`；本地 `live_armed=0`、熔断开启。真实外部探针当时未运行；这只说明当前执行进程未取得所需环境变量，**不代表仓库 `.env` 文件未配置**（按安全约束未读取该文件）。全量 `git diff --check` 仍被既存 bundle 尾随空白阻断，排除该文件后通过。P03 总项仍未完成。

2026-09-22 P03 价格来源调整：按用户要求将 Pyth 留作可选路径，增加链上池 `Sync`→经 RPC 核验→进程内价格缓存路径；当前只实现 EVM 主网 Uniswap V2 WETH/USDC 的受限验证入口，尚无生产 WS/gRPC 订阅或 Solana AMM 解码，因此不宣称实时行情能力完成。`pytest` 223 passed + 29 subtests，Node 21 passed，`ruff`/`pyright` 通过，`portfolio_maintenance check` 为 `ok=true`。全量 `git diff --check` 仍仅被已有的 `sidecar/privy-keeper.bundle.js` 尾随空白阻断，排除该无关文件后通过。未迁移数据库，未广播，P03 总项继续未勾选。

2026-09-22 新路线首轮：按用户确认把 Uniswap V2→V3→V4 提前、Solana 放最后。新增 V2 只读 Factory/Pair 快照、quote-token spot、997/1000 整数报价、独立 approve calldata、双 token swap calldata、本地未签名交易构建与测试签名字节反向解析，以及脱敏只读探针；尚无真实 RPC 池数据核对、执行意图/资金预留接线、生产模拟或广播，因此 P01-V2/P01 总项均未勾选，V3/V4/Solana 未开始。`pytest` 230 passed + 29 subtests，Node 21 passed，`ruff`/`pyright` 通过；`portfolio_maintenance check` 最近一次 `ok=true`。全量 `git diff --check` 仍被既存的 `sidecar/privy-keeper.bundle.js` 尾随空白阻断，排除后通过。未迁移数据库，未广播，live 仍关闭。

2026-09-23 Robinhood Chain 4663 V3 缓存生产局部闭环：`scripts/discover_two_ca_4663.py` 已移除 `.env` 加载，仅接受进程注入的 `RPC_ROBINHOOD_URL`；增加 Factory `PoolCreated` 分段扫描、日志范围退避、原子 JSON 游标与中断恢复、Factory `getPool` 复核、唯一 WETH/V3 候选规则、逐 CA 即时报价发布，以及只刷新已缓存身份且不重复历史扫描的 `--watch`。身份 TTL 为 10 分钟，报价 TTL 为 2 秒；amountIn 由正整数 CLI 参数提供并与缓存精确绑定。独立 SQLite 缓存保持发现端事务 upsert/失效、买入端 `mode=ro`，锁竞争、未命中和过期均 fail-closed 且没有 RPC/0x/Jupiter 回退；4663 缓存记录同时绑定允许 Factory、事件块、固定报价块、block hash 和证据类型，不再只信任 `identity_verified=True`。V2/V4/非零 hook 不进入该生产器。修复 `direct_v3.py` 的 `selected_height` 可能未赋值和 `direct_v4.py` 批量返回类型的 fail-closed 静态检查。未改变 SQLite 表结构，仅扩展独立缓存表中的 JSON 负载，因此无现有数据库 schema/data 迁移、无需备份或回滚数据；删除独立缓存文件及游标即可回滚运行状态。缓存生产相关定向测试 `56 passed + 8 subtests`；全量 `pytest` 为 `327 passed + 51 subtests`，Node `25 passed`，Ruff、Pyright、`git diff --check` 通过，`portfolio_maintenance check` 为 `ok=true`。当前进程未注入 `RPC_ROBINHOOD_URL`，因此本轮未运行真实 4663 只读 RPC，不能把 mock/fixture 结果称为主网证据；真实端点扫描、长期 watch 稳定性及运行托管仍待验收。P01-V3、P05、P12 均保持未勾选；`tradingReady=false`、`live_armed=false`，未签名、未模拟、未广播交易。

2026-09-23 Robinhood Chain 4663 V3 Factory 直查实测：针对已有真实事件证据支持的 CA `0x39db…4571`，仅批准并探测 fee tier `[10000]`，未调用 `eth_getLogs`，不得解释为全 Factory 唯一。真实 RPC 返回 `chainId=4663`；固定区块 `70511464`、block hash `0xd06f…b91c`；批准集合内唯一池为 `0x10cc…26ba`，tick spacing `200`。固定 `amountIn=1000000000000` 的本地 V3 报价为 `amountOut=3869014819501506`，缓存写入年龄 `0ms`；共计 15 个 JSON-RPC 调用，耗时 `22422ms`。随后在同一进程内禁止 RPC、0x 和 Jupiter 回退，从只读 SQLite 缓存构造并反向解析未签名交易，结果为 `built_not_sent`；chainId、Router、tokenIn/tokenOut、fee、amountIn、minOut、recipient、nonce 以及 SwapRouter02 外层 multicall deadline 均与输入/缓存一致。第二枚 CA 因没有已批准且有真实证据的 fee tier 未探测，未猜测补充。P01-V3、P05、P12 继续未勾选；`tradingReady=false`、`live_armed=false`，未接入 `execution_service`，未执行交易模拟，未签名，未广播。

2026-09-23 Robinhood Chain 4663 V3 低延迟与 watch 实测：经用户授权仅从项目 `.env` 加载 `RPC_ROBINHOOD_URL` 到只读探针进程，未输出或记录其值。固定块读取复用不可变上下文，token decimals、Factory/pool 身份、fee/tick spacing、slot0/liquidity 进入严格 Multicall3；bitmap 和必要 initialized ticks 有界读取，末尾复核同高度 block hash 与 provider。缓存 JSON 新增并由证据哈希绑定 `blockTimestampMs`、`readStartedAtMs`、开始/完成块龄、快照读取耗时、快照至提交耗时和发布时链状态年龄，`observedAtMs` 仍是完整验证完成时间，2 秒 TTL 未放宽。优化后真实 Factory probe 固定区块 `70540821`，批准 fee `[10000]`，池 `0x10cc…26ba`，`amountIn=1000000000000`、`amountOut=3916204774232839`；9 次 HTTP/JSON-RPC、14 个 Multicall 子调用、耗时 `18562ms`，无 `eth_getLogs`。最终 watch 单实例串行运行 `612062ms`：56 轮全部成功；唯一 CA 成功 56、失败 0。刷新平均/P50/P95/P99/最大为 `10929/10717/13420/15015/15015ms`；每轮固定 8 次 HTTP、8 个 JSON-RPC 方法和 14 个 Multicall 内部子调用。相邻成功报价平均/P95/最大为 `10968/13421/15015ms`，2 秒新鲜覆盖率 `17.973%`，最长连续过期 `13015ms`；发布时链状态年龄平均/P50/P95/P99/最大为 `9041/8961/11007/11081/11081ms`。provider 变化、限流、timeout、reorg、SQLite 锁和 `eth_getLogs` 均为 0。该端点即使健康时仍明确不足以提供 2 秒持续新鲜缓存，系统继续过期即 fail-closed，未接运行托管。另将 Base 8453 Router02 最终签名字节 deadline 强制为未来且不超过 300 秒，并覆盖过期/过远拒绝；4663 仍为 `v3_signed_chain_unapproved`。全量 `pytest` 为 `341 passed + 72 subtests`，Node `21 passed`，Ruff、Pyright、`git diff --check` 通过，`portfolio_maintenance check` 为 `ok=true`。独立缓存仅扩展 JSON 负载，未迁移执行账本或既有 SQLite schema。P01-V3、P05、P12 继续未勾选；`tradingReady=false`、`live_armed=false`，未接入 `execution_service`，未模拟、未签名、未广播。
