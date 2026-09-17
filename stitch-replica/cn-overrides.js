// 中文画布统一层：保留各工作台布局，替换为 Stitch 中文版命名与路由。
const cnPairs=[
  ['CORE-SUBSYSTEM // MATRIX-04','核心子系统 // 风控矩阵-04'],
  ['风控矩阵与多链节点执行中心','风控矩阵与多链节点执行中心'],
  ['MEMORY ENGINE','内存引擎'],['HOT-RELOAD RULES','热重载规则'],['CIRCUIT BREAKER','熔断器'],
  ['6-链并发节点监控矩阵','六链并发节点监控矩阵'],['RPC NODE POOL TELEMETRY','RPC 节点池遥测'],
  ['NETWORK & ENGINE','网络与执行引擎'],['ROUTER / ENDPOINT','路由器 / 节点'],['LATENCY','延迟'],['PACKET LOSS','丢包率'],['BLOCK HEIGHT','区块高度'],
  ['实时阻断事件流','实时风控阻断事件流'],['CIRCUIT BREAKER EVENTS','熔断事件'],['LIVE FEED','实时流'],
  ['ALL TRAPS','全部拦截'],['HONEYPOT','貔貅检测'],['LP UNLOCKED','LP 未锁定'],
  ['STRICT ENFORCEMENT','严格执行'],['ALPHA-SUBSYSTEM // RADAR-02','Alpha 子系统 // 雷达-02'],
  ['KOL RADAR MATRIX','KOL 雷达矩阵'],['SMART MONEY WHITELIST','聪明钱白名单'],['LIVE ALPHA SIGNALS','实时 Alpha 信号'],
  ['30D ROI','30日收益'],['WIN RATE','胜率'],['AVG HOLD','平均持仓'],['ALLOCATION','分配权重'],
  ['AUTOMATION CORE // CONFIG-05','自动化核心 // 配置-05'],['STRATEGY PROFILE SWITCHER','策略配置切换'],
  ['CHAIN INGESTION & MEV PRIORITY','多链接入与 MEV 优先级'],['SAFETY & HONEYPOT FILTERS','代币安全与貔貅过滤'],
  ['AUTOMATED EXIT TIERS','自动化分阶止盈止损'],['ENABLED','已启用'],
  ['SIMULATION CORE // BACKTEST-06','模拟核心 // 回测-06'],['STRATEGY VS BENCHMARK EQUITY CURVE','策略与基准净值曲线'],
  ['ALPHA ATTRIBUTION','Alpha 收益归因'],['LEDGER CORE // POSITIONS-03','账本核心 // 持仓-03'],
  ['ACTIVE POSITIONS & TRADE REPLAY LOGS','当前持仓与历史成交复盘'],['WAL PERSISTED','WAL 持久化'],
  ['TIME','时间'],['TOKEN / CHAIN','代币 / 链'],['ENTRY','入场价'],['MARK','标记价'],['STATUS','状态'],
  ['Portfolio Valuation','资产估值'],['Today PnL','今日盈亏'],['Unrealized PnL','未实现盈亏'],['Open Positions','未平仓'],['Win Rate','胜率']
];
function cnRender(renderer){return()=>cnPairs.reduce((html,p)=>html.replaceAll(p[0],p[1]),renderer())}
views.risk=cnRender(views.risk);views.kol=cnRender(views.kol);views.positions=cnRender(views.positions);views.config=cnRender(views.config);views.backtest=cnRender(views.backtest);
views.signals=views.kol;views.portfolio=views.positions;views.orders=views.backtest;views.settings=views.config;
const pageChrome={
  workbench:['FOMO HOME DASHBOARD','Shadow Execution','PAPER MOCK'],
  signals:['FOMO SIGNAL STREAM','实时链上信号','18 条 LIVE'],
  kol:['FOMO KOL RADAR','Alpha Monitoring','TIER-1'],
  portfolio:['FOMO POSITION MONITOR','持仓 / 盈亏监控','REALTIME'],
  orders:['FOMO SIMULATION QUEUE','历史回测与归因','PAPER'],
  risk:['FOMO RISK MATRIX','节点执行中心','ARMED'],
  settings:['FOMO AUTOMATION CORE','策略与密钥设置','DEPLOYED']
};
const baseShow=show;
show=function(id){
  baseShow(id);
  const meta=pageChrome[current]||pageChrome.workbench;
  document.querySelector('.homebrand b').textContent=meta[0];
  document.querySelector('.homebrand i').textContent='/　'+meta[1];
  document.querySelector('.homebrand em').textContent=meta[2];
  document.title=meta[0]+' // FOMO.EXEC';
};
show(location.hash.slice(1)||'workbench');
