import { useState } from "react";
import { Chart } from "./Chart";
import { amount, clock, price, useAPI } from "./data";
import type { Asset } from "./types";
import "./Signals.css";

type Window = { minutes: number; from: string; to: string; coverage: number; netCents: number | null; buyCents: number | null; sellCents: number | null; volumeCents: number | null; buyShare: number | null; sellShare: number | null; volumeRatio: number | null };
type Evidence = { factor: string; state: string; text: string; scope: string };
type Check = { name: string; passed: boolean; known: boolean; actual: number | null; required: number | null };
type Assessment = { direction: string; fast: boolean; sustained: boolean; large: boolean; supported: boolean; fastChecks: Check[]; sustainedChecks: Check[] };
type Funding = { venue: string; ratePercent: string; intervalHours: number | null; margin: string; rateKind: string; comparable: boolean; fetchedAt: string; observedAt: string | null; changePercentagePoints: number | null };
type Snapshot = {
  at: string; dataThrough: string; fresh: boolean; headline: string; detail: string; direction: string;
  spot: Record<string, Window>; quarters: Window[]; hours: Window[]; evidence: Evidence[]; assessments: Assessment[];
  baseline: { coverage: number; validDates: number; valid: boolean; from: string; to: string };
  price: { closeUsdt: number | null; return1h: number | null; priorAtr1h: number | null; displacementAtr: number | null };
  context: { futures: Record<string, Window>; oi: { coinChange1h: number | null; coinChange4h: number | null; usdChange1h: number | null; usdChange4h: number | null; regime: string }; funding: Funding[]; liquidations: Record<string, { longCents: number | null; shortCents: number | null; coverage: number }> };
};
type Progress = { at: string; dataThrough: string; lineUsdt: number; closesUsdt: (number | null)[]; outsideCloses: number; flowSame: boolean | null; status: string; observationEnds: string | null };
type Signal = { id: string; direction: string; pattern: string; state: string; level?: string; rulesVersion: string; at: string; dataThrough: string; expiresAt: string; confirmedAt: string | null; confirmedDataThrough?: string; updatedAt: string; frozenHigh: number; frozenLow: number; net15Cents: number; buyShare: number | null; detectionDelaySeconds: number | null; evidence: string[]; conflicts: string[]; missing: string[]; multifactor?: Snapshot; multifactorUpgrade?: Snapshot; confirmationSnapshot?: Snapshot; priceProgress?: Progress; lifecycleRepair?: { reason: string } };
type MailResult = { id: string; signalId: string; kind: string; status: string; createdAt: string; attemptedAt: string | null; completedAt: string | null; error?: string };
type Response = { current: Snapshot | null; currentPrice: { valueUsdt: number; at: string } | null; items: Signal[]; notificationResults: MailResult[]; prices: { at: string; closeUsdt: number | null }[]; mail: { configured: boolean; latestStatus: string; lastAttemptAt: string | null; lastError: { error: string } | null; note: string }; cutoverAt?: string };
const stamp = (s?: string | null) => s ? new Date(s).toLocaleString("zh-CN", { timeZone: "Asia/Shanghai", hour12: false }) : "当时未记录";
const day = (s: string) => new Date(s).toLocaleDateString("en-CA", { timeZone: "Asia/Shanghai" });
const money = (n?: number | null) => n == null ? "缺失" : amount(n, true);
const percent = (n?: number | null) => n == null ? "未知" : `${n > 0 ? "+" : ""}${n.toFixed(2)}%`;
const sideName = (s: string) => s === "sell" ? "卖压" : "买盘";
const tone = (n?: number | null) => n == null || n === 0 ? "neutral" : n > 0 ? "buy" : "sell";
const factorNames: Record<string, string> = { spot: "现货成交", futures: "合约成交", oi: "持仓 OI", funding: "资金费率", liquidations: "已发生清算", price: "价格响应" };
const stateNames: Record<string, string> = { support: "✓ 支持", conflict: "! 分歧", neutral: "· 背景", missing: "? 不足" };
const progressNames: Record<string, string> = { waiting: "等待价格确认", holding: "破位保持，资金仍同向", reclaimed: "原破位已收回", flow_reversed: "价格已确认，资金方向转变", near_line: "确认位附近反复", unknown: "数据不足，暂停判断", not_recorded: "当时未记录确认数据时点", expired: "四小时内未确认，已到期", ended_with_gap: "观察结束，末段数据缺失" };
const progressText = (v?: string) => v?.startsWith("completed_") ? `四小时观察结束 · ${progressNames[v.slice(10)] ?? "已完成"}` : progressNames[v ?? "not_recorded"] ?? "当时未记录";
const mailNames: Record<string, string> = { pending: "排队中", unconfigured: "当时未配置", sending: "提交中", sent: "SMTP 已接受", delivery_unknown: "结果不确定 · 不重发", failed_before_submission: "提交前失败", rejected: "邮件服务器明确拒绝", unknown_after_restart: "重启后结果不确定", suppressed_restart: "重启积压未补发", suppressed_expired_or_validation: "过期或规则切换未发送", suppressed_scope: "范围外未发送" };

function WindowCard({ w, title, side }: { w?: Window; title: string; side: string }) {
  const dominant = side === "sell" ? w?.sellShare : w?.buyShare;
  return <article className={`flow-number ${tone(w?.netCents)}`}>
    <span>{title} · {w?.netCents == null ? "窗口不完整" : w.netCents > 0 ? "主动净买入" : w.netCents < 0 ? "主动净卖出" : "主动买卖相当"}</span>
    <strong>{w?.netCents == null ? "—" : amount(Math.abs(w.netCents))}<small>{w?.netCents != null && "美元"}</small></strong>
    <p>{w?.volumeRatio != null ? `成交量 ${w.volumeRatio.toFixed(2)} 倍` : w?.netCents == null ? `覆盖 ${((w?.coverage ?? 0) * 100).toFixed(0)}%` : "完整闭合窗口"} <span>· {side === "sell" ? "卖出" : "买入"}占比 {dominant == null ? "未知" : `${dominant.toFixed(1)}%`}</span></p>
  </article>;
}
function Continuity({ rows, title }: { rows: Window[]; title: string }) {
  const max = Math.max(1, ...rows.map(w => Math.abs(w.netCents ?? 0)));
  return <section className="flow-continuity"><h3>{title}</h3><p>独立区间，不重复累加滚动窗口</p><div className="flow-strips">
    {rows.map(w => <div className="flow-strip" key={w.from}>
      <span>{clock(w.from)}–{clock(w.to)}</span>
      <div className="flow-track"><i className={tone(w.netCents)} style={{ width: `${Math.abs(w.netCents ?? 0) / max * 50}%`, left: w.netCents != null && w.netCents < 0 ? `${50 - Math.abs(w.netCents) / max * 50}%` : "50%" }} /></div>
      <b className={tone(w.netCents)}>{money(w.netCents)}</b>
    </div>)}
  </div><small>← 净卖出　|　净买入 → · 美元</small></section>;
}
function RawFactors({ s }: { s: Snapshot }) {
  return <details className="flow-raw"><summary>查看金额、OI、费率与清算明细</summary>
    <div className="table-scroll"><table><caption>已闭合现货窗口 · 美元</caption><thead><tr><th>窗口</th><th>主动净买卖</th><th>主动买入</th><th>主动卖出</th><th>量比</th></tr></thead><tbody>{[5, 15, 30, 60, 240].map(m => { const w = s.spot[String(m)]; return <tr key={m}><td>{m} 分钟</td><td>{money(w?.netCents)}</td><td>{money(w?.buyCents)}</td><td>{money(w?.sellCents)}</td><td>{w?.volumeRatio == null ? "未计算" : `${w.volumeRatio.toFixed(2)} 倍`}</td></tr>; })}</tbody></table></div>
    <div className="flow-detail-grid"><div><h4>合约主动成交</h4>{[15, 60].map(m => { const w = s.context.futures?.[String(m)]; return <p key={m}>{m} 分钟：{money(w?.netCents)}美元 · 量比 {w?.volumeRatio?.toFixed(2) ?? "未知"} · 买入 {w?.buyShare?.toFixed(1) ?? "未知"}%</p>; })}<small>Binance、OKX、Bybit，和现货覆盖范围不同。</small></div>
      <div><h4>未平仓合约 OI</h4><p>币计价：1h {percent(s.context.oi.coinChange1h)} · 4h {percent(s.context.oi.coinChange4h)}</p><p>美元：1h {percent(s.context.oi.usdChange1h)} · 4h {percent(s.context.oi.usdChange4h)}</p><small>美元值包含价格影响；持仓增加不能单独判断开多或开空。</small></div>
      <div><h4>已发生清算</h4>{[15, 60].map(m => { const w = s.context.liquidations?.[String(m)]; return <p key={m}>{m} 分钟：多单 {money(w?.longCents)} / 空单 {money(w?.shortCents)} 美元</p>; })}<small>缺口保留为缺失，清算热力图不参与这里的金额。</small></div>
      <div><h4>价格位移</h4><p>1h 涨跌 {percent(s.price.return1h)} · 相对此前小时 ATR {s.price.displacementAtr?.toFixed(2) ?? "未知"} 倍</p><small>同向超过 1.5 倍限制升级；价格来自 Binance BTC/USDT。</small></div></div>
    <div className="table-scroll"><table><caption>资金费率：正值多方付费，负值空方付费；不同周期不平均</caption><thead><tr><th>交易所 / 保证金</th><th>费率 / 周期</th><th>类型</th><th>同口径变化</th><th>采样 / 历史</th></tr></thead><tbody>{s.context.funding?.map((f, i) => <tr key={`${f.venue}-${f.margin}-${i}`}><td>{f.venue} / {f.margin === "stablecoin" ? "稳定币" : "币"}</td><td>{f.ratePercent}% / {f.intervalHours == null ? "未知" : `${f.intervalHours}h`}</td><td>{{ settled: "已结算", predicted: "预测", unknown: "未知" }[f.rateKind] ?? "未知"}</td><td>{f.changePercentagePoints == null ? "不可比较" : `${f.changePercentagePoints.toFixed(5)} 百分点`}</td><td>获取 {clock(f.fetchedAt)} · {f.comparable ? "可比较" : "历史或口径不足"}{!f.observedAt && " · 来源时间未知"}</td></tr>)}</tbody></table>{!s.context.funding?.length && <p>缺少新鲜 Funding 采样。</p>}</div>
  </details>;
}
function EvidenceGrid({ s }: { s: Snapshot }) {
  return <><div className="flow-evidence-grid">{s.evidence.map(e => <article key={e.factor} className={`flow-factor ${e.state}`}><div><h3>{factorNames[e.factor] ?? e.factor}</h3><span>{stateNames[e.state]}</span></div><p>{e.text}</p><small>{e.scope}</small></article>)}</div><RawFactors s={s} /></>;
}
function Confirmation({ s }: { s: Signal }) {
  const p = s.priceProgress, direction = s.direction === "sell" ? "跌破" : "突破", line = s.direction === "sell" ? s.frozenLow : s.frozenHigh;
  return <section className="flow-confirm"><div className="flow-section-title"><h3>{s.confirmedAt ? "确认后观察" : "价格确认进度"} · {sideName(s.direction)}</h3><span>{p && !p.status.startsWith("completed_") && p.status !== "ended_with_gap" && Date.now() - new Date(p.at).getTime() > 12 * 60_000 ? "历史状态 · " : ""}{progressText(p?.status)}</span></div>
    <div className="flow-confirm-grid"><div><span>本事件固定观察线</span><strong>{price(line)} <small>USDT</small></strong><p>发现于 {stamp(s.at)}，区间边界固定不追价。</p></div><div><span>连续已闭合 5 分钟收盘</span><div className="flow-candles">{[0, 1].map(i => { const c = p?.closesUsdt[i]; const outside = c != null && (s.direction === "sell" ? c < line : c > line); return <span key={i} className={outside ? "passed" : ""}>{c == null ? "等待闭合" : `${outside ? "✓" : "·"} ${price(c)}`}</span>; })}</div><p>最近 15 分钟资金：{p?.flowSame == null ? "未知" : p.flowSame ? "✓ 仍同向" : "! 不再同向"}</p></div></div>
    <p>{s.confirmedAt ? `${direction}已于 ${stamp(s.confirmedAt)} 确认。当前状态单独更新，历史确认不会被改写。` : `四小时内，两根收盘均${direction}观察线且资金同向才确认。到期：${stamp(s.expiresAt)}。`}</p>
    <small>{p ? `进度数据截止 ${stamp(p.dataThrough)}${p.observationEnds ? ` · 确认后观察至 ${stamp(p.observationEnds)}` : ""}` : "旧事件当时未记录进度，不根据事后行情补造。"} · 确认不保证后续延续。</small>
  </section>;
}

export function SignalsPage({ asset }: { asset: Asset }) {
  const [rules, setRules] = useState(""); const [history, setHistory] = useState(false); const [selected, setSelected] = useState("");
  const q = useAPI<Response>(`signals?asset=${asset}&rules=${encodeURIComponent(rules)}`, 15000), d = q.data;
  const s = d?.current, items = d?.items ?? [], today = day(new Date().toISOString());
  const shown = items.filter(i => history || day(i.at) === today || (i.confirmedAt && day(i.confirmedAt) === today));
  const chosen = items.find(i => i.id === selected) ?? items.find(i => i.rulesVersion === "flow-multifactor-v1") ?? items[0];
  const fresh = !!s?.fresh && Date.now() - new Date(s.dataThrough).getTime() <= 12 * 60_000;
  const direction = s?.direction ?? "buy";
  const currentAssessment = s?.assessments.find(a => a.direction === direction);
  const priceOption = { tooltip: { trigger: "axis" }, grid: { left: 58, right: 18, top: 24, bottom: 38 }, xAxis: { type: "time", axisLabel: { formatter: (v: number) => clock(new Date(v).toISOString()) } }, yAxis: { type: "value", scale: true, splitLine: { lineStyle: { color: "#293833" } } }, series: [{ type: "line", showSymbol: false, connectNulls: false, lineStyle: { color: "#b9d8ca", width: 2 }, data: d?.prices?.map(p => [p.at, p.closeUsdt]), markLine: { symbol: "none", label: { show: false }, lineStyle: { type: "dashed", color: "#b4b48a" }, data: chosen ? [{ xAxis: chosen.at, name: "发现" }, ...(chosen.confirmedAt ? [{ xAxis: chosen.confirmedAt, name: "确认", lineStyle: { color: "#75e9ad" } }] : []), { yAxis: chosen.direction === "sell" ? chosen.frozenLow : chosen.frozenHigh, name: "固定观察线" }] : [] } }] };
  return <section className="flow-dashboard" aria-label="资金异动预警">
    {q.error && <p role="alert" className="flow-error">读取失败：{q.error}。保留的上一份数据不代表当前状态。</p>}
    <header className="flow-head"><div><div className="flow-kicker">BTC · 双向资金观察 <span>效果验证中</span>{fresh && currentAssessment?.large && <span>资金规模较大</span>}</div><h2>{!s ? "等待当前资金快照" : fresh && !q.error ? s.headline : "数据延迟或缺失，当前判断暂停"}</h2><p>{s ? `成交数据截止 ${stamp(s.dataThrough)} · 已闭合窗口${fresh ? "" : " · 以下为历史快照"}` : "正在检查数据覆盖和闭合窗口"}</p></div><span className="flow-mail-badge">{d?.mail.configured ? "邮件：异动＋价格确认" : "邮件待配置"}</span></header>
    <div className="flow-numbers">{[["15", "最近15分钟"], ["60", "最近1小时"], ["240", "最近4小时"]].map(([key, title]) => <WindowCard key={key} w={s?.spot[key]} title={title} side={direction} />)}</div>
    <p className="flow-explainer">正负表示买卖偏向。资金规模、相对异常、放量和持续性同时达标才称为异动。{d?.currentPrice && <span>最新价格 {price(d.currentPrice.valueUsdt)} USDT · {clock(d.currentPrice.at)}，时点与资金窗口不同。</span>}</p>
    {s && <><div className="flow-continuity-grid"><Continuity rows={s.quarters} title="最近一小时：四段15分钟" /><Continuity rows={s.hours} title="最近四小时：四段1小时" /></div><div className="flow-section-title"><h3>当前证据，哪里一致、哪里有分歧</h3><span>缺失不按中性处理</span></div><EvidenceGrid s={s} /></>}
    {chosen && <Confirmation s={chosen} />}
    <section className="flow-history"><div className="flow-section-title"><h3>{history ? "最近提醒记录" : "今日提醒时间线"}</h3><button className="text-button" onClick={() => setHistory(!history)}>{history ? "只看今天" : "查看历史"}</button></div><p>北京时间 · 选择一条提醒，查看固定观察线及价格图中的发现、确认时点。</p>
      {!!d?.prices?.length && <Chart height={215} label="BTC/USDT 最近24小时价格；虚线对应选中事件的发现、确认时点和固定观察线" option={priceOption} />}
      <label className="flow-filter">记录范围 <select value={rules} aria-label="记录规则筛选" onChange={e => { setRules(e.target.value); setSelected(""); }}><option value="">正式提醒与切换前记录</option><option value="flow-multifactor-v1">新双向规则</option><option value="flow-experiment-2.2.0">旧双向规则 · 研究对照</option><option value="flow-candidate-2.5.0">旧买方候选 · 研究对照</option></select></label>
      {!shown.length && <p className="empty">此范围暂无记录。没有提醒不代表没有行情，未达门槛或数据不足都可能不触发。</p>}
      <ol className="flow-timeline">{shown.map(i => { const mails = (d?.notificationResults ?? []).filter(n => n.signalId === i.id); const minutes = i.multifactor && i.pattern !== "fast" ? "60" : "15"; const net = i.multifactor?.spot[minutes]?.netCents ?? i.net15Cents; return <li key={i.id} className={chosen?.id === i.id ? "selected" : ""}>
        <button className="flow-event-select" onClick={() => setSelected(i.id)} aria-pressed={chosen?.id === i.id}><time>{clock(i.at)}</time><strong className={i.direction === "buy" ? "buy" : "sell"}>{sideName(i.direction)}异动</strong><span>{i.confirmedAt ? "价格已确认" : i.state === "expired" ? "已到期" : i.state === "weakened" ? "资金减弱" : "等待价格"}</span><b>{money(net)}美元 / {minutes === "60" ? "1h" : "15m"}</b></button>
        <div className="flow-event-steps">{i.level && <span>{({supported:"多因素支持",large:"资金规模较大",ordinary:"普通资金异动"} as Record<string,string>)[i.level] ?? i.level}{i.multifactorUpgrade && ` · 升级于 ${stamp(i.multifactorUpgrade.at)}`}</span>}<span>发现 {stamp(i.at)}</span>{i.confirmedAt && <span>→ {i.direction === "sell" ? "跌破" : "突破"}确认 {stamp(i.confirmedAt)}</span>}{["expired", "weakened"].includes(i.state) && <span>→ {i.state === "expired" ? "到期" : "减弱"} {stamp(i.updatedAt)}</span>}</div>
        <div className="flow-mail-results">{mails.length ? mails.map(n => <span key={n.id}>{n.kind === "confirmed" ? "确认邮件" : "早期邮件"}：{mailNames[n.status] ?? n.status}{n.completedAt ? ` · ${clock(n.completedAt)}` : n.attemptedAt ? ` · 尝试 ${clock(n.attemptedAt)}` : ""}{n.error && ` · ${n.error}`}</span>) : <span>无邮件记录{i.rulesVersion !== "flow-multifactor-v1" ? " · 旧规则或研究对照" : ""}</span>}</div>
        <details><summary>查看当时证据与数据时点</summary><p>数据截止 {stamp(i.dataThrough)} · 检测延迟 {i.detectionDelaySeconds == null ? "未知" : `${Math.round(i.detectionDelaySeconds)} 秒`} · {i.direction === "sell" ? "卖出" : "买入"}占比 {i.buyShare == null ? "未知" : `${(i.direction === "sell" ? 100 - i.buyShare : i.buyShare).toFixed(1)}%`}</p>{i.multifactor ? <><p>发现时：{i.multifactor.headline}</p><EvidenceGrid s={i.multifactor} /></> : <><p>该记录未保存新版多因素快照，不事后补造。</p>{[...(i.evidence ?? []), ...(i.conflicts ?? []), ...(i.missing ?? [])].map((v, n) => <p key={n}>{v}</p>)}</>}{i.confirmationSnapshot && <details><summary>价格确认时的独立快照</summary><EvidenceGrid s={i.confirmationSnapshot} /></details>}{i.lifecycleRepair && <p>{i.lifecycleRepair.reason}</p>}<small>规则：{i.rulesVersion}</small></details>
      </li>; })}</ol><small>“SMTP 已接受”是邮件服务器确认，不代表收件箱签收；旧记录未保存完成时间时仅展示发送尝试时间。</small>
    </section>
    <details className="flow-method"><summary>为什么提醒 / 为什么还没有提醒？查看固定门槛</summary><p>买卖两侧分别使用过去30天同方向净额的分位，排除当前窗口。至少95%覆盖、21个有效日期；非交易所充值提现。</p>{s && <p>当前基线：覆盖 {(s.baseline.coverage * 100).toFixed(1)}% · {s.baseline.validDates} 个有效日期 · {stamp(s.baseline.from)} 至 {stamp(s.baseline.to)}</p>}<div className="flow-checks">{s?.assessments.map(a => <section key={a.direction}><h4>{sideName(a.direction)}门槛</h4>{[["15分钟快速异动", a.fastChecks], ["1小时持续异动", a.sustainedChecks]].map(([title, checks]) => <div key={String(title)}><b>{String(title)}</b><ul>{(checks as Check[]).map(c => <li key={c.name}>{!c.known ? "? 未知" : c.passed ? "✓ 达标" : "· 未达"} · {c.name}</li>)}</ul></div>)}</section>)}</div><p>1小时同方向净额≥5000万美元且≥该方向P95时标记规模较大。多因素支持另需四小时同向、合约显著同向、OI可解释、Funding无同向高付费、价格位移不过大；缺失或冲突降级。</p><p>早期异动与价格确认各一类邮件，强度升级不多发。解除条件连续30分钟后才可再次触发。BTC与VIX共享每小时6次发送尝试上限，过期、历史补采、重启积压不补发。</p><p>{d?.mail.note}</p>{d?.mail.lastError && <p className="flow-error">最近通道异常：{d.mail.lastError.error}</p>}<small>固定规则 flow-multifactor-v1 · 效果验证中，不自动交易。</small></details>
  </section>;
}
