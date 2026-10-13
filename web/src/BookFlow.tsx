import { useEffect, useRef, useState } from "react";
import { EventDigest, KeyZones } from "./MarketBrief";
import { liveBook, liveVenue } from "./marketReading";
import { Chart } from "./Chart";
import { api, useAPI } from "./data";
import "./BookFlow.css";

type Zone = { side: string; low: string; high: string; quantity: string | null; notionalQuote: string | null; baselineMedian: string | null; multiple: string | null; increaseUsd: string | null; baselineCoverage: number; enhanced: boolean };
type Point = { sourceAt: string; firstFetchedAt: string; firstKnownAt: string; revision: string; referenceQuote: string | null; fxUsd: string | null; zones: Zone[]; bands: { percent: string; bidUsd: string | null; askUsd: string | null; imbalance: string | null }[] };
type Evidence = { from: string; to: string; acquiredAt: string; certainBuyQuote: string; certainSellQuote: string; boundaryBuyQuote: string; boundarySellQuote: string; depthRetention: string | null; before: unknown; after: unknown };
type Update = { kind: string; publishedAt: string; sourceAt: string; evidence?: Evidence };
type Event = { id: string; venue: string; direction: string; publishedAt: string; computedAt: string; expiresAt: string; lastSeenAt: string; first: Point; zone: Zone; updates: Update[]; ended: boolean; completePath: boolean };
type Trial = { id: string; stage: string; publishedAt: string; start: string; referenceUsdt: string | null; done: boolean; outcomes: { minutes: number; state: string; coverage: number; returnPercent: string | null; mfePercent: string | null; maePercent: string | null; mfeAt: string | null; maeAt: string | null }[] };
type Notice = { id: string; eventId: string; kind: string; status: string; createdAt: string; attemptedAt: string | null; completedAt: string | null; templateVersion: string; enabledAt: string; deliveryResult: string };
type RunStatus = { state: string; label: string; blockedReasons: string[]; warmupProgress: number; warmupEndsAt: string | null; collectionAcceptance: { observedMinutes: number; timelyMinutes: number; coverage: number | null; through: string | null; inputTimelyMinutes: number[]; inputOrder: string[] } };
type Item = { event: Event; observations: Trial[]; notifications: Notice[]; formalEvents?: { id: string; at: string; direction: string; confirmedAt: string | null; notifications: unknown[]; paper: unknown }[] };
type Coverage = { priceEpisodes: number; early: number; following: number; missed: number; unmatched: number; pending: number };
type Group = { horizons: Record<string, {complete: number; pending: number; missing: number; completeEvidence: number}>; completeEvidence: { independent4h: number; buy: number; sell: number; positive: number; meanPercent: string | null }; relativeFormal: { matchedPairs: number; medianLeadMinutes: string | null }; marketCoverage: Coverage; stage: string; all: number; overlapping: number; completeIndependent4h: number; buy: number; sell: number; positive4h: number; nonPositive4h: number; positivePercent: string | null; meanPercent: string | null; medianPercent: string | null; eligible: boolean; interval95: { lowPercent: string; highPercent: string; method: string } | null };
type Data = { runStatus: RunStatus; notifications: Notice[]; prices: { at: string; closeUsdt: string | null }[]; at: string; mode: string; rulesVersion: string; collectionVersion: string; origin: string | null; lastProcessedAt: string | null; runtime: { registered: boolean; lastSuccessAt: string | null; lastFailureAt: string | null; lastFailure?: string; failures: number }; contracts: { venue: string; attempts: number; verifiedAt: string | null; cutoverAt: string | null; activatedAt: string | null; healthy: boolean | null; lastSuccessAt: string | null; lastFailureAt: string | null; lastFailure?: string; recoveredAt: string | null; failure?: string }[]; current: Record<string, { snapshot: Point; fresh: boolean; ageSeconds: number }>; items: Item[]; more: boolean; storageBytes: number; budgetBytes: number; scheduler: { limitPerMinute: number; usedLastMinute: number; jobs: { id: string; dataset: { refreshSeconds: number }; successIntervalSeconds: number | null; sourceLagSeconds: number | null; queueWaitSeconds: number; fetchSeconds: number | null; timelySamples: number; observedSamples: number; lastFailure?: string }[] }; report: { marketCoverageNote: string; formalMarketCoverage: Coverage; groups: Group[]; observedMinutes: number; timelyMinutes: number; bookTimelyMinutes: number; coverage: number | null; truncated: boolean; assessment: string }; note: string };
export type BookFlowData = Data;
export type BookFlowItem = Item;
export type BookFlowZone = Zone;
const stamp = (s?: string | null) => s ? new Date(s).toLocaleString("zh-CN", { timeZone: "Asia/Shanghai", hour12: false }) : "未知 / 尚未取得";
const number = (v?: string | number | null, digits = 2) => v == null ? "未知" : Number(v).toLocaleString("zh-CN", { maximumFractionDigits: digits });
const money = (v?: string | null) => v == null ? "未知" : Math.abs(Number(v)) >= 1e8 ? `${number(Number(v) / 1e8)}亿美元` : `${number(Number(v) / 1e4)}万美元`;
const labels: Record<string, string> = { "mail-persistent": "持续增强邮件接受后", "mail-absorption": "承接升级邮件接受后", enhanced: "挂单增强", persistent: "挂单持续确认", absorption: "被动承接 / 抛压线索", following: "主动成交跟进", faded: "同侧深度消退", broken: "区域失守", data_gap: "盘口数据中断", foot_gap: "分钟成交证据不完整", evidence_unknown: "区域证据未知", expired: "观察期结束" };

function EventCard({ item, current }: { item: Item; current: boolean }) {
  const e = item.event;
  return <article className={`bf-event ${e.direction}`}>
    <EventDigest item={item} current={current} />
    <p>首次 {stamp(e.publishedAt)} · 数量 {number(e.zone.quantity, 5)} BTC · 基准 {number(e.zone.baselineMedian, 5)} BTC · <strong>{number(e.zone.multiple)} 倍</strong> · 数量增量折算 {money(e.zone.increaseUsd)}</p>
    <ol className="bf-timeline"><li><time>{stamp(e.publishedAt)}</time><b>首次挂单增强</b><span>不等待足迹、正式异动或衍生品一致</span></li>{e.updates.map(u => <li key={u.kind}><time>{stamp(u.publishedAt)}</time><b>{labels[u.kind] || u.kind}</b>{u.evidence && <span>成交窗口 {stamp(u.evidence.from)}—{stamp(u.evidence.to)} · 主动买入 {number(u.evidence.certainBuyQuote)} / 卖出 {number(u.evidence.certainSellQuote)} USDT · 深度保持 {u.evidence.depthRetention == null ? "未知" : `${number(Number(u.evidence.depthRetention) * 100)}%`} · 边界未分配 {number(Number(u.evidence.boundaryBuyQuote) + Number(u.evidence.boundarySellQuote))} USDT</span>}</li>)}</ol>
    <details><summary>输入时间与原始依据</summary><dl><dt>盘口源时间</dt><dd>{stamp(e.first.sourceAt)}</dd><dt>本次证据取得</dt><dd>{stamp(e.first.firstFetchedAt)}</dd><dt>研究首次读到</dt><dd>{stamp(e.first.firstKnownAt)}</dd><dt>计算时间</dt><dd>{stamp(e.computedAt)}</dd><dt>基准分钟覆盖</dt><dd>{number(e.zone.baselineCoverage * 100)}%</dd><dt>来源修订</dt><dd>{e.first.revision}</dd></dl><p>量价来自 CoinGlass 返回的现货快照，不是逐笔委托流。消退不能直接认定撤单，深度保持不能证明同一挂单持续存在。</p><pre>{JSON.stringify(e.zone, null, 2)}</pre></details>
    <details><summary>分阶段前向观察 · 非模拟交易收益</summary><p>每个阶段从其实际登记后的下一根完整五分钟现货开盘观察，无费用。后来承接不倒推到首次挂单时点。</p>{item.observations.map(t => <section key={t.id}><h4>{labels[t.stage] || t.stage}</h4><p>发布 {stamp(t.publishedAt)} · 起点 {stamp(t.start)} · 参考 {number(t.referenceUsdt)} USDT</p><div className="bf-outcomes">{t.outcomes.map(o => <div key={o.minutes}><b>{o.minutes}分钟</b><span>{o.state === "pending" ? "待成熟" : o.state === "complete" ? "完整" : "有缺口"}</span><span>方向收益 {number(o.returnPercent, 3)}%</span><span>有利 {number(o.mfePercent, 3)}% / 不利 {number(o.maePercent, 3)}%</span><small>有利时点 {stamp(o.mfeAt)}<br />不利时点 {stamp(o.maeAt)}</small></div>)}</div></section>)}</details>
    <NoticeLog notices={item.notifications} />
    {item.formalEvents && <details><summary>同期正式异动关联</summary><p>时间关联不代表因果关系。</p>{item.formalEvents.length ? item.formalEvents.map(s => <div key={s.id}>{stamp(s.at)} · {s.direction === "buy" ? "买盘" : "卖压"}异动 · 突破确认 {stamp(s.confirmedAt)}<details><summary>通知与模拟关联依据</summary><pre>{JSON.stringify({ notifications: s.notifications, paper: s.paper }, null, 2)}</pre></details></div>) : <p>此观察前后没有已登记的正式异动。</p>}</details>}
  </article>;
}

export function BookFlowPage({ initialEventId = "", initialSection = "" }: { initialEventId?: string; initialSection?: string }) {
  const [direction, setDirection] = useState("");
  const [offset, setOffset] = useState(0);
  const [detail, setDetail] = useState(initialEventId);
  const focused = useRef(false);
  const q = useAPI<Data>(`book-flow?limit=10&offset=${offset}&direction=${direction}${detail ? `&id=${encodeURIComponent(detail)}` : ""}`, 15000), d = q.data;
  useEffect(() => {
    if (!d || focused.current || !initialSection) return;
    const el = document.getElementById(`bf-${initialSection}`);
    if (el) { el.focus({ preventScroll: true }); el.scrollIntoView({ block: "start", behavior: "instant" }); focused.current = true; }
  }, [d, initialSection]);
  const current = Object.entries(d?.current ?? {});
  const scale = Math.max(1, ...current.flatMap(([,v]) => v.snapshot.zones.map(z => z.notionalQuote == null || v.snapshot.fxUsd == null ? 0 : Number(z.notionalQuote) * Number(v.snapshot.fxUsd))));
  const chart = { tooltip: { trigger: "axis" }, grid: { left: 58, right: 18, top: 25, bottom: 35 }, xAxis: { type: "time" }, yAxis: { type: "value", scale: true, splitLine: { lineStyle: { color: "#2d4237" } } }, series: [{ name: "Binance现货5分钟收盘 · USDT", type: "line", showSymbol: false, connectNulls: false, lineStyle: { color: "#b9d8ca", width: 2 }, data: d?.prices.map(p => [p.at, p.closeUsdt == null ? null : Number(p.closeUsdt)]), markLine: { symbol: "none", label: { show: false }, data: d?.items.map(i => ({ xAxis: i.event.publishedAt, name: i.event.venue + "挂单增强", lineStyle: { color: i.event.direction === "buy" ? "#75e9ad" : "#ee8876" } })) } }] };
  const stale = !d?.lastProcessedAt || Date.now() - Date.parse(d.lastProcessedAt) > 180000;
  return <section className="book-flow">
    <div className="bf-heading"><div><span className="bf-kicker">BTC · 现货原始盘口 × 实际主动成交</span><h2>挂单与承接观察</h2><p>先看挂单在哪里增加，再看成交是否打到这里。观察信息不等于多空开仓指令。</p></div><span className="bf-badge">站内前向研究</span></div>
    {q.error && <p role="alert" className="amber">读取失败：{String(q.error)}。状态未知，旧快照不能视为当前正常。</p>}
    {!d && !q.error && <p>正在读取本地已登记证据…</p>}
    {d && <>
      <div className="bf-status brief-focus-target" id="bf-diagnostics" tabIndex={-1}><b role="status">{d.runStatus.label}</b>
        <p>核心两家 · 盘口＋分钟成交：最近已冻结 {d.runStatus.collectionAcceptance.observedMinutes}/60 分钟，联合及时覆盖 {d.runStatus.collectionAcceptance.coverage == null ? "未知" : (d.runStatus.collectionAcceptance.coverage * 100).toFixed(1) + "%"}（{d.runStatus.collectionAcceptance.timelyMinutes}/{d.runStatus.collectionAcceptance.observedMinutes}）。此处与页面上方全站五家覆盖分别计算。</p>
        {!!d.runStatus.blockedReasons.length && <ul>{d.runStatus.blockedReasons.map(reason => <li key={reason}>{reason}</li>)}</ul>}
        {d.origin ? <p>研究起点 {stamp(d.origin)} · 最近登记 {stamp(d.lastProcessedAt)} · 预热 {number(d.runStatus.warmupProgress * 100, 0)}% · 研究及时覆盖 {d.report.coverage == null ? "未知" : (d.report.coverage * 100).toFixed(1) + "%"}</p> : <p>研究尚未开始。采集验收不计入前向成绩；显式启动后重新预热30分钟。</p>}
        <p>最近发生：{d.items[0] ? `${stamp(d.items[0].event.publishedAt)} · ${d.items[0].event.venue} · ${d.items[0].event.completePath ? "证据路径完整" : "证据有缺口"}` : d.origin ? "当前范围零个已登记事件" : "尚无前向事件"}</p>
        <details open={initialSection === "diagnostics"}><summary>版本、契约与历史错误</summary><p>{d.rulesVersion} · {d.collectionVersion} · 保留历史失败 {d.runtime.failures} 次{d.runtime.lastFailure && ` · ${stamp(d.runtime.lastFailureAt)} ${d.runtime.lastFailure}`}</p><div className="bf-contracts">{d.contracts.map(c => <article key={c.venue}><b>{c.venue} 一分钟足迹</b><p>当前 {c.healthy == null ? "未知" : c.healthy ? "健康" : "异常"} · {c.verifiedAt ? "契约已通过" : "契约待验证"} · 请求 {c.attempts} 次</p><small>最近成功 {stamp(c.lastSuccessAt)}<br />首次完整聚合 {stamp(c.activatedAt)}<br />恢复 {stamp(c.recoveredAt)}</small>{c.failure && <p className="amber">当前失败：{c.failure}</p>}{c.lastFailure && <p>历史失败 {stamp(c.lastFailureAt)}：{c.lastFailure}</p>}</article>)}</div></details>
      </div>
      <div id="bf-mail" tabIndex={-1} className="brief-focus-target"><BookMailSettings initialOpen={initialSection === "mail"} /></div>
      <h3 id="bf-regions" className="brief-focus-target" tabIndex={-1}>价格附近的关键挂单</h3>
      <KeyZones data={d} failed={!!q.error} />
      <details><summary>按交易所查看原始价区与附近深度</summary><h3>当前固定价区</h3><p>两侧、两家交易所共用美元线性刻度；基准是此前30分钟同区域数量中位数。覆盖不足保持未知。</p>
      {!current.length && <p>等待启用后的盘口预热；不会用旧行情补造新观察。</p>}
      <div className="bf-venues">{current.map(([venue, value]) => <article key={venue} className={!value.fresh || stale || q.error ? "bf-stale" : ""}><header><b>{venue}</b><span>{value.fresh && !stale && !q.error ? "近期快照" : "过期 / 状态未知"}</span></header><p>参考价 {number(value.snapshot.referenceQuote)} USDT · 源 {stamp(value.snapshot.sourceAt)} · 年龄 {Math.floor(value.ageSeconds)}秒</p>{(["sell", "buy"] as const).map(side => <div key={side}><h4>{side === "sell" ? "卖方深度" : "买方深度"}</h4>{value.snapshot.zones.filter(z => z.side === side && z.quantity != null).sort((a,b) => Number(b.low) - Number(a.low)).map(z => { const usd = z.notionalQuote == null || value.snapshot.fxUsd == null ? null : String(Number(z.notionalQuote) * Number(value.snapshot.fxUsd)); return <div key={`${z.side}/${z.low}`} className={`bf-zone ${side}`}><div className="bf-fill" style={{ width: `${usd == null ? 0 : Number(usd) / scale * 100}%` }} /><span>{number(z.low)}—{number(z.high)}</span><strong>{money(usd)}</strong><small>{number(z.multiple)}倍 · 基准覆盖 {number(z.baselineCoverage * 100)}%{z.enhanced ? " · 增强" : ""}</small></div>})}</div>)}<details><summary>附近深度与不平衡</summary>{value.snapshot.bands.map(b => <p key={b.percent}>±{b.percent}% · 买 {money(b.bidUsd)} / 卖 {money(b.askUsd)} · 不平衡 {b.imbalance == null ? "未知" : number(Number(b.imbalance) * 100) + "%"}</p>)}<p>区间相互重叠，不累计计分。</p></details></article>)}</div>
      </details>
      <h3 id="bf-results" className="brief-focus-target" tabIndex={-1}>阶段结果与背景说明</h3>
      <div className="bf-evidence"><article><b>核心盘口与成交</b><p>参与新观察：区域挂单变化、同场足迹、数据质量。承接与主动跟进独立标记。</p></article><article><b>正式资金背景</b><p>1h / 4h资金与正式异动另行查看，不要求同向后才展示挂单增强。</p></article><article><b>衍生品背景</b><p>OI、资金费、合约成交与清算不作为此观察的必备条件。</p></article></div>
      {!d.origin || !d.report.groups.some(g => g.all > 0) ? <p>尚无可评估的前向样本。零样本不代表观察无效；达到数量与质量门槛后才可审查。</p> : <>
      <h3>前向效果</h3><p>{d.report.marketCoverageNote}</p><p>同期正式异动对照：提前 {d.report.formalMarketCoverage.early} / 跟随 {d.report.formalMarketCoverage.following} / 未匹配 {d.report.formalMarketCoverage.missed}，分母 {d.report.formalMarketCoverage.priceEpisodes} 个已登记行情片段。</p><p>{d.report.assessment} 至少30天、100个独立成熟观察、多空各30、覆盖95%后才给阶段判断。</p><div className="bf-stats">{d.report.groups.filter(g => g.all > 0).map(g => <article key={g.stage}><b>{labels[g.stage]}</b><p>{g.eligible ? "达到阶段审查门槛" : "样本或质量尚不足"}</p><p>全部 {g.all} · 重叠 {g.overlapping} · 独立完整4h {g.completeIndependent4h}（买{g.buy}/卖{g.sell}）</p><p>已登记行情片段 {g.marketCoverage.priceEpisodes} · 提前 {g.marketCoverage.early} · 跟随 {g.marketCoverage.following} · 未匹配 {g.marketCoverage.missed}</p><p>完整证据独立样本 {g.completeEvidence.independent4h}（买{g.completeEvidence.buy}/卖{g.completeEvidence.sell}）· 均值 {number(g.completeEvidence.meanPercent, 3)}%</p><p>同一行情与正式异动配对 {g.relativeFormal.matchedPairs} 个 · 提前量中位数 {number(g.relativeFormal.medianLeadMinutes)}分钟</p><p>全部可计算方向为正 {g.positive4h} / {g.completeIndependent4h} · 均值 {number(g.meanPercent, 3)}% · 中位数 {number(g.medianPercent, 3)}%</p><details><summary>各观察周期成熟与缺口数</summary>{Object.entries(g.horizons).map(([minutes, v]) => <p key={minutes}>{minutes}分钟 · 价格完整 {v.complete} · 输入证据完整 {v.completeEvidence} · 待成熟 {v.pending} · 缺口 {v.missing}</p>)}</details>{g.interval95 && <p>完整证据独立样本的日块区间 [{number(g.interval95.lowPercent, 3)}%, {number(g.interval95.highPercent, 3)}%]</p>}</article>)}</div>{d.report.truncated && <p className="amber">报告读取达到上限，当前不是全量成绩，禁止阶段判断。</p>}
      </>}
      <h3 id="bf-events" className="brief-focus-target" tabIndex={-1}>观察事件与复盘</h3><Chart option={chart} height={260} label="现货价格与当前页挂单观察时点" /><p>价格为Binance现货完整五分钟收盘；竖线为当前页挂单观察登记时点，缺口不连线。</p><div className="bf-controls"><label>方向 <select aria-label="方向" value={direction} onChange={e => { setDirection(e.target.value); setOffset(0); setDetail(""); }}><option value="">全部</option><option value="buy">买方</option><option value="sell">卖方</option></select></label><span>默认近48小时 · 北京时间</span>{detail && <button onClick={() => setDetail("")}>返回列表</button>}</div>
      {!d.items.length && <p>当前范围没有已登记观察。缺少记录不等于没有行情；预热、缺口及登记失败都可能阻止观察。</p>}{d.items.map(i => <div key={i.event.id}><EventCard item={i} current={liveBook(d, !!q.error, Date.now()) && liveVenue(d, i.event.venue, Date.now())} />{!detail && <button onClick={() => { setDetail(i.event.id); setOffset(0); }}>查看同期关联</button>}</div>)}
      {!detail && <div className="bf-controls"><button disabled={offset === 0} onClick={() => setOffset(Math.max(0, offset - 10))}>上一页</button><span>第 {offset / 10 + 1} 页</span><button disabled={!d.more} onClick={() => setOffset(offset + 10)}>下一页</button></div>}
      <NoticeLog notices={d.notifications} />
      <details><summary>采集时效、额度与质量诊断</summary><p>滚动额度 {d.scheduler.usedLastMinute}/{d.scheduler.limitPerMinute} · 研究子预算 {(d.storageBytes / 1048576).toFixed(2)}/{d.budgetBytes / 1048576} MiB</p>{d.scheduler.jobs.map(j => <p key={j.id}>{j.id} · 计划{j.dataset.refreshSeconds}秒 · 实际成功间隔{number(j.successIntervalSeconds)}秒 · 源延迟{number(j.sourceLagSeconds)}秒 · 排队{number(j.queueWaitSeconds)}秒 · 请求耗时{number(j.fetchSeconds)}秒 · 已见及时样本{j.timelySamples}/{j.observedSamples}{j.lastFailure && ` · 最近失败 ${j.lastFailure}`}</p>)}<p>成功请求不等于取得新分钟。自然分钟覆盖包含缺口，不能用已收到样本的及时率替代。</p></details>
    </>}
  </section>;
}

const noticeStates: Record<string, string> = { pending: "待发送", sent: "SMTP已接受（收件箱状态未知）", disabled: "邮件关闭，已记录", suppressed_before_enable: "早于本次启用，未发送", suppressed_restart: "重启前阶段，未补发", suppressed_stale: "超过三分钟，未补发", suppressed_inputs: "当前输入无效，未发送", suppressed_evidence: "证据路径不完整，未发送", suppressed_region: "价区已消退或失守，未发送", suppressed_existing_priority: "原有提醒优先，本次抑制", suppressed_rate_limit: "滚动小时限额抑制", unconfigured: "配置或收件验收未完成", unknown_after_restart: "重启前发送结果不确定，不重发", delivery_unknown: "发送结果不确定，不重发", rejected: "SMTP拒绝，不重发", failed_before_submission: "提交前失败，不自动重发" };
function NoticeLog({ notices }: { notices: Notice[] }) {
  return <details><summary>通知记录{notices.length ? ` · ${notices.length}` : " · 暂无"}</summary><p>每事件每阶段最多一次；抑制或超时不补发。SMTP接受与收件箱确认分别记录。</p>{notices.map(n => <article key={n.id}><p>{stamp(n.createdAt)} · {n.kind.endsWith("persistent") ? "持续增强" : n.kind.endsWith("absorption") ? "承接升级" : "收件测试"} · {noticeStates[n.status] || n.status}</p><details><summary>发送时间与去重依据</summary><p>启用 {stamp(n.enabledAt)} · 尝试 {stamp(n.attemptedAt)} · 结果 {stamp(n.completedAt)}</p><p>{n.templateVersion} · {n.id}</p></details></article>)}</details>;
}
function BookMailSettings({ initialOpen = false }: { initialOpen?: boolean }) {
  const q = useAPI<{ emailEnabled: boolean; configured: boolean; receiptVerified: boolean; enabledAt: string | null; mode: string }>("book-flow/settings", 30000);
  const [busy, setBusy] = useState(false), [message, setMessage] = useState(""), [code, setCode] = useState("");
  async function action(kind: "test" | "confirm" | "toggle") {
    setBusy(true); setMessage("");
    try {
      const result = await api<{ message?: string }>(kind === "toggle" ? "book-flow/settings" : "book-flow/mail-test", { method: kind === "toggle" ? "PUT" : "POST", body: JSON.stringify(kind === "toggle" ? { emailEnabled: !q.data?.emailEnabled } : { action: kind, code: kind === "confirm" ? code : "" }) });
      setMessage(result.message || (kind === "confirm" ? "收件验收已记录" : "邮件设置已保存")); q.refresh();
      if (kind === "confirm") setCode("");
    } catch (error) { setMessage(String(error)); } finally { setBusy(false); }
  }
  return <details open={initialOpen}><summary>两阶段邮件 · {q.error ? "状态未知" : q.data?.emailEnabled ? "已开启" : "未开启"}</summary><p>市场观察提醒，效果验证中。持续增强与承接升级分别提醒；新提醒每小时最多两封，全站最多六次发送尝试，同批价区合并，原有待发提醒优先。</p>{q.error && <p role="alert">{String(q.error)}</p>}<p>本次启用 {stamp(q.data?.enabledAt)} · 收件验收 {q.data?.receiptVerified ? "已确认" : "未完成"}。运行模式由发布配置管理。</p><div className="bf-controls"><button disabled={busy || !q.data?.configured} onClick={() => action("test")}>发送明确标记的测试邮件</button><label>收件验收码 <input aria-label="收件验收码" value={code} onChange={e => setCode(e.target.value)} autoComplete="off" maxLength={32} /></label><button disabled={busy || code.length !== 32} onClick={() => action("confirm")}>确认收到</button><button disabled={busy || !q.data || (!q.data.emailEnabled && (!q.data.receiptVerified || q.data.mode !== "run"))} onClick={() => action("toggle")}>{q.data?.emailEnabled ? "关闭新邮件" : "开启两阶段邮件"}</button></div>{message && <p role="status">{message}</p>}</details>;
}
