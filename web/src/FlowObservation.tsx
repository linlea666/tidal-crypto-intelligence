import { amount, clock, price } from "./data";
import { modelScale } from "./liquidationMath";

export type FlowWindow = { minutes: number; from: string; to: string; coverage: number; netCents: number | null; buyCents: number | null; sellCents: number | null; volumeCents: number | null; buyShare: number | null; sellShare: number | null; volumeRatio: number | null };
export type FlowCheck = { name: string; passed: boolean; known: boolean; actual: number | null; required: number | null };
export type Observation = {
  rulesVersion: string; at: string; dataThrough: string; availableAt: string | null; fresh: boolean; ageSeconds: number; processingDelaySeconds: number | null;
  windows: Record<string, FlowWindow>; segments: FlowWindow[];
  hints: { minutes: number; direction: string; active: boolean; checks: FlowCheck[] }[];
  baseline: { valid: boolean; coverage: number; validDates: number; from: string; to: string; windows: Record<string, { buyP95Cents: number | null; sellP95Cents: number | null; medianVolumeCents: number | null }> };
  prices: Record<string, { from: string; to: string; returnPercent: number | null; displacementAtr: number | null }>;
  priorAtr1h: number | null;
  zones: { id: string; side: string; low: number; high: number; quote: string; strength: string; relativeStrength: number; distancePercent: number; fetchedAt: string; referencePrice: number }[];
  zoneNote: string; coverageRequested: string; coverageNote: string; researchPaused: boolean; researchReason: string;
};
const stamp = (s: string) => new Date(s).getFullYear() > 2000 ? new Date(s).toLocaleString("zh-CN", { timeZone: "Asia/Shanghai", hour12: false }) : "尚未取得";
const pct = (n?: number | null) => n == null ? "未知" : `${n > 0 ? "+" : ""}${n.toFixed(2)}%`;
const tone = (n?: number | null) => n == null || n === 0 ? "neutral" : n > 0 ? "buy" : "sell";
const money = (n?: number | null) => n == null ? "缺失" : amount(n, true);

export function WindowCard({ w, title, stale = false }: { w?: FlowWindow; title: string; stale?: boolean }) {
  const age = w ? Math.max(0, (Date.now() - new Date(w.to).getTime()) / 60000) : null;
  return <article className={`flow-number ${stale ? "neutral flow-stale" : tone(w?.netCents)}`}>
    <span>{title} · {!w ? "等待窗口数据" : w.netCents == null ? "窗口不完整" : w.netCents > 0 ? "主动净买入" : w.netCents < 0 ? "主动净卖出" : "主动买卖相当"}</span>
    <strong title={w?.netCents == null ? undefined : `${(w.netCents / 100).toLocaleString("zh-CN", { maximumFractionDigits: 2 })} USD`}>{w?.netCents == null ? "—" : amount(Math.abs(w.netCents))}<small>{w?.netCents != null && "USD"}</small></strong>
    <p>{!w ? "覆盖未知" : w.volumeRatio != null ? `同周期量比 ${w.volumeRatio.toFixed(2)} 倍` : w.netCents == null ? `时间覆盖 ${(w.coverage * 100).toFixed(0)}%` : "同周期量比待基线"}</p>
    <p>买入 {w?.buyShare == null ? "未知" : `${w.buyShare.toFixed(1)}%`} · 卖出 {w?.sellShare == null ? "未知" : `${w.sellShare.toFixed(1)}%`}</p>
    {w && <small>{clock(w.from)}–{clock(w.to)} · {age?.toFixed(1)}分钟前截止{stale && " · 历史窗口"}</small>}
  </article>;
}

export function Continuity({ rows, title, stale = false }: { rows: FlowWindow[]; title: string; stale?: boolean }) {
  const max = Math.max(1, ...rows.map(w => Math.abs(w.netCents ?? 0)));
  return <section className={`flow-continuity ${stale ? "flow-stale" : ""}`}><h3>{title}</h3><p>独立区间，不重复累加滚动窗口</p><div className="flow-strips">
    {rows.map(w => <div className="flow-strip" key={w.from}><span>{clock(w.from)}–{clock(w.to)}</span><div className="flow-track"><i className={stale ? "neutral" : tone(w.netCents)} style={{ width: `${Math.abs(w.netCents ?? 0) / max * 50}%`, left: w.netCents != null && w.netCents < 0 ? `${50 - Math.abs(w.netCents) / max * 50}%` : "50%" }} /></div><b className={stale ? "neutral" : tone(w.netCents)}>{money(w.netCents)}</b></div>)}
  </div><small>← 净卖出　|　净买入 → · USD</small></section>;
}

export function checkValue(c: FlowCheck): string {
  if (!c.known) return "未知";
  if (c.actual == null || c.required == null) return c.passed ? "达标" : "未达标";
  if (/规模|净额|P9/.test(c.name)) return `${money(c.actual)} / ${money(c.required)} USD`;
  return `${Number(c.actual.toFixed(2))} / ${Number(c.required.toFixed(2))}${c.name.includes("占比") ? "%" : ""}`;
}

export function FlowObservation({ s, failed }: { s?: Observation | null; failed: boolean }) {
  if (!s) return <p className="empty">5／10分钟观察正在初始化；原正式判断仍独立运行。</p>;
  const fresh = !failed && s.fresh && Date.now() - new Date(s.dataThrough).getTime() <= 300000 && Date.now() - new Date(s.at).getTime() <= 90000;
  const hints = fresh ? (s.hints ?? []).filter(h => h.active) : [];
  const netHour = s.windows["60"]?.netCents;
  return <section className="short-flow" aria-label="5／10分钟独立成交观察">
    <div className="flow-section-title"><h3>刚刚发生了什么</h3><span>{fresh ? "已闭合成交窗口" : "窗口滞后 · 暂停新提示"} · 独立观察，不发送邮件</span></div>
    <p className="short-flow-clock">统一截止 {stamp(s.dataThrough)} · 生成 {clock(s.at)}{s.availableAt && ` · 最新5分钟组成数据入库 ${clock(s.availableAt)}`}</p>
    <div className="flow-numbers flow-short-numbers">{[5,10].map(m => <div className="short-flow-column" key={m}><WindowCard w={s.windows[String(m)]} title={`最近${m}分钟`} stale={!fresh}/><p className="short-price">同期价格 {pct(s.prices[String(m)]?.returnPercent)} · 位移 {s.prices[String(m)]?.displacementAtr == null ? "未知" : `${s.prices[String(m)].displacementAtr?.toFixed(2)} ATR`}</p></div>)}</div>
    <div className="short-flow-hints" aria-live="polite">
      {!fresh ? <p>数据截止超过5分钟或读取延迟，金额保留为历史参考。</p> : hints.length ? hints.map(h => <p className={h.direction === "buy" ? "buy" : "sell"} key={`${h.minutes}-${h.direction}`}><strong>{h.minutes}分钟{h.direction === "buy" ? "买入" : "卖出"}{h.minutes === 5 ? "脉冲" : "延续观察"}</strong> · 效果验证中{netHour != null && netHour !== 0 && (netHour > 0) !== (h.direction === "buy") && " · 与1小时方向相反"}</p>) : <p>{s.baseline.valid ? "当前5／10分钟未满足独立观察条件；没有提示不代表没有行情。" : "同周期30天基线正在补齐，先展示完整成交金额。"}</p>}
      {s.researchPaused && <p className="flow-error">独立验证暂停：{s.researchReason || "研究容量或写入保护"}。页面金额继续更新。</p>}
    </div>
    <div className="flow-numbers flow-long-numbers">{[["15","最近15分钟"],["60","最近1小时"],["240","最近4小时"]].map(([key,title]) => <WindowCard key={key} w={s.windows[key]} title={title} stale={!fresh}/>)}</div>
    <p className="flow-explainer">5／10／15分钟相互重叠，金额不可相加。主动买卖差额不等于新增资金。价格缺失时不阻塞成交展示；ATR来自观察窗口之前已完成的小时。</p>
    <Continuity rows={s.segments ?? []} title="最近30分钟：六段独立5分钟" stale={!fresh}/>
    <details className="flow-raw"><summary>短周期条件、数据覆盖与邻近清算区域</summary>
      <p>以下为 {clock(s.at)} 生成时的条件{!fresh && "，当前已过期"}。</p>
      <p>{s.coverageRequested} · {s.coverageNote}</p>
      <p>基线覆盖 {(s.baseline.coverage * 100).toFixed(1)}% · {s.baseline.validDates}个有效日期；{s.baseline.from && stamp(s.baseline.from)} 至 {s.baseline.to && stamp(s.baseline.to)}</p>
      <div className="flow-checks">{(s.hints ?? []).map(h => <section key={`${h.minutes}-${h.direction}`}><h4>{h.minutes}分钟{h.direction === "buy" ? "买入" : "卖出"}</h4><ul>{h.checks.map(c => <li key={c.name}>{c.known ? c.passed ? "✓" : "·" : "?"} {c.name} · {checkValue(c)}</li>)}</ul></section>)}</div>
      <p>{s.zoneNote}</p>{(s.zones ?? []).map(z => <p key={z.id}>{z.side === "short" ? "上方空头风险区" : "下方多头风险区"} {price(z.low)}–{price(z.high)} {z.quote} · 距参考价 {z.distancePercent.toFixed(2)}% · {modelScale(z.strength)} <strong>模型强度·非美元</strong> · 获取 {clock(z.fetchedAt)}</p>)}
      <a href="#liquidations">查看双向清算地图</a>
    </details>
  </section>;
}
