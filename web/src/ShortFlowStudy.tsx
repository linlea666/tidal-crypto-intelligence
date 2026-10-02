import { useAPI } from "./data";
import "./Signals.css";
import { ShortFlowHealth, type ShortRuntime } from "./ShortFlowHealth";

type Outcome = { minutes: number; complete: number; incomplete: number; pending: number; medianReturnPercent: number | null; medianMfePercent: number | null; medianMaePercent: number | null };
export type ShortStudy = {
  rulesVersion: string; origin: string; at: string; from: string; days: number; coverage: number; observedWindows: number; expectedWindows: number;
  groups: { rule: string; direction: string; signals: number; priceEvents: number; excludedEvents: number; excludedSignals: number; early: number; following: number; missed: number; unmatched: number; pending: number; leadMinutesMedian: number | null; earlyRate: number | null; interval: number[] | null; reviewReady: boolean; outcomes: Outcome[] }[];
  gap: { at: string; reason: string; paused: boolean; count: number }; note: string;
  gapReasons?: Record<string, number>; diagnostics?: ShortRuntime | null;
};
const names: Record<string,string> = { "short-5":"5分钟脉冲", "short-10":"10分钟延续", formal:"现有正式规则", "price-breakout":"价格突破对照" };
const reasons: Record<string,string> = { legacy_unknown: "旧记录原因未知", unrecorded_unknown: "未记录窗口，原因未知", research_paused: "研究暂停", stale_flow: "成交滞后", baseline_unavailable: "基线未就绪或覆盖不足", origin_boundary: "跨研究起点", missing_5m: "5分钟不完整", missing_10m: "10分钟不完整", missing_60m: "1小时不完整" };
const percent = (n: number | null) => n == null ? "—" : `${n.toFixed(2)}%`;
export function ShortStudyContent({ d }: { d?: ShortStudy | null }) {
  if (!d) return <p>短周期独立验证正在初始化。</p>;
  return <section className="short-study"><div className="flow-section-title"><h3>5／10分钟独立验证</h3><span>效果验证中 · 不改变正式提醒</span></div>
    <p>已观察 {d.days.toFixed(2)} 天 · 有效时间覆盖 {(d.coverage*100).toFixed(1)}%（{d.observedWindows}/{d.expectedWindows}）</p>
    <small>开始 {new Date(d.origin).toLocaleString("zh-CN",{timeZone:"Asia/Shanghai",hour12:false})} · 报告 {new Date(d.at).toLocaleString("zh-CN",{timeZone:"Asia/Shanghai",hour12:false})}</small>
    {d.gap.paused && <p className="flow-error">研究暂停：{d.gap.reason}</p>}
    {d.gap.count>0 && <p>原全期累计记录 {d.gap.count} 次任务异常；与缺失窗口数分别统计。</p>}
    <p>本报告未有效覆盖 {Math.max(0,d.expectedWindows-d.observedWindows)} 个窗口；原研究起点和全期覆盖未重置。</p>
    {d.gapReasons && <details className="flow-raw"><summary>观察缺口原因</summary>{Object.entries(d.gapReasons).map(([key,n]) => <p key={key}>{reasons[key] ?? "原因未知"}：{n} 个窗口</p>)}<small>同一窗口可能存在多个阻断原因，不可相加。未记录的窗口不事后猜测原因。</small></details>}
    <ShortFlowHealth d={d.diagnostics}/>
    <p>{d.note}</p><div className="short-study-grid">{["buy","sell"].map(side => <section key={side}><h4>{side === "buy" ? "买方" : "卖方"}独立对照</h4>{d.groups.filter(g => g.direction===side).map(g => <details key={g.rule}><summary>{names[g.rule] ?? g.rule} · {g.signals}个观察 / {g.priceEvents}个可比行情</summary>
      <p>领先 {g.early} · 跟随 {g.following} · 漏检 {g.missed} · 未匹配 {g.unmatched} · 待匹配 {g.pending}；另有 {g.excludedEvents} 个行情、{g.excludedSignals} 个观察因覆盖不足排除。</p>
      <p>已匹配领先时间中位数：{g.leadMinutesMedian == null ? "暂无样本" : `${g.leadMinutesMedian.toFixed(1)}分钟`}</p>
      {g.reviewReady && g.earlyRate!=null && g.interval ? <p>样本领先率 {(g.earlyRate*100).toFixed(1)}% · 95%区间 {(g.interval[0]*100).toFixed(1)}–{(g.interval[1]*100).toFixed(1)}%，效果验证中。</p> : <p>达到14天、95%覆盖及本方向30个独立事件后再展示比例。</p>}
      <div className="short-outcomes">{g.outcomes.map(o => <div key={o.minutes}><strong>{o.minutes===240 ? "4小时" : `${o.minutes}分钟`}</strong><span>完整 {o.complete} / 缺失 {o.incomplete} / 待完成 {o.pending}</span><span>方向位移中位数 {percent(o.medianReturnPercent)}</span><span>最大有利 / 不利位移中位数 {percent(o.medianMfePercent)} / {percent(o.medianMaePercent)}</span></div>)}</div>
    </details>)}</section>)}</div></section>;
}
export function ShortFlowStudy() {
  const q=useAPI<{shortTerm: ShortStudy | null}>("studies?asset=BTC",60000);
  return <details className="flow-method"><summary>短周期效果验证：提前多少、漏掉多少</summary>{q.error && <p role="alert">读取失败，保留的报告不是最新结果。</p>}<ShortStudyContent d={q.data?.shortTerm}/></details>;
}
