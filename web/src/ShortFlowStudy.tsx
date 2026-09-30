import { useAPI } from "./data";
import "./Signals.css";

type Outcome = { minutes: number; complete: number; incomplete: number; pending: number; medianReturnPercent: number | null; medianMfePercent: number | null; medianMaePercent: number | null };
export type ShortStudy = {
  rulesVersion: string; origin: string; at: string; from: string; days: number; coverage: number; observedWindows: number; expectedWindows: number;
  groups: { rule: string; direction: string; signals: number; priceEvents: number; excludedEvents: number; excludedSignals: number; early: number; following: number; missed: number; unmatched: number; pending: number; leadMinutesMedian: number | null; earlyRate: number | null; interval: number[] | null; reviewReady: boolean; outcomes: Outcome[] }[];
  gap: { at: string; reason: string; paused: boolean; count: number }; note: string;
};
const names: Record<string,string> = { "short-5":"5分钟脉冲", "short-10":"10分钟延续", formal:"现有正式规则", "price-breakout":"价格突破对照" };
const percent = (n: number | null) => n == null ? "—" : `${n.toFixed(2)}%`;
export function ShortStudyContent({ d }: { d?: ShortStudy | null }) {
  if (!d) return <p>短周期独立验证正在初始化。</p>;
  return <section className="short-study"><div className="flow-section-title"><h3>5／10分钟独立验证</h3><span>效果验证中 · 不改变正式提醒</span></div>
    <p>已观察 {d.days.toFixed(2)} 天 · 有效时间覆盖 {(d.coverage*100).toFixed(1)}%（{d.observedWindows}/{d.expectedWindows}）</p>
    <small>开始 {new Date(d.origin).toLocaleString("zh-CN",{timeZone:"Asia/Shanghai",hour12:false})} · 报告 {new Date(d.at).toLocaleString("zh-CN",{timeZone:"Asia/Shanghai",hour12:false})}</small>
    {d.gap.paused && <p className="flow-error">研究暂停：{d.gap.reason}</p>}
    {d.gap.count>0 && <p>累计记录 {d.gap.count} 次计算／存储异常，未补造缺失观察。</p>}
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
