import { useState } from "react";

type Bounds = { lower: string; upper: string };
type Case = {
  id: string; zone: { side: string; low: string; high: string }; direction: string;
  state: string; quality: string; rulesVersion: string; boundary: string;
  frozenAt: string; from: string; through: string; trackThrough: string;
  confirmedDate: string; priceSource: string; legacy: boolean;
};
export type OnchainUpgrade = {
  capabilities: { structure: boolean; dailyPrice: boolean; fourHourPrice: boolean; note: string };
  contract: { version: string; cohorts: string; denominator: string; completedDayLabel: string; ranks: string };
  dailyPrice: { date: string; value: string; firstSeen: string; validatedAt: string | null; intervalEnd: string } | null;
  marketPrice: { value: string; at: string; source: string } | null;
  fourHourReference: { value: string; closeAt: string; fx: string; fxAt: string; source: string; note: string };
  cases: Case[];
  decomposition: { from: string; to: string; windowEffect: Bounds; distributionEffect: Bounds; total: Bounds; note: string } | null;
  nextAction: string;
};
const number = (v: string | number | undefined | null) => v == null || v === "" ? "—" : Number(v).toLocaleString("zh-CN", { maximumFractionDigits: 3 });
const time = (v: string | undefined | null) => !v || v.startsWith("0001") ? "—" : new Date(v).toLocaleString("zh-CN", { timeZone: "Asia/Shanghai", hour12: false });
const range = (v: Bounds) => v.lower === v.upper ? number(v.lower) : `${number(v.lower)}–${number(v.upper)}`;
const stateName: Record<string, string> = { watching: "等待未来收盘", pending: "等待相邻UTC日确认", confirmed: "两日收盘条件已成立", invalidated: "条件失效" };
export function OnchainReview({ data, historical, showCases }: { data?: OnchainUpgrade; historical: boolean; showCases: boolean }) {
  const [distanceOrder, setDistanceOrder] = useState(false);
  if (!data) return null;
  const live = data.marketPrice && Number(data.marketPrice.value);
  const cases = [...data.cases].sort((a, b) => distanceOrder && live ? Math.abs(Number(a.boundary) - live) - Math.abs(Number(b.boundary) - live) : a.frozenAt.localeCompare(b.frozenAt));
  return <section className="cost-section cost-review" aria-label="分层状态与价格口径">
    {!historical && <>
      <div className="cost-summary">
        <article><span>链上结构</span><strong>{data.capabilities.structure ? "可评估新结构" : "新结构等待数据"}</strong><small>已有冻结区独立跟踪</small></article>
        <article><span>正式日线价格</span><strong>{data.capabilities.dailyPrice ? "日收盘可用" : "当前无法判断"}</strong><small>同源收盘；不静默切换</small></article>
        <article><span>4小时站内关注</span><strong>{data.capabilities.fourHourPrice ? "参考结束价可用" : "等待价格与有效汇率"}</strong><small>只进站内，不发送邮件</small></article>
        <article><span>验证进度</span><strong>前向证据积累中</strong><small>14日为主期限；不预设有效</small></article>
      </div>
      <p className="cost-notice">{data.nextAction}</p>
    </>}
    <details><summary>价格时间与来源口径</summary>
      <p>同源完成日：{data.dailyPrice?.date ?? "缺失"} · ${number(data.dailyPrice?.value)}；对应收盘 {time(data.dailyPrice?.intervalEnd)}（北京时间）。</p>
      <p>首次取得 {time(data.dailyPrice?.firstSeen)} · 校验可用 {time(data.dailyPrice?.validatedAt)}。旧记录未保存的校验时间保持未知。</p>
      {!historical && <><p>当前美元折算参考：${number(data.marketPrice?.value)} · {time(data.marketPrice?.at)} · {data.marketPrice?.source ?? "缺失"}</p><p>4小时参考：${number(data.fourHourReference.value)} · {time(data.fourHourReference.closeAt)}；汇率 {number(data.fourHourReference.fx)}，采样 {time(data.fourHourReference.fxAt)}。{data.fourHourReference.note}</p></>}
      <p>{data.contract.cohorts}。实体调整和特殊地址排除未核实。</p>
      <p>{data.contract.denominator}。{data.contract.completedDayLabel}</p><p>{data.contract.ranks}</p>
    </details>
    <details><summary>集中度变化分解 · 上下各5%</summary>
      {data.decomposition ? <><p>{data.decomposition.from} → {data.decomposition.to}</p><p>价格窗口移动：{range(data.decomposition.windowEffect)} 个百分点；供给分布／分母变化：{range(data.decomposition.distributionEffect)} 个百分点；合计变化：{range(data.decomposition.total)} 个百分点。</p><small>{data.decomposition.note}</small></> : <p>缺少可比的真实前一日快照，不补造变化。</p>}
    </details>
    {showCases && !historical && <div className="cost-frozen"><div className="cost-toolbar"><h3>冻结情景 · 确认后独立跟踪</h3><button onClick={() => setDistanceOrder(!distanceOrder)}>{distanceOrder ? "按冻结时间查看" : "按距当前价排序"}</button></div>
      {!cases.length && <p>等待首个可用观察区；初始化不追认旧条件。</p>}
      {cases.map(c => <details className="cost-event" key={c.id}><summary><strong>{c.direction === "down" ? "向下边界" : "向上边界"} ${number(c.boundary)}</strong><span>{c.quality === "waiting" ? "等待冻结后的首次日收盘" : c.quality === "available" ? stateName[c.state] ?? c.state : "当前无法判断（保留历史状态）"}</span></summary>
        <p>冻结成本区 ${number(c.zone.low)}–${number(c.zone.high)} · {c.zone.side === "inside" ? "包含快照价格" : c.zone.side === "above" ? "快照价格上方" : "快照价格下方"}</p>
        <p>冻结 {time(c.frozenAt)}；发现日期 {c.from}–{c.through} UTC；{c.trackThrough ? `确认后跟踪至 ${c.trackThrough} UTC` : "尚未开始确认后跟踪"}。</p>
        <p>当前参考价距边界 {live ? number((live / Number(c.boundary) - 1) * 100) + "%" : "未知"}。完成日收盘返回边界以内或等于边界时失效。</p>
        <small>{c.rulesVersion} · {c.priceSource}{c.legacy ? " · 旧版原边界继续跟踪，新版统计不混入" : ""}</small>
      </details>)}
    </div>}
  </section>;
}
