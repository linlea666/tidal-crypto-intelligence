import { useState } from "react";
import { Chart } from "./Chart";
import { clock, useAPI } from "./data";
import "./AlertAudit.css";

type Meaning = { publishedLevel: string; laterLevel: string; labels: string[]; note: string };
type Input = { dataThrough: string; net1hUsd: string | null; net4hUsd: string | null; priceUsdt: string | null; priorAtrUsdt: string | null; directionalDisplacementAtr: string | null; confirmationAt: string | null; confirmationDataThrough: string | null; confirmationLineUsdt: string };
type Decision = { id: string; at: string; accepted: boolean; reasons: string[]; input: Input };
type Outcome = { start: string; referenceUsdt: string | null; note: string; outcomes: { minutes: number; state: string; coverage: number; returnPercent: string | null; mfePercent: string | null; maePercent: string | null; mfeBarAt?: string | null; maeBarAt?: string | null }[] };
type Paper = { id: string; group: string; actionableAt: string; enteredAt: string; exitedAt: string | null; entryPrice: string; netPnl: string | null; fees: string; exitReason: string; quality: string[]; fundingPending: boolean; fills: { id: string; at: string; kind: string; price: string; quantity: string }[] };
type Item = { id: string; at: string; direction: string; dataThrough?: string; priceUsdt?: string | null; confirmedAt?: string | null; computedAt?: string | null; confirmationComputedAt?: string | null; coreInputFirstSeenAt?: string | null; coreInputAvailableAt?: string | null; initialInput?: { net1hUsd: string | null; net4hUsd: string | null; displacementAtr: string | null }; meaning?: Meaning; publicationClock?: { firstReadableAt: string }; decisions?: Decision[]; candidates?: Decision[]; notifications?: { id: string; status: string; kind: string; completedAt: string | null; error?: string }[]; candidateOutcome?: Outcome | null; observation?: Outcome; paper?: Paper[]; trials?: (Outcome & { minutes: string })[] };
type Event = { at: string; kind: string; parentId: string; text: string; group?: string; priceUsdt: string | null };
type Audit = { at: string; origin: string | null; rulesVersion: string; runtime: { lastSuccessAt: string | null; lastFailureAt: string | null; lastError: string }; storageBytes: number; budgetBytes: number; items: Item[]; events: Event[]; prices: { at: string; closeUsdt: string | null }[]; more: boolean; qualityMore?: boolean; note: string; timelineNote?: string };
const stamp = (s?: string | null) => s ? new Date(s).toLocaleString("zh-CN", { timeZone: "Asia/Shanghai", hour12: false }) : "未知 / 当时未记录";
const number = (v?: string | null, digits = 2) => v == null ? "未知" : Number(v).toLocaleString("zh-CN", { maximumFractionDigits: digits });
const level: Record<string, string> = { ordinary: "普通异动", large: "规模较大", supported: "多因素支持" };
const reasons: Record<string, string> = { not_new_formal_publication: "不属于新起点后的正式首次发布", parent_expired: "原事件已到期", own_breakout_unconfirmed: "本事件的两根完整5分钟突破尚未确认", stale_input: "输入不新鲜", flow_window_missing: "1h / 4h资金窗口缺失", flow_direction_conflict: "1h与4h主动资金没有同时支持该方向", price_atr_missing: "价格或此前ATR缺失", extended_move: "同向位移已超过1.5 ATR" };
const eventName: Record<string, string> = { formal: "首次异动", confirmation: "已发生突破", notification: "通知结果", candidate: "影子候选", fill: "模拟成交", quality: "采集质量", intake: "模拟消费" };
const bjtInput = (at: number) => new Date(at + 8 * 3600_000).toISOString().slice(0, 16);

function Outcomes({ value }: { value: Outcome }) {
  return <div className="audit-outcomes"><p>{value.note} · 观察起点 {stamp(value.start)} · 参考价 {number(value.referenceUsdt)} USDT</p>{value.outcomes.map(o => <p key={o.minutes}>{o.minutes / 60}小时：{o.state === "pending" ? "尚未成熟" : o.state === "complete" ? "窗口完整" : "窗口不完整"} · 覆盖 {(o.coverage * 100).toFixed(1)}% · 方向收益 {number(o.returnPercent, 3)}% · 最大有利 {number(o.mfePercent, 3)}% / 不利 {number(o.maePercent, 3)}%{o.maeBarAt && ` · 最不利5分钟区间始于 ${stamp(o.maeBarAt)}`}{o.mfeBarAt && ` · 最有利5分钟区间始于 ${stamp(o.mfeBarAt)}`}</p>)}</div>;
}

export function AlertAudit() {
  const [from, setFrom] = useState(() => bjtInput(Date.now() - 48 * 3600_000));
  const [to, setTo] = useState(() => bjtInput(Date.now()));
  const [range, setRange] = useState("");
  const [layer, setLayer] = useState("");
  const [side, setSide] = useState("");
  const [offset, setOffset] = useState(0);
  const [rangeError, setRangeError] = useState("");
  const q = useAPI<Audit>(`alert-audit?limit=10&offset=${offset}&layer=${layer}&direction=${side}${range}`, 20000), d = q.data;
  const events = d?.events ?? [];
  const chart = { tooltip: { trigger: "axis" }, grid: { left: 58, right: 18, top: 25, bottom: 38 }, xAxis: { type: "time", axisLabel: { formatter: (v: number) => clock(new Date(v).toISOString()) } }, yAxis: { type: "value", scale: true, splitLine: { lineStyle: { color: "#293833" } } }, series: [{ name: "现货5分钟收盘", type: "line", showSymbol: false, connectNulls: false, lineStyle: { color: "#b9d8ca", width: 2 }, data: d?.prices.map(p => [p.at, p.closeUsdt == null ? null : Number(p.closeUsdt)]), markLine: { symbol: "none", label: { show: false }, lineStyle: { type: "dashed", color: "#84968d", opacity: .55 }, data: events.filter(e => e.kind !== "notification" && e.kind !== "quality" && e.kind !== "intake").slice(0, 100).map(e => ({ xAxis: e.at, name: eventName[e.kind], lineStyle: { color: e.kind === "fill" ? "#ff8f82" : e.kind === "candidate" ? "#75e9ad" : "#84968d" } })) } }] };
  const submitRange = () => {
    const f = new Date(`${from}:00+08:00`), t = new Date(`${to}:00+08:00`);
    if (!Number.isFinite(+f) || !Number.isFinite(+t) || +t <= +f || +t - +f > 7 * 86400_000 || +t > Date.now() + 60_000) { setRangeError("请选择已发生、最多7天的时间范围。"); return; }
    setRangeError(""); setOffset(0); setRange(`&from=${encodeURIComponent(f.toISOString())}&to=${encodeURIComponent(t.toISOString())}`);
  };
  return <section className="alert-audit" aria-label="本轮行情复盘">
    <div className="flow-section-title"><h3>本轮行情复盘</h3><span>影子研究 · 不开仓</span></div>
    <p>风险观察、正式异动、已发生突破与交易候选分层记录。普通买盘或卖压异动不代表反转，也不构成多空开仓指令。</p>
    <div className="audit-layers"><article><b>01 · 风险观察</b><p>5/10分钟买盘增强、卖压升温。站内展示，不发邮件。</p></article><article><b>02 · 正式异动与确认</b><p>沿用原规则与邮件。首次发布和后续确认分别计时。</p></article><article><b>03 · 影子交易候选</b><p>自身突破、1h/4h同向、位移≤1.5 ATR、数据完整才准入。只作前向研究。</p></article></div>
    <p className="audit-origin">候选规则 {d?.rulesVersion ?? "读取中"} · 起点 {stamp(d?.origin)} · 最近成功 {stamp(d?.runtime.lastSuccessAt)}</p>
    {d?.runtime.lastFailureAt && <p className="flow-error">最近候选研究失败 {stamp(d.runtime.lastFailureAt)}：{d.runtime.lastError}。本次运行的最近失败不会被成功清除；研究未登记不代表没有行情。</p>}
    {d && d.storageBytes >= d.budgetBytes * .95 && <p className="flow-error">候选存储接近保护线，保留原记录；容量不足时停止登记。</p>}
    <div className="audit-controls"><label>开始（北京时间）<input aria-label="复盘开始时间" type="datetime-local" value={from} onChange={e => setFrom(e.target.value)} /></label><label>结束（北京时间）<input aria-label="复盘结束时间" type="datetime-local" value={to} onChange={e => setTo(e.target.value)} /></label><button onClick={submitRange}>查看范围</button><button className="text-button" onClick={() => { setRange(""); setFrom(bjtInput(Date.now() - 48 * 3600_000)); setTo(bjtInput(Date.now())); setOffset(0); }}>最近48小时</button><label>层级<select aria-label="预警层级" value={layer} onChange={e => { setLayer(e.target.value); setOffset(0); }}><option value="">正式事件及关联记录</option><option value="risk">风险观察</option><option value="formal">正式异动</option><option value="confirmation">突破已确认</option><option value="candidate">影子候选</option><option value="rejected">候选拒绝记录</option></select></label><label>方向<select aria-label="预警方向" value={side} onChange={e => { setSide(e.target.value); setOffset(0); }}><option value="">全部</option><option value="buy">买盘</option><option value="sell">卖压</option></select></label></div>
    {(q.error || rangeError) && <p role="alert" className="flow-error">{rangeError || `读取失败：${q.error}，上一份结果不代表当前状态。`}</p>}
    {!!d?.prices.length && <><Chart option={chart} height={245} label="复盘价格与当前页关联事件时间；现货价格缺口不连线" /><small>{d.timelineNote}</small></>}
    {!!events.length && <details><summary>展开时间轴：首次提醒、确认、通知与实际成交</summary><ol className="audit-events">{events.map((e, i) => <li key={`${e.parentId}-${e.kind}-${i}`}><time>{stamp(e.at)}</time><b>{eventName[e.kind]}</b><span>{e.group === "risk" ? "固定风控组 · " : e.group === "opposite" ? "反向退出组 · " : ""}{e.text}{e.priceUsdt != null && ` · ${number(e.priceUsdt)} USDT`}</span></li>)}</ol></details>}
    {d?.qualityMore && <p>此范围质量事件超过200条，时间轴展示最早200条；请缩小时间范围继续核对，未展示部分不视为正常。</p>}
    <div className="audit-records">{d?.items.map(i => <article key={i.id}>
      <div className="flow-section-title"><h4 className={i.direction === "buy" ? "buy" : "sell"}>{i.direction === "buy" ? "买盘" : "卖压"}{layer === "risk" ? "风险观察" : "正式异动"}</h4><time>{stamp(i.at)}</time></div>
      {i.meaning && <><p>首次发布：{level[i.meaning.publishedLevel] ?? "未知"}{i.meaning.laterLevel && ` · 后来升级：${level[i.meaning.laterLevel] ?? i.meaning.laterLevel}`}</p><div className="audit-tags">{i.meaning.labels.map(v => <span key={v}>{v}</span>)}</div><p>{i.meaning.note}</p></>}
      {i.initialInput && <p>当时1h主动净成交 {number(i.initialInput.net1hUsd)} USD · 4h {number(i.initialInput.net4hUsd)} USD · 1h价格位移 {number(i.initialInput.displacementAtr)} ATR</p>}
      {i.trials?.map(t => <div key={t.minutes}><b>{t.minutes}分钟风险观察</b><Outcomes value={t} /></div>)}
      {i.candidates && <p>{i.candidates.length ? `影子候选产生于 ${stamp(i.candidates[0].at)}，不得回填到首次异动。` : i.decisions?.length ? "本事件尚无合格候选，拒绝原因见下方。" : "当时未建立本版候选研究记录；不事后补造。"}</p>}
      <details><summary>核对输入、真实时点与拒绝原因</summary>
        <dl className="audit-clocks"><dt>成交数据闭合</dt><dd>{stamp(i.dataThrough)}</dd><dt>核心输入首次取得上界</dt><dd>{stamp(i.coreInputFirstSeenAt)}</dd><dt>所选修订可用</dt><dd>{stamp(i.coreInputAvailableAt)}</dd><dt>正式计算完成</dt><dd>{stamp(i.computedAt)}</dd><dt>提交后首次读取证实</dt><dd>{stamp(i.publicationClock?.firstReadableAt)}</dd><dt>突破确认</dt><dd>{stamp(i.confirmedAt)}</dd><dt>确认计算完成</dt><dd>{stamp(i.confirmationComputedAt)}</dd></dl>
        {i.decisions?.map(v => <div className="audit-decision" key={v.id}><b>{stamp(v.at)} · {v.accepted ? "符合影子候选条件" : "未准入"}</b><p>{v.reasons.map(x => reasons[x] ?? x).join("；") || "全部固定条件满足"}</p><small>数据截止 {stamp(v.input.dataThrough)} · 1h / 4h主动净成交 {number(v.input.net1hUsd)} / {number(v.input.net4hUsd)} USD · 同向位移 {number(v.input.directionalDisplacementAtr)} ATR · 原固定线 {number(v.input.confirmationLineUsdt)} USDT</small></div>)}
        {i.notifications?.map(n => <p key={n.id}>{n.kind === "confirmed" ? "突破确认邮件" : "首次异动邮件"}：{n.status === "sent" ? "SMTP已接受" : n.status} · {stamp(n.completedAt)}{n.error && ` · ${n.error}`}</p>)}<small>SMTP接受不等于收件箱签收。缺失的历史时点保持未知。</small>
      </details>
      {i.observation && <details><summary>现货方向观察：收益、反弹及回撤</summary><Outcomes value={i.observation} /></details>}
      {i.candidateOutcome && <Outcomes value={i.candidateOutcome} />}
      {!!i.paper?.length && <details><summary>实际模拟记录：含异常退出与费用</summary>{i.paper.map(p => <div className="audit-decision" key={p.id}><b>{p.group === "risk" ? "B 固定风控" : "A 反向信号"}</b><p>首次消费 {stamp(p.actionableAt)} · 开仓 {stamp(p.enteredAt)} / {number(p.entryPrice)} USDT · 退出 {stamp(p.exitedAt)}</p><p>退出原因 {p.exitReason === "data_gap" ? "数据中断退出" : p.exitReason || "仍持有"} · 净收益 {number(p.netPnl, 4)} USDT · 手续费 {number(p.fees, 4)} USDT{p.fundingPending && " · 资金费待取得，完整净收益未知"}</p><p>路径标记：{p.quality.length ? p.quality.join("、") : "未记录异常"}</p>{p.fills.map(f => <small key={f.id}>{stamp(f.at)} {f.kind === "open" ? "开仓" : "平仓"} {number(f.quantity, 6)} BTC / {number(f.price)} USDT<br /></small>)}</div>)}<p>异常交易保留在总账；不能用它们替代完整路径的策略效果。</p></details>}
      <small className="audit-id">事件 {i.id}</small>
    </article>)}</div>
    {d && !d.items.length && <p>此范围和筛选下没有已登记记录。缺少记录不等于无行情；旧事件不补成候选前向成绩。</p>}
    <div className="audit-pagination"><button disabled={offset === 0} onClick={() => setOffset(Math.max(0, offset - 10))}>上一页</button><span>第 {offset / 10 + 1} 页 · 每页至多10条</span><button disabled={!d?.more} onClick={() => setOffset(offset + 10)}>下一页</button></div>
    <p className="audit-method">效果固定区分行情覆盖与提前量、1/4小时方向观察、包含异常的账户总账、完整路径成本后表现。两组模拟继续使用原起点和参数；候选为新假设。每组30天、100笔完整平仓、多空各30笔、至少95%覆盖之前不作效果判断。</p>
  </section>;
}
