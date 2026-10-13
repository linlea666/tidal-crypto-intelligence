import { useEffect, useState } from "react";
import { ArrowRight } from "@phosphor-icons/react";
import { amount, useAPI } from "./data";
import type { Activity } from "./Activity";
import type { Asset } from "./types";
import type { BookFlowData, BookFlowItem } from "./BookFlow";
import { buildBrief, comparable, eventStage, featuredZones, keyZones, liveBook, liveMeta, liveVenue, type KeyZone } from "./marketReading";
import "./MarketBrief.css";

export type BriefTarget = { tab: string; eventId?: string; section?: string };
const number = (n: number | string | null | undefined, digits = 2) => n == null || !Number.isFinite(Number(n)) ? "未知" : Number(n).toLocaleString("zh-CN", { maximumFractionDigits: digits });
export const briefTime = (at?: string | null) => at && Number.isFinite(Date.parse(at)) ? new Date(at).toLocaleString("zh-CN", { timeZone: "Asia/Shanghai", hour12: false, month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit" }) : "未知";
const usd = (n: number | null) => n == null ? "美元金额未知" : `${number(n / 10000)} 万美元`;
function useClock() {
  const [, setTick] = useState(0);
  useEffect(() => { const timer = setInterval(() => setTick(tick => tick + 1), 5000); return () => clearInterval(timer); }, []);
  return Date.now();
}
function DetailButton({ children, onClick }: { children: React.ReactNode; onClick: () => void }) {
  return <button className="brief-link" onClick={onClick}>{children}<ArrowRight size={16} aria-hidden="true" /></button>;
}

export function KeyZones({ data, failed = false, onDetails }: { data: BookFlowData | null; failed?: boolean; onDetails?: () => void }) {
  const now = useClock(), [expanded, setExpanded] = useState(false);
  const rows = keyZones(data, failed, now);
  const max = Math.max(1, ...rows.map(r => r.usd ?? 0));
  const references = [...new Map(rows.map(r => [r.venue, r])).values()];
  function row(r: KeyZone) {
    const valid = comparable(r.zone);
    return <article className={`brief-zone ${r.zone.side}`} key={`${r.venue}/${r.zone.side}/${r.zone.low}`}>
      {r.usd != null && <div className="brief-zone-fill" style={{ width: `${Math.max(0, Math.min(100, r.usd / max * 100))}%` }} aria-hidden="true" />}
      <div className="brief-zone-title"><strong>{number(r.zone.low, 0)}–{number(r.zone.high, 0)} <small>USDT</small></strong><strong>{usd(r.usd)}</strong></div>
      <p>{r.venue} · {r.zone.side === "buy" ? "买单" : "卖单"} · {r.distance === 0 ? "参考价位于区域内" : `距本所参考价 ${number(r.distance)}%`}</p>
      <b>{valid ? r.zone.enhanced ? "挂单达到增强条件，成交验证另看事件" : "本次未达到挂单增强条件" : "比较所需历史不足，暂不判断增强"}</b>
      <small>{valid ? `相对基准 ${number(r.zone.multiple)} 倍 · 数量增量折算 ${r.zone.increaseUsd == null ? "未知" : usd(Number(r.zone.increaseUsd))}` : `已取得可比分钟 ${number(r.zone.baselineCoverage * 100, 1)}% · 未知不代表没有挂单`}</small>
      <small>盘口源时间 {briefTime(r.sourceAt)} · {Math.floor((now - Date.parse(r.sourceAt)) / 1000)} 秒前</small>
    </article>;
  }
  return <div className="brief-zones">
    <p className="brief-muted">先看价格附近的变化。默认每侧至多两处，增强区域优先、再看距离；相同来源的重叠区域不重复选入。</p>
    {!rows.length ? <div className="brief-empty">{failed ? "挂单读取失败，暂停当前价区判断。" : "暂无有效价区；等待当前盘口与研究时钟就绪。"} 缺少数据不等于没有挂单。</div> : <>
      <div className="brief-side-label sell">上方／附近卖单 · 尚未成交的卖出意愿</div>
      <div className="brief-zone-grid">{(expanded ? rows.filter(r => r.zone.side === "sell") : featuredZones(rows, "sell")).map(row)}</div>
      {!rows.some(r => r.zone.side === "sell") && <p>当前可读范围没有卖方价区，不能推断为零。</p>}
      <div className="brief-price-line"><span>当前参考价</span>{references.map(r => <strong key={r.venue}>{r.venue} {number(r.reference)} <small>USDT</small></strong>)}</div>
      <div className="brief-side-label buy">下方／附近买单 · 尚未成交的买入意愿</div>
      <div className="brief-zone-grid">{(expanded ? rows.filter(r => r.zone.side === "buy") : featuredZones(rows, "buy")).map(row)}</div>
      {!rows.some(r => r.zone.side === "buy") && <p>当前可读范围没有买方价区，不能推断为零。</p>}
      <button className="brief-link" aria-expanded={expanded} onClick={() => setExpanded(!expanded)}>{expanded ? "收起其余价区" : `展开全部 ${rows.length} 个来源价区`}</button>
    </>}
    <p className="brief-muted">横条只比较挂单美元金额，共用线性刻度。价区可能重叠，不相加；不同交易所不视为同一笔资金。美元换算未知时不画金额条。</p>
    {onDetails && <DetailButton onClick={onDetails}>查看价区完整证据</DetailButton>}
  </div>;
}

export function EventDigest({ item, current, onOpen }: { item: BookFlowItem; current: boolean; onOpen?: () => void }) {
  const now = useClock(), e = item.event, stage = eventStage(item, current, now);
  const updates = e.updates.filter(u => Date.parse(u.publishedAt) <= now);
  const persistent = updates.find(u => u.kind === "persistent");
  const absorption = updates.find(u => u.kind === "absorption" && u.evidence?.before != null && u.evidence?.after != null && u.evidence.depthRetention != null);
  const following = updates.find(u => u.kind === "following");
  return <article className={`brief-event ${e.direction}`}>
    <div className="brief-event-head"><strong>{e.venue} · {e.direction === "buy" ? "买方" : "卖方"} {number(e.zone.low, 0)}–{number(e.zone.high, 0)} USDT</strong><span>{stage.label}</span></div>
    <p>{stage.note}</p>
    <ol className="brief-steps" aria-label="已经登记的证据阶段">
      <li><b>挂单增加</b><span>{briefTime(e.publishedAt)}</span></li>
      <li className={!persistent ? "pending" : ""}><b>后续仍保持</b><span>{persistent ? briefTime(persistent.publishedAt) : e.ended ? "未登记" : "尚未登记"}</span></li>
      <li className={!absorption ? "pending" : ""}><b>{e.direction === "buy" ? "买方承接线索" : "卖方抛压线索"}</b><span>{absorption ? briefTime(absorption.publishedAt) : "尚无完整成交验证"}</span></li>
    </ol>
    {following && <p>另有主动{e.direction === "buy" ? "买入" : "卖出"}跟进 · {briefTime(following.publishedAt)}，独立于承接阶段。</p>}
    <small>这是已发生的观察，不是必然完成的步骤。挂单减少、区域失守或断流都可能结束跟踪。</small>
    {onOpen && <DetailButton onClick={onOpen}>查看这条事件的原始证据与后续结果</DetailButton>}
  </article>;
}

export function MarketBrief({ asset, hours, onHours, onNavigate }: { asset: Asset; hours: number; onHours: (h: number) => void; onNavigate: (target: BriefTarget) => void }) {
  const now = useClock();
  const aq = useAPI<Activity>(`activity?asset=${asset}&hours=${hours}&range=5`, 30000);
  const bq = useAPI<BookFlowData>(asset === "BTC" ? "book-flow?asset=BTC&limit=20" : null, 30000);
  const mq = useAPI<{ emailEnabled: boolean; receiptVerified: boolean; enabledAt: string | null }>(asset === "BTC" ? "book-flow/settings" : null, 60000);
  const a = aq.data, b = bq.data, v = buildBrief(a, b, !!aq.error, !!bq.error, now);
  const bookTarget = (section: string) => onNavigate({ tab: "book-flow", section });
  const groups = ["enhanced", "persistent", "absorption", "following"];
  const groupLabels = ["首次增加", "持续确认", "承接／抛压", "主动跟进"];
  const stages = b?.report.groups ?? [];
  const bgOK = !aq.error && !!a && now - Date.parse(a.at) >= 0 && now - Date.parse(a.at) <= 45000;
  return <section className="market-brief" aria-label={`${asset}市场解读`}>
    <header className="brief-header"><div><span className="brief-eyebrow">{asset} · 看懂市场正在发生什么</span><h2>市场解读</h2><p>先看结论，再看价区、证据进展与后续验证。</p></div><label>成交观察窗口<select value={hours} onChange={e => onHours(Number(e.target.value))}>{[1, 4, 24].map(h => <option key={h} value={h}>最近{h}小时</option>)}</select></label></header>
    {(aq.error || bq.error) && <p role="alert" className="brief-alert">{aq.error && "成交读取失败，暂停成交方向判断。"}{bq.error && "挂单读取失败，暂停挂单判断。"}已取得的历史记录保留。</p>}
    <section className={`brief-verdict ${v.direction}`} aria-labelledby="brief-verdict-title">
      <span className="brief-eyebrow">01 / 当前判断 · 市场观察，效果验证中</span>
      <h3 id="brief-verdict-title">{!a && aq.loading ? "正在读取市场证据…" : v.headline}</h3>
      <p className="brief-window">{a ? `${briefTime(a.from)} — ${briefTime(a.to)} · 北京时间 · 截至闭合5分钟` : "成交窗口尚未取得"}</p>
      <div className="brief-reasons">
        <div><b>实际成交</b><p>{v.flowOK ? `已覆盖成交中，主动买入 ${number(a!.buyShare, 1)}% · ${a!.reason}` : "当前成交或价格窗口不足，暂不判断谁更主动。"}</p><DetailButton onClick={() => onNavigate({ tab: "snapshot" })}>看同一窗口成交依据</DetailButton></div>
        <div><b>价格反馈</b><p>{v.flowOK && a?.priceChangePercent != null ? `同窗价格 ${a.priceChangePercent > 0 ? "+" : ""}${number(a.priceChangePercent)}%。${a.pressureReaction}` : "同窗价格反馈待确认，不用其他窗口补作结论。"}</p></div>
        <div><b>价区挂单</b><p>{asset !== "BTC" ? "ETH尚未启用挂单承接研究，不借用BTC结果。" : v.bookLine}</p>{asset === "BTC" && <DetailButton onClick={() => bookTarget("events")}>看挂单事件</DetailButton>}</div>
      </div>
      <div className="brief-next"><b>接下来关注</b><p>{v.next}</p></div>
      <p className="brief-muted">成交描述与挂单快照保留各自时点；相同数据的多个指标不重复计分。结论不表示上涨概率或开仓建议。</p>
    </section>
    <div className="brief-quality"><span>成交有效分钟 <b>{a ? `${number(a.flow.coverage * 100, 1)}%` : "未知"}</b>{a && !v.flowOK ? " · 当前不可判方向" : ""}</span>{asset === "BTC" && <span>核心两家采集 <b>{b && v.bookOK ? b.runStatus.collectionAcceptance.coverage == null ? "未知" : `${number(b.runStatus.collectionAcceptance.coverage * 100, 1)}%` : "待确认"}</b> · 已冻结 {b && v.bookOK ? `${b.runStatus.collectionAcceptance.observedMinutes}/60` : "未知"} 分钟</span>}<span>历史有效性 <b>按样本另行审查</b></span></div>
    {asset === "BTC" && <>
      <section className="brief-section"><div className="brief-section-head"><div><span className="brief-eyebrow">02 / 重点看哪里</span><h3>价格附近的关键挂单</h3></div><span>{b && v.bookOK ? b.runStatus.label : "当前状态待确认"}</span></div><KeyZones data={b} failed={!!bq.error} onDetails={() => bookTarget("regions")} /></section>
      <section className="brief-section"><span className="brief-eyebrow">03 / 证据走到哪一步</span><h3>正在发生与最近结束的观察</h3><p className="brief-muted">{b ? `近48小时最新${Math.min(b.items.length, 3)}条登记，按首次发布时间排列；不代表全市场所有挂单。` : "登记范围尚未读取，不能将未知计为零条。"}阶段不会自动升级。</p>
        {!b?.items.length && <div className="brief-empty">{!b ? "事件数据尚未取得。" : b.runStatus.state === "warming" ? `正在预热，预计${briefTime(b.runStatus.warmupEndsAt)}结束；以实际运行状态为准。` : b.mode !== "run" ? "研究尚未启动，没有前向事件。" : "当前范围没有已登记观察；这不代表市场没有变化。"}</div>}
        {b?.items.slice(0, 3).map(item => <EventDigest key={item.event.id} item={item} current={v.bookOK && b.mode === "run" && b.runStatus.state === "running" && liveVenue(b, item.event.venue, now)} onOpen={() => onNavigate({ tab: "book-flow", eventId: item.event.id, section: "events" })} />)}
        <div className="brief-actions"><DetailButton onClick={() => bookTarget("events")}>查看全部观察与筛选</DetailButton><DetailButton onClick={() => bookTarget("mail")}>{mq.error || !mq.data ? "查看提醒状态" : mq.data.emailEnabled ? "两阶段提醒已开启" : mq.data.receiptVerified ? "收件已确认 · 提醒关闭" : "设置两阶段提醒"}</DetailButton></div>
      </section>
    </>}
    <section className="brief-section"><span className="brief-eyebrow">04 / 背景与分歧</span><h3>合约与其他资金背景分开看</h3><div className="brief-background">
      <article><b>合约持仓变化</b><p>{bgOK && liveMeta(a?.derivatives.meta, now) && a?.derivatives.oiChangeCents != null ? `${amount(a.derivatives.oiChangeCents, true)} USD` : "当前窗口不足，暂不解释"}</p><small>最近{hours}小时端点；持仓变化同时包含双方，不能直接认定开多或开空。</small><DetailButton onClick={() => onNavigate({ tab: "snapshot" })}>查看同窗合约背景</DetailButton></article>
      <article><b>正式异动与观察</b><p>{asset === "BTC" ? "正式预警使用自己的确认规则" : "正式异动研究仅支持BTC"}</p><small>与某条挂单同时出现不代表因果；从事件详情查看同期关联。</small>{asset === "BTC" && <DetailButton onClick={() => onNavigate({ tab: "signals" })}>查看正式异动预警</DetailButton>}</article>
      <article><b>钱包与慢频背景</b><p>按各自来源与更新时间查看</p><small>钱包快照不是成交；链上与ETF数据不混进当前分钟判断。</small><DetailButton onClick={() => onNavigate({ tab: "wallet" })}>查看钱包趋势与时间范围</DetailButton></article>
    </div></section>
    {asset === "BTC" && <section className="brief-section"><span className="brief-eyebrow">05 / 过去表现怎样</span><h3>每个阶段，单独验证</h3><p>{!b ? "研究结果尚未取得。" : b.report.truncated ? "当前报告已截断，不能给出全量判断。" : stages.some(g => g.eligible) ? "部分阶段已达到审查门槛，仍需审查，不能直接认定有效。" : "当前样本或质量未达到审查门槛，暂不能判断观察是否有效。"}</p>
      {stages.some(g => g.completeEvidence.independent4h > 0) ? <div className="brief-study-grid">{groups.map((stage, index) => { const g = stages.find(s => s.stage === stage); return <article key={stage}><b>{groupLabels[index]}</b><p>独立完整4小时样本 <strong>{b ? g?.completeEvidence.independent4h ?? 0 : "未知"}</strong></p><small>同一完整证据样本均值 {g?.completeEvidence.meanPercent == null ? "待形成" : `${number(g.completeEvidence.meanPercent, 3)}%`}</small><small>重叠观察另列，不累计放大样本。</small></article>; })}</div> : <p className="brief-empty">{b ? `已登记 ${stages.find(g => g.stage === "enhanced")?.all ?? 0} 次首次增强，暂无成熟且证据完整的独立4小时样本。结果仍在积累，暂不展示收益成绩卡。` : "等待读取前向研究记录，未知不显示为零。"}</p>}
      <p className="brief-muted">至少30天、100个独立成熟完整样本、多空各30、及时覆盖95%才可阶段审查。这里是现货价格观察，不含交易成本；模拟账户另看。</p><div className="brief-actions"><DetailButton onClick={() => bookTarget("results")}>看分阶段结果、缺口与统计范围</DetailButton><DetailButton onClick={() => onNavigate({ tab: "studies" })}>看正式规则历史复盘</DetailButton><DetailButton onClick={() => onNavigate({ tab: "paper" })}>看模拟账户与成本</DetailButton></div>
    </section>}
    <details className="brief-diagnostics"><summary>这张看板的范围、时间与判断方式</summary><p>解读规则 market-reading-v1，只解释现有事实，不登记新信号、不改变研究或邮件门槛。</p><p>成交响应 {briefTime(a?.at)} · 原成交规则 {a?.rulesVersion || "未知"}；挂单响应 {briefTime(b?.at)} · 研究最近处理 {briefTime(b?.lastProcessedAt)}。页面时间为北京时间。</p><p>核心两家覆盖与页面顶部全站五家盘口覆盖分别计算。采集正常、事件证据完整、历史有效性是三件不同的事。</p>{b && <p>研究起点 {briefTime(b.origin)} · 规则 {b.rulesVersion} · 采集版本 {b.collectionVersion}</p>}{asset === "BTC" && <DetailButton onClick={() => bookTarget("diagnostics")}>查看契约、失败历史与采集诊断</DetailButton>}</details>
  </section>;
}
