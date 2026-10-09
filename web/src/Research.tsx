import { CollectionCohorts, type CollectionCohort } from "./CollectionCohorts";
import { useState } from "react";
import { Chart } from "./Chart";
import { api, amount, price, useAPI } from "./data";
import { Metric, type Meta } from "./Pages";
import type { Asset } from "./types";
import { ShortStudyContent, type ShortStudy } from "./ShortFlowStudy";

const stamp = (s?: string | null) =>
  s && new Date(s).getFullYear() > 2000
    ? new Date(s).toLocaleString("zh-CN", {
        timeZone: "Asia/Shanghai",
        hour12: false,
      })
    : "尚未获取";
const labels: Record<string, string> = {
 freezing: "正在冻结研究输入",
  unfinished: "交易日未结束",
  non_trading_day: "非交易日",
  unreported: "尚未披露",
  unreconciled: "基金合计待核对",
  zero_unconfirmed: "零值待确认",
  reported: "已报告 · 可能修订",
  missing: "缺少数据",
  anomaly: "资金异动",
  confirmed: "价格已确认",
  weakened: "动向减弱",
  expired: "已到期",
  queued: "等待补采",
  collecting: "正在补采",
  partial_queue: "部分任务待排队",
  incomplete: "数据不足 · 研究未完成",
  complete: "关联研究已计算",
  partial: "部分可用",
  unavailable: "当前窗口不可用",
  untried: "尚未尝试",
  hourly_available: "小时对比可用",
  calculated: "策略关联已计算",
  calculating: "正在分批计算，可恢复进度",
};
const axis = {
  axisLabel: { color: "#9dafaa" },
  splitLine: { lineStyle: { color: "#283932" } },
  axisLine: { lineStyle: { color: "#385146" } },
};
function bars(
  points: [string, number | null][],
  unit: string,
  neutral = false,
) {
  return {
    grid: { left: 65, right: 25, top: 25, bottom: 60 },
    tooltip: {
      trigger: "axis",
      valueFormatter: (v: unknown) =>
        v == null ? "数据缺失" : `${price(Number(v), 2)} ${unit}`,
    },
    xAxis: {
      ...axis,
      type: "category",
      data: points.map((p) => p[0]),
      axisLabel: { color: "#9dafaa", hideOverlap: true },
    },
    yAxis: { ...axis, type: "value", name: unit },
    dataZoom: [{ type: "inside" }, { type: "slider", height: 18, bottom: 5 }],
    series: [
      {
        type: "bar",
        data: points.map(([_, v]) => ({
          value: v,
          itemStyle: {
            color: neutral
              ? Number(v) >= 0
                ? "#b7b9ef"
                : "#8190b4"
              : Number(v) >= 0
                ? "#81e9af"
                : "#ee7168",
          },
        })),
      },
    ],
  };
}
function LoadError({ error }: { error: string }) {
  return error ? (
    <p role="alert" className="error-banner">
      {error}
    </p>
  ) : null;
}
function Provenance({ meta }: { meta?: Meta }) {
  return (
    <p className="helper">
      来源：CoinGlass代理 · 数据时点 {stamp(meta?.observedAt)} · 获取{" "}
      {stamp(meta?.fetchedAt)} ·{" "}
      {!meta
        ? "正在读取本地数据"
        : meta?.status === "retrieval_only"
          ? "来源发布时间未知"
          : meta?.status === "stale"
            ? "获取状态过期，保留历史供查看"
            : meta?.status === "missing"
              ? "等待契约检查及首次数据"
              : "定时更新"}
    </p>
  );
}
type Change = {
  from: string;
  to: string;
  hours: number;
  delta: string | null;
  percent: number | null;
  venues: string[];
  excluded: string[];
};
type Wallet = {
  asset: Asset;
  meta: Meta;
  historyMeta: Meta;
  balanceTotal: string | null;
  covered: number;
  balances:
    | {
        venue: string;
        balance: string | null;
        change1d?: string;
        change7d?: string;
        change30d?: string;
      }[]
    | null;
  changes: Change[];
  trends: Record<string, Change | null>;
  note: string;
};
export function WalletPage({ asset }: { asset: Asset }) {
  const q = useAPI<Wallet>(`wallet-trends?asset=${asset}`, 60000);
  const d = q.data;
  const [days, setDays] = useState(30);
  const points = (d?.changes ?? []).slice(-days);
  return (
    <section className="research-page">
      <div className="research-heading">
        <div>
          <span className="eyebrow">低频观察 · 决策权重 0</span>
          <h2>交易所钱包余额净变化</h2>
          <p>币移出了已识别的钱包，不等于已经买入。先看覆盖，再看变化。</p>
        </div>
        <label>
          观察范围{" "}
          <select value={days} onChange={(e) => setDays(+e.target.value)}>
            {[7, 30, 90].map((n) => (
              <option key={n} value={n}>
                {n}个历史时点
              </option>
            ))}
          </select>
        </label>
      </div>
      <LoadError error={q.error} />
      <div className="metric-strip">
        <Metric
          title="当前列表已覆盖余额"
          value={d?.balanceTotal != null ? price(+d.balanceTotal, 2) : "—"}
          unit={asset}
        />
        <Metric
          title="列表有效交易所"
          value={d ? String(d.covered) : "—"}
          unit="家"
        />
        <Metric title="数据用途" value="背景观察" unit="不触发预警" />
      </div>
      <Provenance meta={d?.meta} />
      <div className="research-trends">
        {[1, 3, 7, 30].map((n) => {
          const r = d?.trends[String(n)];
          return (
            <div className="research-tile" key={n}>
              <span>约{n}天可比变化</span>
              <strong>
                {r?.delta != null ? price(+r.delta, 2) : "—"}
                <small> {asset}</small>
              </strong>
              <p>
                {r
                  ? `${r.hours.toFixed(1)}小时 · ${r.venues.length}家共同来源`
                  : "窗口不足"}
              </p>
              {!!r?.excluded.length && (
                <small className="amber">排除：{r.excluded.join("、")}</small>
              )}
            </div>
          );
        })}
      </div>
      <Chart
        option={bars(
          points.map((p) => [stamp(p.to), p.delta == null ? null : +p.delta]),
          asset,
          true,
        )}
        height={300}
        label="共同覆盖钱包余额净变化，正负不代表利多利空"
      />
      <p className="helper">
        紫色代表余额增加，灰蓝代表减少；没有买卖方向含义。每根柱的实际间隔可能不同。
        {d?.note}
      </p>
      <Provenance meta={d?.historyMeta} />
      <details className="data-section">
        <summary>查看每个时点的覆盖与实际间隔</summary>
        <div className="table-scroll">
          <table>
            <thead>
              <tr>
                <th>截止时间（北京时间）</th>
                <th>实际间隔</th>
                <th>余额净变化</th>
                <th>共同来源 / 排除</th>
              </tr>
            </thead>
            <tbody>
              {[...points].reverse().map((r) => (
                <tr key={r.to}>
                  <td>{stamp(r.to)}</td>
                  <td>{r.hours.toFixed(1)}小时</td>
                  <td>
                    {r.delta == null ? "—" : `${price(+r.delta, 2)} ${asset}`}
                  </td>
                  <td>
                    {r.venues.length}家 / {r.excluded.join("、") || "无"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </details>
      <section className="data-section">
        <h3>各交易所当前余额</h3>
        <div className="table-scroll">
          <table>
            <thead>
              <tr>
                <th>来源</th>
                <th>余额 {asset}</th>
                <th>上游1天变化</th>
                <th>上游7天变化</th>
                <th>上游30天变化</th>
              </tr>
            </thead>
            <tbody>
              {d?.balances?.map((r) => (
                <tr key={r.venue}>
                  <td>{r.venue}</td>
                  {[r.balance, r.change1d, r.change7d, r.change30d].map(
                    (v, i) => (
                      <td key={i}>{v == null ? "未返回" : price(+v, 2)}</td>
                    ),
                  )}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <p className="helper">
          已合并确认的交易所别名。列表变化保留平台原值，与上方按共同来源重算的窗口口径分别展示。
        </p>
      </section>
    </section>
  );
}
type ETFPoint = {
  record: {
    date: string;
    flowUsd: string | null;
    funds: Record<string, string | null>;
    reconciled: boolean;
  };
  state: string;
};
type ETFData = {
  meta: Meta;
  points: ETFPoint[];
  latestReportedDate: string;
  totals: Record<string, string | null>;
  streak: number;
  direction: number;
  note: string;
};
export function ETFPage({ asset }: { asset: Asset }) {
  const q = useAPI<ETFData>(`etf?asset=${asset}`, 60000);
  const d = q.data;
  const [selected, setSelected] = useState("");
  const latest = d?.points.find((p) => p.record.date === d.latestReportedDate);
  const current = d?.points.find((p) => p.record.date === selected) ?? latest;
  const [days, setDays] = useState(20);
  return (
    <section className="research-page">
      <div className="research-heading">
        <div>
          <span className="eyebrow">独立观察 · 不参与日内信号</span>
          <h2>{asset} ETF资金</h2>
          <p>了解最近已报告交易日的资金方向，避免将尚未披露看成零流动。</p>
        </div>
        <label>
          显示{" "}
          <select value={days} onChange={(e) => setDays(+e.target.value)}>
            {[5, 20, 60].map((n) => (
              <option key={n} value={n}>
                {n}个报告时点
              </option>
            ))}
          </select>
        </label>
      </div>
      <LoadError error={q.error} />
      <div className="metric-strip">
        <Metric
          title={`最新已报告 ${d?.latestReportedDate || "等待披露"}`}
          value={
            latest?.record.flowUsd != null
              ? amount(+latest.record.flowUsd * 100, true)
              : "—"
          }
        />
        <Metric
          title="近5个交易日累计"
          value={
            d?.totals["5"] != null ? amount(+d.totals["5"] * 100, true) : "—"
          }
        />
        <Metric
          title="近20个交易日累计"
          value={
            d?.totals["20"] != null ? amount(+d.totals["20"] * 100, true) : "—"
          }
        />
        <Metric
          title={
            d?.direction === 1
              ? "连续流入"
              : d?.direction === -1
                ? "连续流出"
                : "连续情况"
          }
          value={d?.latestReportedDate ? String(d.streak) : "—"}
          unit="交易日"
        />
      </div>
      <Provenance meta={d?.meta} />
      <Chart
        option={bars(
          (d?.points ?? [])
            .slice(-days)
            .map((p) => [
              p.record.date,
              p.state === "reported" && p.record.flowUsd != null
                ? +p.record.flowUsd / 1e6
                : null,
            ]),
          "百万USD",
        )}
        height={310}
        label="ETF每日已报告净流向，未披露处留空"
      />
      <p className="helper">
        {d?.note}{" "}
        北京时间每天08:30采集；披露后的修订在下次采集反映。交易日按美国现金股票市场日历判断。
      </p>
      <div className="activity-columns">
        <section className="data-section">
          <h3>逐日报告</h3>
          <div className="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>交易日</th>
                  <th>净流向 USD</th>
                  <th>状态</th>
                </tr>
              </thead>
              <tbody>
                {[...(d?.points ?? [])]
                  .reverse()
                  .slice(0, days)
                  .map((p) => (
                    <tr key={p.record.date}>
                      <td>
                        <button
                          className="text-button"
                          onClick={() => setSelected(p.record.date)}
                        >
                          {p.record.date}
                        </button>
                      </td>
                      <td>
                        {p.state === "reported" && p.record.flowUsd != null
                          ? amount(+p.record.flowUsd * 100, true)
                          : "—"}
                      </td>
                      <td>{labels[p.state] ?? p.state}</td>
                    </tr>
                  ))}
              </tbody>
            </table>
          </div>
        </section>
        <section className="data-section">
          <h3>{current?.record.date ?? "所选交易日"} · 基金贡献</h3>
          {current &&
            Object.entries(current.record.funds).map(([ticker, v]) => (
              <div className="fund-row" key={ticker}>
                <span>{ticker}</span>
                <strong>
                  {current.state === "reported" && v != null
                    ? amount(+v * 100, true)
                    : "待确认"}
                </strong>
              </div>
            ))}
          {!current && <p className="empty">等待有效披露数据</p>}
          <p className="helper">
            基金分项与总额核对后才计入汇总；周末、缺失日和全零待确认记录不补零。
          </p>
        </section>
      </div>
    </section>
  );
}
export { SignalsPage } from "./Signals";
type Experiment = {
  name: string;
  samples: number;
  favorable: number;
  adverse: number;
  ambiguous: number;
  incomplete: number;
  timedOut: number;
  coverage: number;
  rate: number | null;
  interval: [number, number];
};
type RuleComparison = {
	  direction?: string;
  rulesVersion: string; signals: number; priceEpisodes: number; early: number;
  following: number; missed: number; unmatchedSignals: number; pendingSignals?: number;
  matches: { eventId: string; signalId?: string; timing: string; leadMinutes: number | null }[];
  delayMinutes?: number; return1hMedian?: number | null; return4hMedian?: number | null;
  mfe4hMedian?: number | null; mae4hMedian?: number | null;
};
function ComparisonTable({items}: {items: RuleComparison[]}) {
  const pct = (n?: number | null) => n == null ? "—" : `${n.toFixed(2)}%`;
  return <div className="table-scroll"><table><thead><tr><th>规则 / 延迟</th><th>提醒 / 行情</th><th>提前 / 跟随 / 漏报</th><th>未匹配 / 待完成</th><th>1h / 4h中位</th><th>4h有利 / 不利波动</th></tr></thead><tbody>
    {items.map((r,i)=><tr key={i}><td>{r.direction ? `${r.direction === "sell" ? "卖方" : "买方"} · ` : ""}{r.rulesVersion} / {r.delayMinutes ?? 0}分钟</td><td>{r.signals} / {r.priceEpisodes}</td><td>{r.early} / {r.following} / {r.missed}</td><td>{r.unmatchedSignals} / {r.pendingSignals ?? 0}</td><td>{pct(r.return1hMedian)} / {pct(r.return4hMedian)}</td><td>{pct(r.mfe4hMedian)} / {pct(r.mae4hMedian)}</td></tr>)}
  </tbody></table></div>;
}
type StudyItem = {
 parentStudyId?: string; inputSnapshotId?: string; inputFrozenAt?: string; inputIntegrity?: string; terminalReason?: string;
  validationId?: string;
  id: string;
  asset: Asset;
  from: string;
  to: string;
  state: string;
  mode: string;
  updatedAt: string;
  error?: string;
  jobs: string[];
  result: {
	  multifactorComparison?: { state: string; calculatedThrough: string; note: string; groups: { name: string; phase: string; commonWindows: number; trials: RuleComparison[]; equalBudget: RuleComparison[] }[] };
    candidateComparison?: { commonWindows: number; development: RuleComparison[]; holdout: RuleComparison[]; auxiliary?: RuleComparison[]; auxiliaryWindows?: number; equalBudget?: RuleComparison[]; dailyBudget?: Record<string, number>; note: string } | null;
    strategyState?: string;
    caseState?: string;
    coverage?: {
      dataset: string;
      resolutionSeconds: number;
      from: string;
      to: string;
      cursor: string;
      state: string;
      reason: string;
      errorKind: string;
      purpose: string;
      gaps: { from: string; to: string; reason: string }[];
    }[];
    flowCoverage: number;
    candleCoverage: number;
    events: {
      at: string;
      direction: string;
      phase: string;
      outcome: {
        barrier: string;
        return1h: number | null;
        return4h: number | null;
        return24h: number | null;
      };
    }[];
    experiments: Experiment[];
    delays: Experiment[];
    missing: string[];
    notes: string[];
    cases: {
      reconciliation?: { comparedHours: number; conflictHours: number; maxNetDifferenceCents: number; note: string };
      date: string;
      observations: number | null;
      note: string;
      state?: string;
      flowNote?: string;
      errors?: string[];
      coverage?: {
        expectedHours: number;
        flowHours: number;
        fineFlowHours: number;
        priceHours: number;
        oiHours: number;
        premiumHours: number;
      };
      wallet: Change[];
      hourly: {
        end: string;
        netCents: number | null;
        priceChange: number | null;
        priceClose?: number | null;
        oiChange?: number | null;
        premiumUsd?: number | null;
        flowResolutionSeconds?: number;
      }[];
    }[];
  } | null;
};
function FrozenValidation({id, asset}: {id: string; asset: Asset}) {
  const [open, setOpen] = useState(false);
  return <details onToggle={(e) => setOpen(e.currentTarget.open)}>
    <summary>查看冻结评估快照</summary>
    <p className="helper">{id}。后续事实保留期变化不改写此证据；新完成的修订需重新审查。</p>
    {open && <FrozenValidationResult id={id} asset={asset} />}
  </details>;
}
function FrozenValidationResult({id, asset}: {id: string; asset: Asset}) {
  const q = useAPI<{calculatedAt: string; from: string; to: string; inputVersion: string; comparison: NonNullable<NonNullable<StudyItem["result"]>["candidateComparison"]>}>(`study-validations/${encodeURIComponent(id)}?asset=${asset}`, 60000);
  const c = q.data?.comparison;
  return <div className="data-section">
    <LoadError error={q.error} />
    {!q.data && !q.error && <p>正在读取冻结评估…</p>}
    {q.data && <p className="helper">{stamp(q.data.from)} → {stamp(q.data.to)} · 计算于 {stamp(q.data.calculatedAt)} · 输入版本 {q.data.inputVersion}</p>}
    {c && <>
      <p className="helper">{c.note}</p>
      <h4>冻结的独立留出</h4><ComparisonTable items={c.holdout} />
      {!!c.auxiliary?.length && <><h4>合约背景对照</h4><ComparisonTable items={c.auxiliary} /></>}
      {!!c.equalBudget?.length && <><h4>相同提醒数量对照</h4><ComparisonTable items={c.equalBudget} /></>}
      <details><summary>冻结的开发期与延迟对照</summary><ComparisonTable items={c.development} /></details>
    </>}
  </div>;
}
export function StudiesPage({ asset }: { asset: Asset }) {
  const q = useAPI<{
    items: StudyItem[];
    shortTerm?: ShortStudy | null;
    forward?: {
	  multifactor?: { byCollection?: CollectionCohort[]; days: number; coverage: number; episodes: number; reviewReady: boolean; comparisons: RuleComparison[]; equalBudget: RuleComparison[]; note: string };
      candidateDays?: number; candidateCoverage?: number; candidateEpisodes?: number;
      candidateReady?: boolean; comparisons?: RuleComparison[];
      evaluationVersion?: string; matches?: RuleComparison["matches"];
      days?: number;
      coverage?: number;
      eligible?: number;
      signals?: number;
      confirmed?: number;
      early?: number;
      following?: number;
      missed?: number;
      ready: boolean;
      note: string;
    };
  }>(`studies?asset=${asset}`, 30000);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [id, setId] = useState("");
  const [created, setCreated] = useState<StudyItem | null>(null);
  const items = created && !q.data?.items.some((s) => s.id === created.id) ? [created, ...(q.data?.items ?? [])] : (q.data?.items ?? []);
  const d = items.find((s) => s.id === id) ?? items[0];
  const start = async (parentStudyId?: string) => {
    setBusy(true);
    setError("");
    try {
      const s = await api<StudyItem>("studies", {
        method: "POST",
        body: JSON.stringify({ asset, parentStudyId }),
      });
      setCreated(s);
      setId(s.id);
      q.refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : "研究创建失败");
    } finally {
      setBusy(false);
    }
  };
  return (
    <section className="research-page">
      {asset === "BTC" && <div className="flow-dashboard"><ShortStudyContent d={q.data?.shortTerm}/></div>}
      <div className="research-heading">
        <div>
          <span className="eyebrow">验证先于权重 · 不自动调参</span>
          <h2>历史复盘与对照实验</h2>
          <p>
            先验证数据完整性，再比较加入指标是否改善结果。指定行情只作案例。
          </p>
        </div>
        <button
          className="action"
          disabled={busy || asset !== "BTC"}
          onClick={() => start()}
        >
          {busy ? "正在排队…" : "创建90天研究"}
        </button>
      </div>
      <LoadError error={error || q.error} />
      {d && <section className="data-section"><h3>研究输入与版本</h3><p>输入状态：{d.inputIntegrity || (d.inputSnapshotId ? "正在冻结" : "尚未冻结")}</p>{d.inputFrozenAt && <p>采集截止 {stamp(d.inputFrozenAt)} · 快照 {d.inputSnapshotId}</p>}{d.parentStudyId && <p>原研究 {d.parentStudyId}</p>}{d.terminalReason && <p>{d.terminalReason}</p>}<p className="helper">缺失范围保留；冻结结果不随滚动清理或后续补采改变。历史关联、行情覆盖率与交易胜率分别统计。</p><button className="action" disabled={busy || asset !== "BTC"} onClick={() => start(d.id)}>基于当前数据创建新版本</button></section>}

	  {q.data?.forward?.multifactor && <section className="data-section"><h3>新双向规则 · {q.data.forward.multifactor.reviewReady ? "达到阶段审查样本门槛" : "效果验证中"}</h3><p>{q.data.forward.multifactor.days.toFixed(1)} 天 / {(q.data.forward.multifactor.coverage*100).toFixed(1)}%覆盖 / {q.data.forward.multifactor.episodes}个独立行情事件</p><p className="helper">{q.data.forward.multifactor.note}</p><details><summary>分买卖方向查看前向与延迟对照</summary><ComparisonTable items={q.data.forward.multifactor.comparisons}/><h4>相同提醒数量对照</h4><ComparisonTable items={q.data.forward.multifactor.equalBudget ?? []}/></details><CollectionCohorts title="正式前向" items={q.data.forward.multifactor.byCollection}/><small>沿用原前向共同有效窗口；该分组不改变原研究起点或门槛。</small></section>}
      <section className="data-section">
        <h3>
          实时旁路观察 ·{" "}
          {q.data?.forward?.ready
            ? "采样达到门槛，仍需评估信号样本"
            : "尚未完成14天验证"}
        </h3>
        <div className="metric-strip">
          <Metric
            title="已观察"
            value={(q.data?.forward?.days ?? 0).toFixed(1)}
            unit="天"
          />
          <Metric
            title="合格窗口覆盖"
            value={((q.data?.forward?.coverage ?? 0) * 100).toFixed(1)}
            unit="%"
          />
          <Metric
            title="提前 / 跟随 / 未提醒事件"
            unit="次"
            value={`${q.data?.forward?.early ?? 0} / ${q.data?.forward?.following ?? 0} / ${q.data?.forward?.missed ?? 0}`}
          />
        </div>
        <p className="helper">{q.data?.forward?.note}</p><p className="helper">行情覆盖的分母是行情事件；方向收益的分母是已成熟完整价格窗口，均不是实际成交胜率。现货观察不含永续手续费、点差或资金费；全部模拟账户与完整路径样本分别查看模拟仓位。</p>
        <h4>候选规则独立观察 · {q.data?.forward?.candidateReady ? "仅达到人工审查门槛" : "样本未达门槛"}</h4>
        <p>{(q.data?.forward?.candidateDays ?? 0).toFixed(1)}天 / 覆盖 {((q.data?.forward?.candidateCoverage ?? 0)*100).toFixed(1)}% / {q.data?.forward?.candidateEpisodes ?? 0}个独立买方行情事件。旧规则观察天数不计入候选门槛。</p>
        {!!q.data?.forward?.comparisons?.length && <ComparisonTable items={q.data.forward.comparisons} />}
        <details><summary>查看一对一匹配记录</summary>{q.data?.forward?.comparisons?.map(r=><div key={r.rulesVersion}><h4>{r.rulesVersion}</h4>{r.matches?.map(m=><p key={m.eventId}>{m.eventId} → {m.signalId ?? "未匹配"} · {m.timing === "early" ? "提前" : m.timing === "following" ? "跟随" : "漏报"}{m.leadMinutes != null ? ` ${m.leadMinutes.toFixed(1)}分钟` : ""}</p>)}</div>)}</details>
      </section>
      <p className="helper">
        30天基线 → 30天开发 →
        30天留出。补采走共享队列，不挤占当前行情；可能需要数小时至数天。记录首次获取与修订版本，未知历史发布时间的数据只能做关联分析。
      </p>
      {!!items.length && (
        <label>
          研究记录{" "}
          <select value={d?.id ?? ""} onChange={(e) => setId(e.target.value)}>
            {items.map((s) => (
              <option key={s.id} value={s.id}>
                {s.asset} · {stamp(s.updatedAt)} · {labels[s.state] ?? s.state}
              </option>
            ))}
          </select>
        </label>
      )}
      {d ? (
        <>
          <div className="signal-quality">
            <strong>{labels[d.state] ?? d.state}</strong>
            <p>
              {stamp(d.from)} → {stamp(d.to)} · {d.jobs.length}个共享采集任务
            </p>
            {d.error && <p className="amber">{d.error}</p>}
            {d.validationId && <FrozenValidation key={d.validationId} id={d.validationId} asset={d.asset} />}
          </div>
          <h3>
            完整策略检验 ·{" "}
            {labels[d.result?.strategyState ?? "incomplete"] ?? "等待计算"}
          </h3>
          <div className="metric-strip">
            <Metric
              title="五分钟现货成交覆盖"
              value={d.result ? (d.result.flowCoverage * 100).toFixed(1) : "—"}
              unit="%"
            />
            <Metric
              title="五分钟价格覆盖"
              value={
                d.result ? (d.result.candleCoverage * 100).toFixed(1) : "—"
              }
              unit="%"
            />
            <Metric title="历史发布时间" value="不可证明" unit="只分析关联" />
          </div>
          {d.result?.missing?.map((s, i) => (
            <p className="amber" key={i}>
              {s}
            </p>
          ))}
		  {d.result?.multifactorComparison && <section className="data-section"><h3>双向多因素历史对照 · {d.result.multifactorComparison.state === "calculating" ? "分批计算中" : "关联结果"}</h3><p className="helper">{d.result.multifactorComparison.note}</p><p>已计算至 {stamp(d.result.multifactorComparison.calculatedThrough)}</p>{d.result.multifactorComparison.groups.map(g => <details key={g.name+g.phase}><summary>{g.name} · {g.phase === "holdout" ? "独立留出" : "开发期"} · {g.commonWindows}个共同有效窗口</summary>{g.commonWindows ? <><ComparisonTable items={g.trials}/><h4>相同提醒数量</h4><ComparisonTable items={g.equalBudget}/></> : <p>无完整可比数据，未计算效果，不视为零收益。</p>}</details>)}</section>}
          {d.result?.candidateComparison && <section className="data-section">
            <h3>固定候选历史对照 · {d.result.candidateComparison.commonWindows}个共同有效窗口</h3>
            <p className="helper">{d.result.candidateComparison.note}</p>
            <h4>独立留出（排除指定案例）</h4><ComparisonTable items={d.result.candidateComparison.holdout} />
            {!!d.result.candidateComparison.auxiliary?.length && <details><summary>合约背景独立过滤对照 · {d.result.candidateComparison.auxiliaryWindows ?? 0}个共同窗口</summary><ComparisonTable items={d.result.candidateComparison.auxiliary} /></details>}
            {!!d.result.candidateComparison.equalBudget?.length && <details><summary>相同提醒数量对照 · {Object.keys(d.result.candidateComparison.dailyBudget ?? {}).length}个共同日期</summary><p className="helper">每个UTC日按三规则最小提醒数，各保留最早提醒；仅作事后人工审查辅助，不是已验证的线上策略。</p><ComparisonTable items={d.result.candidateComparison.equalBudget} /></details>}
            <details><summary>开发期与延迟对照</summary><ComparisonTable items={d.result.candidateComparison.development} /></details>
          </section>}
          {!!d.result?.experiments?.length && (
            <div className="table-scroll">
              <table>
                <thead>
                  <tr>
                    <th>留出对照</th>
                    <th>样本 / 覆盖</th>
                    <th>先达有利2ATR</th>
                    <th>先达不利1ATR</th>
                    <th>无法判定 / 缺失 / 超时</th>
                  </tr>
                </thead>
                <tbody>
                  {[...d.result.experiments, ...(d.result.delays ?? [])].map((e) => (
                    <tr key={e.name}>
                      <td>{e.name}</td>
                      <td>
                        {e.samples} / {(e.coverage * 100).toFixed(0)}%
                      </td>
                      <td>
                        {e.favorable}
                        {e.rate != null && (
                          <small className="helper">
                            {" "}
                            可判定样本比例 {(e.rate * 100).toFixed(1)}
                            %；粗略95%区间 {(e.interval[0] * 100).toFixed(1)}–
                            {(e.interval[1] * 100).toFixed(1)}%
                          </small>
                        )}
                      </td>
                      <td>{e.adverse}</td>
                      <td>
                        {e.ambiguous} / {e.incomplete} / {e.timedOut}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
          <p className="helper">
            同一根5分钟K线同时触及两边记为无法判定。ATR使用触发前完成的一小时K线；结果不称为策略胜率。真实提前量、误报和漏报仍需至少14天前向观察。
          </p>
          {!!d.result?.cases?.length && (
            <section className="data-section">
              <h3>指定行情 · 前48小时至后72小时</h3>
              {d.result.cases.map((c) => (
                <details key={c.date} className="data-section">
                  <summary>
                    {c.date} · {labels[c.state ?? "partial"]} ·
                    案例关联（非独立验证）
                  </summary>
                  <p className="helper">{c.note}</p>
                  {c.reconciliation && <p className="amber">跨粒度核对：{c.reconciliation.comparedHours}小时可比，{c.reconciliation.conflictHours}小时差异超过1美元，最大净额差 {amount(c.reconciliation.maxNetDifferenceCents, true)}。{c.reconciliation.note}</p>}
                  {c.coverage && (
                    <p className="case-coverage">
                      成交 {c.coverage.flowHours}/{c.coverage.expectedHours}{" "}
                      小时 · 价格 {c.coverage.priceHours}/
                      {c.coverage.expectedHours} · OI {c.coverage.oiHours}/
                      {c.coverage.expectedHours} · 溢价{" "}
                      {c.coverage.premiumHours}/{c.coverage.expectedHours}
                    </p>
                  )}
                  <p className="helper">{c.flowNote}</p>
                  {c.errors?.map((e, i) => (
                    <p className="amber" key={i}>
                      {e}
                    </p>
                  ))}
                  {!!c.hourly?.some((h) => h.netCents != null) && (
                    <Chart
                      label="案例小时主动净买卖"
                      height={250}
                      option={bars(
                        c.hourly.map((h) => [
                          stamp(h.end),
                          h.netCents == null ? null : h.netCents / 100,
                        ]),
                        "USD",
                      )}
                    />
                  )}
                  {!!c.hourly?.some((h) => h.priceClose != null) && (
                    <Chart
                      label="案例小时收盘价格"
                      height={230}
                      option={{
                        grid: { left: 65, right: 20, top: 25, bottom: 40 },
                        tooltip: { trigger: "axis" },
                        xAxis: {
                          ...axis,
                          type: "category",
                          data: c.hourly.map((h) => stamp(h.end)),
                          axisLabel: { hideOverlap: true, color: "#9dafaa" },
                        },
                        yAxis: {
                          ...axis,
                          type: "value",
                          scale: true,
                          name: "USDT",
                        },
                        series: [
                          {
                            type: "line",
                            showSymbol: false,
                            connectNulls: false,
                            data: c.hourly.map((h) => h.priceClose ?? null),
                            lineStyle: { color: "#d6c987" },
                          },
                        ],
                      }}
                    />
                  )}
                  <h4>实际钱包快照变化</h4>
                  {c.wallet?.length ? (
                    c.wallet.map((w) => (
                      <p key={w.to}>
                        {stamp(w.to)} ·{" "}
                        {w.delta == null
                          ? "数据不足"
                          : `${price(+w.delta, 2)} ${asset}`}{" "}
                        · {w.hours.toFixed(1)}小时 · {w.venues.length}家可比
                        {w.excluded.length
                          ? `，排除${w.excluded.join("、")}`
                          : ""}
                      </p>
                    ))
                  ) : (
                    <p>尚无可比钱包快照。</p>
                  )}
                  {!!c.hourly?.some((h) => h.netCents != null) && (
                    <details>
                      <summary>逐小时数字与实际粒度</summary>
                      <div className="table-scroll">
                        <table>
                          <thead>
                            <tr>
                              <th>小时截止</th>
                              <th>现货主动净买卖 USD</th>
                              <th>同期价格变化</th>
                              <th>OI变化</th>
                              <th>溢价 · USD</th>
                              <th>成交粒度</th>
                            </tr>
                          </thead>
                          <tbody>
                            {c.hourly
                              ?.filter((h) => h.netCents != null)
                              .map((h) => (
                                <tr key={h.end}>
                                  <td>{stamp(h.end)}</td>
                                  <td>
                                    {h.netCents == null
                                      ? "数据不足"
                                      : amount(h.netCents, true)}
                                  </td>
                                  <td>
                                    {h.priceChange == null
                                      ? "—"
                                      : `${h.priceChange.toFixed(2)}%`}
                                  </td>
                                  <td>
                                    {h.oiChange == null
                                      ? "—"
                                      : `${h.oiChange.toFixed(2)}%`}
                                  </td>
                                  <td>
                                    {h.premiumUsd == null
                                      ? "—"
                                      : price(h.premiumUsd, 2)}
                                  </td>
                                  <td>
                                    {h.flowResolutionSeconds === 300
                                      ? "5分钟汇总"
                                      : "1小时"}
                                  </td>
                                </tr>
                              ))}
                          </tbody>
                        </table>
                      </div>
                    </details>
                  )}
                </details>
              ))}
            </section>
          )}
          {!!d.result?.coverage?.length && (
            <details className="data-section">
              <summary>补采进度、实际范围与失败原因</summary>
              {d.result.coverage.map((c, i) => (
                <div key={i} className="coverage-job">
                  <strong>
                    {c.dataset} · {c.resolutionSeconds / 60}分钟 ·{" "}
                    {labels[c.state] ?? c.state}
                  </strong>
                  <p>
                    {stamp(c.from)} → {stamp(c.to)}
                  </p>
                  <p className="helper">
                    游标 {stamp(c.cursor)} · {c.gaps?.length ?? 0}个已记录缺口{" "}
                    {c.reason}
                  </p>
                  {c.errorKind && (
                    <small>
                      失败类型：{c.errorKind}
                      。仅描述本窗口；其他粒度和时间段独立核验。
                    </small>
                  )}
                </div>
              ))}
            </details>
          )}
          <details className="data-section">
            <summary>事件与1/4/24小时方向表现</summary>
            <div className="table-scroll">
              <table>
                <thead>
                  <tr>
                    <th>时点</th>
                    <th>方向 / 阶段</th>
                    <th>1小时</th>
                    <th>4小时</th>
                    <th>24小时</th>
                    <th>障碍结果</th>
                  </tr>
                </thead>
                <tbody>
                  {d.result?.events?.map((e, i) => (
                    <tr key={i}>
                      <td>{stamp(e.at)}</td>
                      <td>
                        {e.direction === "buy" ? "买方" : "卖方"} /{" "}
                        {e.phase === "holdout" ? "留出" : "开发"}
                      </td>
                      {[
                        e.outcome.return1h,
                        e.outcome.return4h,
                        e.outcome.return24h,
                      ].map((v, j) => (
                        <td key={j}>{v == null ? "—" : v.toFixed(2) + "%"}</td>
                      ))}
                      <td>
                        {{
                          favorable: "有利先达",
                          adverse: "不利先达",
                          ambiguous: "无法判定",
                          incomplete: "数据不足",
                          timeout: "24小时未达",
                        }[e.outcome.barrier] ?? e.outcome.barrier}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </details>
          {d.result?.notes?.map((v, i) => (
            <p className="helper" key={i}>
              {v}
            </p>
          ))}
        </>
      ) : (
        <p className="empty">
          尚无研究记录。创建后将复用已存数据，缺失部分去重排队。
        </p>
      )}
    </section>
  );
}
