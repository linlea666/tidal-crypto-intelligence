import { useState } from "react";
import { Chart } from "./Chart";
import { api, amount, price, useAPI } from "./data";
import { Metric, type Meta } from "./Pages";
import type { Asset } from "./types";

const stamp = (s?: string | null) =>
  s && new Date(s).getFullYear() > 2000
    ? new Date(s).toLocaleString("zh-CN", {
        timeZone: "Asia/Shanghai",
        hour12: false,
      })
    : "尚未获取";
const labels: Record<string, string> = {
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
type SignalItem = {
  id: string;
  asset: string;
  direction: string;
  pattern: string;
  state: string;
  at: string;
  dataThrough: string;
  expiresAt: string;
  confirmedAt: string | null;
  frozenHigh: number;
  frozenLow: number;
  net15Cents: number;
  buyShare: number;
  evidence: string[];
  conflicts: string[];
  missing: string[];
};
type Signals = {
  items: SignalItem[];
  quality: {
    reason: string;
    fresh: boolean;
    baseline: { valid: boolean; coverage: number; validDates: number };
  } | null;
  observers: {
    kind: string;
    meta: Meta;
    data: {
      premium?: { premiumUsd: string; rawRate: string; rateUnit: string };
      openInterest?: { usd: string }[];
    };
  }[];
  rulesVersion: string;
  mail: {
    configured: boolean;
    note: string;
    lastError?: { error: string } | null;
  };
  note: string;
};
export function SignalsPage({ asset }: { asset: Asset }) {
  const q = useAPI<Signals>(`signals?asset=${asset}`, 15000);
  const d = q.data;
  const [selected, setSelected] = useState("");
  const items = d?.items ?? [];
  return (
    <section className="research-page">
      <div className="research-heading">
        <div>
          <span className="eyebrow">实验规则 · 对称识别买卖异动</span>
          <h2>先发现资金变化，再等价格确认</h2>
          <p>钱包、ETF、溢价和合约指标暂不提高告警等级。</p>
        </div>
        <span className="research-badge">
          {d?.mail.configured ? "站内＋邮件" : "站内记录 · 邮件待配置"}
        </span>
      </div>
      <LoadError error={q.error} />
      <div className="signal-quality">
        <strong>{d?.quality?.reason ?? "等待首轮数据检查"}</strong>
        <p>
          30天基线：
          {d?.quality
            ? `${(d.quality.baseline.coverage * 100).toFixed(1)}%样本 · ${d.quality.baseline.validDates}个有效日期`
            : "正在检查"}
          。达到95%样本、21个有效日期且当前窗口完整才触发。
        </p>
      </div>
      <div className="signal-stages">
        <div>
          <b>01 资金异动</b>
          <p>异常净买卖＋持续性＋成交量</p>
        </div>
        <div>
          <b>02 价格确认</b>
          <p>两根完成K线突破冻结的4小时区间</p>
        </div>
        <div>
          <b>03 减弱或到期</b>
          <p>方向反转或4小时未确认</p>
        </div>
      </div>
      {!items.length && (
        <p className="empty">
          暂无符合条件的异动记录。没有提醒并不代表没有行情；数据不足时不会补造信号。
        </p>
      )}
      <div className="signal-list">
        {items.map((s) => (
          <article className="signal-card" key={s.id}>
            <div className="signal-title">
              <strong className={s.direction === "buy" ? "buy" : "sell"}>
                {s.direction === "buy" ? "买方" : "卖方"}资金异动
              </strong>
              <span>
                {labels[s.state] ?? s.state} ·{" "}
                {s.pattern === "burst" ? "15分钟突增" : "连续三小时"}
              </span>
            </div>
            <p>
              发现 {stamp(s.at)} · 成交窗口截止 {stamp(s.dataThrough)}
            </p>
            <div className="range-summary compact">
              <div>
                <span>15分钟主动净买卖</span>
                <strong>{amount(s.net15Cents, true)}</strong>
              </div>
              <div>
                <span>主动买入占比</span>
                <strong>{s.buyShare.toFixed(1)}%</strong>
              </div>
            </div>
            <p>
              冻结确认价：
              {price(s.direction === "buy" ? s.frozenHigh : s.frozenLow)} USDT ·{" "}
              {s.confirmedAt
                ? `确认 ${stamp(s.confirmedAt)}`
                : `到期 ${stamp(s.expiresAt)}`}
            </p>
            <button
              className="text-button"
              onClick={() => setSelected(selected === s.id ? "" : s.id)}
            >
              {selected === s.id ? "收起" : "查看"}支持、冲突与缺失证据
            </button>
            {selected === s.id && (
              <div className="signal-evidence">
                <div>
                  <h4>支持</h4>
                  {s.evidence.map((v, i) => (
                    <p key={i}>{v}</p>
                  ))}
                </div>
                <div>
                  <h4>冲突 / 待确认</h4>
                  {s.conflicts.map((v, i) => (
                    <p key={i}>{v}</p>
                  ))}
                </div>
                <div>
                  <h4>缺失与限制</h4>
                  {s.missing.map((v, i) => (
                    <p key={i}>{v}</p>
                  ))}
                </div>
              </div>
            )}
          </article>
        ))}
      </div>
      <section className="data-section">
        <h3>辅助观察 · 权重为零</h3>
        <div className="activity-columns">
          {d?.observers?.map((o) => (
            <div key={o.kind}>
              <h4>
                {o.kind === "premium"
                  ? "Coinbase 相对币安溢价"
                  : "全市场聚合OI历史末值"}
              </h4>
              <strong>
                {o.kind === "premium"
                  ? o.data.premium
                    ? `${o.data.premium.premiumUsd} USD`
                    : "等待数据"
                  : o.data.openInterest?.[0]
                    ? amount(+o.data.openInterest[0].usd * 100)
                    : "等待数据"}
              </strong>
              <Provenance meta={o.meta} />
              <p className="helper">
                {o.kind === "premium"
                  ? "价差可反映跨交易所压力，不代表全部美国或亚洲买盘。原始比率单位尚未独立核实，不转换为百分比。"
                  : "与三家合约主动成交的覆盖口径不同；不可由OI增加单独推断开多或开空。"}
              </p>
            </div>
          ))}
        </div>
      </section>
      <details className="data-section">
        <summary>查看固定规则与提醒边界</summary>
        <p>
          15分钟型：净买卖达到同口径P95/P05，主动方向占比≥55%，连续三根5分钟同向，60分钟净额同向，成交量不低于中位数。持续型：连续三小时同向，一小时净额达到P90/P10，三小时主动方向占比≥55%。
        </p>
        <p>
          价格确认必须在4小时内完成；CVD不重复计分。解除条件30分钟后才能重发。每小时最多6封邮件，多余合并摘要；重启、历史补采不补发旧邮件。
        </p>
        <p>{d?.mail.note}</p>
        {d?.mail.lastError && (
          <p className="amber">邮件状态：{d.mail.lastError.error}</p>
        )}
        <small>
          {d?.rulesVersion} · {d?.note}
        </small>
      </details>
    </section>
  );
}
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
type StudyItem = {
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
export function StudiesPage({ asset }: { asset: Asset }) {
  const q = useAPI<{
    items: StudyItem[];
    forward?: {
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
  const d = q.data?.items.find((s) => s.id === id) ?? q.data?.items[0];
  const start = async () => {
    setBusy(true);
    setError("");
    try {
      const s = await api<StudyItem>("studies", {
        method: "POST",
        body: JSON.stringify({ asset }),
      });
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
          onClick={start}
        >
          {busy ? "正在排队…" : "创建90天研究"}
        </button>
      </div>
      <LoadError error={error || q.error} />
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
        <p className="helper">{q.data?.forward?.note}</p>
      </section>
      <p className="helper">
        30天基线 → 30天开发 →
        30天留出。补采走共享队列，不挤占当前行情；可能需要数小时至数天。记录首次获取与修订版本，未知历史发布时间的数据只能做关联分析。
      </p>
      {!!q.data?.items.length && (
        <label>
          研究记录{" "}
          <select value={d?.id ?? ""} onChange={(e) => setId(e.target.value)}>
            {q.data.items.map((s) => (
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
          {d.result?.missing.map((s, i) => (
            <p className="amber" key={i}>
              {s}
            </p>
          ))}
          {!!d.result?.experiments.length && (
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
                  {[...d.result.experiments, ...d.result.delays].map((e) => (
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
          {!!d.result?.cases.length && (
            <section className="data-section">
              <h3>指定行情 · 前48小时至后72小时</h3>
              {d.result.cases.map((c) => (
                <details key={c.date} className="data-section">
                  <summary>
                    {c.date} · {labels[c.state ?? "partial"]} ·
                    案例关联（非独立验证）
                  </summary>
                  <p className="helper">{c.note}</p>
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
                  {d.result?.events.map((e, i) => (
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
          {d.result?.notes.map((v, i) => (
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
