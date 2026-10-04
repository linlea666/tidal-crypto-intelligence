import { useEffect, useMemo, useRef, useState } from "react";
import { api, price, useAPI } from "./data";
import { Chart } from "./Chart";
import {
  ArrowSquareOut,
  ArrowsClockwise,
  Info,
  Bell,
} from "@phosphor-icons/react";
import "./onchainCost.css";
import { OnchainReview, type OnchainUpgrade } from "./OnchainReview";

type Bounds = { lower: string; upper: string };
type Supply = { sth: Bounds; lth: Bounds; total: Bounds };
type Zone = { side: string; low: string; high: string; supply: string };
type Metrics = {
  concentration: Record<string, Bounds>;
  denominator: string;
  volatility: number | null;
  concentrationRank: Bounds | null;
  volatilityRank: number | null;
  baselineSamples: number;
  volatilitySamples: number;
  zones: Zone[];
};
type Evidence = {
  source: string;
  kind: string;
  title: string;
  status: string;
  from: string;
  to: string;
  asOf: string;
  coverage: number | null;
  data: Record<string, unknown> | null;
  note: string;
};
type CostEvent = {
  id: string;
  kind: string;
  date: string;
  detectedAt: string;
  rulesVersion: string;
  revision: string;
  price: string;
  note: string;
  zone: Zone | null;
  evidence: Evidence[] | null;
  noticeStatus?: string;
  direction?: string; shadow?: boolean; priceSource?: string;
  occurredAt?: string; firstSeen?: string; validatedAt?: string;
  attemptedAt?: string; submittedAt?: string;
  marketPriceAt?: string; discoveredMarketPrice?: string | null; marketPrice?: string | null; boundaryDistancePercent?: string | null; discoveryDistancePercent?: string | null; decisionDistancePercent?: string | null;
};
type Health = {
  eventsEnabled: boolean;
  offline: boolean;
  status: string;
  reason: string;
  usedBytes: number;
  budgetBytes: number;
  capacityWarning: boolean;
  feed: {
    lastCheck: string | null;
    lastDate: string;
    lastError: string;
    nextAttempt: string | null;
  };
};
type Board = {
  upgrade?: OnchainUpgrade;
  frame: {
    date: string;
    price: string;
    firstSeen: string;
    builtAt: string | null;
    origin: string;
    revision: string;
    sth: { total: string };
    lth: { total: string };
  } | null;
  dates: string[];
  health: Health;
  source: string;
  sourceURL: string;
  rulesVersion: string;
  methodNote: string;
  historical: boolean;
  metrics: Metrics;
  bins: {
    low: string;
    high: string;
    sth: string;
    lth: string;
    total: string;
  }[];
  actualStep: string;
  scaleMaxBTC: string;
  selected: {
    low: string;
    high: string;
    supply: Supply;
    changes: {
      from: string;
      to: string;
      status: string;
      change: Supply | null;
    }[];
  };
  evidence: Evidence[];
  observation: {
    from: string;
    through: string;
    frozenAt: string;
    watches: { zone: Zone; state: string }[];
    compression: boolean;
  };
  settings: { emailEnabled: boolean };
  mailConfigured: boolean;
 mailReadiness?: {ready:boolean;reason:string};
  note: string;
};
type History = {
  events: { date: string; kind: string; detectedAt: string }[];
  from: string;
  to: string;
  points: {
    date: string;
    concentration: Bounds;
    supply: Supply;
    origin: string;
  }[];
  prices: { date: string; value: string }[];
  cells: [string, string, string][];
  total: number;
  hasMore: boolean;
  offset: number;
  note: string;
};
type Result = {
  directionalReturn?: number | null; volatilityRatio?: number | null; discoveryToAnchor?: number | null; anchorDate?: string; selected?: boolean;
  eventId: string;
  horizon: number;
  from: string;
  through: string;
  status: string;
  return: number | null;
  pathMax: number | null;
  pathMin: number | null;
  volatility: number | null;
};
type Research = {
  mode: string;
  note: string;
  total: number;
  nextOffset: number | null;
  dailyCount?: number;
  daily?: { id: string; date: string; firstSeen: string; structure: string; price: string; reason: string }[];
  groups?: {
    rateLabel?: string; median?: number | null; quartiles?: number[]; invalidatedRate?: number | null; invalidatedSamples?: number; attentionDelayMedianHours?: number | null;
    rulesVersion: string;
    kind: string;
    side: string;
    horizon: number;
    complete: number;
    independent: number;
    waiting: number;
    missing: number;
    overlapping: number;
    riseRate: number | null;
    confidenceInterval: number[];
  }[];
  items: (
    | Result
    | {
        date: string;
        concentration: Bounds;
        volatility: number | null;
        results: Result[];
      }
  )[];
};
export const costStatuses: Record<string, string> = {
  fresh: "日线有效",
  missing: "等待采集",
  delayed: "日线延迟",
  unreachable: "来源检查中断",
  error: "采集异常",
  unavailable: "存储不可用",
  disabled: "已停用",
  capacity: "容量暂停",
};
const eventNames: Record<string, string> = {
  initialized: "开始观察",
  pending: "收盘越界待确认",
  confirmed: "连续两日收于成本区外",
  invalidated: "确认失效",
  unconfirmed: "未延续确认",
  concentrated: "集中且波动偏低",
  expired: "观察到期",
  gap: "日快照缺口",
  revision: "来源修订",
  discovery_expired: "发现窗口到期", pending_expired: "待确认到期", tracking_expired: "确认跟踪到期",
  attention_near: "4小时接近边界", attention_outside: "4小时收于边界之外", attention_returned: "4小时参考价回到区间",
  unconditional: "无条件基线", low_volatility: "仅低波动基线", price_only: "20日收盘通道", onchain: "筹码边界条件", structure: "集中且低波动", "onchain_buffer_0.5": "0.5%缓冲影子",
};
const watchNames: Record<string, string> = {
  watching: "等待越过边界",
  pending: "等待第二日确认",
  confirmed: "连续两日确认",
  invalidated: "确认失效",
};
const qty = (v: string | number | null | undefined, digits = 2) =>
  v == null || v === "" || !Number.isFinite(Number(v)) ? "—" : price(Number(v), digits);
const bucketDigits = (step: string | number) =>
  Math.min(8, Math.max(0, Math.ceil(-Math.log10(Number(step)))));
const compactBTC = (v: string) =>
  Number(v) >= 10000 ? `${qty(Number(v) / 10000)}万` : qty(v);
const bounds = (b: Bounds | null | undefined, suffix = "", digits = 2) =>
  !b
    ? "—"
    : b.lower === b.upper
      ? `${qty(b.lower, digits)}${suffix}`
      : `${qty(b.lower, digits)}–${qty(b.upper, digits)}${suffix}`;
const stamp = (s: string | null | undefined) =>
  s
    ? new Date(s).toLocaleString("zh-CN", {
        timeZone: "Asia/Shanghai",
        hour12: false,
      })
    : "未知";
const labels: Record<string, string> = {
  startUsd: "期初名义价值 USD",
  endUsd: "期末名义价值 USD",
  changeUsd: "名义价值变化 USD",
  buyUsd: "主动买入 USD",
  sellUsd: "主动卖出 USD",
  netUsd: "主动净买入 USD",
  longUsd: "多头已清算 USD",
  shortUsd: "空头已清算 USD",
  startBTC: "期初折算 OI · BTC",
  endBTC: "期末折算 OI · BTC",
  changeBTC: "折算 OI 变化 · BTC",
  flowUsd: "净流入 USD",
};
const dayMs = 86400000;
function days(from: string, to: string) {
  const a: string[] = [];
  for (
    let t = Date.parse(from);
    t <= Date.parse(to) && a.length < 20000;
    t += dayMs
  )
    a.push(new Date(t).toISOString().slice(0, 10));
  return a;
}

export function OnchainCostPage() {
  const [date, setDate] = useState("");
  const [step, setStep] = useState("1000");
  const [range, setRange] = useState("20");
  const [selection, setSelection] = useState<{
    low: string;
    high: string;
  } | null>(null);
  const [compare, setCompare] = useState("");
  const [tab, setTab] = useState("structure");
  const [period, setPeriod] = useState("1y");
  const [heatmap, setHeatmap] = useState(false);
  const [historyOffset, setHistoryOffset] = useState(0);
  const [eventOffset, setEventOffset] = useState(0);
  const [researchOffset, setResearchOffset] = useState(0);
  const [researchMode, setResearchMode] = useState("forward");
  const binsRef = useRef<HTMLDivElement>(null);
  const centeredKey = useRef("");
  const [mobileExpanded, setMobileExpanded] = useState(false);
  const [mailValue, setMailValue] = useState<boolean | null>(null);
  const [saving, setSaving] = useState(false);
  const [notice, setNotice] = useState("");
  const query = new URLSearchParams({ step, range });
  if (date) query.set("date", date);
  if (selection) {
    query.set("low", selection.low);
    query.set("high", selection.high);
  }
  if (compare) query.set("compare", compare);
  const { data, error, loading } = useAPI<Board>(
    `onchain-cost?${query}`,
    60000,
  );
  const active = data?.frame ? data : null;
  useEffect(() => {
    setCompare("");
    setHistoryOffset(0);
  }, [date]);
  useEffect(() => {
    setHistoryOffset(0);
  }, [period, heatmap, selection]);
  const hq = new URLSearchParams(query);
  hq.set("period", period);
  hq.set("limit", "200");
  hq.set("offset", String(historyOffset));
  if (heatmap) hq.set("mode", "heatmap");
  const hist = useAPI<History>(
    active && tab === "history" ? `onchain-cost/history?${hq}` : null,
    60000,
  );
  const events = useAPI<{ items: CostEvent[]; hasMore: boolean }>(
    tab === "events"
      ? `onchain-cost/events?offset=${eventOffset}&limit=30`
      : null,
    60000,
  );
  const research = useAPI<Research>(
    tab === "research"
      ? `onchain-cost/research?mode=${researchMode}&offset=${researchOffset}&limit=30`
      : null,
    60000,
  );
  const toggleMail = async () => {
    if (!active) return;
    setSaving(true);
    setNotice("");
    try {
      const r = await api<{ emailEnabled: boolean }>("onchain-cost/settings", {
        method: "PUT",
        body: JSON.stringify({
          emailEnabled: !(mailValue ?? active.settings.emailEnabled),
        }),
      });
      setMailValue(r.emailEnabled);
      setNotice(
        r.emailEnabled
          ? "邮件已开启，仅通知之后的新事件；不会补发历史。"
          : "邮件已关闭，站内观察继续。",
      );
    } catch (e) {
      setNotice((e as Error).message);
    } finally {
      setSaving(false);
    }
  };
  const selectBin = (low: string, high: string) => {
    setMobileExpanded(
      active?.selected.low === low && active.selected.high === high
        ? !mobileExpanded
        : true,
    );
    setSelection({ low, high });
  };
  useEffect(() => {
    const panel = binsRef.current;
    if (!active || !panel) {
      centeredKey.current = "";
      return;
    }
    if (window.innerWidth <= 760) return;
    const key = `${active.frame!.date}/${active.actualStep}/${range}`;
    if (centeredKey.current === key) return;
    const row = panel.querySelector<HTMLElement>(".at-price");
    if (row) {
      panel.scrollTop +=
        row.getBoundingClientRect().top -
        panel.getBoundingClientRect().top -
        (panel.clientHeight - row.clientHeight) / 2;
      centeredKey.current = key;
    }
  }, [active, range, tab]);
  return (
    <section className="cost-page" aria-label="BTC链上筹码观察">
      <div className="cost-source">
        <div>
          <span
            className={`cost-dot ${data?.health.status === "fresh" ? "ok" : "warn"}`}
          />{" "}
          <strong>
            {loading && !data
              ? "读取中"
              : error
                ? "查询异常"
                : costStatuses[data?.health.status ?? "missing"]}
          </strong>
          <span>
            BlockHorizon · 每日快照 · BTC
            {data?.health.offline && " · 本地离线验收"}
          </span>
        </div>
        <a
          href="https://charts.blockhorizon.io/charts/cost-basis-distribution"
          target="_blank"
          rel="noreferrer"
        >
          来源与方法 <ArrowSquareOut size={14} />
        </a>
      </div>
      {(error || notice) && (
        <p className="cost-notice" role="status">
          {error || notice}
        </p>
      )}
      {data && data.health.status !== "fresh" && (
        <p className="cost-notice" role="status">
          {data.health.reason}。{data.health.feed?.lastError}
        </p>
      )}
      {!active ? (
        <div className="cost-empty">
          <ArrowsClockwise size={25} />
          <h2>{loading ? "正在读取本地链上快照" : "等待首份有效成本分布"}</h2>
          <p>
            采集在后端独立运行。两类供给与同日价格通过校验后才会展示；缺失不代表零。
          </p>
        </div>
      ) : (
        <>
          <div className="cost-summary">
            <article>
              <span>观察日期 · UTC完成日</span>
              <strong>{active.frame!.date}</strong>
              <small>同日收盘 ${qty(active.frame!.price)}</small>
            </article>
            <article>
              <span>价格上下各5% · 供给集中度</span>
              <strong>{bounds(active.metrics.concentration["5"], "%")}</strong>
              <small>
                周频参考{" "}
                {active.metrics.concentrationRank
                  ? `P${bounds(active.metrics.concentrationRank)}`
                  : `不足42周（${active.metrics.baselineSamples}/52）`}
              </small>
            </article>
            <article>
              <span>21日收盘收益波动 · 年化</span>
              <strong>
                {active.metrics.volatility == null
                  ? "数据不足"
                  : `${qty(active.metrics.volatility)}%`}
              </strong>
              <small>
                参考{" "}
                {active.metrics.volatilityRank == null
                  ? "不足42周或缺日线"
                  : `P${qty(active.metrics.volatilityRank, 0)}`}{" "}
                · 不是日内振幅
              </small>
            </article>
            <article>
              <span>日线价格条件</span>
              <strong>
                {active.historical
                  ? "历史结构"
                  : error ||
                      !active.upgrade?.capabilities.dailyPrice ||
                      !active.health.eventsEnabled
                    ? "判断暂停"
                    : active.observation.watches?.some(
                          (w) => w.state === "confirmed",
                        )
                      ? "价格条件确认"
                      : "方向未确认"}
              </strong>
              <small>集中不等于吸筹 · 候选窗口不保证反应</small>
            </article>
          </div>
          <div className="cost-toolbar">
            <label>
              快照日期
              <select
                aria-label="快照日期"
                value={date}
                onChange={(e) => {
                  setCompare("");
                  setDate(e.target.value);
                }}
              >
                <option value="">最新完成日</option>
                {[...active.dates].reverse().map((d) => (
                  <option key={d}>{d}</option>
                ))}
              </select>
            </label>
            <div className="cost-mail">
              <Bell size={16} />
              <button
                className="secondary"
                disabled={saving || (!active.mailConfigured && !(mailValue ?? active.settings.emailEnabled))}
                onClick={toggleMail}
              >
                {(mailValue ?? active.settings.emailEnabled)
                  ? "关闭本页邮件"
                  : "开启本页邮件"}
              </button>
              <small>
                {active.mailConfigured ? ((mailValue ?? active.settings.emailEnabled) ? `邮件已开启 · ${active.mailReadiness?.reason ?? "发送前独立核验条件"}` : "邮件已关闭 · 站内仍记录") : "SMTP尚未配置"}
              </small>
            </div>
          </div>
          <OnchainReview data={active.upgrade} historical={active.historical} showCases={tab === "events"} />
          <nav className="cost-tabs" aria-label="链上筹码视图">
            {[
              ["structure", "成本结构"],
              ["history", "历史变化"],
              ["events", "情景与事件"],
              ["research", "效果验证"],
            ].map(([id, name]) => (
              <button
                key={id}
                className={tab === id ? "active" : ""}
                aria-pressed={tab === id}
                onClick={() => setTab(id)}
              >
                {name}
              </button>
            ))}
          </nav>
          {tab === "structure" && (
            <>
              <div className="cost-layout">
                <section className="cost-profile">
                  <div className="cost-section-head">
                    <div>
                      <h2>筹码集中在哪里？</h2>
                      <p>柱长表示 BTC 数量，两类共用线性比例尺</p>
                    </div>
                    <div className="cost-legend">
                      <span className="sth">● STH 短期分类</span>
                      <span className="lth">● LTH 长期分类</span>
                    </div>
                  </div>
                  <div className="cost-toolbar">
                    <label>
                      展示桶宽
                      <select
                        value={step}
                        onChange={(e) => setStep(e.target.value)}
                      >
                        {["500", "1000", "2000"].map((s) => (
                          <option key={s} value={s}>
                            ${price(Number(s), 0)}
                          </option>
                        ))}
                      </select>
                    </label>
                    <label>
                      同日价格范围
                      <select
                        value={range}
                        onChange={(e) => setRange(e.target.value)}
                      >
                        <option value="10">±10%</option>
                        <option value="20">±20%</option>
                        <option value="all">全部</option>
                      </select>
                    </label>
                    <small>
                      实际桶宽 $
                      {qty(active.actualStep, bucketDigits(active.actualStep))}
                    </small>
                  </div>
                  <div className="cost-axis">
                    <span>价格区间 · USD</span>
                    <span>
                      0 <b>{compactBTC(active.scaleMaxBTC)} BTC</b>
                    </span>
                    <span>供给</span>
                  </div>
                  <div className="cost-bins" ref={binsRef}>
                    {[...active.bins].reverse().map((b) => {
                      const selected =
                        active.selected.low === b.low &&
                        active.selected.high === b.high;
                      const atPrice =
                        Number(b.low) <= Number(active.frame!.price) &&
                        Number(b.high) > Number(active.frame!.price);
                      return (
                        <div key={b.low}>
                          <button
                            className={`cost-bin ${selected ? "selected" : ""} ${atPrice ? "at-price" : ""}`}
                            aria-expanded={selected && mobileExpanded}
                            aria-label={`选择${b.low}到${b.high}美元区间`}
                            onClick={() => selectBin(b.low, b.high)}
                          >
                            <span className="cost-price">
                              ${qty(b.low, bucketDigits(active.actualStep))}–
                              {qty(b.high, bucketDigits(active.actualStep))}
                              {atPrice && <small>同日价格所在区间</small>}
                            </span>
                            <span className="cost-bar-track">
                              <i
                                className="sth"
                                style={{
                                  width: `${(Number(b.sth) / Math.max(1, Number(active.scaleMaxBTC))) * 100}%`,
                                }}
                              />
                              <i
                                className="lth"
                                style={{
                                  width: `${(Number(b.lth) / Math.max(1, Number(active.scaleMaxBTC))) * 100}%`,
                                }}
                              />
                            </span>
                            <span>
                              {compactBTC(b.total)}
                              <small>BTC</small>
                            </span>
                          </button>
                          {selected && mobileExpanded && (
                            <div className="cost-mobile-detail">
                              <CostDetail
                                data={active}
                                compare={compare}
                                setCompare={setCompare}
                                onHistory={() => setTab("history")}
                              />
                            </div>
                          )}
                        </div>
                      );
                    })}
                  </div>
                  {!active.bins.length && (
                    <p className="cost-notice">所选价格范围没有非零供给桶。</p>
                  )}
                  <p className="cost-helper">
                    同日收盘 ${qty(active.frame!.price)} · 区间左闭右开 ·
                    最后移动成本，不是挂单或实际买入量。
                  </p>
                </section>
                <aside className="cost-desktop-detail">
                  <CostDetail
                    data={active}
                    compare={compare}
                    setCompare={setCompare}
                    onHistory={() => setTab("history")}
                  />
                </aside>
              </div>
              <section className="cost-section">
                <h2>
                  {active.historical
                    ? "所选日结构与条件"
                    : "当前结构与确认条件"}
                </h2>
                <div className="cost-scenarios">
                  {active.metrics.zones.map((z) => (
                    <article key={z.side}>
                      <span>
                        {z.side === "inside" ? "包含现价" : z.side === "above" ? "上方" : "下方"}供给最多的候选窗口
                      </span>
                      <strong>
                        ${qty(z.low, 0)}–${qty(z.high, 0)}
                      </strong>
                      <p>{compactBTC(z.supply)} BTC · 占同源覆盖 {qty(Number(z.supply) / Number(active.metrics.denominator) * 100)}%</p>
 <p>窗口宽度 / 参考价 {qty((Number(z.high) - Number(z.low)) / Number(active.frame!.price) * 100)}% · 距参考价 {qty(Math.max(Number(z.low) - Number(active.frame!.price), Number(active.frame!.price) - Number(z.high), 0) / Number(active.frame!.price) * 100)}%</p>
                      <small>新候选不移动已有观察轮次的冻结边界。</small>
                    </article>
                  ))}
                  <article>
                    <span>集中度敏感性</span>
                    <p>
                      上下各2.5%：
                      {bounds(active.metrics.concentration["2.5"], "%")}
                    </p>
                    <p>
                      上下各10%：
                      {bounds(active.metrics.concentration["10"], "%")}
                    </p>
                    <small>移动价格窗口本身会改变覆盖供给。</small>
                  </article>
                </div>
              </section>
              <EvidencePanel items={active.evidence} />
            </>
          )}
          {tab === "history" && (
            <section className="cost-section">
              <div className="cost-toolbar">
                <h2>结构如何变化？</h2>
                <label>
                  时间范围
                  <select
                    value={period}
                    onChange={(e) => setPeriod(e.target.value)}
                  >
                    <option value="3m">3个月</option>
                    <option value="1y">1年</option>
                    <option value="4y">4年</option>
                    <option value="all">全部</option>
                  </select>
                </label>
                <button
                  className="secondary"
                  onClick={() => setHeatmap(!heatmap)}
                >
                  {heatmap ? "返回趋势图" : "进阶：历史热力图"}
                </button>
              </div>
              <p className="cost-helper">
                固定区间 ${qty(active.selected.low, 0)}–$
                {qty(active.selected.high, 0)}；仅真实快照，缺日留空。
              </p>
              {hist.error && <p role="alert">{hist.error}</p>}
              {hist.data ? (
                <>
                  <CostHistoryChart data={hist.data} heatmap={heatmap} />
                  <div className="cost-pagination">
                    <button
                      disabled={!historyOffset}
                      onClick={() =>
                        setHistoryOffset(Math.max(0, historyOffset - 200))
                      }
                    >
                      较新快照
                    </button>
                    <span>
                      本页 {hist.data.points.length} / {hist.data.total} 个快照
                    </span>
                    <button
                      disabled={!hist.data.hasMore}
                      onClick={() => setHistoryOffset(historyOffset + 200)}
                    >
                      较早快照
                    </button>
                  </div>
                  <details>
                    <summary>查看精确历史表</summary>
                    <div className="cost-table-wrap">
                      <table>
                        <thead>
                          <tr>
                            <th>真实日期</th>
                            <th>集中度 ±5%</th>
                            <th>固定区间 BTC</th>
                            <th>取得方式</th>
                          </tr>
                        </thead>
                        <tbody>
                          {[...hist.data.points].reverse().map((p) => (
                            <tr key={p.date}>
                              <td>
                                <button onClick={() => setDate(p.date)}>
                                  {p.date}
                                </button>
                              </td>
                              <td>{bounds(p.concentration, "%")}</td>
                              <td>{bounds(p.supply.total)}</td>
                              <td>
                                {p.origin === "forward"
                                  ? "本地前向采集"
                                  : "导入历史"}
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </details>
                  <p className="cost-helper">{hist.data.note}</p>
                </>
              ) : (
                <p>正在读取历史…</p>
              )}
            </section>
          )}
          {tab === "events" && (
            <section className="cost-section">
              <h2>先看条件，再看应对</h2>
              <div className="cost-scenarios">
                <article>
                  <h3>向上情景</h3>
                  <p>
                    冻结后连续两个UTC日收盘位于上边界之外，记录“两日收于成本区上方”；交叉查看现货与杠杆事实。
                  </p>
                  <small>随后收盘回到边界以内，原确认失效。</small>
                </article>
                <article>
                  <h3>向下情景</h3>
                  <p>
                    冻结后连续两个UTC日收盘位于下边界之外，记录“两日收于成本区下方”；关注成交与去杠杆风险。
                  </p>
                  <small>下方供给多，不等于自动补仓条件。</small>
                </article>
                <article>
                  <h3>方向未确认</h3>
                  <p>集中、低波动或证据分歧时，列出仍缺少的确认条件。</p>
                  <small>不从集中度直接推断主力意图。</small>
                </article>
              </div>
              <h3>事件账本 · 实际发现时间</h3>
              {events.error && <p role="alert">{events.error}</p>}
              {!events.data?.items.length && (
                <p className="cost-empty">
                  暂无事件。历史补采不会生成过去的提醒。
                </p>
              )}
              {events.data?.items.map((e) => (
                <details className="cost-event" key={e.id}>
                  <summary>
                    <strong>{eventNames[e.kind] ?? e.kind}{e.direction ? (e.direction === "up" ? " · 向上" : " · 向下") : ""}{e.shadow ? " · 影子观察" : ""}</strong>
                    <span>
                      {e.date} · {stamp(e.detectedAt)} 发现
                    </span>
                  </summary>
                  <p>{e.note}</p>
                  <p>
                    同日收盘 ${qty(e.price)} · 规则 {e.rulesVersion}
                  </p>
                  {e.zone && (
                    <p>
                      冻结区间 ${qty(e.zone.low, 0)}–${qty(e.zone.high, 0)}
                    </p>
                  )}
                  <p>邮件：{e.noticeStatus === "sent" ? "SMTP已接受（不等于已收到）" : e.noticeStatus ?? "未申请发送"}</p>
                  <p>价格来源：{e.priceSource ?? "旧记录未单独保存"}；发生 {stamp(e.occurredAt)} · 首次取得 {stamp(e.firstSeen)} · 校验 {stamp(e.validatedAt)}。</p>
                  <p>尝试发送 {stamp(e.attemptedAt)} · SMTP接受 {stamp(e.submittedAt)}。</p>
                  <p>当前参考价 ${qty(e.marketPrice)}（{stamp(e.marketPriceAt)}）；距边界 {qty(e.boundaryDistancePercent)}%，距首次条件价 {qty(e.discoveryDistancePercent)}%，距事件发现时市场参考价 {qty(e.decisionDistancePercent)}%。</p>
                  <EvidencePanel items={e.evidence ?? []} />
                  <small>修订标识 {e.revision}</small>
                </details>
              ))}
              <div className="cost-pagination">
                <button
                  disabled={!eventOffset}
                  onClick={() => setEventOffset(Math.max(0, eventOffset - 30))}
                >
                  上一页
                </button>
                <button
                  disabled={!events.data?.hasMore}
                  onClick={() => setEventOffset(eventOffset + 30)}
                >
                  下一页
                </button>
              </div>
            </section>
          )}
          {tab === "research" && (
            <section className="cost-section">
              <div className="cost-toolbar">
                <h2>让结论接受后续验证</h2>
                <select
                  aria-label="验证方式"
                  value={researchMode}
                  onChange={(e) => {
                    setResearchMode(e.target.value);
                    setResearchOffset(0);
                  }}
                >
                  <option value="forward">前向观察 · 当时实际取得</option>
                  <option value="historical">历史探索 · 事后可得</option>
                  <option value="legacy">旧版规则 · 原始结果</option>
                </select>
              </div>
              {research.error && <p role="alert">{research.error}</p>}
              {research.data && (
                <>
                  <p className="cost-notice">{research.data.note}</p>
                  {research.data.daily && <details><summary>逐日评估 · 已记录 {research.data.dailyCount ?? 0} 日（含无信号及缺失）</summary><div className="cost-table-wrap"><table><thead><tr><th>UTC日期</th><th>结构 / 价格</th><th>说明</th><th>首次记录</th></tr></thead><tbody>{research.data.daily.map(d => <tr key={d.id}><td>{d.date}</td><td>{({ met: "满足", not_met: "不满足", uncertain: "不确定", missing: "缺失" } as Record<string,string>)[d.structure] ?? d.structure} / {d.price === "available" ? "可用" : "缺失"}</td><td>{d.reason}</td><td>{stamp(d.firstSeen)}</td></tr>)}</tbody></table></div></details>}
                  {research.data.groups && (
                    <div className="cost-table-wrap">
                      <table>
                        <thead>
                          <tr>
                            <th>规则 / 方向 / 期限</th>
                            <th>完整 / 去重完整</th>
                            <th>等待 / 缺失 / 重叠</th>
                            <th>对应结果比例 · 95%区间</th>
                          </tr>
                        </thead>
                        <tbody>
                          {research.data.groups.map((g) => (
                            <tr
                              key={`${g.rulesVersion}/${g.kind}/${g.side}/${g.horizon}`}
                            >
                              <td>
                                {g.rulesVersion || "规则未知"} ·{" "}
                                {eventNames[g.kind] ?? g.kind} ·{" "}
                                {(g.side === "above" || g.side === "up")
                                  ? "上方"
                                  : (g.side === "below" || g.side === "down")
                                    ? "下方"
                                    : "无方向"}{" "}
                                · {g.horizon}日
                              </td>
                              <td>
                                {g.complete} / {g.independent}
                              </td>
                              <td>
                                {g.waiting} / {g.missing} / {g.overlapping}
                              </td>
                              <td>
                                <small>{g.rateLabel ?? "旧版收盘上涨比例"}</small><br />
                                {g.riseRate == null
                                  ? "不足30个去重完整可算样本"
                                  : `${qty(g.riseRate * 100)}% · ${g.confidenceInterval.map((x) => qty(x * 100)).join("–")}%`}
                                {g.median != null && <small>中位数 {qty(g.median)} · 四分位 {g.quartiles?.map(x => qty(x)).join("–")}</small>}
                                <small>7日失效 {g.invalidatedRate == null ? `样本不足（${g.invalidatedSamples ?? 0}）` : `${qty(g.invalidatedRate*100)}%`} · 关注至确认延迟中位数 {g.attentionDelayMedianHours == null ? "待积累" : `${qty(g.attentionDelayMedianHours)}小时`}</small>
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  )}
                  {!research.data.items.length && (
                    <div className="cost-empty">
                      等待自然事件与后续日线积累。功能已准备好，统计结论尚未形成。
                    </div>
                  )}
                  <div className="cost-table-wrap">
                    <table>
                      <thead>
                        <tr>
                          <th>日期 / 观察区间</th>
                          <th>期限</th>
                          <th>状态</th>
                          <th>收盘收益</th>
                          <th>收盘路径最高 / 最低</th>
                          <th>后续年化波动</th>
                          <th>方向调整收益 / 波动比</th>
                        </tr>
                      </thead>
                      <tbody>
                        {research.data.items.flatMap((item, i) =>
                          "results" in item
                            ? item.results.map((r) => (
                                <ResultRow
                                  key={`${i}-${r.horizon}`}
                                  r={r}
                                  label={`${item.date} · 集中度 ${bounds(item.concentration, "%")} · 波动 ${qty(item.volatility)}%`}
                                />
                              ))
                            : [
                                <ResultRow
                                  key={i}
                                  r={item}
                                  label={`${item.from}–${item.through}`}
                                />,
                              ],
                        )}
                      </tbody>
                    </table>
                  </div>
                  <div className="cost-pagination">
                    <button
                      disabled={!researchOffset}
                      onClick={() =>
                        setResearchOffset(Math.max(0, researchOffset - 30))
                      }
                    >
                      上一页
                    </button>
                    <span>共{research.data.total}项</span>
                    <button
                      disabled={research.data.nextOffset == null}
                      onClick={() =>
                        setResearchOffset(research.data!.nextOffset!)
                      }
                    >
                      下一页
                    </button>
                  </div>
                </>
              )}
            </section>
          )}
          <details className="cost-method">
            <summary>
              <Info size={16} /> 数据口径、更新时间与使用边界
            </summary>
            <p>{active.methodNote}</p>
            <p>
              集中度分母：{qty(active.metrics.denominator)}{" "}
              BTC（来源覆盖的两类供给合计，不是独立核验的全网供给）。
            </p>
            <p>
              STH {qty(active.frame!.sth.total)} BTC · LTH{" "}
              {qty(active.frame!.lth.total)} BTC
            </p>
            <p>
              首次取得：{stamp(active.frame!.firstSeen)}；包构建：
              {stamp(active.frame!.builtAt)}；最近成功检查：
              {stamp(active.health.feed.lastCheck)}
              （以上为北京时间）。指标日期按UTC。
            </p>
            <p>{active.note}</p>
            <p>
              规则 {active.rulesVersion} · 存储{" "}
              {qty(active.health.usedBytes / 1048576)} /{" "}
              {qty(active.health.budgetBytes / 1048576, 0)} MiB
            </p>
          </details>
        </>
      )}
    </section>
  );
}
function CostDetail({
  data,
  compare,
  setCompare,
  onHistory,
}: {
  data: Board;
  compare: string;
  setCompare: (d: string) => void;
  onHistory: () => void;
}) {
  return (
    <div className="cost-detail">
      <span>所选固定区间 · USD</span>
      <h2>
        $
        {qty(
          data.selected.low,
          bucketDigits(Number(data.selected.high) - Number(data.selected.low)),
        )}
        –$
        {qty(
          data.selected.high,
          bucketDigits(Number(data.selected.high) - Number(data.selected.low)),
        )}
      </h2>
      <strong>
        {bounds(data.selected.supply.total)} <small>BTC</small>
      </strong>
      <dl>
        <div>
          <dt className="sth">STH</dt>
          <dd>{bounds(data.selected.supply.sth)} BTC</dd>
        </div>
        <div>
          <dt className="lth">LTH</dt>
          <dd>{bounds(data.selected.supply.lth)} BTC</dd>
        </div>
        <div>
          <dt>占来源供给</dt>
          <dd>
            {bounds(
              {
                lower: String(
                  (Number(data.selected.supply.total.lower) /
                    Number(data.metrics.denominator)) *
                    100,
                ),
                upper: String(
                  (Number(data.selected.supply.total.upper) /
                    Number(data.metrics.denominator)) *
                    100,
                ),
              },
              "%",
            )}
          </dd>
        </div>
      </dl>
      <p className="cost-helper">
        距同日收盘：
        {qty((Number(data.selected.low) / Number(data.frame!.price) - 1) * 100)}
        % 至{" "}
        {qty(
          (Number(data.selected.high) / Number(data.frame!.price) - 1) * 100,
        )}
        %
      </p>
      <h3>同一美元区间发生了什么？</h3>
      {data.selected.changes.map((c, i) => (
        <div className="cost-change" key={`${c.from}-${i}`}>
          <span>
            {c.from} → {c.to}（
            {Math.round((Date.parse(c.to) - Date.parse(c.from)) / dayMs)}日）
          </span>
          <p>{c.change ? `${bounds(c.change.total)} BTC` : "该日无可比快照"}</p>
          {c.change && (
            <small>
              STH {bounds(c.change.sth)} / LTH {bounds(c.change.lth)}
            </small>
          )}
        </div>
      ))}
      <label>
        另选真实比较日期
        <select value={compare} onChange={(e) => setCompare(e.target.value)}>
          <option value="">选择日期</option>
          {[...data.dates]
            .reverse()
            .filter((d) => d < data.frame!.date)
            .map((d) => (
              <option key={d}>{d}</option>
            ))}
        </select>
      </label>
      <button className="secondary" onClick={onHistory}>
        查看固定区间历史
      </button>
      <p className="cost-helper">
        分类变化可能包含币龄迁移。出现数值范围表示原始桶被边界切开，未按比例估算。
      </p>
    </div>
  );
}
function EvidencePanel({ items }: { items: Evidence[] }) {
  return (
    <section className="cost-section">
      <h2>交叉核对 · 各项证据独立</h2>
      <div className="cost-evidence">
        {items.map((e) => (
          <article key={e.kind}>
            <div>
              <h3>{e.kind === "oi" ? "BTC折算OI变化" : e.title}</h3>
              <span className={e.status === "available" ? "" : "amber"}>
                {e.status === "available"
                  ? "可用"
                  : e.status === "partial"
                    ? "部分覆盖"
                    : "缺失"}
              </span>
            </div>
            <small>
              {e.source || "来源标识未记录"} · {e.from} UTC日 ·{" "}
              {e.coverage == null
                ? "独立口径"
                : `覆盖 ${qty(e.coverage * 100)}%`}
            </small>
            {e.data &&
              Object.entries(e.data)
                .filter(([k]) => labels[k])
                .map(([k, v]) => (
                  <p key={k}>
                    {labels[k]}：{v == null ? "未知" : qty(String(v))}
                  </p>
                ))}
            {e.kind === "funding" &&
              Array.isArray(e.data?.items) &&
              (
                e.data.items as {
                  venue: string;
                  ratePercent: string;
                  intervalHours: number | null;
                  rateKind: string;
                }[]
              ).map((f, i) => (
                <p key={i}>
                  {f.venue} {qty(f.ratePercent, 4)}% ·{" "}
                  {f.intervalHours ?? "未知"}小时 · {f.rateKind || "类型未知"}
                </p>
              ))}
            {e.kind === "flow" && (
              <p className="cost-helper">
                最长连续缺口：{e.data?.longestGapMinutes == null ? "未知（原记录未保存）" : `${qty(String(e.data.longestGapMinutes))}分钟`}；实际共同来源集合与来源变化未核实，时间覆盖不能代替来源完整性。
              </p>
            )}
            {e.kind === "oi" ? (
              <>
                <p className="cost-helper">BTC折算OI；原生合约数量、合约类型与面值未核实，不能单独推断增减仓。该说明同样适用于旧版保存的数值，原始证据不改写。</p>
                <details>
                  <summary>查看采集时原始说明（不作为当前口径）</summary>
                  <p className="cost-helper">{e.note}</p>
                </details>
              </>
            ) : <p className="cost-helper">{e.note}</p>}
            <small>证据截至 {stamp(e.asOf)}（北京时间）</small>
          </article>
        ))}
      </div>
    </section>
  );
}
function ResultRow({ r, label }: { r: Result; label: string }) {
  return (
    <tr>
      <td>{label}</td>
      <td>{r.horizon}日</td>
      <td>
        {{ complete: "完整", waiting: "等待", missing: "缺失" }[r.status] ??
          r.status}
      </td>
      <td>{r.return == null ? "—" : `${qty(r.return)}%`}</td>
      <td>
        {r.pathMax == null ? "—" : `${qty(r.pathMax)}% / ${qty(r.pathMin)}%`}
      </td>
      <td>{r.volatility == null ? "—" : `${qty(r.volatility)}%`}</td>
      <td>{r.directionalReturn == null ? "—" : `${qty(r.directionalReturn)}%`} / {r.volatilityRatio == null ? "—" : `${qty(r.volatilityRatio)}倍`}<small>锚点 {r.anchorDate ?? "原规则"}；发现至锚点 {r.discoveryToAnchor == null ? "未知" : `${qty(r.discoveryToAnchor)}%`}{r.selected === false ? " · 重叠样本" : ""}</small></td>
    </tr>
  );
}
function CostHistoryChart({
  data,
  heatmap,
}: {
  data: History;
  heatmap: boolean;
}) {
  const option = useMemo(() => {
    const dayList = days(data.from, data.to);
    const costs = new Map(data.points.map((p) => [p.date, p]));
    const prices = new Map(data.prices.map((p) => [p.date, p.value]));
    if (heatmap) {
      const levels = [...new Set(data.cells.map((c) => c[1]))].sort(
        (a, b) => Number(a) - Number(b),
      );
      const xs = new Map(dayList.map((d, i) => [d, i]));
      const ys = new Map(levels.map((d, i) => [d, i]));
      return {
        tooltip: {
          formatter: (p: { value: number[] }) =>
            `${dayList[p.value[0]]} · $${levels[p.value[1]]}<br/>${qty(p.value[2])} BTC`,
        },
        grid: { left: 80, right: 30, top: 20, bottom: 70 },
        xAxis: {
          type: "category",
          data: dayList,
          axisLabel: { hideOverlap: true },
        },
        yAxis: { type: "category", data: levels },
        visualMap: {
          min: 0,
          max: Math.max(1, ...data.cells.map((c) => Number(c[2]))),
          orient: "horizontal",
          bottom: 0,
          left: "center",
          inRange: { color: ["#1b2c38", "#496bcc", "#bed1ff"] },
        },
        series: [
          {
            type: "heatmap",
            data: data.cells.map((c) => [
              xs.get(c[0]),
              ys.get(c[1]),
              Number(c[2]),
            ]),
          },
        ],
      };
    }
    return {
      tooltip: { trigger: "axis" },
      legend: { data: ["同源日收盘 USD", "集中度下限 %", "集中度上限 %"] },
      grid: { left: 85, right: 55, bottom: 55, top: 40 },
      xAxis: {
        type: "category",
        data: dayList,
        axisLabel: { hideOverlap: true },
      },
      yAxis: [
        { type: "value", scale: true, name: "USD" },
        { type: "value", scale: true, name: "%" },
      ],
      dataZoom: [{ type: "inside" }],
      series: [
        {
          name: "同源日收盘 USD",
          type: "line",
          showSymbol: false,
          connectNulls: false,
          itemStyle: { color: "#c1cbc6" },
          data: dayList.map((d) =>
            prices.has(d) ? Number(prices.get(d)) : null,
          ),
          markLine: {
            symbol: "none",
            silent: true,
            label: { formatter: "{b}", fontSize: 10 },
            lineStyle: { color: "#acbac8", type: "dashed" },
            data: (data.events ?? []).map((e) => ({
              name: eventNames[e.kind] ?? e.kind,
              xAxis: e.date,
            })),
          },
        },
        ...["lower", "upper"].map((key, i) => ({
          name: i ? "集中度上限 %" : "集中度下限 %",
          type: "line",
          yAxisIndex: 1,
          showSymbol: true,
          symbolSize: 4,
          connectNulls: false,
          itemStyle: { color: i ? "#f2b778" : "#7598ef" },
          data: dayList.map((d) =>
            costs.has(d)
              ? Number(costs.get(d)!.concentration[key as keyof Bounds])
              : null,
          ),
        })),
      ],
    };
  }, [data, heatmap]);
  const supply = useMemo(
    () => ({
      tooltip: { trigger: "axis" },
      grid: { left: 85, right: 30, top: 30, bottom: 45 },
      xAxis: { type: "time" },
      yAxis: { type: "value", name: "固定区间 BTC", scale: true },
      series: [
        {
          type: "line",
          showSymbol: true,
          connectNulls: false,
          itemStyle: { color: "#98b7f5" },
          data: days(data.from, data.to).map((d) => {
            const p = data.points.find((p) => p.date === d);
            return [d, p ? Number(p.supply.total.lower) : null];
          }),
        },
        {
          type: "line",
          showSymbol: true,
          connectNulls: false,
          itemStyle: { color: "#f2b778" },
          data: days(data.from, data.to).map((d) => {
            const p = data.points.find((p) => p.date === d);
            return [d, p ? Number(p.supply.total.upper) : null];
          }),
        },
      ],
    }),
    [data],
  );
  return (
    <>
      <Chart
        option={option}
        height={350}
        label={
          heatmap
            ? "真实快照历史热力图，空白表示缺失"
            : "日价格与集中度上下限，缺日断开"
        }
      />
      {!heatmap && (
        <Chart
          option={supply}
          height={230}
          label="固定美元区间供给上下限历史"
        />
      )}
    </>
  );
}
