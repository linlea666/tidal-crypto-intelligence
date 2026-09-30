import { useEffect, useMemo, useState } from "react";
import { api, clock, price, useAPI } from "./data";
import { Chart } from "./Chart";
import type { Asset } from "./types";
import { modelScale, liquidationUSD, selectZoneRows } from "./liquidationMath";
import "./liquidations.css";

type Zone = {
  id: string;
  low: number;
  high: number;
  quote: string;
  side: string;
  strength: string;
  relative: number;
  lowUsd: number | null;
  highUsd: number | null;
  distancePercent: number | null;
  distanceNative: number | null;
  recordedAt?: string;
  lastBar: string;
  state: string;
  trend: string;
  changePercent: number | null;
  continuousSince: string;
  firstVisibleAt: string;
  lastSeenAt: string;
  samples: number;
  missingSamples: number;
  touchedAt: string | null;
  crossedAt: string | null;
  reclaimedAt: string | null;
  uncertain?: string;
  eligible: boolean;
  whaleCents: number | null;
  whaleCount: number | null;
  contributions: {
    venue: string;
    instrument: string;
    quote: string;
    strength: string;
  }[];
};
type Evidence = {
  netCents: number | null;
  funding?: {
    venue: string;
    ratePercent: string;
    intervalHours: number | null;
    margin: string;
    rateKind: string;
  }[];
  name: string;
  state: string;
  detail: string;
  at: string | null;
};
type Model = {
  data: { prices?: number[]; times?: number[]; cells?: number[][] } | null;
  meta: {
    expiresAt: string;
    fetchedAt: string;
    observedAt: string | null;
    status: string;
  };
};
type Risk = {
  snapshot: {
    asset: string;
    period: string;
    fetchedAt: string;
    sourceAt: string | null;
    availableAt: string;
    complete: boolean;
    reason: string;
    coverage: string[];
    maximum: string;
    zones: Zone[];
    reference: number;
    quote: string;
  };
  reference: {
    at: string;
    native: number | null;
    usd: number | null;
    quote: string;
    priceAt: string | null;
    fx: string;
    fxAt: string | null;
    fxValid: boolean;
    atr: number | null;
  };
  fresh: boolean;
  unitLabel: string;
  unitBasis: string;
  whaleCoverage: {
    covered: number | null;
    excludedNull: number | null;
    excludedStale: number | null;
    excludedInvalid: number | null;
    available: boolean;
    reason: string;
  };
  realized: {
    from: string;
    to: string;
    coverage: number;
    longCents: number | null;
    shortCents: number | null;
  };
  realizedError: string;
  realizedVenues: string[];
  evidence: Record<string, Evidence[]>;
  gap: { paused: boolean; reason: string; at: string };
};
type Response = {
  map: Model;
  heatmap: Model;
  risk: Risk | null;
  riskError?: string;
  note: string;
};
type History = { items: Zone[]; nextCursor: string; hours: number };
type Outcome = {
  state: string;
  hit: boolean | null;
  coverage: number;
  minutesToTouch: number | null;
  crossed: boolean | null;
  reclaimed: boolean | null;
  mfeNative: number | null;
  maeNative: number | null;
  reason: string;
};
type Study = {
  supported: boolean;
  reason?: string;
  startedAt: string;
  status: string;
  note: string;
  groups: {
    side: string;
    hours: number;
    events: number;
    complete: number;
    observing: number;
    uncertain: number;
    unmatched: number;
    matched: number;
    coverage: number;
    modelCoverage: number;
    days: number;
    ready: boolean;
    touchRate: number | null;
    confidenceInterval: number[] | null;
    controlTouchRate: number | null;
    matchedTouchRate: number | null;
    matchedConfidenceInterval: number[] | null;
    controlConfidenceInterval: number[] | null;
    evidenceGroups: {
      name: string;
      state: string;
      events: number;
      complete: number;
      observing: number;
      coverage: number;
      touchRate: number | null;
      confidenceInterval: number[] | null;
    }[];
  }[];
  recent: {
    id: string;
    side: string;
    selectedAt: string;
    unmatchedReason: string;
    outcomes: Record<string, Outcome>;
  }[];
};
const states: Record<string, string> = {
  new: "新观察",
  persistent: "持续观察",
  approaching: "接近",
  touched: "已触及",
  crossed: "穿越",
  reclaimed: "收回",
  disappeared: "模型不再显示",
  stale: "数据过期",
  untracked: "历史／等待验证",
};
const trends: Record<string, string> = {
  new: "首次观察",
  stronger: "增强",
  weaker: "减弱",
  stable: "变化未达20%",
  unknown: "变化未知",
  incomparable: "口径或连续性变化",
  from_zero: "由零转为有值",
};
const sideName = (side: string) =>
  side === "short" ? "空头风险" : side === "long" ? "多头风险" : "中性区域";
const validDate = (s?: string | null) =>
  !!s && !s.startsWith("0001") && Number.isFinite(Date.parse(s));
const stamp = (s?: string | null) =>
  validDate(s)
    ? new Date(s!).toLocaleString("zh-CN", {
        hour12: false,
        timeZone: "Asia/Shanghai",
      })
    : "未知";
const dist = (z: Zone, live: boolean) =>
  live && z.distancePercent != null
    ? `${z.distancePercent.toFixed(2)}%`
    : "距离未知";

export function LiquidationPage({ asset }: { asset: Asset }) {
  const [period, setPeriod] = useState("24h"),
    [span, setSpan] = useState("5"),
    [order, setOrder] = useState("price"),
    [expanded, setExpanded] = useState(false),
    [selected, setSelected] = useState(""),
    [message, setMessage] = useState(""),
    [heat, setHeat] = useState(false),
    [now, setNow] = useState(Date.now());
  const { data, error } = useAPI<Response>(
    `liquidations?asset=${asset}&period=${period}`,
    10000,
  );
  const { data: study, error: studyError } = useAPI<Study>(
    data ? `liquidation-study?asset=${asset}` : null,
    60000,
  );
  useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(t);
  }, []);
  useEffect(() => {
    setSelected("");
    setExpanded(false);
    setMessage("");
  }, [asset, period]);
  const risk = data?.risk,
    snapshot = risk?.snapshot,
    ref = risk?.reference;
  const fresh = !!risk?.fresh && now < Date.parse(data!.map.meta.expiresAt);
  const live =
    !!ref?.priceAt &&
    ref.native != null &&
    now - Date.parse(ref.priceAt) <= 15000;
  const fx =
    !!ref?.fxValid &&
    (ref.quote === "USD" ||
      (!!ref.fxAt && now - Date.parse(ref.fxAt) <= 30000));
  const labelPrice = (z: Zone) =>
    fx && z.lowUsd != null && z.highUsd != null
      ? `$${price(z.lowUsd, 0)}–${price(z.highUsd, 0)}`
      : `${price(z.low, 0)}–${price(z.high, 0)} ${z.quote || "原币未知"}`;
  const displayZones = (snapshot?.zones ?? []).map((z) => ({
    ...z,
    state: !fresh
      ? "stale"
      : z.state === "approaching" && !live
        ? z.samples >= 3 &&
          Date.parse(z.lastSeenAt) - Date.parse(z.continuousSince) >= 1800000
          ? "persistent"
          : "new"
        : z.state,
  }));
  const filtered = displayZones.filter(
    (z) =>
      span === "all" ||
      !live ||
      ref?.native == null ||
      (z.high >= ref.native * (1 - Number(span) / 100) &&
        z.low <= ref.native * (1 + Number(span) / 100)),
  );
  const chosen =
    filtered.find((z) => z.id === selected) ??
    filtered.find((z) => z.eligible) ??
    filtered[0];
  const { data: history, error: historyError } = useAPI<History>(
    chosen
      ? `liquidation-zones/history?asset=${asset}&zoneId=${chosen.id}&hours=24`
      : null,
    60000,
  );
  useEffect(() => {
    if (period === "24h") return;
    let current = true;
    void (async () => {
      try {
        for (const kind of ["map", "heatmap"]) {
          if (!current) return;
          await api("data-requests", {
            method: "POST",
            body: JSON.stringify({
              dataset: `${kind}.${asset.toLowerCase()}..futures`,
              range: period,
            }),
          });
        }
        if (current) setMessage("所选周期进入共享加载队列，完成后自动显示。");
      } catch (e) {
        if (current) setMessage((e as Error).message);
      }
    })();
    return () => {
      current = false;
    };
  }, [asset, period]);
  const heatOption = useMemo(
    () => ({
      grid: { left: 65, right: 55, bottom: 40, top: 15 },
      tooltip: {},
      xAxis: {
        type: "category",
        data: data?.heatmap.data?.times?.map(clock) ?? [],
      },
      yAxis: { type: "category", data: data?.heatmap.data?.prices ?? [] },
      visualMap: {
        min: 0,
        max: (data?.heatmap.data?.cells ?? []).reduce(
          (max, c) => Math.max(max, c[2]),
          1,
        ),
        right: 0,
        inRange: { color: ["#18231f", "#365b45", "#7deba9"] },
      },
      series: [{ type: "heatmap", data: data?.heatmap.data?.cells ?? [] }],
    }),
    [data],
  );
  function summary(side: string) {
    const eligible = filtered.filter(
      (z) => z.side === side && z.eligible && fresh,
    );
    const strongest = [...eligible].sort((a, b) => b.relative - a.relative)[0];
    const nearest = live
      ? eligible
          .filter((z) => z.relative >= 50 && z.distancePercent != null)
          .sort((a, b) => a.distancePercent! - b.distancePercent!)[0]
      : undefined;
    return (
      <section className={`lz-summary lz-${side}`} key={side}>
        <h3>
          {side === "short" ? "上方 · 空头风险" : "下方 · 多头风险"}
          <small>{side === "short" ? "潜在买入压力" : "潜在卖出压力"}</small>
        </h3>
        {[
          ["最强区域", strongest],
          ["最近较强区域", nearest],
        ].map(([title, raw]) => {
          const z = raw as Zone | undefined;
          return (
            <div key={title as string}>
              <span>{title as string}</span>
              {z ? (
                <button onClick={() => setSelected(z.id)}>
                  <b>{labelPrice(z)}</b>
                  <strong>
                    {modelScale(z.strength)} <small>模型强度·非美元</small>
                  </strong>
                  <span>
                    {dist(z, live)} · {states[z.state]}
                  </span>
                </button>
              ) : (
                <p>
                  {!fresh
                    ? "模型过期或缺失，暂停判断"
                    : !snapshot?.complete
                      ? snapshot?.reason || "覆盖未知"
                      : title === "最近较强区域" && !live
                        ? "现价无效，距离暂停"
                        : "当前范围未发现符合条件的区域"}
                </p>
              )}
            </div>
          );
        })}
      </section>
    );
  }
  const detail =
    chosen && risk ? (
      <ZoneDetail
        key={chosen.id}
        zone={chosen}
        risk={risk}
        label={labelPrice(chosen)}
        live={live}
        fx={fx}
        history={history}
        historyError={historyError}
        asset={asset}
      />
    ) : null;
  function sideRows(side: string) {
    const zones = filtered.filter((z) => z.side === side),
      rows = selectZoneRows(zones, expanded, order);
    return (
      <section className={`lz-side lz-${side}`} key={side}>
        <h3>
          {side === "short"
            ? "上方空头风险区 · 潜在买入压力"
            : side === "long"
              ? "下方多头风险区 · 潜在卖出压力"
              : "横跨模型参考价 · 中性"}
          <small>
            {rows.length} / {zones.length} 个区域
          </small>
        </h3>
        {rows.length ? (
          rows.map((z) => (
            <div key={z.id}>
              <button
                className={`lz-row ${chosen?.id === z.id ? "is-selected" : ""} ${!fresh || !z.eligible ? "lz-muted" : ""}`}
                onClick={() => setSelected(z.id)}
                aria-pressed={chosen?.id === z.id}
              >
                <span className="lz-row-price">
                  <b>{labelPrice(z)}</b>
                  <small>
                    {sideName(z.side)} · {fresh ? states[z.state] : "数据过期"}
                  </small>
                </span>
                <span
                  className="lz-bar"
                  aria-label={`相对强度 ${z.relative.toFixed(1)}，全地图共用尺度`}
                >
                  <i
                    style={{
                      width: `${Math.max(0, Math.min(100, z.relative))}%`,
                    }}
                  />
                </span>
                <strong
                  className="lz-scale"
                  title={`${z.strength} · 模型强度·非美元`}
                >
                  {modelScale(z.strength)}
                  <small>模型强度·非美元</small>
                </strong>
                <span className="lz-row-meta">
                  <b>{dist(z, live)}</b>
                  <small>
                    {z.relative.toFixed(1)} ·{" "}
                    {z.relative >= 80
                      ? "高度集中"
                      : z.relative >= 50
                        ? "较集中"
                        : "一般"}
                  </small>
                  <small>
                    {validDate(z.continuousSince)
                      ? `连续 ${Math.max(0, Math.floor((Date.parse(z.lastSeenAt) - Date.parse(z.continuousSince)) / 60000))} 分钟`
                      : "尚无连续观察"}
                  </small>
                </span>
              </button>
              {chosen?.id === z.id && (
                <div className="lz-mobile-detail">{detail}</div>
              )}
            </div>
          ))
        ) : (
          <p className="lz-empty">
            {snapshot?.complete && fresh
              ? "当前范围未发现该侧区域。"
              : "该侧暂无有效覆盖；缺失不代表零。"}
          </p>
        )}
      </section>
    );
  }
  return (
    <div className="liquidation-page">
      <div className="toolbar page-toolbar">
        <div className="segmented">
          {["24h", "7d", "30d"].map((p) => (
            <button
              key={p}
              className={period === p ? "selected" : ""}
              onClick={() => setPeriod(p)}
            >
              {p === "24h" ? "24小时" : p === "7d" ? "7天" : "30天"}
            </button>
          ))}
        </div>
        <label>
          现价范围{" "}
          <select
            aria-label="现价范围"
            value={span}
            onChange={(e) => setSpan(e.target.value)}
          >
            {["3", "5", "10", "all"].map((s) => (
              <option key={s} value={s}>
                {s === "all" ? "全部返回范围" : `±${s}%`}
              </option>
            ))}
          </select>
        </label>
        <button className="secondary" onClick={() => setHeat(!heat)}>
          {heat ? "返回风险地图" : "高级：历史热力图"}
        </button>
      </div>
      <p className="helper">
        {data?.note ?? "清算模型、已发生清算与持仓金额独立展示。"}
      </p>
      <div className="lz-source">
        <span>获取 {stamp(snapshot?.fetchedAt)}</span>
        <span>来源时间 {stamp(snapshot?.sourceAt)}</span>
        <span>
          {snapshot?.sourceAt
            ? "按来源时效判断"
            : "仅知道获取时间，无法证明模型发布时间"}
        </span>
        <span>
          覆盖{" "}
          {snapshot?.coverage?.length ? snapshot.coverage.join(" · ") : "未知"}
        </span>
      </div>
      {(error || data?.riskError) && (
        <p role="alert" className="sell">
          {error || data?.riskError}
        </p>
      )}
      {message && <p role="status">{message}</p>}
      {!fresh && (
        <p className="lz-warning">
          当前仅供历史浏览或等待数据；过期区域退出重点判断。
        </p>
      )}
      {snapshot?.reason && <p className="lz-warning">{snapshot.reason}</p>}
      {risk?.gap.paused && (
        <p className="lz-warning">验证暂停：{risk.gap.reason}</p>
      )}
      {heat ? (
        <section className="data-section">
          <h2>历史模型强度 · 独立高级视图</h2>
          <p className="helper">
            获取 {stamp(data?.heatmap.meta.fetchedAt)} ·{" "}
            {data?.heatmap.meta.status === "stale"
              ? "过期历史快照"
              : "模型历史切片"}
            ；不与风险地图相加。
          </p>
          {data?.heatmap.data?.cells?.length ? (
            <Chart option={heatOption} height={420} label="历史清算热力图" />
          ) : (
            <p>所选周期暂无热力图覆盖。</p>
          )}
        </section>
      ) : (
        <>
          <div className="lz-summaries">
            {summary("short")}
            {summary("long")}
          </div>
          <div className="lz-layout">
            <section className="lz-map">
              <div className="lz-map-heading">
                <h2>双向清算风险地图</h2>
                <label>
                  排序{" "}
                  <select
                    aria-label="区域排序"
                    value={order}
                    onChange={(e) => setOrder(e.target.value)}
                  >
                    <option value="price">价格</option>
                    <option value="strength">强度</option>
                    <option value="distance">距离</option>
                  </select>
                </label>
                <button
                  className="secondary"
                  onClick={() => setExpanded(!expanded)}
                >
                  {expanded ? "收起区域" : "展开全部区域"}
                </button>
              </div>
              <p className="helper">
                上下侧共用完整地图线性比例尺；100对应{" "}
                {modelScale(snapshot?.maximum)}{" "}
                模型强度·非美元。折叠和筛选不改变尺度。相对强度不是触达概率。
              </p>
              {sideRows("short")}
              <div className="lz-current">
                <strong>
                  {live && ref?.native != null
                    ? fx && ref.usd != null
                      ? `$${price(ref.usd)} USD`
                      : `${price(ref.native)} ${ref.quote}`
                    : "现价无效 · 距离和接近判断暂停"}
                </strong>
                <span>
                  {live ? `现价冻结于 ${clock(ref!.priceAt!)}` : "等待有效现价"}
                </span>
                <small>
                  模型参考价{" "}
                  {snapshot?.reference
                    ? `${price(snapshot.reference)} ${snapshot.quote}`
                    : "未知"}{" "}
                  · 事件方向冻结
                </small>
                {!fx && <small>FX缺失或过期，显示原币报价</small>}
              </div>
              {sideRows("long")}
              {filtered.some((z) => z.side === "neutral") &&
                sideRows("neutral")}
            </section>
            <aside className="lz-desktop-detail">
              {detail ?? <p className="lz-empty">选择一个区域查看证据。</p>}
            </aside>
          </div>
        </>
      )}
      <section className="data-section lz-realized">
        <h2>已发生清算 · 独立真实金额</h2>
        <p className="helper">
          请求范围 {risk?.realizedVenues?.join(" / ")} ·{" "}
          {stamp(risk?.realized.from)} — {stamp(risk?.realized.to)} ·
          时间窗口覆盖{" "}
          {risk ? (risk.realized.coverage * 100).toFixed(0) + "%" : "未知"}
        </p>
        <div className="lz-summaries">
          <div>
            <span>过去1小时已观察多头清算</span>
            <strong className="sell">
              {liquidationUSD(risk?.realized.longCents)}
            </strong>
          </div>
          <div>
            <span>过去1小时已观察空头清算</span>
            <strong className="buy">
              {liquidationUSD(risk?.realized.shortCents)}
            </strong>
          </div>
        </div>
        {risk?.realizedError && <p>{risk.realizedError}</p>}
        <p className="helper">
          窗口覆盖不完整时金额未知；上游聚合接口未提供逐交易所完整性证明。这些金额不与上方模型强度或持仓金额相加。
        </p>
      </section>
      <StudyPanel study={study} error={studyError} />
      <LeverageTool
        reference={ref?.native ?? null}
        quote={ref?.quote || "USDT"}
      />
    </div>
  );
}

function ZoneDetail({
  zone: z,
  risk,
  label,
  live,
  fx,
  history,
  historyError,
  asset,
}: {
  zone: Zone;
  risk: Risk;
  label: string;
  live: boolean;
  fx: boolean;
  history: History | null;
  historyError: string;
  asset: Asset;
}) {
  const [extra, setExtra] = useState<Zone[]>([]),
    [cursor, setCursor] = useState<string | null>(null),
    [loadError, setLoadError] = useState("");
  const items = [...(history?.items ?? []), ...extra];
  const next = cursor ?? history?.nextCursor;
  async function loadMore() {
    try {
      const result = await api<History>(
        `liquidation-zones/history?asset=${asset}&zoneId=${z.id}&hours=24&cursor=${encodeURIComponent(next ?? "")}`,
      );
      setExtra((v) => [...v, ...result.items]);
      setCursor(result.nextCursor);
    } catch (e) {
      setLoadError((e as Error).message);
    }
  }
  const historyOption = {
    grid: { left: 50, right: 12, top: 10, bottom: 35 },
    tooltip: {
      trigger: "axis",
      valueFormatter: (v: unknown) => modelScale(String(v)) + " · 非美元",
    },
    xAxis: {
      type: "category",
      data: items.map((v) => clock(v.recordedAt ?? v.lastSeenAt)),
    },
    yAxis: {
      type: "value",
      axisLabel: { formatter: (v: number) => modelScale(String(v)) },
    },
    series: [
      {
        type: "line",
        showSymbol: true,
        connectNulls: false,
        data: items.map((v) => Number(v.strength)),
        lineStyle: { color: z.side === "short" ? "#7deba9" : "#f17369" },
      },
    ],
  };
  const wc = risk.whaleCoverage;
  return (
    <section className="lz-detail">
      <small>所选区域 · {sideName(z.side)}</small>
      <h2>{label}</h2>
      <strong className="lz-detail-number">{modelScale(z.strength)}</strong>
      <p>模型强度·非美元 · {dist(z, live)}</p>
      <details>
        <summary>查看完整原始值与口径</summary>
        <p className="lz-wrap">{z.strength} · 模型强度·非美元</p>
        <p>
          {price(z.low)}–{price(z.high)} {z.quote || "报价未知"}
        </p>
        <p>{risk.unitBasis}</p>
      </details>
      <div className="lz-detail-status">
        <b>
          {states[z.state]}
          {z.uncertain ? " · 判定有缺口" : ""}
        </b>
        <span>
          {trends[z.trend] ?? "变化未知"}
          {z.changePercent != null
            ? ` ${z.changePercent > 0 ? "+" : ""}${z.changePercent.toFixed(1)}%`
            : ""}
        </span>
        <small>首次可见 {stamp(z.firstVisibleAt)}</small>
        <small>
          连续 {z.samples} 次 · 起于 {stamp(z.continuousSince)}
        </small>
        {z.uncertain && <p className="lz-warning">{z.uncertain}</p>}
      </div>
      <h3>交易所／合约贡献</h3>
      <ul className="lz-contributions">
        {z.contributions.map((c, i) => (
          <li key={`${c.venue}/${c.instrument}/${i}`}>
            <span>
              {c.venue}
              <small>
                {c.instrument} · {c.quote || "报价未知"}
              </small>
            </span>
            <b title={c.strength}>{modelScale(c.strength)}</b>
          </li>
        ))}
      </ul>
      <h3>最近24小时模型规模</h3>
      {items.length ? (
        <Chart option={historyOption} height={150} label="区域模型强度历史" />
      ) : (
        <p className="helper">尚无新规则历史；旧数据不回填为前向证据。</p>
      )}
      <details>
        <summary>状态时间线 · {items.length} 条</summary>
        <ul className="lz-timeline">
          {items.map((v, i) => (
            <li key={`${v.id}/${i}`}>
              <time>
                {stamp(
                  v.recordedAt ??
                    (validDate(v.lastBar) ? v.lastBar : v.lastSeenAt),
                )}
              </time>{" "}
              · {states[v.state]} · {modelScale(v.strength)}
              <small>{v.uncertain}</small>
            </li>
          ))}
        </ul>
        {next && (
          <button className="secondary" onClick={() => void loadMore()}>
            加载下一页
          </button>
        )}
      </details>
      {(historyError || loadError) && (
        <p className="sell">{historyError || loadError}</p>
      )}
      <h3>已覆盖Hyperliquid仓位</h3>
      <strong>{fx ? liquidationUSD(z.whaleCents) : "—"}</strong>
      <p className="helper">
        清算参考价落在此区间的已覆盖仓位名义价值；不是保证金或必然清算金额。
      </p>
      <p className="helper">
        区间 {z.whaleCount ?? "未知"} 笔 · 全部有效覆盖 {wc.covered ?? "未知"}{" "}
        笔；排除空清算价 {wc.excludedNull ?? "未知"}、过期{" "}
        {wc.excludedStale ?? "未知"}、无效 {wc.excludedInvalid ?? "未知"}。
        {wc.reason}
        {!fx && " FX不可用，暂停跨报价归集。"}
      </p>
      <h3>BTC资金证据 · 独立判断</h3>
      {asset !== "BTC" ? (
        <p className="helper">完整多因素研究首版仅支持BTC。</p>
      ) : (
        (risk.evidence[z.side] ?? []).map((e) => (
          <div className="lz-evidence" key={e.name}>
            <b>{e.name}</b>
            <span
              className={
                e.state === "support"
                  ? "buy"
                  : e.state === "conflict"
                    ? "sell"
                    : ""
              }
            >
              {{ support: "支持", conflict: "冲突", unknown: "未知" }[
                e.state
              ] ?? "未知"}
            </span>
            <small>
              {e.netCents != null && <b>{liquidationUSD(e.netCents)} · </b>}
              {e.detail} · {stamp(e.at)}
              {e.funding?.map((f) => (
                <span
                  className="lz-funding-point"
                  key={f.venue + f.margin + f.rateKind}
                >
                  {f.venue} · {f.ratePercent}% / {f.intervalHours ?? "未知"}小时
                  ·{" "}
                  {f.rateKind === "predicted"
                    ? "预测费率"
                    : f.rateKind === "settled"
                      ? "已结算费率"
                      : "费率类型未知"}
                </span>
              ))}
            </small>
          </div>
        ))
      )}
      <p className="helper">
        触及指公开现货价格进入模型区域，不确认交易所标记价格已触发强平。
      </p>
    </section>
  );
}
function StudyPanel({ study, error }: { study: Study | null; error: string }) {
  return (
    <section className="data-section">
      <h2>
        1小时／4小时前向验证 <small>效果验证中</small>
      </h2>
      {error && <p className="sell">{error}</p>}
      {!study ? (
        <p>等待本地验证报告。</p>
      ) : !study.supported ? (
        <p>{study.reason}</p>
      ) : (
        <>
          <p className="helper">
            规则开始 {stamp(study.startedAt)} · {study.note}
          </p>
          <div className="lz-study-grid">
            {study.groups.map((g) => (
              <section key={`${g.side}/${g.hours}`}>
                <h3>
                  {sideName(g.side)} · {g.hours}小时
                </h3>
                <strong>{g.events} 个独立事件</strong>
                <p>
                  完成 {g.complete} · 观察中 {g.observing} · 缺数／歧义{" "}
                  {g.uncertain}
                </p>
                <p>
                  有效窗口覆盖 {(g.coverage * 100).toFixed(1)}% · 模型时段覆盖{" "}
                  {(g.modelCoverage * 100).toFixed(1)}% · 已观察{" "}
                  {g.days.toFixed(1)} 天
                </p>
                <p>
                  匹配对照 {g.matched} · 未匹配 {g.unmatched}
                </p>
                {g.ready && g.touchRate != null ? (
                  <p>
                    样本触达率 {(g.touchRate * 100).toFixed(1)}%<br />
                    95%区间{" "}
                    {g.confidenceInterval
                      ?.map((v) => (v * 100).toFixed(1) + "%")
                      .join("–")}
                    <br />
                    {g.controlTouchRate != null
                      ? `匹配组高／低强度触达率 ${((g.matchedTouchRate ?? 0) * 100).toFixed(1)}% ／ ${(g.controlTouchRate * 100).toFixed(1)}%`
                      : "匹配对照样本不足"}
                  </p>
                ) : (
                  <p className="helper">
                    达到14天、95%有效覆盖、30个独立事件后展示样本率。
                  </p>
                )}
                {g.controlConfidenceInterval && (
                  <p className="helper">
                    匹配高强度95%区间{" "}
                    {g.matchedConfidenceInterval
                      ?.map((v) => (v * 100).toFixed(1) + "%")
                      .join("–")}
                    ；对照95%区间{" "}
                    {g.controlConfidenceInterval
                      .map((v) => (v * 100).toFixed(1) + "%")
                      .join("–")}
                  </p>
                )}
                <details>
                  <summary>资金证据分组</summary>
                  {g.evidenceGroups?.length ? (
                    g.evidenceGroups.map((f) => (
                      <p key={f.name + f.state}>
                        {f.name} ·{" "}
                        {{ support: "支持", conflict: "冲突", unknown: "未知" }[
                          f.state
                        ] ?? "未知"}
                        <br />
                        {f.events}个事件 / {f.complete}个完整窗口 · 覆盖
                        {(f.coverage * 100).toFixed(1)}%<br />
                        {f.touchRate == null
                          ? "该分组样本尚未达标"
                          : `样本触达率 ${(f.touchRate * 100).toFixed(1)}%；95%区间 ${f.confidenceInterval?.map((v) => (v * 100).toFixed(1) + "%").join("–")}`}
                      </p>
                    ))
                  ) : (
                    <p>尚无可分组事件。</p>
                  )}
                </details>
              </section>
            ))}
          </div>
          <details>
            <summary>最近事件与观察结果</summary>
            {study.recent.length ? (
              study.recent.map((e) => (
                <article className="lz-study-event" key={e.id}>
                  <b>
                    {sideName(e.side)} · {stamp(e.selectedAt)}
                  </b>
                  {Object.entries(e.outcomes).map(([h, o]) => (
                    <p key={h}>
                      {h}小时 ·{" "}
                      {{
                        observing: "观察中",
                        complete: "完整",
                        incomplete: "缺数据",
                        uncertain: "先后不明",
                      }[o.state] ?? o.state}{" "}
                      ·{" "}
                      {o.hit == null
                        ? "触达未知"
                        : o.hit
                          ? `触达耗时 ${o.minutesToTouch} 分钟`
                          : "未触达"}
                      <br />
                      穿越{" "}
                      {o.crossed == null ? "未知" : o.crossed ? "是" : "否"} ·
                      收回{" "}
                      {o.reclaimed == null ? "未知" : o.reclaimed ? "是" : "否"}{" "}
                      · 有利／不利位移 {o.mfeNative ?? "—"} /{" "}
                      {o.maeNative ?? "—"} USDT {o.reason}
                    </p>
                  ))}
                  {e.unmatchedReason && (
                    <small>未匹配：{e.unmatchedReason}</small>
                  )}
                </article>
              ))
            ) : (
              <p>尚无符合持续性、强度、ATR和价格覆盖要求的独立事件。</p>
            )}
          </details>
        </>
      )}
    </section>
  );
}
function LeverageTool({
  reference,
  quote,
}: {
  reference: number | null;
  quote: string;
}) {
  const [anchor, setAnchor] = useState(""),
    [leverage, setLeverage] = useState(20);
  const value = Number(anchor || reference);
  return (
    <details className="data-section lz-leverage">
      <summary>杠杆情景计算 · 简化保证金耗尽线</summary>
      <p>
        忽略维持保证金、手续费、资金费率和账户抵押品变化；不等于真实强平价，不进入模型或研究样本。
      </p>
      <label>
        锚点{" "}
        <input
          aria-label="杠杆锚点"
          inputMode="decimal"
          type="number"
          min="0"
          value={anchor}
          placeholder={reference?.toString() ?? "输入价格"}
          onChange={(e) => setAnchor(e.target.value)}
        />
      </label>
      <label>
        杠杆{" "}
        <select
          aria-label="情景杠杆"
          value={leverage}
          onChange={(e) => setLeverage(Number(e.target.value))}
        >
          {[100, 20, 10, 5].map((v) => (
            <option key={v} value={v}>
              {v}倍
            </option>
          ))}
        </select>
      </label>
      <p>
        多头耗尽线{" "}
        {value > 0 && Number.isFinite(value)
          ? price(value * (1 - 1 / leverage))
          : "—"}{" "}
        {quote} · 空头耗尽线{" "}
        {value > 0 && Number.isFinite(value)
          ? price(value * (1 + 1 / leverage))
          : "—"}{" "}
        {quote}
      </p>
    </details>
  );
}
