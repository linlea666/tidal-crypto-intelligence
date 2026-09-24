import { ActivityPage } from "./Activity";
import { useEffect, useMemo, useState } from "react";
import { ArrowRight, Copy, WarningCircle } from "@phosphor-icons/react";
import { api, amount, price, clock, venue, useAPI } from "./data";
import { Chart } from "./Chart";
import { Empty } from "./App";
import type {
  Asset,
  Frame,
  Health,
  Settings,
  Flow,
  WhalesResponse,
} from "./types";

type Props = {
  view: string;
  asset: Asset;
  frame: Frame | null;
  health: Health | null;
  onNavigate: (v: string) => void;
};
export type Meta = {
  venue?: string;
  status: string;
  observedAt: string | null;
  fetchedAt: string;
  expiresAt: string;
  resolutionSeconds: number;
};
type FlowData = Flow & {
  hasData: boolean;
  meta: Meta;
  footprintVenues: string[];
  footprintPartial: boolean;
  anchor: string;
};
export type Derivatives = {
  oiChangeCents: number | null;
  liquidationMeta: Meta;
  totalOiCents: number | null;
  items: {
    venue: string;
    oiUsdCents: number;
    oiBase: number;
    valid: boolean;
  }[];
  series: { time: number; oiUsdCents: number }[];
  funding: {
    asset: string;
    venue: string;
    ratePercent: string;
    intervalHours: number | null;
    margin: string;
  }[];
  meta: Meta;
  fundingMeta: Meta;
  longLiquidationCents: number | null;
  shortLiquidationCents: number | null;
  coverage: string;
};
type Whales = WhalesResponse & {
  hasData: boolean;
  longCents: number;
  shortCents: number;
  nearLiquidationCents: number;
  meta: Meta;
  distributionScope: string;
  monitor: WhalesResponse["monitor"] & { fresh: number };
};
type Dataset = {
  id: string;
  kind: string;
  asset: string;
  venue?: string;
  source: string;
  refreshSeconds: number;
  ttlSeconds: number;
};
type Job = {
  id: string;
  dataset: Dataset;
  next: string;
  error?: string;
  inFlight: boolean;
  mode: string;
  completed: boolean;
  disabled: boolean;
  calls: number;
};
type DataStatus = {
  datasets: {
    dataset: Dataset;
    status: string;
    observedAt: string | null;
    fetchedAt: string;
  }[];
  scheduler: {
    enabled: boolean;
    limitPerMinute: number;
    usedLastMinute: number;
    calls: number;
    rateLimited: number;
    authFailed: boolean;
    jobs: Job[];
  };
  storage: {
    usedBytes: number;
    freeBytes: number;
    fineHistoryPaused: boolean;
    sqlite: string;
    orderHistoryBytes: number;
    orderHistoryPaused: boolean;
  };
  rulesVersion: string;
  legacyCollectorsRunning: boolean;
};
type Model = {
  bins: { price: number; strength: number; venue?: string }[];
  prices?: number[];
  times?: number[];
  cells?: [number, number, number][];
  referencePrice: number;
};
type Liquidations = {
  map: { data: Model | null; meta: Meta };
  heatmap: { data: Model | null; meta: Meta };
  note: string;
};
type Large = {
  initialQuantity?: string | null;
  quantity: string;
  executedQuantity?: string | null;
  id: string;
  venue: string;
  side: string;
  price: string;
  quote: string;
  usdCents: number | null;
  executedUsd: string;
  endAt?: string | null;
  historical?: boolean;
  startAt: string | null;
  changedAt: string | null;
  valid: boolean;
  state: string;
};
const baseAxis = {
  axisLabel: { color: "#95a29d" },
  splitLine: { lineStyle: { color: "#293330" } },
  axisLine: { lineStyle: { color: "#35423d" } },
};
const names: Record<string, string> = {
  fresh: "有效",
  retrieval_only: "已获取 · 来源时间未知",
  stale: "已过期",
  missing: "等待数据",
};
export function Status({ meta }: { meta?: Meta }) {
  if (!meta) return null;
  const expired =
    meta.status === "stale" ||
    meta.status === "missing" ||
    new Date(meta.expiresAt).getTime() < Date.now();
  return (
    <p className={`source-status ${expired ? "amber" : ""}`}>
      <span className={`status-dot ${expired ? "warn" : ""}`} />
      {meta.venue ? `${meta.venue} · ` : ""}
      {expired ? "缺失或过期，暂停实时判断" : names[meta.status]} ·{" "}
      {meta.observedAt ? `来源 ${clock(meta.observedAt)}` : "来源时间未知"} ·
      获取{" "}
      {new Date(meta.fetchedAt).getFullYear() > 2000
        ? clock(meta.fetchedAt)
        : "等待首次采集"}
      {meta.resolutionSeconds > 0
        ? ` · ${meta.resolutionSeconds / 60}分钟精度`
        : ""}
    </p>
  );
}
export function Metric({
  title,
  value,
  unit = "USD",
  tone = "",
}: {
  title: string;
  value: string;
  unit?: string;
  tone?: string;
}) {
  return (
    <div className="metric">
      <span>{title}</span>
      <strong className={tone}>
        {value}
        <small>{unit}</small>
      </strong>
    </div>
  );
}
function WindowSelect({
  hours,
  set,
}: {
  hours: number;
  set: (n: number) => void;
}) {
  return (
    <label>
      统计窗口{" "}
      <select value={hours} onChange={(e) => set(+e.target.value)}>
        {[1, 4, 24, 168, 720, 2160].map((h) => (
          <option value={h} key={h}>
            {h < 24 ? `${h}小时` : `${h / 24}天`}
          </option>
        ))}
      </select>
    </label>
  );
}
export function lineOption(points: unknown[], name: string) {
  return {
    grid: { left: 80, right: 25, top: 20, bottom: 35 },
    tooltip: { trigger: "axis" },
    xAxis: { type: "time", ...baseAxis },
    yAxis: {
      type: "value",
      ...baseAxis,
      axisLabel: { formatter: (n: number) => amount(n) },
    },
    series: [
      {
        name,
        type: "line",
        showSymbol: false,
        connectNulls: false,
        lineStyle: { color: "#7deba9", width: 2 },
        data: points,
      },
    ],
  };
}
function Backfill({
  asset,
  market,
  hours,
}: {
  asset: Asset;
  market: string;
  hours: number;
}) {
  const [message, set] = useState("");
  return (
    <div className="backfill">
      <button
        className="secondary"
        onClick={async () => {
          const to = new Date(),
            from = new Date(to.getTime() - hours * 3600000);
          try {
            await api("data-requests", {
              method: "POST",
              body: JSON.stringify({
                dataset: `flow.${asset.toLowerCase()}..${market}`,
                from: from.toISOString(),
                to: to.toISOString(),
                resolutionSeconds: hours > 720 ? 3600 : hours > 24 ? 900 : 60,
              }),
            });
            set("已加入共享补采队列，按剩余额度推进。");
          } catch (e) {
            set((e as Error).message);
          }
        }}
      >
        补齐此窗口历史 <ArrowRight size={15} />
      </button>
      <small role="status">{message}</small>
    </div>
  );
}
export function MarketPages(p: Props) {
  if (p.view === "activity")
    return <ActivityPage key={p.asset} asset={p.asset} />;
  if (p.view === "flow") return <FlowPage {...p} />;
  if (p.view === "derivatives") return <DerivativesPage {...p} />;
  if (p.view === "whales") return <WhalesPage {...p} />;
  if (p.view === "liquidations") return <LiquidationPage {...p} />;
  if (p.view === "health") return <HealthPage {...p} />;
  return <Empty text="选择上方页面查看数据。" />;
}
function FlowPage({ asset }: Props) {
  const [hours, setHours] = useState(1),
    [market, setMarket] = useState("spot");
  const anchor = useMemo(
    () => new Date(Date.now() - hours * 3600000).toISOString(),
    [asset, hours, market],
  );
  const { data, error } = useAPI<FlowData>(
    `flow?asset=${asset}&hours=${hours}&market=${market}&anchor=${encodeURIComponent(anchor)}`,
  );
  const bins = Object.entries(data?.footprint ?? {})
      .map(([p, [buy, sell]]) => ({
        p: +p.split(":")[0],
        high: +p.split(":")[1],
        buy,
        sell,
      }))
      .sort((a, b) => b.buy + b.sell - (a.buy + a.sell))
      .slice(0, 40)
      .sort((a, b) => b.p - a.p),
    max = Math.max(1, ...bins.flatMap((b) => [b.buy, b.sell]));
  const option = useMemo(
    () =>
      lineOption(
        data?.series.map((p) => [p.time * 1000, p.cvdCents]) ?? [],
        "已观察CVD",
      ),
    [data],
  );
  return (
    <>
      <div className="toolbar page-toolbar">
        <div className="segmented">
          {[
            ["spot", "现货"],
            ["futures", "合约"],
          ].map(([v, t]) => (
            <button
              key={v}
              className={market === v ? "selected" : ""}
              onClick={() => setMarket(v)}
            >
              {t}
            </button>
          ))}
        </div>
        <WindowSelect hours={hours} set={setHours} />
        <span className="helper">主动净买入 = 主动买入额 − 主动卖出额</span>
      </div>
      {error && <p className="sell">{error}</p>}
      <Status meta={data?.meta} />
      <div className="metric-strip">
        <Metric
          title="主动买入"
          value={data?.hasData ? amount(data.buyCents) : "—"}
          tone="buy"
        />
        <Metric
          title="主动卖出"
          value={data?.hasData ? amount(data.sellCents) : "—"}
          tone="sell"
        />
        <Metric
          title="主动净买入"
          value={data?.hasData ? amount(data.netCents, true) : "—"}
          tone={(data?.netCents ?? 0) >= 0 ? "buy" : "sell"}
        />
        <Metric
          title="成交量加权均价 · VWAP"
          value={data?.vwap ? price(data.vwap) : "—"}
          unit="USDT"
        />
      </div>
      <p className="helper">
        这是成交方向统计，不是充值提现。
        {data?.partial && (
          <strong className="amber">
            {" "}
            当前窗口未完整覆盖，金额仅包含已观察成交。
          </strong>
        )}
      </p>
      <Backfill asset={asset} market={market} hours={hours} />
      <section className="data-section">
        <h2>累计主动净买入 · CVD</h2>
        <p className="helper">
          起算点 {data ? new Date(data.anchor).toLocaleString("zh-CN") : "—"} ·
          缺口处断线，累计值仅含已观察成交。
        </p>
        {data?.series.length ? (
          <Chart option={option} label="累计已观察净买入" />
        ) : (
          <Empty text="该窗口暂无有效成交统计。" />
        )}
      </section>
      <section className="data-section">
        <div className="section-heading">
          <h2>成交足迹与价位成交分布</h2>
          <span>{data?.footprintVenues?.join(" / ") || "等待来源"} · USDT</span>
        </div>
        <p className="helper">
          按已完成5分钟周期保存；柱长表示成交额。足迹覆盖与上方聚合买卖量可能不同。
        </p>
        <div className="footprint-header">
          <span>价格区间 · USDT</span>
          <span className="sell">主动卖出</span>
          <span className="buy">主动买入</span>
          <span>净买入</span>
        </div>
        {bins.map((b) => (
          <div className="footprint-row" key={`${b.p}/${b.high}`}>
            <span>
              {price(b.p, 0)}–{price(b.high, 0)}
            </span>
            <div className="sell">
              <meter max={max} value={b.sell} />
              <span>{amount(b.sell)}</span>
            </div>
            <div className="buy">
              <meter max={max} value={b.buy} />
              <span>{amount(b.buy)}</span>
            </div>
            <strong className={b.buy >= b.sell ? "buy" : "sell"}>
              {amount(b.buy - b.sell, true)}
            </strong>
          </div>
        ))}
        {!bins.length && (
          <Empty text="足迹尚未到达或此周期不受支持，空白不表示零成交。" />
        )}
      </section>
    </>
  );
}
function DerivativesPage({ asset }: Props) {
  const [hours, setHours] = useState(24);
  const { data, error } = useAPI<Derivatives>(
    `derivatives?asset=${asset}&hours=${hours}`,
    30000,
  );
  const option = useMemo(
    () =>
      lineOption(
        data?.series.map((x) => [x.time * 1000, x.oiUsdCents]) ?? [],
        "上游汇总OI",
      ),
    [data],
  );
  return (
    <>
      <div className="toolbar page-toolbar">
        <WindowSelect hours={hours} set={setHours} />
        <span className="helper">OI是未平仓规模，不代表某一方向真实开仓量</span>
      </div>
      {error && <p className="sell">{error}</p>}
      <Status meta={data?.meta} />
      <div className="metric-strip">
        <Metric
          title="上游汇总 OI"
          value={data?.totalOiCents != null ? amount(data.totalOiCents) : "—"}
        />
        <Metric
          title="已观察多头清算"
          value={
            data?.longLiquidationCents != null
              ? amount(data.longLiquidationCents)
              : "—"
          }
          tone="sell"
        />
        <Metric
          title="已观察空头清算"
          value={
            data?.shortLiquidationCents != null
              ? amount(data.shortLiquidationCents)
              : "—"
          }
          tone="buy"
        />
      </div>
      <p className="helper">
        {data?.coverage} 已发生清算仅统计收到的历史窗口。
      </p>
      <section className="data-section">
        <h2>持仓规模变化</h2>
        {data?.series.length ? (
          <Chart option={option} label="未平仓合约规模变化" />
        ) : (
          <Empty text="历史正在积累" />
        )}
      </section>
      <div className="v2-columns">
        <section className="data-section">
          <h2>交易所持仓规模</h2>
          <div className="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>交易所</th>
                  <th>OI · USD</th>
                  <th>基础币数量</th>
                  <th>状态</th>
                </tr>
              </thead>
              <tbody>
                {data?.items.map((r) => (
                  <tr key={r.venue} className={!r.valid ? "stale-row" : ""}>
                    <td>{r.venue}</td>
                    <td>{amount(r.oiUsdCents)}</td>
                    <td>{price(r.oiBase, 2)}</td>
                    <td>{r.valid ? "有效" : "过期"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
        <section className="data-section">
          <h2>资金费率</h2>
          <Status meta={data?.fundingMeta} />
          <div className="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>交易所</th>
                  <th>保证金类型</th>
                  <th>原始费率</th>
                  <th>周期</th>
                </tr>
              </thead>
              <tbody>
                {data?.funding.map((r, i) => (
                  <tr key={`${r.venue}/${r.margin}/${i}`}>
                    <td>{r.venue}</td>
                    <td>{r.margin === "stablecoin" ? "稳定币" : "币本位"}</td>
                    <td className={+r.ratePercent >= 0 ? "buy" : "sell"}>
                      {(+r.ratePercent).toFixed(5)}%
                    </td>
                    <td>
                      {r.intervalHours ? `${r.intervalHours}小时` : "未知"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <p className="helper">不同结算周期的原始费率不可直接相加比较。</p>
        </section>
      </div>
    </>
  );
}
function WhalesPage({ asset }: Props) {
  const [limit, setLimit] = useState(50),
    [side, setSide] = useState("all"),
    [kind, setKind] = useState("liquidation"),
    [sort, setSort] = useState("amount"),
    [address, setAddress] = useState(""),
    [message, setMessage] = useState(""),
    [wallet, setWallet] = useState<string | null>(null);
  const { data, error } = useAPI<Whales>(
    `whales?asset=${asset}&limit=${limit}&side=${side}&sort=${sort}`,
  );
  const detail = useAPI<{ meta: Meta; data: { wallet?: Wallet } }>(
    wallet ? `data/${wallet}` : null,
  );
  const buckets = (data?.buckets ?? [])
      .filter((b) => b.kind === kind && (side === "all" || side === b.side))
      .sort((a, b) => b.usdCents - a.usdCents)
      .slice(0, 20)
      .sort((a, b) => b.price - a.price),
    max = Math.max(1, ...buckets.map((b) => b.usdCents));
  return (
    <>
      <div className="toolbar page-toolbar">
        <div className="segmented">
          {[
            ["liquidation", "清算参考价分布"],
            ["entry", "持仓均价分布"],
          ].map(([v, t]) => (
            <button
              key={v}
              className={kind === v ? "selected" : ""}
              onClick={() => setKind(v)}
            >
              {t}
            </button>
          ))}
        </div>
        <label>
          方向{" "}
          <select value={side} onChange={(e) => setSide(e.target.value)}>
            <option value="all">全部</option>
            <option value="long">多头</option>
            <option value="short">空头</option>
          </select>
        </label>
        <span className="helper">
          有效 {data?.monitor.fresh ?? 0} / 已覆盖{" "}
          {data?.monitor.candidates ?? 0} 个大仓
        </span>
      </div>
      <Status meta={data?.meta} />
      {error && <p className="sell">{error}</p>}
      <div className="metric-strip">
        <Metric
          title="有效多头仓位"
          value={data?.hasData ? amount(data.longCents) : "—"}
          tone="buy"
        />
        <Metric
          title="有效空头仓位"
          value={data?.hasData ? amount(data.shortCents) : "—"}
          tone="sell"
        />
        <Metric
          title="距清算参考价不足3%"
          value={data?.hasData ? amount(data.nearLiquidationCents) : "—"}
        />
      </div>
      <div className="whale-layout">
        <section className="data-section">
          <h2>
            {kind === "liquidation"
              ? "哪些清算参考价附近仓位最多？"
              : "当前大仓的持仓均价在哪里？"}
          </h2>
          <p className="helper">
            {data?.distributionScope}
            。柱长表示涉及仓位金额；清算参考价会随保证金和其他持仓变化。
          </p>
          <div className="whale-bucket-head">
            <span>价格 · 平台报价</span>
            <span>涉及仓位金额</span>
            <span>地址数</span>
            <span>最大单一占比</span>
          </div>
          {buckets.map((b) => (
            <div
              className={`whale-bucket ${b.side === "long" ? "buy" : "sell"}`}
              key={`${b.side}/${b.price}`}
            >
              <span>
                ${price(b.price, 0)}
                <small>{b.side === "long" ? "多头" : "空头"}</small>
              </span>
              <div>
                <meter value={b.usdCents} max={max} />
                <strong>{amount(b.usdCents)}</strong>
              </div>
              <span>{b.addresses}</span>
              <span>{(b.largestShare * 100).toFixed(0)}%</span>
            </div>
          ))}
          {!buckets.length && (
            <Empty text="每5分钟采集；只有来源时间8分钟内、字段有效的持仓参与分布。" />
          )}
        </section>
        <aside className="watchlist">
          <h2>公开钱包详情</h2>
          <p className="helper">
            详情共用本地缓存。首次查询进入共享队列，不启动逐地址常驻轮询。
          </p>
          <form
            onSubmit={async (e) => {
              e.preventDefault();
              try {
                const j = await api<Job>("data-requests", {
                  method: "POST",
                  body: JSON.stringify({
                    dataset: "whales.all.hyperliquid.futures",
                    address,
                  }),
                });
                setWallet(j.dataset.id);
                setMessage("查询已入队，等待剩余额度。");
              } catch (e) {
                setMessage((e as Error).message);
              }
            }}
          >
            <label className="sr-only" htmlFor="wallet-address">
              公开地址
            </label>
            <input
              id="wallet-address"
              value={address}
              onChange={(e) => setAddress(e.target.value)}
              placeholder="0x…"
              required
              pattern="0x[0-9a-fA-F]{40}"
            />
            <button className="primary full">
              查询钱包 <ArrowRight size={16} />
            </button>
          </form>
          <p className="helper" role="status">
            {message}
          </p>
          {detail.data && (
            <>
              <Status meta={detail.data.meta} />
              <WalletSummary wallet={detail.data.data.wallet} />
              {detail.error && <p className="amber">{detail.error}</p>}
            </>
          )}
        </aside>
      </div>
      <section className="data-section">
        <div className="section-heading">
          <h2>已覆盖地址持仓榜</h2>
          <div className="segmented">
            {[50, 100].map((n) => (
              <button
                key={n}
                className={limit === n ? "selected" : ""}
                onClick={() => setLimit(n)}
              >
                前{n}
              </button>
            ))}
          </div>
          <label>
            排序{" "}
            <select value={sort} onChange={(e) => setSort(e.target.value)}>
              <option value="amount">仓位金额</option>
              <option value="distance">最接近清算价</option>
            </select>
          </label>
        </div>
        <div className="table-scroll">
          <table>
            <thead>
              <tr>
                <th>公开地址</th>
                <th>方向</th>
                <th>仓位金额</th>
                <th>持仓均价</th>
                <th>设置杠杆</th>
                <th>清算参考价</th>
                <th>距清算</th>
                <th>浮盈亏</th>
                <th>较前次快照数量变化</th>
                <th>状态</th>
              </tr>
            </thead>
            <tbody>
              {data?.items.map((w) => (
                <tr
                  key={`${w.address}/${w.asset}`}
                  className={!w.valid ? "stale-row" : ""}
                >
                  <td>
                    <button
                      className="address"
                      title={w.address}
                      onClick={() =>
                        navigator.clipboard
                          .writeText(w.address)
                          .then(() => setMessage("地址已复制"))
                      }
                    >
                      {w.address.slice(0, 8)}…{w.address.slice(-4)}{" "}
                      <Copy size={13} />
                    </button>
                  </td>
                  <td className={w.side === "long" ? "buy" : "sell"}>
                    {w.side === "long" ? "多头" : "空头"}
                  </td>
                  <td>{amount(w.usdCents)}</td>
                  <td>${price(+w.entry)}</td>
                  <td>
                    {w.margin === "cross"
                      ? "全仓"
                      : w.margin === "isolated"
                        ? "逐仓"
                        : w.margin}{" "}
                    {w.leverage}×
                  </td>
                  <td>
                    {w.liquidation
                      ? "$" + price(+w.liquidation)
                      : "暂无可用清算价"}
                  </td>
                  <td>
                    {w.distance === null ? "—" : w.distance.toFixed(2) + "%"}
                  </td>
                  <td className={w.unrealizedCents >= 0 ? "buy" : "sell"}>
                    {amount(w.unrealizedCents, true)}
                  </td>
                  <td>
                    {w.changeSize == null
                      ? "无可比快照"
                      : price(+w.changeSize, 4)}
                  </td>
                  <td>{w.valid ? clock(w.at) + "更新" : "过期，未参与聚合"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <p className="helper">
          每5分钟采集，逐条来源时间8分钟内有效。CoinGlass覆盖的Hyperliquid百万美元级持仓，不是全市场巨鲸榜。清算距离使用平台标记价格；名单消失不等于平仓。
        </p>
      </section>
    </>
  );
}
function LiquidationPage({ asset, frame }: Props) {
  const [period, setPeriod] = useState("24h"),
    [heat, setHeat] = useState(false),
    [message, setMessage] = useState("");
  const { data, error } = useAPI<Liquidations>(
    `liquidations?asset=${asset}&period=${period}`,
    30000,
  );
  const model = heat ? data?.heatmap : data?.map;
  const bins = useMemo(() => {
    const groups = new Map<number, number>(),
      step = asset === "BTC" ? 250 : 10;
    for (const b of data?.map.data?.bins ?? []) {
      const p = Math.floor(b.price / step) * step;
      groups.set(p, (groups.get(p) ?? 0) + b.strength);
    }
    return [...groups]
      .map(([p, n]) => ({ p, n }))
      .sort((a, b) => b.n - a.n)
      .slice(0, 24)
      .sort((a, b) => b.p - a.p);
  }, [data, asset]);
  const max = Math.max(1, ...bins.map((b) => b.n));
  const option = useMemo(
    () => ({
      grid: { left: 85, right: 85, top: 20, bottom: 45 },
      tooltip: { position: "top" },
      xAxis: {
        type: "category",
        data: data?.heatmap.data?.times?.map((t) => clock(t)) ?? [],
        ...baseAxis,
      },
      yAxis: {
        type: "category",
        data: data?.heatmap.data?.prices ?? [],
        ...baseAxis,
      },
      visualMap: {
        min: 0,
        max: (data?.heatmap.data?.cells ?? []).reduce(
          (max, c) => Math.max(max, c[2]),
          1,
        ),
        calculable: true,
        orient: "vertical",
        right: 0,
        inRange: { color: ["#18231f", "#365b45", "#7deba9"] },
        textStyle: { color: "#b0c2b8" },
      },
      series: [{ type: "heatmap", data: data?.heatmap.data?.cells ?? [] }],
    }),
    [data],
  );
  async function changePeriod(p: string) {
    setPeriod(p);
    if (p !== "24h") {
      try {
        for (const kind of ["map", "heatmap"]) {
          await api("data-requests", {
            method: "POST",
            body: JSON.stringify({
              dataset: `${kind}.${asset.toLowerCase()}..futures`,
              range: p,
            }),
          });
        }
        setMessage("所选周期进入共享加载队列，完成后自动显示。");
      } catch (e) {
        setMessage((e as Error).message);
      }
    }
  }
  return (
    <>
      <div className="toolbar page-toolbar">
        <div className="segmented">
          {["24h", "7d", "30d"].map((p) => (
            <button
              key={p}
              className={period === p ? "selected" : ""}
              onClick={() => void changePeriod(p)}
            >
              {p === "24h" ? "24小时" : p === "7d" ? "7天" : "30天"}
            </button>
          ))}
        </div>
        <button className="secondary" onClick={() => setHeat(!heat)}>
          {heat ? "返回清算价位柱状图" : "高级：历史清算热力图"}
        </button>
      </div>
      <p className="helper">{data?.note}</p>
      <Status meta={model?.meta} />
      {message && (
        <p className="helper" role="status">
          {message}
        </p>
      )}
      {error && <p className="sell">{error}</p>}
      <section className="data-section">
        <h2>{heat ? "清算模型随时间的变化" : "清算模型集中价位"}</h2>
        {heat ? (
          data?.heatmap.data?.cells?.length ? (
            <Chart option={option} height={430} label="模型清算历史热力图" />
          ) : (
            <Empty text="所选周期尚无可用模型数据。" />
          )
        ) : (
          <>
            <p className="helper">
              同一周期内最长柱为100，表示相对强度；不同周期单独比较。
            </p>
            {bins.map((b) => (
              <div
                className={`liquidation-bar ${(frame?.price ?? 0) > b.p ? "buy" : "sell"}`}
                key={b.p}
              >
                <span>
                  ${price(b.p, 0)}
                  <small>
                    {frame?.price
                      ? ((b.p / frame.price - 1) * 100).toFixed(2) + "%"
                      : "—"}
                  </small>
                </span>
                <meter max={max} value={b.n} />
                <strong>
                  {((b.n / max) * 100).toFixed(0)}
                  <small>
                    {b.n / max >= 0.8
                      ? "高度集中"
                      : b.n / max >= 0.5
                        ? "较集中"
                        : "一般"}
                  </small>
                </strong>
              </div>
            ))}
            {!bins.length && <Empty text="清算地图正在按计划更新。" />}
          </>
        )}
      </section>
      <div className="notice">
        <WarningCircle size={18} />
        模型清算分布、已发生清算、账户清算参考价是三种不同口径，分别查看。
      </div>
    </>
  );
}
export function LargeOrders({
  asset,
  panorama,
}: {
  asset: Asset;
  panorama: boolean;
}) {
  const [open, setOpen] = useState(panorama);
  const [history, setHistory] = useState(false);
  const [offset, setOffset] = useState(0);
  useEffect(() => setOffset(0), [asset, history]);
  useEffect(() => {
    if (panorama) setOpen(true);
  }, [panorama]);
  const { data, error } = useAPI<{
    items: Large[];
    note: string;
    hasMore: boolean;
  }>(
    `large-orders?asset=${asset}&history=${history ? 1 : 0}&limit=100&offset=${offset}`,
    30000,
  );
  const rows = (data?.items ?? [])
    .filter(
      (x) =>
        !panorama ||
        asset !== "BTC" ||
        (+x.price >= 10000 && +x.price <= 200000),
    )
    .slice(0, open ? 300 : 0);
  return (
    <section className="large-orders data-section">
      <div className="section-heading">
        <h2>
          {panorama
            ? "远景大额挂单 · 仅发现大单，不代表完整覆盖"
            : "大额挂单跟踪"}
        </h2>
        <button className="text-button" onClick={() => setOpen(!open)}>
          {open ? "收起" : "展开大单"}
        </button>
      </div>
      {panorama && (
        <p className="amber">
          没有柱子的区间可能未被盘口覆盖，不能读作零挂单。
        </p>
      )}
      <p className="helper">{data?.note}</p>
      {open && (
        <label>
          <input
            type="checkbox"
            checked={history}
            onChange={(e) => setHistory(e.target.checked)}
          />{" "}
          查看历史（含上游结束／撤销标记）
        </label>
      )}
      {error && <p className="sell">{error}</p>}
      {open && (
        <div className="table-scroll">
          <table>
            <thead>
              <tr>
                <th>交易所</th>
                <th>方向</th>
                <th>原始价格</th>
                <th>金额 · USD</th>
                <th>首次记录</th>
                <th>最后变更</th>
                <th>初始／当前余量 · 币</th>
                <th>累计成交数量 · 币</th>
                <th>记录成交 · 上游USD</th>
                <th>状态</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => (
                <tr key={r.id} className={r.valid ? "" : "stale-row"}>
                  <td>{r.venue}</td>
                  <td className={r.side === "bid" ? "buy" : "sell"}>
                    {r.side === "bid"
                      ? "买单"
                      : r.side === "ask"
                        ? "卖单"
                        : "未知"}
                  </td>
                  <td>
                    {price(+r.price)} {r.quote}
                  </td>
                  <td>
                    {r.historical
                      ? "已结束 · 不计当前挂单"
                      : r.usdCents == null
                        ? "汇率不可用"
                        : amount(r.usdCents)}
                  </td>
                  <td>
                    {r.startAt
                      ? new Date(r.startAt).toLocaleDateString("zh-CN")
                      : "—"}
                  </td>
                  <td>
                    {r.changedAt
                      ? new Date(r.changedAt).toLocaleString("zh-CN")
                      : "—"}
                  </td>
                  <td>{r.initialQuantity??"未知"} / {r.quantity}</td>
                  <td>{r.executedQuantity??"未知"}</td>
                  <td>{amount(+r.executedUsd * 100)}</td>
                  <td>
                    {r.valid ? r.state : "过期"}
                    {r.endAt && (
                      <small>
                        {" "}
                        · {new Date(r.endAt).toLocaleString("zh-CN")}
                      </small>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {!rows.length && (
            <Empty text="此页尚无已获取记录，历史可能仍在补采。" />
          )}
          <div className="pagination">
            <button
              className="secondary"
              disabled={offset === 0}
              onClick={() => setOffset(Math.max(0, offset - 100))}
            >
              上一页
            </button>
            <span>第{offset / 100 + 1}页 · 每页最多100条</span>
            <button
              className="secondary"
              disabled={!data?.hasMore}
              onClick={() => setOffset(offset + 100)}
            >
              下一页
            </button>
          </div>
        </div>
      )}
    </section>
  );
}
function HealthPage({ health, frame }: Props) {
  const { data, error } = useAPI<DataStatus>("data-status");
  const settings = useAPI<Settings>("settings", 60000);
  const [message, setMessage] = useState(""),
    [detail, setDetail] = useState(false);
  const s = data?.scheduler;
  return (
    <>
      <div className="metric-strip">
        <Metric
          title="上游额度 · 滚动60秒"
          value={s ? `${s.usedLastMinute} / ${s.limitPerMinute}` : "—"}
          unit="次"
        />
        <Metric
          title="项目占用"
          value={data ? (data.storage.usedBytes / 1073741824).toFixed(2) : "—"}
          unit="GB"
        />
        <Metric
          title="磁盘剩余"
          value={data ? (data.storage.freeBytes / 1073741824).toFixed(1) : "—"}
          unit="GB"
        />
        <Metric title="生产版本" value={health?.version ?? "—"} unit="" />
      </div>
      {error && <p className="sell">{error}</p>}
      {s?.authFailed && (
        <div className="notice danger">
          上游认证失败，采集已暂停。请检查代理密钥和订阅期限。
        </div>
      )}
      <p className="helper">
        大单历史约{" "}
        {((data?.storage.orderHistoryBytes ?? 0) / 1048576).toFixed(1)} / 512
        MiB ·{" "}
        {data?.storage.orderHistoryPaused
          ? "细历史暂停，当前快照继续"
          : "容量正常"}
      </p>
      {data?.storage.fineHistoryPaused && (
        <div className="notice danger">
          磁盘预算保护：暂停补采和细历史写入，实时查询继续提供。
        </div>
      )}
      <section className="data-section">
        <h2>采集与存储</h2>
        <p className="helper">
          旧采集器：{data?.legacyCollectorsRunning ? "运行中" : "已下线"} ·
          SQLite {data?.storage.sqlite ?? "—"} · 规则{" "}
          {data?.rulesVersion ?? "—"} · 上游累计 {s?.calls ?? 0} 次 · 429{" "}
          {s?.rateLimited ?? 0} 次
        </p>
        <div className="toolbar">
          <label>
            历史保留{" "}
            <select
              value={settings.data?.retentionDays ?? 90}
              onChange={async (e) => {
                try {
                  await api("settings", {
                    method: "PUT",
                    body: JSON.stringify({
                      ...settings.data,
                      retentionDays: +e.target.value,
                    }),
                  });
                  settings.refresh();
                  setMessage("保留策略已保存，后台按精度清理。");
                } catch (e) {
                  setMessage((e as Error).message);
                }
              }}
            >
              <option value={30}>30天</option>
              <option value={90}>90天（长期降采样）</option>
            </select>
          </label>
          <span className="helper">总预算20GB · 巨鲸2GB · 至少8GB空闲</span>
          <span role="status">{message}</span>
        </div>
      </section>
      <section className="data-section">
        <h2>现货实际覆盖</h2>
        <div className="table-scroll">
          <table>
            <thead>
              <tr>
                <th>交易所 / 市场</th>
                <th>状态</th>
                <th>盘口下界</th>
                <th>盘口上界</th>
                <th>来源时间</th>
              </tr>
            </thead>
            <tbody>
              {frame?.coverage.map((c) => (
                <tr key={`${c.venue}/${c.symbol}`}>
                  <td>
                    {venue(c.venue)} · {c.symbol}
                  </td>
                  <td className={c.valid ? "buy" : "amber"}>
                    {c.valid ? "有效" : (c.reason ?? "未覆盖")}
                  </td>
                  <td>{c.bidLow ? "$" + price(c.bidLow) : "未知"}</td>
                  <td>{c.askHigh ? "$" + price(c.askHigh) : "未知"}</td>
                  <td>{c.observedAt ? clock(c.observedAt) : "未提供"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
      <section className="data-section">
        <div className="section-heading">
          <h2>共享数据集新鲜度</h2>
          <button className="text-button" onClick={() => setDetail(!detail)}>
            {detail ? "收起调度详情" : "查看调度详情"}
          </button>
        </div>
        <div className="table-scroll">
          <table>
            <thead>
              <tr>
                <th>数据集</th>
                <th>币种 / 来源</th>
                <th>状态</th>
                <th>目标刷新</th>
                <th>来源时间</th>
                <th>最近获取</th>
              </tr>
            </thead>
            <tbody>
              {data?.datasets.map((x) => (
                <tr key={x.dataset.id}>
                  <td>{x.dataset.kind}</td>
                  <td>
                    {x.dataset.asset} · {x.dataset.venue || x.dataset.source}
                  </td>
                  <td
                    className={
                      x.status === "stale" || x.status === "missing"
                        ? "amber"
                        : ""
                    }
                  >
                    {names[x.status] ?? x.status}
                  </td>
                  <td>{x.dataset.refreshSeconds}秒</td>
                  <td>{x.observedAt ? clock(x.observedAt) : "未提供"}</td>
                  <td>
                    {new Date(x.fetchedAt).getFullYear() > 2000
                      ? clock(x.fetchedAt)
                      : "等待首次采集"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
      {detail && (
        <section className="data-section">
          <h2>统一调度队列</h2>
          <div className="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>任务</th>
                  <th>类型</th>
                  <th>状态</th>
                  <th>下一次</th>
                  <th>错误</th>
                </tr>
              </thead>
              <tbody>
                {s?.jobs.map((j) => (
                  <tr key={j.id}>
                    <td>{j.id}</td>
                    <td>{j.mode}</td>
                    <td>
                      {j.disabled
                        ? "暂停"
                        : j.completed
                          ? "完成"
                          : j.inFlight
                            ? "正在获取"
                            : "排队"}
                    </td>
                    <td>{clock(j.next)}</td>
                    <td>{j.error ?? "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      )}
    </>
  );
}
export function OverviewSummary({
  asset,
  onNavigate,
}: {
  asset: Asset;
  onNavigate: (v: string) => void;
}) {
  const f = useAPI<FlowData>(`flow?asset=${asset}&hours=1`),
    d = useAPI<Derivatives>(`derivatives?asset=${asset}&hours=24`),
    w = useAPI<Whales>(`whales?asset=${asset}&limit=50&side=all&sort=amount`);
  return (
    <>
      <div className="metric-strip">
        <button className="summary-link" onClick={() => onNavigate("flow")}>
          <Metric
            title="1小时主动净买入 · 现货"
            value={f.data?.hasData ? amount(f.data.netCents, true) : "—"}
            tone={(f.data?.netCents ?? 0) >= 0 ? "buy" : "sell"}
          />
          <small>{f.data?.partial ? "窗口未完整覆盖" : "查看成交证据"}</small>
        </button>
        <button
          className="summary-link"
          onClick={() => onNavigate("derivatives")}
        >
          <Metric
            title="上游汇总合约 OI"
            value={
              d.data?.totalOiCents != null ? amount(d.data.totalOiCents) : "—"
            }
          />
          <small>与分交易所数据分开展示</small>
        </button>
        <button className="summary-link" onClick={() => onNavigate("whales")}>
          <Metric
            title="距清算参考价不足3%的仓位"
            value={w.data?.hasData ? amount(w.data.nearLiquidationCents) : "—"}
          />
          <small>Hyperliquid已覆盖大仓</small>
        </button>
        <div className="summary-link">
          <Metric title="判断顺序" value="金额 → 持续 → 成交" unit="" />
          <small>历史金额等级不等于支撑胜率</small>
        </div>
      </div>
      <PriceHistory asset={asset} />
    </>
  );
}
export function PriceHistory({ asset }: { asset: Asset }) {
  const { data } = useAPI<{
    points: {
      time: number;
      open: number;
      close: number;
      low: number;
      high: number;
    }[];
  }>(`candles?asset=${asset}&hours=24`, 60000);
  const option = useMemo(
    () => ({
      grid: { left: 80, right: 20, top: 15, bottom: 35 },
      tooltip: { trigger: "axis" },
      xAxis: {
        type: "category",
        data: data?.points.map((c) => clock(c.time)) ?? [],
        ...baseAxis,
      },
      yAxis: { type: "value", scale: true, ...baseAxis },
      dataZoom: [{ type: "inside" }],
      series: [
        {
          type: "candlestick",
          data: data?.points.map((c) => [c.open, c.close, c.low, c.high]) ?? [],
          itemStyle: {
            color: "#7deba9",
            color0: "#f17369",
            borderColor: "#7deba9",
            borderColor0: "#f17369",
          },
        },
      ],
    }),
    [data],
  );
  return (
    <section className="data-section">
      <h2>价格背景 · 币安5分钟K线</h2>
      <p className="helper">BTC/ETH原始USDT报价 · 仅已完成周期</p>
      {data?.points.length ? (
        <Chart option={option} label="价格K线" />
      ) : (
        <Empty text="等待K线数据" />
      )}
    </section>
  );
}

type Wallet = {
  margin_summary?: {
    account_value?: number;
    total_ntl_pos?: number;
    total_margin_used?: number;
  };
  withdrawable?: number;
  asset_positions?: {
    position: {
      coin: string;
      szi: number;
      entry_px: number;
      position_value: number;
      unrealized_pnl: number;
      leverage?: { type: string; value: number };
    };
  }[];
};
function WalletSummary({ wallet }: { wallet?: Wallet }) {
  if (!wallet)
    return <p className="helper">数据准备中，失败的查询不会显示为零仓位。</p>;
  return (
    <div className="wallet-summary">
      <p className="helper">账户级权益保持平台原值，包含其他币种的影响。</p>
      <dl>
        <dt>账户权益 · USD</dt>
        <dd>
          {wallet.margin_summary?.account_value != null
            ? amount(wallet.margin_summary.account_value * 100)
            : "未提供"}
        </dd>
        <dt>已用保证金 · USD</dt>
        <dd>
          {wallet.margin_summary?.total_margin_used != null
            ? amount(wallet.margin_summary.total_margin_used * 100)
            : "未提供"}
        </dd>
        <dt>可提取 · USD</dt>
        <dd>
          {wallet.withdrawable != null
            ? amount(wallet.withdrawable * 100)
            : "未提供"}
        </dd>
      </dl>
      {wallet.asset_positions?.map(({ position: p }, i) => (
        <div key={`${p.coin}/${i}`}>
          <h3>
            {p.coin} · {+p.szi >= 0 ? "多头" : "空头"}
          </h3>
          <p>
            持仓均价 {price(+p.entry_px)} · {p.leverage?.value ?? "—"}×
          </p>
          <p>
            仓位 {amount(+p.position_value * 100)} USD · 浮盈亏{" "}
            {amount(+p.unrealized_pnl * 100, true)}
          </p>
        </div>
      ))}
      {!wallet.asset_positions?.length && <p>该响应未列出BTC/ETH仓位。</p>}
    </div>
  );
}
