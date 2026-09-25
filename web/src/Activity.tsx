import { useMemo, useState } from "react";
import { Chart } from "./Chart";
import { amount, clock, price, useAPI } from "./data";
import {
  Metric,
  Status,
  lineOption,
  type Meta,
  type Derivatives,
} from "./Pages";
import type { Asset } from "./types";
import { SignalsPage, StudiesPage, WalletPage } from "./Research";

type FlowWindow = {
  buyCents: number;
  sellCents: number;
  rows: number;
  coverage: number;
  boundaries: boolean;
  partial: boolean;
  series: { time: number; cvdCents: number | null }[];
};
type Order = {
  fetchedAt: string;
  id: string;
  venue: string;
  side: string;
  priceUsd: number;
  usdCents: number;
  startAt: string | null;
  changedAt: string | null;
  executedQuantity: string | null;
  quantity: string;
  quote: string;
};
type Event = {
  key: string;
  venue: string;
  side: string;
  price: string;
  quote: string;
  kind: string;
  at: string;
  from: string | null;
  quantityDelta: string | null;
  executedQuantityDelta: string | null;
  executedDeltaUsd: string | null;
  note: string;
  after?: { endAt: string | null };
};
type Activity = {
  orderHistoryGap: boolean;
  pressureReaction: string;
  asset: Asset;
  from: string;
  to: string;
  hours: number;
  bias: string;
  reason: string;
  buyShare: number | null;
  flow: FlowWindow;
  flowMeta: Meta;
  priceChangePercent: number | null;
  priceReaction: string;
  priceQuote: string;
  candles: {
    time: number;
    open: number;
    close: number;
    low: number;
    high: number;
  }[];
  perpFlow: FlowWindow;
  perpMeta: Meta;
  derivatives: Derivatives;
  footprint: Record<string, [number, number]>;
  footprintVenues: string[];
  footprintPartial: boolean;
  matchedFootprintVenues: string[];
  orders: Order[];
  orderCount: number;
  orderBidCents: number;
  orderAskCents: number;
  orderHasData: boolean;
  interpretation?: { flow: string; price: string };
  orderSources: Meta[];
  events: Event[];
  eventsHasMore: boolean;
  historyStatus: {
    venue: string;
    state: string;
    from: string | null;
    through: string | null;
    pendingPages: number;
    gaps: number;
    error: string;
  }[];
  rulesVersion: string;
  tradeFeedNote: string;
  storage: { orderHistoryPaused: boolean };
  at: string;
};
const kind: Record<string, string> = {
  discovered: "新发现",
  change: "采样变化",
  ended: "上游记录结束",
  revoked: "上游标记撤销",
  correction: "上游修正",
};
const stamp = (s: string) =>
  new Date(s).toLocaleString("zh-CN", {
    timeZone: "Asia/Shanghai",
    hour12: false,
  });
export function ActivityPage({ asset }: { asset: Asset }) {
  const [tab, setTab] = useState("snapshot");
  return (
    <>
      <nav className="research-tabs" aria-label="大资金动向视图">
        {[
          ["snapshot", "当前动向"],
          ["signals", "异动预警"],
          ["studies", "历史复盘"],
          ["wallet", "钱包趋势"],
        ].map(([id, name]) => (
          <button
            key={id}
            aria-pressed={tab === id}
            className={tab === id ? "active" : ""}
            disabled={asset !== "BTC" && (id === "signals" || id === "studies")}
            title={
              asset !== "BTC" && (id === "signals" || id === "studies")
                ? "仅BTC启用"
                : ""
            }
            onClick={() => setTab(id)}
          >
            {name}
            {asset !== "BTC" && (id === "signals" || id === "studies")
              ? " · 仅BTC"
              : ""}
          </button>
        ))}
      </nav>
      {tab === "signals" ? (
        <SignalsPage asset={asset} />
      ) : tab === "studies" ? (
        <StudiesPage asset={asset} />
      ) : tab === "wallet" ? (
        <WalletPage asset={asset} />
      ) : (
        <ActivitySnapshot asset={asset} />
      )}
    </>
  );
}
function ActivitySnapshot({ asset }: { asset: Asset }) {
  const [hours, setHours] = useState(1),
    [span, setSpan] = useState(5),
    [advanced, setAdvanced] = useState(false);
  const { data: d, error } = useAPI<Activity>(
    `activity?asset=${asset}&hours=${hours}&range=${span}`,
    15000,
  );
  const cvd = useMemo(
    () =>
      lineOption(
        d?.flow.series.map((p) => [p.time * 1000, p.cvdCents]) ?? [],
        "现货CVD · USD",
      ),
    [d],
  );
  const candles = useMemo(
    () => ({
      grid: { left: 70, right: 24, bottom: 30, top: 24 },
      tooltip: { trigger: "axis" },
      xAxis: {
        type: "category",
        data: d?.candles.map((c) => clock(c.time)) ?? [],
      },
      yAxis: { scale: true, type: "value" },
      series: [
        {
          name: `${asset}/USDT · 币安`,
          type: "candlestick",
          itemStyle: {
            color: "#7deba9",
            color0: "#ef7166",
            borderColor: "#7deba9",
            borderColor0: "#ef7166",
          },
          data: d?.candles.map((c) => [c.open, c.close, c.low, c.high]) ?? [],
        },
      ],
    }),
    [d, asset],
  );
  const bookMax = Math.max(1, ...(d?.orders.map((o) => o.usdCents) ?? []));
  const feet = Object.entries(d?.footprint ?? {})
    .map(([p, n]) => ({ price: +p.split(":")[0], buy: n[0], sell: n[1] }))
    .sort((a, b) => b.buy + b.sell - a.buy - a.sell)
    .slice(0, 8)
    .sort((a, b) => b.price - a.price);
  const footMax = Math.max(1, ...feet.flatMap((f) => [f.buy, f.sell]));
  const isFresh = (meta?: Meta) =>
    !!meta &&
    ["fresh", "retrieval_only"].includes(meta.status) &&
    new Date(meta.expiresAt).getTime() > Date.now();
  const headline =
    d && isFresh(d.flowMeta) && Date.now() - new Date(d.at).getTime() < 45000
      ? d.bias
      : "证据不足";
  return (
    <>
      <div className="toolbar page-toolbar">
        <label>
          统计窗口{" "}
          <select value={hours} onChange={(e) => setHours(+e.target.value)}>
            {[1, 4, 24].map((n) => (
              <option key={n} value={n}>
                最近{n}小时
              </option>
            ))}
          </select>
        </label>
        <span className="helper">现货主动成交 ＋ 同窗价格反馈</span>
      </div>
      {error && (
        <p className="sell" role="alert">
          {error}
        </p>
      )}
      <section className="activity-brief">
        <div>
          <span className="eyebrow">已覆盖成交所呈现的压力</span>
          <h2
            className={
              headline === "买方较主动"
                ? "buy"
                : headline === "卖方较主动"
                  ? "sell"
                  : ""
            }
          >
            {headline}
          </h2>
          <p>{d?.reason ?? "正在读取共享数据；窗口未补齐时不推断方向。"}</p>
        </div>
        <div className="activity-window">
          <strong>{d ? `${clock(d.from)}–${clock(d.to)}` : "等待数据"}</strong>
          <span>北京时间 · 截至最近已闭合5分钟</span>
          <span>
            有效分钟 {d ? `${(d.flow.coverage * 100).toFixed(0)}%` : "—"} ·{" "}
            {d?.flow.boundaries ? "起止齐全" : "窗口边界待补齐"}
          </span>
          <span>{d ? `${stamp(d.from)} → ${stamp(d.to)}` : ""}</span>
        </div>
      </section>
      <Status meta={d?.flowMeta} />
      <p className="helper">
        成交金额是选定期间的流量；价格反馈来自同一窗口的币安USDT K线。
        {d?.pressureReaction ? d.pressureReaction + "。" : ""}
        数字不足以识别“洗盘、诱多或对倒”。
      </p>
      <section className="activity-verdict data-section">
        <h2>谁更主动，价格有没有跟上？</h2>
        <div className="verdict-grid">
          <div>
            <span>① 谁更主动</span>
            <h3>{d?.interpretation?.flow ?? headline}</h3>
            <div className="buy-sell-share" aria-label="主动买卖占比">
              <span style={{ width: `${d?.buyShare ?? 50}%` }}>
                买 {d?.buyShare?.toFixed(1) ?? "—"}%
              </span>
              <span>
                卖 {d?.buyShare != null ? (100 - d.buyShare).toFixed(1) : "—"}%
              </span>
            </div>
            <p>
              {d?.flow.rows
                ? `主动买 ${amount(d.flow.buyCents)}，主动卖 ${amount(d.flow.sellCents)}；${d.flow.buyCents >= d.flow.sellCents ? "买方" : "卖方"}多 ${amount(Math.abs(d.flow.buyCents - d.flow.sellCents))} USD`
                : "等待成交数据"}
            </p>
          </div>
          <div>
            <span>② 价格有没有跟随</span>
            <h3>{d?.interpretation?.price ?? "等待数据"}</h3>
            <p>
              {d?.priceChangePercent == null
                ? "价格窗口尚未补齐"
                : `同窗价格 ${d.priceChangePercent > 0 ? "+" : ""}${d.priceChangePercent.toFixed(2)}%`}
            </p>
            <p>{d?.pressureReaction}</p>
          </div>
          <div>
            <span>③ 证据够不够</span>
            <h3>{d?.bias === "证据不足" ? "证据不足" : "可描述本窗口"}</h3>
            <p>
              已覆盖 {(100 * (d?.flow.coverage ?? 0)).toFixed(1)}% ·{" "}
              {d?.flow.boundaries ? "起止齐全" : "边界待补齐"}
            </p>
            <p>{d?.reason}</p>
          </div>
        </div>
      </section>
      <details className="data-section">
        <summary>查看详细证据 · CVD与成交价位</summary>
        <section className="data-section">
          <h3>主动成交累计变化</h3>
          <div className="flow-compare">
            <span className="buy">
              主动买 {d?.flow.rows ? amount(d.flow.buyCents) : "—"}
            </span>
            <span className="sell">
              主动卖 {d?.flow.rows ? amount(d.flow.sellCents) : "—"}
            </span>
          </div>
          <Chart option={cvd} label="同一窗口内的累计主动净买卖" height={240} />
          <p className="helper">
            CVD起算点：{d ? stamp(d.from) : "—"}
            。与主动净买卖共用一份成交数据，只算一类证据；缺口不补零。
          </p>
          <h3>成交活跃价位 · USDT</h3>
          <div className="footprint-head">
            <span>价格</span>
            <span className="buy">主动买</span>
            <span className="sell">主动卖</span>
          </div>
          {feet.map((f) => (
            <div className="footprint-row" key={f.price}>
              <span>{price(f.price, 0)}</span>
              <span className="buy">
                <meter max={footMax} value={f.buy} />
                {amount(f.buy)}
              </span>
              <span className="sell">
                <meter max={footMax} value={f.sell} />
                {amount(f.sell)}
              </span>
            </div>
          ))}
          {!feet.length && <p className="empty">足迹数据暂缺</p>}
          <p className="helper">
            5分钟粒度、约15分钟采集。足迹来源：
            {d?.footprintVenues.join("、") || "待补齐"}；
            {!d || d.footprintPartial ? "覆盖不完整" : "已获取"}
            。与五家盘口共同且新鲜的来源：
            {d?.matchedFootprintVenues.join("、") || "暂无"}
            。本图是独立成交分布，未将成交关联为某笔大单。
          </p>
        </section>
      </details>
      <section className="data-section">
        <h2>合约背景 · 辅助观察</h2>
        <div className="metric-strip">
          <Metric
            title="窗口内OI变化（端点采样）"
            value={
              d?.derivatives.oiChangeCents != null
                ? amount(d.derivatives.oiChangeCents, true)
                : "—"
            }
          />
          <Metric
            title="已观察合约主动净买卖"
            value={
              d?.perpFlow.rows
                ? amount(d.perpFlow.buyCents - d.perpFlow.sellCents, true)
                : "—"
            }
          />
          <Metric
            title="已发生多单／空单清算"
            value={
              d?.derivatives.longLiquidationCents != null &&
              d.derivatives.shortLiquidationCents != null
                ? `${amount(d.derivatives.longLiquidationCents)} / ${amount(d.derivatives.shortLiquidationCents)}`
                : "—"
            }
          />
        </div>
        <Status meta={d?.derivatives.meta} />
        <Status meta={d?.perpMeta} />
        <Status meta={d?.derivatives.liquidationMeta} />
        <p className="helper">
          合约主动成交有效分钟{" "}
          {d ? `${(d.perpFlow.coverage * 100).toFixed(0)}%` : "—"}
          。OI同时包含交易双方，不能据此把现货卖墙解释为开空。缺失窗口边界时OI变化显示为空。
        </p>
        <details>
          <summary>资金费率与结算周期</summary>
          <Status meta={d?.derivatives.fundingMeta} />
          <div className="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>交易所</th>
                  <th>保证金类别</th>
                  <th>原始费率</th>
                  <th>周期</th>
                </tr>
              </thead>
              <tbody>
                {d?.derivatives.funding.map((f, i) => (
                  <tr key={i}>
                    <td>{f.venue}</td>
                    <td>{f.margin}</td>
                    <td>{(+f.ratePercent).toFixed(5)}%</td>
                    <td>
                      {f.intervalHours ? `${f.intervalHours}小时` : "未知"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </details>
      </section>
      <details className="data-section">
        <summary>查看同一窗口K线</summary>
        <Chart option={candles} label="币安5分钟K线" height={300} />
      </details>
      <p className="helper">
        规则 {d?.rulesVersion ?? "evidence-2.1"} · 55% /
        45%为成交描述阈值，不是预测胜率。所有页面复用本地数据，本页不会直接请求上游。
      </p>
    </>
  );
}
