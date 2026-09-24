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
        <label>
          当前大单价格范围{" "}
          <select value={span} onChange={(e) => setSpan(+e.target.value)}>
            {[5, 10, 25, 1000].map((n) => (
              <option key={n} value={n}>
                {n === 1000 ? "全部已覆盖范围" : `现价±${n}%`}
              </option>
            ))}
          </select>
        </label>
        <span className="helper">现货大单 ＋ 合约背景</span>
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
      <div className="metric-strip">
        <Metric
          title="已观察现货主动净买卖"
          value={
            d?.flow.rows
              ? amount(d.flow.buyCents - d.flow.sellCents, true)
              : "—"
          }
          tone={d && d.flow.buyCents >= d.flow.sellCents ? "buy" : "sell"}
        />
        <Metric
          title="主动买入占比"
          value={d?.buyShare != null ? d.buyShare.toFixed(1) : "—"}
          unit="%"
        />
        <Metric
          title={d?.priceReaction ?? "价格反馈"}
          value={
            d?.priceChangePercent != null
              ? `${d.priceChangePercent > 0 ? "+" : ""}${d.priceChangePercent.toFixed(2)}`
              : "—"
          }
          unit="%"
        />
      </div>
      <Status meta={d?.flowMeta} />
      <p className="helper">
        成交金额是选定期间的流量；价格反馈来自同一窗口的币安USDT K线。
        {d?.pressureReaction ? d.pressureReaction + "。" : ""}
        数字不足以识别“洗盘、诱多或对倒”。
      </p>
      <div className="activity-columns">
        <section className="data-section">
          <h2>01　当前大单集中在哪里？</h2>
          <div className="range-summary compact">
            <div>
              <span>已发现大买单</span>
              <strong className="buy">
                {d?.orderHasData ? amount(d.orderBidCents) : "—"}
              </strong>
            </div>
            <div>
              <span>已发现大卖单</span>
              <strong className="sell">
                {d?.orderHasData ? amount(d.orderAskCents) : "—"}
              </strong>
            </div>
          </div>
          <p className="helper">
            当前存量 · USD ·
            约5分钟采集，12分钟有效。与普通盘口可能重叠，不重复相加。已发现
            {d?.orderCount ?? 0}条，展示金额前12条。
          </p>
          {d?.orders.slice(0, 12).map((o) => (
            <div
              className={`activity-order ${o.side === "bid" ? "buy" : "sell"}`}
              key={o.id}
            >
              <span>
                ${price(o.priceUsd, 0)}
                <small>
                  {o.venue} · {o.side === "bid" ? "买挂单" : "卖挂单"}
                </small>
              </span>
              <div>
                <meter value={o.usdCents} max={bookMax} />
                <strong>{amount(o.usdCents)}</strong>
              </div>
            </div>
          ))}
          {!d?.orders.length && (
            <p className="empty">
              暂无该范围内有效大单，不能据此判断没有挂单。
            </p>
          )}
          <details className="source-details">
            <summary>各来源获取状态</summary>
            {d?.orderSources.map((m, i) => (
              <Status key={i} meta={m} />
            ))}
          </details>
        </section>
        <section className="data-section">
          <h2>02　成交是否配合？</h2>
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
      </div>
      <section className="data-section">
        <h2>03　合约背景</h2>
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
      <section className="data-section">
        <h2>04　大单发生了什么变化？</h2>
        <p className="helper">
          {d?.tradeFeedNote ?? "逐笔大额成交暂未接入。"}{" "}
          事件覆盖全部价位。首次获取的累计成交不计入本期；买挂单被成交是被动承接，不能当成主动买入。
        </p>
        {(d?.storage.orderHistoryPaused || d?.orderHistoryGap) && (
          <p className="amber">
            本窗口包含容量保护期间的历史缺口，当前快照仍可查看。
          </p>
        )}
        <ol className="activity-events">
          {d?.events.map((e) => (
            <li key={e.key}>
              <time title={stamp(e.at)}>{clock(e.at)}发现</time>
              <div>
                <strong>
                  {kind[e.kind] ?? e.kind} · {e.venue}{" "}
                  <span className={e.side === "bid" ? "buy" : "sell"}>
                    {e.side === "bid" ? "买挂单" : "卖挂单"} {price(+e.price)}{" "}
                    {e.quote}
                  </span>
                </strong>
                <p>
                  {e.executedQuantityDelta != null &&
                  +e.executedQuantityDelta > 0
                    ? `累计成交数量增加 ${price(+e.executedQuantityDelta, 4)} ${asset}；`
                    : ""}
                  {e.quantityDelta != null
                    ? `余量变化 ${price(+e.quantityDelta, 4)} ${asset}；`
                    : ""}
                  {e.note}
                </p>
                <small>
                  {e.from
                    ? `采样区间 ${stamp(e.from)} → ${stamp(e.at)}`
                    : "首次观察，不归因当前时段的成交"}
                  {e.after?.endAt ? ` · 上游结束 ${stamp(e.after.endAt)}` : ""}
                </small>
              </div>
            </li>
          ))}
        </ol>
        {!d?.events.length && (
          <p className="empty">本窗口尚无已跟踪变化，历史补采状态见下方。</p>
        )}
        {d?.eventsHasMore && (
          <p className="helper">
            仅展示最近100条变化，完整记录保存于共享历史仓库。
          </p>
        )}
        <details className="source-details">
          <summary>历史完整范围与缺口</summary>
          <div className="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>来源／状态</th>
                  <th>已处理时间范围</th>
                  <th>待补页／缺口</th>
                </tr>
              </thead>
              <tbody>
                {d?.historyStatus.map((s) => (
                  <tr key={s.venue + s.state}>
                    <td>
                      {s.venue} · {s.state === "2" ? "结束" : "撤销"}
                    </td>
                    <td>
                      {s.from ? stamp(s.from) : "待首次补采"} →{" "}
                      {s.through ? stamp(s.through) : "—"}
                    </td>
                    <td>
                      {s.pendingPages} / {s.gaps}
                      {s.error && <p className="amber">{s.error}</p>}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </details>
      </section>
      <section className="data-section">
        <button className="text-button" onClick={() => setAdvanced(!advanced)}>
          {advanced ? "收起" : "展开"}高级视图 · K线与挂单存续
        </button>
        {advanced && (
          <>
            <Chart
              option={candles}
              label="币安5分钟K线，不含逐笔成交圆圈"
              height={300}
            />
            <p className="helper">
              下面的线展示上游创建时间至最后一次获取之间的记录跨度，不承诺两次采样之间一直存在；粗细表示当前金额。颜色仅表示买卖方向。
            </p>
            {d?.orders.slice(0, 12).map((o) => {
              const from = +new Date(d.from),
                to = +new Date(d.at),
                start = o.startAt ? Math.max(from, +new Date(o.startAt)) : to;
              const end = Math.min(to, +new Date(o.fetchedAt));
              const length = Math.max(
                0,
                Math.min(1, (end - start) / (to - from)),
              );
              return (
                <div
                  key={o.id}
                  className={`duration-row ${o.side === "bid" ? "buy" : "sell"}`}
                >
                  <span>
                    {o.venue} ${price(o.priceUsd, 0)}
                  </span>
                  <div>
                    <span
                      style={{
                        width: `${length * 100}%`,
                        marginRight: `${Math.max(0, ((to - end) / (to - from)) * 100)}%`,
                        height: Math.max(2, (12 * o.usdCents) / bookMax),
                      }}
                    />
                  </div>
                  <small>{o.startAt ? stamp(o.startAt) : "创建时间未知"}</small>
                </div>
              );
            })}
          </>
        )}
      </section>
      <p className="helper">
        规则 {d?.rulesVersion ?? "evidence-2.1"} · 55% /
        45%为成交描述阈值，不是预测胜率。所有页面复用本地数据，本页不会直接请求上游。
      </p>
    </>
  );
}
