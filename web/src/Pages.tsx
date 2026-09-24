import { useMemo, useState } from "react";
import { ArrowRight, Copy, Trash, WarningCircle } from "@phosphor-icons/react";
import { api, amount, price, clock, venue, useAPI } from "./data";
import { Chart } from "./Chart";
import { Empty } from "./App";
import type {
  Asset,
  Frame,
  Health,
  Flow,
  DerivativesResponse,
  WhalesResponse,
  Settings,
  Annotation,
} from "./types";
type Props = {
  view: string;
  asset: Asset;
  frame: Frame | null;
  health: Health | null;
  onNavigate: (v: string) => void;
};
const baseAxis = {
  axisLabel: { color: "#95a29d" },
  splitLine: { lineStyle: { color: "#293330" } },
  axisLine: { lineStyle: { color: "#35423d" } },
};
export function MarketPages(p: Props) {
  if (p.view === "flow") return <FlowPage {...p} />;
  if (p.view === "derivatives") return <DerivativesPage {...p} />;
  if (p.view === "whales") return <WhalesPage {...p} />;
  if (p.view === "health") return <HealthPage {...p} />;
  return <Empty text="选择上方页面查看数据。" />;
}
function FlowPage({ asset }: Props) {
  const [hours, setHours] = useState(1);
  const [market, setMarket] = useState("spot");
  const { data, error } = useAPI<Flow>(
    `flow?asset=${asset}&hours=${hours}&market=${market}`,
    15000,
  );
  const option = useMemo(
    () => ({
      grid: { left: 85, right: 30, top: 25, bottom: 35 },
      tooltip: {
        trigger: "axis",
        valueFormatter: (v: number) => amount(v) + " USD",
      },
      xAxis: {
        type: "time",
        ...baseAxis,
        axisLabel: { formatter: (t: number) => clock(t / 1000) },
      },
      yAxis: {
        type: "value",
        ...baseAxis,
        axisLabel: { formatter: (n: number) => amount(n) },
      },
      series: [
        {
          name: "累计主动净买入",
          type: "line",
          symbol: "none",
          lineStyle: { color: "#7deba9", width: 2 },
          data: data?.series.map((p) => [p.time * 1000, p.cvdCents]) ?? [],
        },
      ],
    }),
    [data],
  );
  const bins = Object.entries(data?.footprint ?? {})
    .map(([p, [buy, sell]]) => ({ price: +p * (data?.step ?? 1), buy, sell }))
    .sort((a, b) => b.buy + b.sell - a.buy - a.sell)
    .slice(0, 30)
    .sort((a, b) => b.price - a.price);
  const max = Math.max(1, ...bins.map((b) => Math.max(b.buy, b.sell)));
  return (
    <>
      <div className="toolbar page-toolbar">
        <div className="segmented">
          {[
            ["spot", "现货"],
            ["perp", "合约"],
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
        <label>
          统计窗口{" "}
          <select value={hours} onChange={(e) => setHours(+e.target.value)}>
            {[
              [0.25, "15分钟"],
              [1, "1小时"],
              [4, "4小时"],
              [24, "24小时"],
              [168, "7天"],
              [720, "30天"],
            ].map(([v, t]) => (
              <option value={v} key={v}>
                {t}
              </option>
            ))}
          </select>
        </label>
        <span className="helper">主动净买入 = 主动买入额 − 主动卖出额</span>
      </div>
      {error && <div className="notice danger">{error}</div>}
      <div className="metric-strip">
        <Metric
          title="主动买入"
          value={data ? amount(data.buyCents) : "—"}
          tone="buy"
        />
        <Metric
          title="主动卖出"
          value={data ? amount(data.sellCents) : "—"}
          tone="sell"
        />
        <Metric
          title="主动净买入"
          value={data ? amount(data.netCents, true) : "—"}
          tone={(data?.netCents ?? 0) >= 0 ? "buy" : "sell"}
        />
        <Metric
          title="成交量加权均价 · VWAP"
          value={data?.vwap ? "$" + price(data.vwap) : "—"}
        />
      </div>
      <p className="helper">
        这是已接收成交的主动方向统计，不是交易所充值提现或新增资金。
        {data?.partial && (
          <strong className="amber">
            {" "}
            当前窗口包含缺口或未完整覆盖，仅为已观察成交。
          </strong>
        )}
        {data && new Date(data.startedAt) > new Date(data.from)
          ? "采集起点晚于所选窗口，当前为部分历史。"
          : ""}
      </p>
      <section className="data-section">
        <h2>累计主动净买入 · CVD</h2>
        {data?.series.length ? (
          <Chart option={option} label="累计主动净买入时间曲线" />
        ) : (
          <Empty text="正在积累实际成交，暂无该窗口的有效统计。" />
        )}
      </section>
      <section className="data-section">
        <div className="section-heading">
          <h2>成交足迹</h2>
          <span>成交最活跃的30个价位 · 按价格排列</span>
        </div>
        <div className="footprint-header">
          <span>价格区间</span>
          <span className="sell">主动卖出</span>
          <span className="buy">主动买入</span>
          <span>净买入</span>
        </div>
        {bins.map((b) => (
          <div className="footprint-row" key={b.price}>
            <span>${price(b.price, 0)}</span>
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
        {!bins.length && <Empty text="暂无有效成交价位。" />}
      </section>
    </>
  );
}
function DerivativesPage({ asset }: Props) {
  const [hours, setHours] = useState(1);
  const { data, error } = useAPI<DerivativesResponse>(
    `derivatives?asset=${asset}&hours=${hours}`,
    30000,
  );
  const option = useMemo(
    () => ({
      color: ["#7deba9", "#f17369", "#80a9e0"],
      grid: { left: 90, right: 25, top: 40, bottom: 30 },
      legend: { textStyle: { color: "#aebfb5" } },
      tooltip: {
        trigger: "axis",
        valueFormatter: (n: number) => amount(n) + " USD",
      },
      xAxis: { type: "time", ...baseAxis },
      yAxis: {
        type: "value",
        ...baseAxis,
        axisLabel: { formatter: (n: number) => amount(n) },
        scale: true,
      },
      series: ["binance", "okx", "bybit"].map((v, i) => ({
        name: venue(v),
        type: "line",
        symbol: "none",
        lineStyle: { color: ["#7deba9", "#f17369", "#80a9e0"][i] },
        data: (data?.series ?? [])
          .filter((d) => d.venue === v)
          .map((d) => [d.time * 1000, d.oiUsdCents]),
      })),
    }),
    [data],
  );
  return (
    <>
      <div className="toolbar page-toolbar">
        <label>
          观察窗口{" "}
          <select value={hours} onChange={(e) => setHours(+e.target.value)}>
            {[
              [1, "1小时"],
              [24, "1天"],
              [168, "7天"],
              [720, "30天"],
            ].map(([h, t]) => (
              <option value={h} key={h}>
                {t}
              </option>
            ))}
          </select>
        </label>
        <span className="helper">变化从窗口内最早有效采样计算</span>
      </div>
      <div className="notice">
        <WarningCircle size={18} />
        现货与合约分开统计；OI 采用单边持仓口径，美元价值变化也受价格影响。
      </div>
      {error && <p className="sell">{error}</p>}
      <section className="data-section">
        <h2>永续合约概览</h2>
        <div className="table-scroll">
          <table>
            <thead>
              <tr>
                <th>平台</th>
                <th>OI · {asset}</th>
                <th>OI · 美元</th>
                <th>窗口内 OI 数量变化</th>
                <th>资金费率 / 周期</th>
                <th>下次结算</th>
                <th>标记价格</th>
                <th>相对指数基差</th>
                <th>状态</th>
              </tr>
            </thead>
            <tbody>
              {data?.items.map((d) => (
                <tr key={d.venue} className={!d.valid ? "stale-row" : ""}>
                  <td>{venue(d.venue)}</td>
                  <td>{price(d.oiBase, 2)}</td>
                  <td>{amount(d.oiUsdCents)}</td>
                  <td>
                    {data?.changes[d.venue]
                      ? price(data.changes[d.venue].oiBase, 2) + " " + asset
                      : "历史不足"}
                  </td>
                  <td className={d.funding >= 0 ? "buy" : "sell"}>
                    {(d.funding * 100).toFixed(4)}%{" "}
                    <small>/ {d.intervalHours}h</small>
                  </td>
                  <td>{d.nextFunding ? clock(d.nextFunding / 1000) : "—"}</td>
                  <td>${price(d.mark)}</td>
                  <td>
                    {d.index
                      ? ((d.mark / d.index - 1) * 100).toFixed(3) + "%"
                      : "—"}
                  </td>
                  <td>
                    <span className={`status-dot ${!d.valid ? "warn" : ""}`} />
                    {d.valid ? "有效" : "过期 / 不完整"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        {!data?.items.length && <Empty text="正在读取合约持仓和资金费率。" />}
      </section>
      <section className="data-section">
        <h2>OI 变化轨迹</h2>
        {data?.series.length ? (
          <Chart label="分交易所 OI 历史" option={option} />
        ) : (
          <Empty text="历史正在积累。" />
        )}
      </section>
      <section className="data-section">
        <div className="section-heading">
          <h2>已观察到的清算</h2>
          <span>最近24小时已观察事件 · 最新100条</span>
        </div>
        <p className="helper">
          Binance 为每秒最新事件快照，OKX 为抽样发布，Bybit
          为平台公开清算推送。破产价格与实际成交均价分别标注，无法保证全市场完整性。
        </p>
        <div className="table-scroll">
          <table>
            <thead>
              <tr>
                <th>时间</th>
                <th>平台</th>
                <th>被清算方向</th>
                <th>涉及金额</th>
                <th>价格</th>
                <th>价格含义</th>
              </tr>
            </thead>
            <tbody>
              {data?.liquidations
                .slice()
                .reverse()
                .slice(0, 100)
                .map((l, i) => (
                  <tr key={`${l.venue}-${l.at}-${i}`}>
                    <td>{clock(l.at)}</td>
                    <td>{venue(l.venue)}</td>
                    <td className={l.side === "long" ? "buy" : "sell"}>
                      {l.side === "long" ? "多头" : "空头"}
                    </td>
                    <td>{amount(l.usdCents)}</td>
                    <td>${price(l.price)}</td>
                    <td>
                      {l.priceType === "bankruptcy"
                        ? "破产价格"
                        : "实际成交均价"}
                    </td>
                  </tr>
                ))}
            </tbody>
          </table>
        </div>
        {!data?.liquidations.length && (
          <Empty text="当前未收到该币种的清算事件；这不代表市场没有发生清算。" />
        )}
      </section>
    </>
  );
}
function WhalesPage({ asset }: Props) {
  const [limit, setLimit] = useState(10);
  const [side, setSide] = useState("all");
  const [kind, setKind] = useState("liquidation");
  const [address, setAddress] = useState("");
  const [msg, setMsg] = useState("");
  const { data, error, refresh } = useAPI<WhalesResponse>(
    `whales?asset=${asset}&limit=${limit}&side=${side}`,
    15000,
  );
  const buckets = (data?.buckets ?? [])
    .filter((b) => b.kind === kind && (side === "all" || b.side === side))
    .sort((a, b) => b.usdCents - a.usdCents)
    .slice(0, 15)
    .sort((a, b) => b.price - a.price);
  const max = Math.max(1, ...buckets.map((b) => b.usdCents));
  return (
    <>
      <div className="toolbar page-toolbar">
        <div className="segmented">
          {[
            ["liquidation", "预估清算分布"],
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
            <option value="all">全部方向</option>
            <option value="long">多头</option>
            <option value="short">空头</option>
          </select>
        </label>
        <span className="helper">
          Hyperliquid · {data?.monitor.candidates ?? 0}个候选地址 ·
          最多100个核心监控地址
        </span>
      </div>
      {error && <p className="sell">{error}</p>}
      <div className="whale-layout">
        <section className="data-section">
          <h2>
            {kind === "liquidation" ? "动态预估清算价位" : "当前持仓均价集中区"}
          </h2>
          <p className="helper">
            柱长表示涉及的当前仓位金额。
            {kind === "liquidation"
              ? "不是必然一次性爆仓金额；空清算价不参与统计。"
              : "持仓均价不是含全部费用的盈亏平衡价。"}
          </p>
          <div className="whale-bucket-head">
            <span>价格 · USD</span>
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
            <Empty text="正在发现并查询公开地址。只有90秒内成功更新且字段有效的核心持仓才参与分布。" />
          )}
        </section>
        <aside className="watchlist">
          <h2>关注公开地址</h2>
          <p className="helper">
            最多20个，优先纳入监控。地址可能对应多个策略，不据此判断真实身份或整体方向。
          </p>
          <form
            onSubmit={async (e) => {
              e.preventDefault();
              try {
                await api("watchlist", {
                  method: "POST",
                  body: JSON.stringify({ address, pinned: true }),
                });
                setAddress("");
                setMsg("已加入监控队列");
                refresh();
              } catch (e) {
                setMsg((e as Error).message);
              }
            }}
          >
            <label className="sr-only" htmlFor="watch-address">
              Hyperliquid 地址
            </label>
            <input
              id="watch-address"
              placeholder="0x…"
              value={address}
              onChange={(e) => setAddress(e.target.value)}
              required
            />
            <button className="primary full">
              加入关注 <ArrowRight size={17} />
            </button>
          </form>
          {msg && (
            <p className="helper" role="status">
              {msg}
            </p>
          )}
          {data?.monitor.pinned.map((a) => (
            <div className="pin-row" key={a}>
              <span title={a}>
                {a.slice(0, 8)}…{a.slice(-6)}
              </span>
              <button
                className="icon-button"
                aria-label="取消关注"
                onClick={() =>
                  api("watchlist", {
                    method: "POST",
                    body: JSON.stringify({ address: a, pinned: false }),
                  }).then(refresh)
                }
              >
                <Trash size={17} />
              </button>
            </div>
          ))}
          <p className="helper">
            实时订阅 {data?.monitor.websocketUsers ?? 0}/10
            个重点地址，其余核心持仓约每30秒查询。
          </p>
        </aside>
      </div>
      <section className="data-section">
        <div className="section-heading">
          <h2>已监控地址持仓榜</h2>
          <div className="segmented">
            {[10, 50].map((n) => (
              <button
                key={n}
                className={limit === n ? "selected" : ""}
                onClick={() => setLimit(n)}
              >
                前{n}
              </button>
            ))}
          </div>
          <span className="push-right">按当前仓位金额排序 · 非全市场排名</span>
        </div>
        <div className="table-scroll">
          <table>
            <thead>
              <tr>
                <th>地址</th>
                <th>方向</th>
                <th>仓位金额</th>
                <th>持仓均价</th>
                <th>设置杠杆</th>
                <th>预估清算价</th>
                <th>距清算</th>
                <th>浮盈亏</th>
                <th>较上次采样数量变化</th>
                <th>状态</th>
              </tr>
            </thead>
            <tbody>
              {data?.items.map((w) => (
                <tr key={w.address} className={!w.valid ? "stale-row" : ""}>
                  <td>
                    <button
                      className="address"
                      title={w.address}
                      onClick={() =>
                        navigator.clipboard
                          .writeText(w.address)
                          .then(() => setMsg("地址已复制"))
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
                  <td title={`原始 ${w.entry} USDC`}>
                    ${price(+w.entry * +w.rate)}
                  </td>
                  <td>
                    {w.margin === "cross" ? "全仓" : "逐仓"} {w.leverage}×
                  </td>
                  <td>
                    {w.liquidation
                      ? "$" + price(+w.liquidation * +w.rate)
                      : "暂无可用价格"}
                  </td>
                  <td>
                    {w.distance === null ? "—" : w.distance.toFixed(2) + "%"}
                  </td>
                  <td className={w.unrealizedCents >= 0 ? "buy" : "sell"}>
                    {amount(w.unrealizedCents, true)}
                  </td>
                  <td>
                    {+w.changeSize === 0
                      ? "—"
                      : price(+w.changeSize, 4) + " " + asset}
                  </td>
                  <td>{w.valid ? clock(w.at) + "更新" : "过期，未参与聚合"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        {!data?.items.length && (
          <Empty text="尚无已确认的核心持仓。发现地址后会逐步形成榜单。" />
        )}
        <p className="helper">
          清算以 Hyperliquid
          标记价格判断；保证金、其他持仓盈亏与资金费变化都可能移动清算位置。持仓均价和清算价按采样时
          USDC/USD 换算。
        </p>
      </section>
    </>
  );
}
function HealthPage({ health, frame, asset }: Props) {
  const settings = useAPI<Settings>("settings", 60000);
  const annotations = useAPI<Annotation[]>("annotations?asset=" + asset, 30000);
  const [retention, setRetention] = useState<number | null>(null);
  const [message, setMessage] = useState("");
  const gb = (n: number) => (n / 2 ** 30).toFixed(2) + " GB";
  return (
    <>
      <div className="metric-strip">
        <Metric
          title="项目磁盘占用"
          value={gb(health?.storage.usedBytes ?? 0)}
          unit=""
        />
        <Metric
          title="磁盘剩余空间"
          value={gb(health?.storage.freeBytes ?? 0)}
          unit=""
        />
        <Metric
          title="当前采集模式"
          value={health?.storage.fineHistoryPaused ? "保护模式" : "正常采集"}
          unit=""
        />
        <Metric title="版本" value={health?.version ?? "—"} unit="" />
      </div>
      {health?.storage.error && (
        <div className="notice danger">{health.storage.error}</div>
      )}
      <div className="settings-grid">
        <section className="data-section">
          <h2>自动保留与清理</h2>
          <form
            className="settings-form"
            onSubmit={async (e) => {
              e.preventDefault();
              try {
                await api("settings", {
                  method: "PUT",
                  body: JSON.stringify({
                    ...settings.data,
                    retentionDays: retention ?? settings.data?.retentionDays,
                  }),
                });
                setMessage("保留策略已保存，后台执行清理");
                settings.refresh();
              } catch (e) {
                setMessage((e as Error).message);
              }
            }}
          >
            <label>
              最长保留{" "}
              <select
                value={retention ?? settings.data?.retentionDays ?? 90}
                onChange={(e) => setRetention(+e.target.value)}
              >
                <option value={30}>30天</option>
                <option value={90}>90天（长期降采样）</option>
              </select>
            </label>
            <p>
              5秒盘口：24小时
              <br />
              1分钟统计：30天
              <br />
              地址持仓：5分钟采样，30天
              <br />
              15分钟汇总：最长90天
            </p>
            <p className="helper">
              项目预算 {settings.data?.budgetGB ?? 20} GB，保留至少{" "}
              {settings.data?.minFreeGB ?? 8} GB
              空闲。自动清理旧细数据；空间不足暂停细历史写入。
            </p>
            <button className="primary">保存保留策略</button>
            {message && <p role="status">{message}</p>}
          </form>
        </section>
        <section className="data-section">
          <h2>美元换算</h2>
          <table>
            <thead>
              <tr>
                <th>报价</th>
                <th>美元汇率</th>
                <th>采样时间</th>
              </tr>
            </thead>
            <tbody>
              {frame?.rates
                .filter((r) => r.quote)
                .map((r) => (
                  <tr key={r.quote}>
                    <td>{r.quote}</td>
                    <td>{r.usd || "暂无"}</td>
                    <td>{r.quote === "USD" ? "基准" : clock(r.observedAt)}</td>
                  </tr>
                ))}
            </tbody>
          </table>
          <p className="helper">
            Kraken 买卖中间价，每5秒更新；过期汇率不用于有效实时汇总。
          </p>
        </section>
      </div>
      <section className="data-section">
        <h2>现货盘口覆盖 · {asset}</h2>
        <div className="table-scroll">
          <table>
            <thead>
              <tr>
                <th>平台 / 交易对</th>
                <th>状态</th>
                <th>已知买单下界</th>
                <th>已知卖单上界</th>
                <th>档位数</th>
                <th>重建次数</th>
                <th>说明</th>
              </tr>
            </thead>
            <tbody>
              {frame?.coverage.map((c) => (
                <tr key={c.venue + c.symbol}>
                  <td>
                    {venue(c.venue)} <small>{c.symbol}</small>
                  </td>
                  <td>
                    <span className={`status-dot ${c.valid ? "" : "warn"}`} />
                    {c.valid ? "有效" : "未覆盖"}
                  </td>
                  <td>{c.bidLow ? "$" + price(c.bidLow) : "—"}</td>
                  <td>{c.askHigh ? "$" + price(c.askHigh) : "—"}</td>
                  <td>{c.levels}</td>
                  <td>{c.resyncs}</td>
                  <td>{c.reason || "已通过盘口校验"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
      <section className="data-section">
        <h2>采集连接</h2>
        <div className="feed-grid">
          {health?.feeds.map((f) => (
            <div className="feed" key={f.component}>
              <span className={`status-dot ${f.ok ? "" : "warn"}`} />
              <div>
                <strong>{f.component}</strong>
                <p>{f.detail || "正常运行"}</p>
                <small>
                  {clock(f.at)} · 累计异常 {f.errors}
                </small>
              </div>
            </div>
          ))}
        </div>
      </section>
      <section className="data-section">
        <h2>我的价格标注 · {asset}</h2>
        <table>
          <thead>
            <tr>
              <th>方向</th>
              <th>开仓</th>
              <th>止损</th>
              <th>止盈</th>
              <th>盈亏比</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {annotations.data?.map((a) => (
              <tr key={a.id}>
                <td>{a.side === "long" ? "多头" : "空头"}</td>
                <td>{price(a.entry)}</td>
                <td>{price(a.stop)}</td>
                <td>{price(a.target)}</td>
                <td>
                  {(
                    Math.abs(a.target - a.entry) / Math.abs(a.entry - a.stop)
                  ).toFixed(2)}{" "}
                  : 1
                </td>
                <td>
                  <button
                    className="icon-button"
                    aria-label="删除标注"
                    onClick={() =>
                      api("annotations", {
                        method: "DELETE",
                        body: JSON.stringify({ id: a.id }),
                      }).then(annotations.refresh)
                    }
                  >
                    <Trash size={17} />
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        {!annotations.data?.length && (
          <p className="helper">可在买卖墙右侧创建标注。标注不会触发交易。</p>
        )}
      </section>
    </>
  );
}
function Metric({
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

export function OverviewSummary({
  asset,
  onNavigate,
}: {
  asset: Asset;
  onNavigate: (v: string) => void;
}) {
  const f = useAPI<Flow>(`flow?asset=${asset}&hours=1`, 30000);
  const d = useAPI<DerivativesResponse>(`derivatives?asset=${asset}`, 30000);
  const w = useAPI<WhalesResponse>(`whales?asset=${asset}`, 30000);
  return (
    <>
      <div className="metric-strip">
        <button onClick={() => onNavigate("flow")} className="summary-link">
          <Metric
            title="1小时主动净买入 · 现货"
            value={f.data ? amount(f.data.netCents, true) : "—"}
            tone={f.data && f.data.netCents < 0 ? "sell" : "buy"}
          />
          <small>{f.data?.partial ? "窗口未完整覆盖" : "查看成交证据"}</small>
        </button>
        <button
          className="summary-link"
          onClick={() => onNavigate("derivatives")}
        >
          <Metric
            title="三家有效合约 OI"
            value={
              d.data
                ? amount(
                    d.data.items
                      .filter((x) => x.valid)
                      .reduce((s, x) => s + x.oiUsdCents, 0),
                  )
                : "—"
            }
          />
          <small>美元价值也随价格变化</small>
        </button>
        <button className="summary-link" onClick={() => onNavigate("whales")}>
          <Metric
            title="已监控仓位"
            value={String(w.data?.count ?? "—")}
            unit="个地址"
          />
          <small>Hyperliquid · 非全市场排名</small>
        </button>
        <div className="summary-link">
          <Metric title="先看什么？" value="金额 → 持续 → 成交" unit="" />
          <small>挂单只是意愿，成交提供证据</small>
        </div>
      </div>
      <PriceHistory asset={asset} />
    </>
  );
}
export function PriceHistory({ asset }: { asset: Asset }) {
  const [hours, setHours] = useState(24);
  const [period, setPeriod] = useState(900);
  const { data, error } = useAPI<{
    candles: {
      time: number;
      open: number;
      close: number;
      high: number;
      low: number;
    }[];
    period: number;
    note: string;
  }>(`candles?asset=${asset}&hours=${hours}&period=${period}`, 60000);
  const option = useMemo(
    () => ({
      grid: { left: 80, right: 25, top: 15, bottom: 55 },
      tooltip: { trigger: "axis" },
      xAxis: {
        type: "category",
        ...baseAxis,
        data:
          data?.candles.map((c) =>
            new Date(c.time * 1000).toLocaleString("zh-CN", {
              month: "2-digit",
              day: "2-digit",
              hour: "2-digit",
              minute: "2-digit",
            }),
          ) ?? [],
        boundaryGap: true,
      },
      yAxis: { type: "value", ...baseAxis, scale: true },
      dataZoom: [
        { type: "inside" },
        {
          type: "slider",
          height: 18,
          bottom: 0,
          borderColor: "#35443b",
          textStyle: { color: "#879e8e" },
        },
      ],
      series: [
        {
          type: "candlestick",
          itemStyle: {
            color: "#7deba9",
            color0: "#f17369",
            borderColor: "#7deba9",
            borderColor0: "#f17369",
          },
          data:
            data?.candles.map((c) => [c.open, c.close, c.low, c.high]) ?? [],
        },
      ],
    }),
    [data],
  );
  return (
    <section className="data-section">
      <div className="section-heading">
        <h2>综合现货价格</h2>
        <label>
          图表周期
          <select value={period} onChange={(e) => setPeriod(+e.target.value)}>
            {[
              [60, "1分钟"],
              [300, "5分钟"],
              [900, "15分钟"],
              [3600, "1小时"],
              [14400, "4小时"],
              [86400, "1天"],
            ].map(([p, t]) => (
              <option value={p} key={p}>
                {t}
              </option>
            ))}
          </select>
        </label>
        <label>
          回看
          <select value={hours} onChange={(e) => setHours(+e.target.value)}>
            {[
              [1, "1小时"],
              [24, "1天"],
              [168, "7天"],
              [720, "30天"],
              [2160, "90天"],
            ].map(([h, t]) => (
              <option value={h} key={h}>
                {t}
              </option>
            ))}
          </select>
        </label>
      </div>
      {error && <p className="helper sell">{error}</p>}
      {data?.candles.length ? (
        <Chart label="综合现货价格 K 线" height={220} option={option} />
      ) : (
        <Empty text="价格历史正在积累。" />
      )}
      <p className="helper">
        有效现货报价中位数的观察区间，非单一交易所成交 K 线。
        {data && `实际周期 ${data.period / 60} 分钟；长窗口自动限制点数。`}
      </p>
    </section>
  );
}
