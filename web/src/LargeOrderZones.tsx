import { useEffect, useMemo, useState } from "react";
import { amount, price, age, clock, useAPI } from "./data";
import { Chart } from "./Chart";
import { LargeOrderBoard } from "./LargeOrders";
import type { Asset } from "./types";
import "./orderZones.css";

type Source = {
  venue: string;
  quantity: string;
  usdCents: number;
  orders: number;
};
type Item = {
  key: string;
  venue: string;
  price: string;
  quote: string;
  quantity: string;
  initialQuantity: string | null;
  executedQuantity: string | null;
  usdCents: number;
  startAt: string | null;
  fetchedAt: string;
  observedAt: string | null;
  localFirstAt?: string;
  presenceNote: string;
  fxRate: string;
  fxAt: string | null;
};
type Zone = {
  id: string;
  side: string;
  center: number;
  low: number;
  high: number;
  quantity: string;
  usdCents: number;
  rank: number;
  focus: boolean;
  distancePercent: number;
  longestSeconds: number | null;
  localFirstAt: string | null;
  sources: Source[];
  largestSourcePercent: number;
  items: Item[];
  evidence: string;
  changeQuantity: string | null;
  changeFrom: string | null;
  changeTo: string | null;
  executedQuantity: string | null;
  uncertainExecutedQuantity: string | null;
};
type Trade = {
  center: number;
  buyQuantity: string;
  sellQuantity: string;
  buyCents: number;
  sellCents: number;
  approximate: boolean;
  sources: string[];
};
type Board = {
  asset: string;
  version: string;
  at: string;
  expiresAt: string;
  referencePrice: number | null;
  priceAt: string | null;
  step: number;
  range: number;
  hours: number;
  zones: Zone[];
  scaleMaxCents: number;
  supportId: string;
  resistanceId: string;
  partial: boolean;
  fetchedCount: number;
  validCount: number;
  excludedCount: number;
  eventsPartial: boolean;
  sources: {
    venue: string;
    usable: boolean;
    fxAt: string | null;
    fetchedAt: string;
    status: string;
  }[];
  trades: {
    from: string;
    to: string;
    partial: boolean;
    zones: Trade[];
    coverage: {
      venue: string;
      samples: number;
      expectedSamples: number;
      missingFX: number;
    }[];
    note: string;
  };
};
type HistoryCell = [number, number, number, string, number, number];
type History = {
  layer: string;
  from: string;
  to: string;
  actualFrom: string | null;
  actualTo: string | null;
  referencePrice: number | null;
  points: {
    time: number;
    cells: HistoryCell[];
    sources: {
      venue: string;
      at: string;
      hourlyFX: boolean;
      partial: boolean;
    }[];
    partial: boolean;
  }[];
  candles: {
    time: number;
    open: number;
    close: number;
    high: number;
    low: number;
    hourlyFX: boolean;
  }[];
  track: {
    time: number;
    price: number | null;
    quantity: string | null;
    state: string;
  }[];
  sampleSeconds: number;
  displaySeconds: number;
  candleSeconds: number;
  expectedSlots: number;
  coveredSlots: number;
  missingFX: number;
  partial: boolean;
  venues: string[];
  storageBytes: number;
  capacityGapAt: string | null;
  note: string;
};
const stamp = (s: string | null | undefined) =>
  s
    ? new Date(s).toLocaleString("zh-CN", {
        timeZone: "Asia/Shanghai",
        hour12: false,
      })
    : "未知";
const qty = (s: string | null | undefined) =>
  s == null
    ? "未知"
    : price(Number(s), 4).replace(/(\.\d*?[1-9])0+$|\.0+$/, "$1");
const dollars = (c: number | null | undefined) =>
  c == null ? "未知" : "$" + amount(c);
const precisions = (asset: Asset) =>
  asset === "BTC" ? [25, 100, 250, 500, 1000, 2500] : [1, 5, 10, 25, 50, 100];
const zoneLabel = (z?: Zone) =>
  z ? "$" + price(z.center, 0) + " 附近" : "证据不足";

export function LargeOrderZones({ asset }: { asset: Asset }) {
  const [tab, setTab] = useState("zones");
  const [step, setStep] = useState(asset === "BTC" ? 250 : 10);
  const [range, setRange] = useState(10);
  const [track, setTrack] = useState<{ key: string; label: string } | null>(
    null,
  );
  const trace = (r: Item) => {
    setTrack({ key: r.key, label: `${r.venue} · ${r.price} ${r.quote}` });
    setTab("history");
  };
  return (
    <section className="oz-page" aria-label={`${asset}大额挂单分析`}>
      <div className="oz-tabs" aria-label="挂单视图">
        {[
          ["zones", "价格区域"],
          ["history", "历史 · 进阶"],
          ["detail", "大单明细"],
        ].map(([id, text]) => (
          <button
            key={id}
            aria-pressed={tab === id}
            className={tab === id ? "selected" : ""}
            onClick={() => setTab(id)}
          >
            {text}
          </button>
        ))}
      </div>
      {tab === "zones" && (
        <ZoneBoard
          asset={asset}
          step={step}
          range={range}
          setStep={setStep}
          setRange={setRange}
          trace={trace}
        />
      )}
      {tab === "history" && (
        <ZoneHistory
          asset={asset}
          step={step}
          range={range}
          track={track}
          clearTrack={() => setTrack(null)}
        />
      )}
      {tab === "detail" && <LargeOrderBoard asset={asset} />}
    </section>
  );
}
function ZoneBoard({
  asset,
  step,
  range,
  setStep,
  setRange,
  trace,
}: {
  asset: Asset;
  step: number;
  range: number;
  setStep: (n: number) => void;
  setRange: (n: number) => void;
  trace: (r: Item) => void;
}) {
  const [hours, setHours] = useState(4);
  const [frozen, setFrozen] = useState<Board | null>(null);
  const [selected, setSelected] = useState("");
  const [all, setAll] = useState(false);
  const [rank, setRank] = useState(false);
  const [now, setNow] = useState(Date.now());
  const query = `large-order-zones?asset=${asset}&step=${step}&range=${range}&hours=${hours}`;
  const result = useAPI<Board>(frozen ? null : query, 15000);
  const d = frozen || result.data;
  useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(t);
  }, []);
  const stale = d ? now > Date.parse(d.expiresAt) : false;
  const support = d?.zones.find((z) => z.id === d.supportId);
  const resistance = d?.zones.find((z) => z.id === d.resistanceId);
  const chosen = d?.zones.find((z) => z.id === selected);
  const select = (id: string) => setSelected((s) => (s === id ? "" : id));
  const jump = (z: Zone) => {
    setSelected(z.id);
    requestAnimationFrame(() => {
      const el = document.getElementById("oz-row-" + z.id);
      if (window.matchMedia("(max-width:760px)").matches) {
        el?.scrollIntoView({ block: "start" });
        el?.focus({ preventScroll: true });
      }
    });
  };
  return (
    <>
      <div className="oz-toolbar">
        <label>
          附近{" "}
          <select
            disabled={!!frozen}
            value={range}
            onChange={(e) => setRange(+e.target.value)}
          >
            {[3, 5, 10, 20, 30].map((n) => (
              <option key={n} value={n}>
                ±{n}%
              </option>
            ))}
          </select>
        </label>
        <label>
          每档{" "}
          <select
            disabled={!!frozen}
            value={step}
            onChange={(e) => setStep(+e.target.value)}
          >
            {precisions(asset).map((n) => (
              <option key={n} value={n}>
                ${price(n, 0)}
              </option>
            ))}
          </select>
        </label>
        <button
          className="oz-freeze"
          disabled={!d}
          aria-pressed={!!frozen}
          onClick={() => setFrozen(frozen ? null : d)}
        >
          {frozen ? "恢复实时" : "冻结查看"}
        </button>
        <span className={stale ? "oz-warning" : "oz-muted"} role="status">
          {frozen ? "已冻结" : "15秒更新"}
          {d ? ` · ${clock(d.at)} 快照` : " · 正在读取"}
          {stale ? " · 已过期" : ""}
        </span>
      </div>
      {result.error && (
        <p role="alert" className="oz-warning">
          读取失败：{result.error}。保留的金额仅代表旧快照。
          <button onClick={result.refresh}>重试</button>
        </p>
      )}
      <div className={`oz-summary ${stale ? "oz-expired" : ""}`}>
        <button
          className="oz-candidate oz-bid"
          disabled={!support}
          onClick={() => support && jump(support)}
        >
          <span>下方买单最多 · 支撑候选</span>
          <strong>{zoneLabel(support)}</strong>
          <b>
            {support
              ? `${qty(support.quantity)} ${asset} · ${dollars(support.usdCents)}`
              : "等待有效来源"}
          </b>
        </button>
        <div className="oz-reference">
          <span>{frozen ? "冻结参考价" : "快照参考价"}</span>
          <strong>
            {d?.referencePrice ? "$" + price(d.referencePrice, 1) : "未知"}
          </strong>
          <small>
            观察区间{" "}
            {support && resistance
              ? `${price(support.center, 0)}—${price(resistance.center, 0)}`
              : "证据不足"}
          </small>
        </div>
        <button
          className="oz-candidate oz-ask"
          disabled={!resistance}
          onClick={() => resistance && jump(resistance)}
        >
          <span>上方卖单最多 · 阻力候选</span>
          <strong>{zoneLabel(resistance)}</strong>
          <b>
            {resistance
              ? `${qty(resistance.quantity)} ${asset} · ${dollars(resistance.usdCents)}`
              : "等待有效来源"}
          </b>
        </button>
      </div>
      <p className="oz-caption">
        按{stale ? "所选过期快照" : "当前已获取"}挂单金额排名 ·
        候选区不代表一定守住{stale ? " · 金额保持原样，过期不代表资金撤走" : ""}
      </p>
      <div className="oz-layout">
        <div className={`oz-board ${stale ? "oz-expired" : ""}`}>
          <div className="oz-board-heading">
            <h2>买卖集中在哪儿？</h2>
            <div>
              <button aria-pressed={all} onClick={() => setAll(!all)}>
                {all ? "只看重点" : "查看全部"}
              </button>
              <button aria-pressed={rank} onClick={() => setRank(!rank)}>
                {rank ? "按价格排列" : "金额排名"}
              </button>
            </div>
          </div>
          <div className="oz-scale">
            <span>挂单美元金额 · 买卖共用线性比例尺</span>
            <span>$0 — {dollars(d?.scaleMaxCents)}</span>
          </div>
          {(["ask", "bid"] as const).map((side) => {
            const rows = (d?.zones || [])
              .filter((z) => z.side === side && (all || z.focus))
              .sort((a, b) => (rank ? a.rank - b.rank : b.center - a.center));
            return (
              <div key={side}>
                {side === "bid" && (
                  <div className="oz-divider">
                    参考价{" "}
                    {d?.referencePrice
                      ? "$" + price(d.referencePrice, 1)
                      : "未知"}
                    <span>上方卖单 / 下方买单</span>
                  </div>
                )}
                <p className={`oz-side-title oz-${side}`}>
                  {side === "bid"
                    ? "下方买单 · 潜在承接"
                    : "上方卖单 · 潜在压力"}
                  <small>{rows.length} 个区域</small>
                </p>
                {rows.length === 0 && (
                  <p className="oz-empty">
                    {result.loading && !d
                      ? "正在整理价位…"
                      : "当前范围暂无可用区域；不代表市场没有挂单。"}
                  </p>
                )}
                {rows.map((z) => (
                  <div key={z.id} className="oz-row-wrap">
                    <button
                      id={"oz-row-" + z.id}
                      className={`oz-row oz-${side} ${selected === z.id ? "selected" : ""}`}
                      aria-expanded={selected === z.id}
                      aria-controls={
                        selected === z.id
                          ? "oz-evidence oz-evidence-mobile"
                          : undefined
                      }
                      onClick={() => select(z.id)}
                    >
                      <span className="oz-row-price">
                        <strong>
                          ${price(z.center, 0)}
                          <small> 附近</small>
                        </strong>
                        <small>
                          {d?.referencePrice &&
                          z.low <= d.referencePrice &&
                          z.high > d.referencePrice
                            ? "跨参考价区间"
                            : `${z.distancePercent > 0 ? "+" : ""}${z.distancePercent.toFixed(2)}%`}{" "}
                          · 金额第{z.rank}
                        </small>
                      </span>
                      <span className="oz-row-bar">
                        <i
                          style={{
                            width: `${Math.max(0, (z.usdCents / (d?.scaleMaxCents || 1)) * 100)}%`,
                          }}
                        />
                        <b>{dollars(z.usdCents)}</b>
                        <small>
                          {qty(z.quantity)} {asset}
                        </small>
                      </span>
                      <span className="oz-row-evidence">
                        <b>
                          {z.sources.length} 家交易所 ·{" "}
                          {z.longestSeconds == null
                            ? "跨度未知"
                            : "最长 " + age(z.longestSeconds)}
                        </b>
                        <small>
                          {z.changeQuantity == null
                            ? "变化未明"
                            : `已观察净变 ${Number(z.changeQuantity) > 0 ? "+" : ""}${qty(z.changeQuantity)} ${asset}`}{" "}
                          · 查看证据 →
                        </small>
                      </span>
                    </button>
                    {chosen?.id === z.id && (
                      <div className="oz-mobile-detail">
                        <ZoneDetail
                          z={chosen}
                          asset={asset}
                          trace={trace}
                          partial={!!d?.eventsPartial}
                          mobile
                        />
                      </div>
                    )}
                  </div>
                ))}
              </div>
            );
          })}
          <p className="oz-caption">
            重点＝每侧金额前三与最近三处的并集。多个区域之间的空档不代表有挂单。
          </p>
        </div>
        <aside className="oz-inspector" aria-label="所选区域证据">
          {chosen ? (
            <ZoneDetail
              z={chosen}
              asset={asset}
              trace={trace}
              partial={!!d?.eventsPartial}
            />
          ) : (
            <div className="oz-inspector-empty">
              <span>价位背后的证据</span>
              <h2>点一个区域，展开来源</h2>
              <p>交易所贡献、原始报价、剩余数量和观察记录，都保留在这里。</p>
              {selected && (
                <p role="status">该区域已不在新快照中，状态不推定为撤销。</p>
              )}
            </div>
          )}
        </aside>
      </div>
      <section className="oz-trades">
        <div className="oz-board-heading">
          <div>
            <h2>哪些价位实际成交最多？</h2>
            <p className="oz-muted">独立成交分布 · 不与挂单金额相加</p>
          </div>
          <label>
            成交观察{" "}
            <select
              disabled={!!frozen}
              value={hours}
              onChange={(e) => setHours(+e.target.value)}
            >
              {[1, 4, 24].map((n) => (
                <option key={n} value={n}>
                  {n}小时
                </option>
              ))}
            </select>
          </label>
        </div>
        <TradeProfile d={d?.trades} asset={asset} />
      </section>
      <details className="oz-coverage">
        <summary>
          {d?.partial ? "覆盖不完整" : "当前大单来源覆盖"} ·{" "}
          {d?.sources.filter((s) => s.usable).length || 0}/5 家 ·{" "}
          {d?.validCount || 0} 条区域内有效记录
        </summary>
        <p>
          读取完整本地列表，共 {d?.fetchedCount || 0}{" "}
          条；无效、状态冲突或与参考价方向不一致排除 {d?.excludedCount || 0}{" "}
          条。只代表上游筛选的大额记录。
        </p>
        <ul>
          {d?.sources.map((s) => (
            <li key={s.venue}>
              {s.venue} · {s.usable ? "可用" : "状态不完整／过期"} · 最新获取{" "}
              {stamp(s.fetchedAt)} · 汇率{" "}
              {s.fxAt ? stamp(s.fxAt) : "原生美元或缺失"}
            </li>
          ))}
        </ul>
        <p>
          快照版本 {d?.version || "—"}
          。来源持续跨度与本地观察时间不同；金额最多不是反弹概率。
        </p>
      </details>
    </>
  );
}
function ZoneDetail({
  z,
  asset,
  trace,
  partial,
  mobile = false,
}: {
  z: Zone;
  asset: Asset;
  trace: (r: Item) => void;
  partial: boolean;
  mobile?: boolean;
}) {
  return (
    <div
      id={mobile ? "oz-evidence-mobile" : "oz-evidence"}
      className="oz-detail"
    >
      <small>{z.side === "bid" ? "买单区" : "卖单区"} · 区域证据</small>
      <h2>${price(z.center, 0)} 附近</h2>
      <p className="oz-muted">
        ${price(z.low, 2)} ≤ 报价 &lt; ${price(z.high, 2)}
      </p>
      <div className="oz-detail-total">
        <strong>{dollars(z.usdCents)}</strong>
        <span>
          {qty(z.quantity)} {asset}
        </span>
      </div>
      <h3>来自哪些交易所</h3>
      {z.sources.map((s) => (
        <div className="oz-contribution" key={s.venue}>
          <span>
            {s.venue}
            <small>{s.orders} 条记录</small>
          </span>
          <b>
            {dollars(s.usdCents)}
            <small>
              {qty(s.quantity)} {asset}
            </small>
          </b>
          <i style={{ width: `${(s.usdCents / z.usdCents) * 100}%` }} />
        </div>
      ))}
      <p className="oz-muted">
        最大单一来源占 {z.largestSourcePercent.toFixed(1)}%
      </p>
      <div className="oz-evidence-list">
        <p>
          <b>来源跟踪跨度</b>
          <span>
            {z.longestSeconds == null
              ? "未知"
              : "最长 " + age(z.longestSeconds)}
          </span>
        </p>
        <small>截至来源最新快照；不表示当前数量始终存在。</small>
        <p>
          <b>本地最早观察</b>
          <span>{stamp(z.localFirstAt)}</span>
        </p>
        <p>
          <b>最近数量变化</b>
          <span>
            {z.changeQuantity == null
              ? "状态不明／无可用差分"
              : `${Number(z.changeQuantity) > 0 ? "+" : ""}${qty(z.changeQuantity)} ${asset}`}
          </span>
        </p>
        {z.changeFrom && (
          <small>
            {stamp(z.changeFrom)}—{stamp(z.changeTo)}
            ，各订单最近差分的合计；减少不等于撤单。
          </small>
        )}
        <p>
          <b>窗口内本单成交增量</b>
          <span>
            {z.executedQuantity == null
              ? "尚无可靠增量"
              : `${qty(z.executedQuantity)} ${asset}`}
          </span>
        </p>
        {z.uncertainExecutedQuantity != null && (
          <small>
            跨窗口不确定增量 {qty(z.uncertainExecutedQuantity)} {asset}
            ，未归入上方数值。
          </small>
        )}
        <p>{z.evidence}</p>
        {partial && (
          <small>
            历史结束／撤销记录存在缺口或延迟，不能据此计算全量撤单率。
          </small>
        )}
      </div>
      <h3>逐条核对 · {z.items.length} 条</h3>
      {z.items.map((r) => (
        <details key={r.key} className="oz-order">
          <summary>
            {r.venue} · {qty(r.quantity)} {asset}
            <small>
              {r.price} {r.quote}
            </small>
          </summary>
          <dl>
            <dt>原始报价</dt>
            <dd>
              {r.price} {r.quote}
            </dd>
            <dt>当前剩余</dt>
            <dd>
              {qty(r.quantity)} {asset}
            </dd>
            <dt>初始数量</dt>
            <dd>
              {qty(r.initialQuantity)} {asset}
            </dd>
            <dt>来源累计成交</dt>
            <dd>
              {qty(r.executedQuantity)} {asset}
            </dd>
            <dt>当前美元金额</dt>
            <dd>{dollars(r.usdCents)}</dd>
            <dt>换算汇率</dt>
            <dd>
              {r.fxRate} USD / {r.quote}
              <small>{r.fxAt ? stamp(r.fxAt) : "原生美元"}</small>
            </dd>
            <dt>来源创建</dt>
            <dd>{stamp(r.startAt)}</dd>
            <dt>本地首次</dt>
            <dd>{stamp(r.localFirstAt)}</dd>
            <dt>最新获取</dt>
            <dd>{stamp(r.fetchedAt)}</dd>
          </dl>
          <p>{r.presenceNote}</p>
          <button className="oz-trace-button" onClick={() => trace(r)}>
            查看本单实际采样轨迹 →
          </button>
        </details>
      ))}
    </div>
  );
}
function TradeProfile({
  d,
  asset,
}: {
  d: Board["trades"] | undefined;
  asset: Asset;
}) {
  const list = d?.zones || [];
  const leaders = [
    list.reduce<Trade | undefined>(
      (p, c) => (!p || c.buyCents > p.buyCents ? c : p),
      undefined,
    ),
    list.reduce<Trade | undefined>(
      (p, c) => (!p || c.sellCents > p.sellCents ? c : p),
      undefined,
    ),
    list[0],
  ];
  const max = Math.max(1, ...list.map((t) => t.buyCents + t.sellCents));
  return (
    <>
      <div className="oz-trade-leaders">
        {["主动买入最多", "主动卖出最多", "总成交最多"].map((label, i) => (
          <div key={label}>
            <small>{label}</small>
            <strong>
              {leaders[i]
                ? "$" + price(leaders[i]!.center, 0) + " 附近"
                : "证据不足"}
            </strong>
            <span>
              {leaders[i]
                ? dollars(
                    i === 0
                      ? leaders[i]!.buyCents
                      : i === 1
                        ? leaders[i]!.sellCents
                        : leaders[i]!.buyCents + leaders[i]!.sellCents,
                  )
                : "未覆盖不填零"}
            </span>
          </div>
        ))}
      </div>
      {list.slice(0, 5).map((t) => (
        <div className="oz-trade-row" key={t.center}>
          <b>
            ${price(t.center, 0)}
            <small>{t.approximate ? "近似归档" : "区域成交"}</small>
          </b>
          <span
            className="oz-trade-bars"
            role="img"
            aria-label={`主动买入 ${dollars(t.buyCents)} / ${qty(t.buyQuantity)} ${asset}；主动卖出 ${dollars(t.sellCents)} / ${qty(t.sellQuantity)} ${asset}`}
            title={`主动买入 ${dollars(t.buyCents)} / ${qty(t.buyQuantity)} ${asset}；主动卖出 ${dollars(t.sellCents)} / ${qty(t.sellQuantity)} ${asset}`}
          >
            <i
              className="oz-buy-fill"
              style={{ width: `${(t.buyCents / max) * 100}%` }}
            />
            <i
              className="oz-sell-fill"
              style={{ width: `${(t.sellCents / max) * 100}%` }}
            />
          </span>
          <span>
            {dollars(t.buyCents + t.sellCents)}
            <small>
              {qty(String(Number(t.buyQuantity) + Number(t.sellQuantity)))}{" "}
              {asset}
            </small>
          </span>
        </div>
      ))}
      <p className="oz-caption">
        <span className="oz-bid">■ 主动买入</span>　
        <span className="oz-ask">■ 主动卖出</span>　窗口 {stamp(d?.from)}—
        {stamp(d?.to)}（北京时间）
      </p>
      <p className={d?.partial ? "oz-warning" : "oz-muted"}>
        {d?.partial ? "成交覆盖不完整 · " : ""}
        {d?.coverage
          .map(
            (c) =>
              `${c.venue} ${c.samples}/${c.expectedSamples} 个5分钟时段${c.missingFX ? ` · ${c.missingFX} 缺汇率` : ""}`,
          )
          .join("；")}
      </p>
      <p className="oz-caption">{d?.note || "读取已验证的现货成交足迹。"}</p>
    </>
  );
}

function ZoneHistory({
  asset,
  step,
  range,
  track,
  clearTrack,
}: {
  asset: Asset;
  step: number;
  range: number;
  track: { key: string; label: string } | null;
  clearTrack: () => void;
}) {
  const [period, setPeriod] = useState("7d");
  const [layer, setLayer] = useState(track ? "orders" : "book");
  const {
    data: d,
    error,
    loading,
    refresh,
  } = useAPI<History>(
    `large-order-zones/history?asset=${asset}&step=${step}&range=${range}&period=${period}&layer=${layer}${track && layer === "orders" ? "&order=" + encodeURIComponent(track.key) : ""}`,
    60000,
  );
  const option = useMemo(() => {
    if (!d) return {};
    const observed = d.points.flatMap((p) => p.cells.map((c) => c[0]));
    const low = observed.length ? Math.min(...observed) : 0;
    const high = observed.length ? Math.max(...observed) : 0;
    const centers = Array.from(
      { length: Math.min(401, Math.round((high - low) / step) + 1) },
      (_, i) => low + i * step,
    );
    const xi = d.points.map((p) => p.time);
    const data: number[][] = [];
    d.points.forEach((p, i) =>
      p.cells.forEach((c) =>
        data.push([
          i,
          centers.indexOf(c[0]),
          c[2] * (c[1] ? -1 : 1),
          Number(c[3]),
          c[4],
          c[5],
        ]),
      ),
    );
    const max = Math.max(1, ...data.map((c) => Math.abs(c[2])));
    const candles = new Map(d.candles.map((c) => [c.time, c]));
    return {
      grid: [
        { left: 74, right: 16, top: 16, height: 95 },
        { left: 74, right: 16, top: 140, bottom: 75 },
      ],
      tooltip: {
        trigger: "item",
        renderMode: "richText",
        formatter: (x: {
          seriesType: string;
          value: number[];
          dataIndex: number;
        }) => {
          if (x.seriesType === "candlestick") {
            const c = candles.get(xi[x.dataIndex]);
            return c
              ? `Binance 现货 · 美元换算\n${stamp(new Date(c.time * 1000).toISOString())}\n开 ${price(c.open)}  收 ${price(c.close)}\n高 ${price(c.high)}  低 ${price(c.low)}${c.hourlyFX ? "\n小时汇率近似" : ""}`
              : "该时段K线不完整";
          }
          const [i, j, cents, q, mask, coverage] = x.value;
          const p = d.points[i];
          return `${stamp(new Date(p.time * 1000).toISOString())}\n$${price(centers[j], 0)} 附近 · ${cents < 0 ? "卖单" : "买单"}\n${dollars(Math.abs(cents))} · ${qty(String(q))} ${asset}\n贡献：${d.venues.filter((_, v) => mask & (1 << v)).join("、")}\n已覆盖该单元：${d.venues.filter((_, v) => coverage & (1 << v)).join("、") || "不完整"}\n${p.sources
            .filter((s) =>
              d.venues.some((v, n) => v === s.venue && mask & (1 << n)),
            )
            .map(
              (s) =>
                `${s.venue} ${clock(s.at)}${s.hourlyFX ? " · 小时汇率" : ""}`,
            )
            .join("\n")}`;
        },
      },
      xAxis: [
        {
          type: "category",
          data: xi,
          gridIndex: 0,
          axisLabel: { show: false },
        },
        {
          type: "category",
          data: xi,
          gridIndex: 1,
          axisLabel: {
            formatter: (v: string) =>
              new Date(+v * 1000).toLocaleString("zh-CN", {
                month: "2-digit",
                day: "2-digit",
                hour: "2-digit",
                hour12: false,
                timeZone: "Asia/Shanghai",
              }),
          },
        },
      ],
      yAxis: [
        {
          type: "value",
          gridIndex: 0,
          scale: true,
          axisLabel: { formatter: (v: number) => price(v, 0) },
        },
        {
          type: "category",
          gridIndex: 1,
          data: centers,
          axisLabel: { formatter: (v: string) => price(+v, 0) },
        },
      ],
      visualMap: {
        min: -max,
        max,
        dimension: 2,
        seriesIndex: 1,
        show: false,
        inRange: { color: ["#f17369", "#17201d", "#7deba9"] },
      },
      dataZoom: [
        { type: "slider", xAxisIndex: [0, 1], bottom: 12, height: 22 },
      ],
      series: [
        {
          type: "candlestick",
          xAxisIndex: 0,
          yAxisIndex: 0,
          data: xi.map((t) => {
            const c = candles.get(t);
            return c ? [c.open, c.close, c.low, c.high] : ["-", "-", "-", "-"];
          }),
          itemStyle: {
            color: "#7deba9",
            color0: "#f17369",
            borderColor: "#7deba9",
            borderColor0: "#f17369",
          },
        },
        { type: "heatmap", xAxisIndex: 1, yAxisIndex: 1, data, progressive: 0 },
      ],
    };
  }, [d, asset, step]);
  return (
    <section className="oz-history">
      <div className="oz-toolbar">
        <label>
          图层{" "}
          <select value={layer} onChange={(e) => setLayer(e.target.value)}>
            <option value="book">盘口热力图</option>
            <option value="orders">已跟踪大单观察</option>
          </select>
        </label>
        <label>
          回看{" "}
          <select value={period} onChange={(e) => setPeriod(e.target.value)}>
            <option value="7d">7天</option>
            <option value="24h">24小时</option>
          </select>
        </label>
        <span>
          ±{range}% · 每档 ${step}
        </span>
      </div>
      <h2>
        {layer === "book"
          ? "普通盘口 · 历史挂单存量"
          : "已跟踪大单 · 实际观察点"}
      </h2>
      <p className="oz-caption">
        {period === "7d" ? "4小时" : "15分钟"}K线 · 盘口历史保存约5分钟 /
        大单采集目标5分钟 · 显示抽样 {d ? d.displaySeconds / 60 : "—"}{" "}
        分钟（每所最后一帧）
      </p>
      {error && (
        <p role="alert" className="oz-warning">
          {error}
          <button onClick={refresh}>重试</button>
        </p>
      )}
      {loading && !d ? (
        <p className="oz-empty">正在读取本地历史…</p>
      ) : d?.points.some((p) => p.cells.length) ? (
        <Chart
          option={option}
          height={490}
          label="历史K线与挂单存量：绿色买单、红色卖单，空白为缺口"
        />
      ) : (
        <p className="oz-empty">
          暂无可用历史。新大单轨迹从上线后的真实采样开始积累。
        </p>
      )}
      <p className="oz-caption">
        <span className="oz-bid">■ 买单</span>　
        <span className="oz-ask">■ 卖单</span>　亮度＝美元存量（共用线性尺度）·
        空白＝缺口 · K线缺少任一基础时段则不显示
      </p>
      {track && layer === "orders" && (
        <div className="oz-track">
          <h3>{track.label} · 实际采样轨迹</h3>
          <button onClick={clearTrack}>取消本单选择</button>
          {d?.track.length ? (
            <Chart
              label="所选大单剩余数量随时间的实际观察，缺口断开"
              height={180}
              option={{
                tooltip: { trigger: "axis", renderMode: "richText" },
                grid: { left: 60, right: 20, top: 20, bottom: 35 },
                xAxis: { type: "time" },
                yAxis: { type: "value", name: asset, scale: true },
                series: [
                  {
                    type: "line",
                    connectNulls: false,
                    showSymbol: true,
                    symbolSize: 5,
                    lineStyle: { color: "#a4cbb8" },
                    data: d.track.map((t) => [
                      t.time * 1000,
                      t.quantity == null ? null : Number(t.quantity),
                    ]),
                  },
                ],
              }}
            />
          ) : (
            <p>暂无该订单实际观察记录，不按来源创建时间补画。</p>
          )}
        </div>
      )}
      {layer === "orders" && !track && (
        <p className="oz-caption">
          查看单笔轨迹：返回价格区域，展开订单并选择“查看本单实际采样轨迹”。
        </p>
      )}
      <details className="oz-coverage">
        <summary>
          {d?.partial ? "历史覆盖不完整" : "历史覆盖"} · {d?.coveredSlots || 0}/
          {d?.expectedSlots || 0} 个显示时段有来源
        </summary>
        <p>
          实际可用：{stamp(d?.actualFrom)}—{stamp(d?.actualTo)}。缺汇率采样{" "}
          {d?.missingFX || 0} 个。
        </p>
        {layer === "orders" && (
          <p>
            新增观察占用 {((d?.storageBytes || 0) / 1048576).toFixed(2)} / 64
            MiB，计入大单总预算512 MiB，最多7天。
            {d?.capacityGapAt ? `最近容量缺口 ${stamp(d.capacityGapAt)}` : ""}
          </p>
        )}
        <p>{d?.note}</p>
        <p>大单轨迹、普通盘口、实际成交是不同统计总体，不能重复相加。</p>
      </details>
    </section>
  );
}
