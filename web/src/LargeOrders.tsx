import { useEffect, useState, useRef } from "react";
import { api, amount, price, age, clock, useAPI } from "./data";
import { Status } from "./Pages";
import type { Meta } from "./Pages";
import type { Asset } from "./types";
type Order = {
  id: string;
  venue: string;
  side: string;
  price: string;
  quote: string;
  priceUsd: number | null;
  usdCents: number | null;
  quantity: string;
  initialQuantity: string | null;
  executedQuantity: string | null;
  executedUsd: string;
  startAt: string | null;
  changedAt: string | null;
  fetchedAt: string;
  endAt?: string;
  valid: boolean;
  historical?: boolean;
  state: string;
  rawState: number;
  fxRate?: string;
  fxAt?: string;
  durationSeconds: number | null;
  durationBasis: string;
  durationThrough: string;
  distancePercent: number | null;
};
type Board = {
  items: Order[];
  version: string;
  at: string;
  total: number;
  fetchedCount: number;
  validCount: number;
  bidCents: number;
  askCents: number;
  scaleMaxCents: number;
  hasMore: boolean;
  snapshotExpired?: boolean;
  newSnapshotAvailable?: boolean;
  sources: Meta[];
  note: string;
  historyStatus?: {
    venue: string;
    state: string;
    error: string;
    gaps: number;
  }[];
  events?: { key: string; at: string; venue: string; note: string }[];
};
const stamp = (s?: string | null) =>
  s
    ? new Date(s).toLocaleString("zh-CN", {
        timeZone: "Asia/Shanghai",
        hour12: false,
      })
    : "未知";
export function LargeOrderBoard({ asset }: { asset: Asset }) {
  const [side, setSide] = useState("all"),
    [venue, setVenue] = useState("all"),
    [sort, setSort] = useState("amount_desc"),
    [min, setMin] = useState(""),
    [limit, setLimit] = useState(50),
    [offset, setOffset] = useState(0),
    [history, setHistory] = useState(false);
  const [version, setVersion] = useState(""),
    [refresh, setRefresh] = useState(0),
    [d, setData] = useState<Board | null>(null),
    [error, setError] = useState(""),
    [loading, setLoading] = useState(false),
    [selected, setSelected] = useState<Order | null>(null),
    [hasNew, setHasNew] = useState(false);
  const [eventsOpen, setEventsOpen] = useState(false),
    [eventHours, setEventHours] = useState(1),
    [eventOffset, setEventOffset] = useState(0);
  const events = useAPI<{
    items: {
      key: string;
      at: string;
      venue: string;
      side: string;
      price: string;
      kind: string;
      note: string;
    }[];
    hasMore: boolean;
  }>(
    eventsOpen
      ? `large-orders?asset=${asset}&events=1&hours=${eventHours}&offset=${eventOffset}`
      : null,
    300000,
  );
  const dialog = useRef<HTMLDialogElement>(null);
  const [tick, setTick] = useState(0);
  useEffect(() => {
    const t = setInterval(() => setTick((n) => n + 1), 15000);
    return () => clearInterval(t);
  }, []);
  useEffect(() => {
    if (selected) dialog.current?.showModal();
  }, [selected]);
  useEffect(() => {
    let alive = true;
    setLoading(true);
    setError("");
    const q = new URLSearchParams({
      asset,
      side,
      venue,
      sort,
      minUsd: min || "0",
      limit: String(limit),
      offset: String(offset),
      history: history ? "1" : "0",
      version,
    });
    api<Board>("large-orders?" + q)
      .then((v) => {
        if (!alive) return;
        setData(v);
        setHasNew(!!v.newSnapshotAvailable);
        if (!version && v.version) setVersion(v.version);
      })
      .catch((e) => alive && setError(e.message))
      .finally(() => alive && setLoading(false));
    return () => {
      alive = false;
    };
  }, [
    asset,
    side,
    venue,
    sort,
    min,
    limit,
    offset,
    history,
    version,
    refresh,
    tick,
  ]);
  function fresh() {
    setVersion("");
    setOffset(0);
    setSelected(null);
    setHasNew(false);
    setRefresh((n) => n + 1);
  }
  const changed = (f: () => void) => {
    f();
    setOffset(0);
    setSelected(null);
  };
  return (
    <section className="order-board">
      <div className="research-heading">
        <div>
          <span className="eyebrow">独立观察 · 不计入普通买卖墙或BTC信号</span>
          <h2>大额挂单看板</h2>
          <p>五家交易所已获取的大单样本 · 约5分钟采集，12分钟有效</p>
        </div>
        <button className="secondary" onClick={fresh} disabled={loading}>
          {loading ? "读取中…" : hasNew ? "新快照可用 · 刷新" : "刷新快照"}
        </button>
      </div>
      <div className="research-tabs">
        <button
          aria-pressed={!history}
          className={!history ? "active" : ""}
          onClick={() =>
            changed(() => {
              setHistory(false);
              setVersion("");
            })
          }
        >
          当前挂单
        </button>
        <button
          aria-pressed={history}
          className={history ? "active" : ""}
          onClick={() =>
            changed(() => {
              setHistory(true);
              setVersion("");
            })
          }
        >
          历史结束记录
        </button>
      </div>
      {!history && (
        <>
          <div className="order-filters">
            <label>
              方向
              <select
                value={side}
                onChange={(e) => changed(() => setSide(e.target.value))}
              >
                <option value="all">全部买卖</option>
                <option value="bid">买单</option>
                <option value="ask">卖单</option>
              </select>
            </label>
            <label>
              交易所
              <select
                value={venue}
                onChange={(e) => changed(() => setVenue(e.target.value))}
              >
                {[
                  "all",
                  "Binance",
                  "OKX",
                  "Coinbase",
                  "Kraken",
                  "Bitfinex",
                ].map((x) => (
                  <option key={x} value={x}>
                    {x === "all" ? "全部来源" : x}
                  </option>
                ))}
              </select>
            </label>
            <label>
              最低金额 · USD
              <input
                type="number"
                min="0"
                placeholder="不限"
                value={min}
                onChange={(e) => changed(() => setMin(e.target.value))}
              />
            </label>
            <label>
              排序
              <select
                value={sort}
                onChange={(e) => changed(() => setSort(e.target.value))}
              >
                {[
                  ["amount_desc", "金额：从大到小"],
                  ["amount_asc", "金额：从小到大"],
                  ["price_asc", "价格：从低到高"],
                  ["price_desc", "价格：从高到低"],
                  ["distance_asc", "距现价：从近到远"],
                  ["duration_desc", "时长：从长到短"],
                ].map(([v, n]) => (
                  <option value={v} key={v}>
                    {n}
                  </option>
                ))}
              </select>
            </label>
          </div>
          <div className="range-summary">
            <div>
              <span>筛选范围内有效买单</span>
              <strong className="buy">
                {d?.bidCents != null ? amount(d.bidCents) : "—"}
              </strong>
            </div>
            <div>
              <span>筛选范围内有效卖单</span>
              <strong className="sell">
                {d?.askCents != null ? amount(d.askCents) : "—"}
              </strong>
            </div>
            <div>
              <span>已获取 / 筛选后 / 有效</span>
              <strong>
                {d?.total != null
                  ? `${d.fetchedCount} / ${d.total} / ${d.validCount}`
                  : "—"}
              </strong>
            </div>
          </div>
        </>
      )}
      {error && (
        <p className="sell" role="alert">
          {error}
        </p>
      )}
      {d?.snapshotExpired ? (
        <p className="amber">快照已过期，请刷新；筛选条件会保留。</p>
      ) : (
        <>
          <p className="helper">
            {d?.note} {d?.at && `快照 ${stamp(d.at)}`} ·{" "}
            {history
              ? "历史记录分批展示"
              : `当前展示 ${d?.items.length ? offset + 1 : 0}–${offset + (d?.items.length || 0)} / ${d?.total ?? 0} 条`}
          </p>
          <div className="order-board-head">
            <span>来源 / 方向 / 价格</span>
            <span>剩余金额 · USD</span>
            <span>已出现时长</span>
            <span>数据状态</span>
          </div>
          <div className="order-board-list">
            {d?.items.map((o) => (
              <button
                key={o.id}
                className={`order-board-row ${o.side === "bid" ? "buy" : "sell"} ${!o.valid ? "stale-row" : ""}`}
                onClick={() => setSelected(o)}
              >
                <span>
                  <b>
                    {o.side === "bid" ? "买单" : "卖单"} · {o.venue}
                  </b>
                  <strong>
                    {price(+o.price)} <small>{o.quote}</small>
                  </strong>
                  <small>
                    {o.distancePercent == null
                      ? "距现价未知"
                      : `${o.distancePercent > 0 ? "+" : ""}${o.distancePercent.toFixed(2)}%`}
                  </small>
                </span>
                <span className="order-amount">
                  <meter
                    min={0}
                    max={Math.max(1, d.scaleMaxCents || 1)}
                    value={o.usdCents || 0}
                  />
                  <strong>
                    {o.historical
                      ? "已结束"
                      : o.usdCents == null
                        ? "汇率不可用"
                        : amount(o.usdCents)}
                  </strong>
                </span>
                <span>
                  {o.durationSeconds == null
                    ? "时长未知"
                    : age(o.durationSeconds)}
                  <small>
                    {o.durationSeconds != null
                      ? o.durationBasis === "local_observed"
                        ? "本地已观察"
                        : "自来源创建"
                      : "创建时间未知"}
                  </small>
                  <small>截至 {clock(o.durationThrough || o.fetchedAt)}</small>
                </span>
                <span className="order-state">
                  {o.historical || o.rawState > 1
                    ? o.state
                    : o.valid
                      ? "快照有效"
                      : "快照或换算已过期"}
                  <small>{clock(o.fetchedAt)} 获取</small>
                </span>
              </button>
            ))}
          </div>
          {!d?.items.length && !loading && (
            <p className="empty">
              没有符合筛选条件的已获取记录，不代表市场没有挂单。
            </p>
          )}
          <div className="pagination">
            <button
              disabled={loading || offset === 0}
              onClick={() => {
                setOffset((n) => Math.max(0, n - limit));
                setSelected(null);
              }}
            >
              上一页
            </button>
            <span>第{Math.floor(offset / limit) + 1}页</span>
            <button
              disabled={loading || !d?.hasMore}
              onClick={() => {
                setOffset((n) => n + limit);
                setSelected(null);
              }}
            >
              下一页
            </button>
            <label>
              每页
              <select
                value={limit}
                onChange={(e) => changed(() => setLimit(+e.target.value))}
              >
                <option value={50}>50条</option>
                <option value={100}>100条</option>
              </select>
            </label>
          </div>
        </>
      )}
      {selected && (
        <dialog
          ref={dialog}
          className="order-inspector"
          aria-label="挂单详情"
          onClose={() => setSelected(null)}
        >
          <div className="section-heading">
            <h3>
              {selected.venue} · {selected.side === "bid" ? "买单" : "卖单"}{" "}
              {price(+selected.price)} {selected.quote}
            </h3>
            <button onClick={() => setSelected(null)}>关闭详情</button>
          </div>
          <dl>
            <dt>来源创建</dt>
            <dd>{stamp(selected.startAt)}</dd>
            <dt>最后变更</dt>
            <dd>{stamp(selected.changedAt)}</dd>
            <dt>本次获取</dt>
            <dd>{stamp(selected.fetchedAt)}</dd>
            <dt>初始数量 / 当前余量</dt>
            <dd>
              {selected.initialQuantity ?? "未知"} / {selected.quantity} {asset}
            </dd>
            <dt>累计成交数量</dt>
            <dd>
              {selected.executedQuantity ?? "未知"} {asset}
            </dd>
            <dt>美元汇率 / 汇率时点</dt>
            <dd>
              {selected.fxRate || "未知"} / {stamp(selected.fxAt)}
            </dd>
            <dt>上游状态</dt>
            <dd>
              {selected.state} · raw {selected.rawState}
            </dd>
          </dl>
          <p className="helper">
            累计成交不是本小时成交，也不是主动买入。余量减少或记录消失不能直接认定撤单；创建至获取的跨度不证明金额一直不变。
          </p>
        </dialog>
      )}
      <details className="data-section">
        <summary>高级视图 · 本页挂单已出现时长</summary>
        <p className="helper">
          线长代表创建至快照的跨度；本地观察另行标注，不表示金额始终未变。全部数值截至各自快照。
        </p>
        {d?.items
          .filter((o) => o.durationSeconds != null)
          .map((o) => (
            <div
              key={o.id}
              className={`duration-row ${o.side === "bid" ? "buy" : "sell"}`}
            >
              <span>
                {o.venue} {price(+o.price)} {o.quote}
              </span>
              <meter
                max={Math.max(
                  1,
                  ...(d?.items ?? []).map((v) => v.durationSeconds ?? 0),
                )}
                value={o.durationSeconds ?? 0}
              />
              <span>
                {age(o.durationSeconds ?? 0)} ·{" "}
                {o.durationBasis === "local_observed" ? "本地观察" : "来源创建"}
              </span>
            </div>
          ))}
      </details>
      <details
        className="data-section"
        open={eventsOpen}
        onToggle={(e) => setEventsOpen(e.currentTarget.open)}
      >
        <summary>已观察的变化与结束事件</summary>
        <label>
          观察窗口
          <select
            value={eventHours}
            onChange={(e) => {
              setEventHours(+e.target.value);
              setEventOffset(0);
            }}
          >
            {[1, 4, 24].map((n) => (
              <option value={n} key={n}>
                {n}小时
              </option>
            ))}
          </select>
        </label>
        {events.error && <p className="amber">{events.error}</p>}
        {events.data?.items.map((e) => (
          <div className="coverage-job" key={e.key}>
            <strong>
              {stamp(e.at)} · {e.venue} · {e.side === "bid" ? "买单" : "卖单"}{" "}
              {price(+e.price)}
            </strong>
            <p>{e.note}</p>
          </div>
        ))}
        {!events.data?.items.length && (
          <p className="helper">
            此窗口暂无已记录变化；不代表期间未发生成交或撤销。
          </p>
        )}
        <div className="pagination">
          <button
            disabled={!eventOffset}
            onClick={() => setEventOffset((n) => Math.max(0, n - 50))}
          >
            前50条
          </button>
          <button
            disabled={!events.data?.hasMore}
            onClick={() => setEventOffset((n) => n + 50)}
          >
            后50条
          </button>
        </div>
      </details>
      <details className="source-details">
        <summary>来源与历史覆盖</summary>
        {d?.sources?.map((m, i) => (
          <Status key={i} meta={m} />
        ))}
        {d?.historyStatus?.map((s, i) => (
          <p key={i}>
            {s.venue} · {s.state === "2" ? "结束" : "撤销筛选"} · 缺口{s.gaps}{" "}
            {s.error}
          </p>
        ))}
      </details>
    </section>
  );
}
