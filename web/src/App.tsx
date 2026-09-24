import { useEffect, useMemo, useRef, useState } from "react";
import {
  ArrowRight,
  ArrowsClockwise,
  SignOut,
  WarningCircle,
  ChartBar,
  ShieldCheck,
  X,
  ArrowSquareOut,
} from "@phosphor-icons/react";
import { api, amount, price, age, clock, venue, useAPI } from "./data";
import type { Asset, Frame, Zone, Health, History } from "./types";
import { Chart } from "./Chart";
import { MarketPages, OverviewSummary, LargeOrders } from "./Pages";
import { designFixture } from "./demo";
const nav = [
  ["overview", "总览"],
  ["liquidity", "现货挂单"],
  ["flow", "成交分析"],
  ["activity", "大资金动向"],
  ["derivatives", "合约态势"],
  ["liquidations", "清算分布"],
  ["whales", "巨鲸持仓"],
  ["health", "数据健康"],
] as const;
const fixture =
  import.meta.env.DEV &&
  new URLSearchParams(location.search).get("demo") === "1";
export function App() {
  const [authenticated, setAuthenticated] = useState<boolean | null>(
    fixture ? true : null,
  );
  const [asset, setAsset] = useState<Asset>("BTC");
  const [view, setView] = useState(location.hash.slice(1) || "liquidity");
  const [step, setStep] = useState(100);
  const [span, setSpan] = useState(10);
  const [minAge, setMinAge] = useState(fixture ? 300 : 0);
  const [frame, setFrame] = useState<Frame | null>(
    fixture ? designFixture() : null,
  );
  const [connected, setConnected] = useState(false);
  const [error, setError] = useState("");
  const [selectedKey, setSelectedKey] = useState(fixture ? "bid/83800" : "");
  const [expanded, setExpanded] = useState(false);
  const [hours, setHours] = useState(24);
  const [historyMode, setHistoryMode] = useState(false);
  const [annotationOpen, setAnnotationOpen] = useState(false);
  const [clockNow, setClockNow] = useState(Date.now());
  useEffect(() => {
    if (!fixture)
      api<{ authenticated: boolean }>("session")
        .then((d) => setAuthenticated(d.authenticated))
        .catch(() => setAuthenticated(false));
    const expired = () => setAuthenticated(false);
    window.addEventListener("tidal-session-expired", expired);
    const hash = () => setView(location.hash.slice(1) || "liquidity");
    window.addEventListener("hashchange", hash);
    const timer = setInterval(() => setClockNow(Date.now()), 1000);
    return () => {
      window.removeEventListener("tidal-session-expired", expired);
      window.removeEventListener("hashchange", hash);
      clearInterval(timer);
    };
  }, []);
  const health = useAPI<Health>(
    authenticated && !fixture ? "health" : null,
    15000,
  );
  useEffect(() => {
    if (!authenticated || fixture) return;
    let active = true;
    let socket: WebSocket | undefined;
    let reconnect: ReturnType<typeof setTimeout>;
    const abort = new AbortController();
    const query = `asset=${asset}&step=${step}&range=${span}&minAge=${minAge}`;
    setFrame(null);
    setError("");
    const load = () =>
      api<Frame>("overview?" + query, { signal: abort.signal })
        .then((d) => {
          if (active) {
            setFrame(d);
            setError("");
          }
        })
        .catch((e) => {
          if (active && e.name !== "AbortError") setError(e.message);
        });
    const connect = () => {
      socket = new WebSocket(
        `${location.protocol === "https:" ? "wss:" : "ws:"}//${location.host}/api/v2/stream?${query}`,
      );
      socket.onopen = () => {
        if (active) setConnected(true);
      };
      socket.onmessage = (ev) => {
        if (active) {
          try {
            setFrame(JSON.parse(ev.data));
            setError("");
          } catch {
            setError("实时消息解析失败");
          }
        }
      };
      socket.onclose = () => {
        if (active) {
          setConnected(false);
          reconnect = setTimeout(connect, 3000);
        }
      };
      socket.onerror = () => socket?.close();
    };
    load();
    connect();
    const fallback = setInterval(() => {
      if (!socket || socket.readyState !== WebSocket.OPEN) load();
    }, 5000);
    return () => {
      active = false;
      abort.abort();
      clearTimeout(reconnect);
      clearInterval(fallback);
      socket?.close();
    };
  }, [authenticated, asset, step, span, minAge]);
  const zones = frame?.zones ?? [];
  const selected =
    zones.find((z) => `${z.side}/${z.price}` === selectedKey) ??
    [...zones]
      .filter((z) => z.side === "bid")
      .sort((a, b) => b.usdCents - a.usdCents)[0] ??
    zones[0];
  useEffect(() => {
    if (
      selected &&
      !zones.some((z) => `${z.side}/${z.price}` === selectedKey)
    ) {
      setSelectedKey(`${selected.side}/${selected.price}`);
    }
  }, [zones, selected, selectedKey]);
  const history = useAPI<History>(
    authenticated &&
      !fixture &&
      selected &&
      (view === "liquidity" || view === "overview")
      ? `history?asset=${asset}&hours=${hours}&price=${selected.price}&step=${step}&range=${span}${historyMode ? "&heatmap=1" : ""}`
      : null,
    30000,
  );
  const peak = Math.max(1, ...zones.map((z) => z.usdCents));
  const axisRef = useRef({ asset, step, max: peak, at: Date.now() });
  if (
    axisRef.current.asset !== asset ||
    axisRef.current.step !== step ||
    peak > axisRef.current.max ||
    Date.now() - axisRef.current.at > 15000
  ) {
    const unit = Math.pow(10, Math.floor(Math.log10(peak)));
    axisRef.current = {
      asset,
      step,
      max: Math.ceil(peak / unit) * unit,
      at: Date.now(),
    };
  }
  const scale = fixture ? 12800000000 : axisRef.current.max;
  const visible = useMemo(() => {
    if (expanded) return zones;
    const important = ["ask", "bid"].flatMap((side) =>
      zones
        .filter((z) => z.side === side)
        .sort((a, b) => b.usdCents - a.usdCents)
        .slice(0, 4),
    );
    if (selected && !important.includes(selected)) {
      const replace = important.map((z) => z.side).lastIndexOf(selected.side);
      if (replace >= 0) important[replace] = selected;
      else important.push(selected);
    }
    return important.sort((a, b) => b.price - a.price);
  }, [zones, expanded, selected]);
  const go = (name: string) => {
    location.hash = name;
    setView(name);
  };
  const changeAsset = (a: Asset) => {
    setAsset(a);
    setFrame(null);
    setSelectedKey("");
    setStep(a === "BTC" ? 100 : 5);
    setHistoryMode(false);
  };
  const valid = frame?.coverage.filter((c) => c.valid) ?? [];
  const validVenues = new Set(valid.map((c) => c.venue));
  const stale =
    !fixture &&
    !!frame &&
    (clockNow - new Date(frame.at).getTime() > 15000 ||
      frame.priceValid === false);
  if (authenticated === null)
    return (
      <div className="boot">
        <span className="wordmark">TIDAL</span>
        <p>正在连接情报台…</p>
      </div>
    );
  if (!authenticated) return <Login onLogin={() => setAuthenticated(true)} />;
  return (
    <>
      <header className="topbar">
        <a className="wordmark" href="#liquidity" aria-label="Tidal 首页">
          TIDAL<span className="brand-caption">潮汐</span>
        </a>
        <nav aria-label="主导航">
          {nav.map(([id, label]) => (
            <button
              key={id}
              onClick={() => go(id)}
              className={view === id ? "active" : ""}
            >
              {label}
            </button>
          ))}
        </nav>
        <div className="top-status">
          <span
            className={`status-dot ${validVenues.size < 5 || stale ? "warn" : ""}`}
          />
          <span>{stale ? "数据已过期" : `${validVenues.size}/5 家现货`}</span>
          <span className="desktop-only">USD 实时换算</span>
          <time>
            {new Date(clockNow).toLocaleDateString("zh-CN", {
              month: "2-digit",
              day: "2-digit",
            })}{" "}
            {clock(clockNow / 1000)}
          </time>
          <button
            className="icon-button"
            aria-label="退出登录"
            onClick={() =>
              api("logout", { method: "POST" }).then(() =>
                setAuthenticated(false),
              )
            }
          >
            <SignOut size={18} />
          </button>
        </div>
      </header>
      <main>
        <section className="headline">
          <div>
            <div className="headline-row">
              <h1>
                {(
                  {
                    liquidity: "现货买卖墙",
                    overview: "市场总览",
                    flow: "成交证据",
                    activity: "大资金动向",
                    derivatives: "合约态势",
                    whales: "公开巨鲸持仓",
                    liquidations: "清算集中在哪里？",
                    health: "数据与运行健康",
                  } as Record<string, string>
                )[view] ?? "现货买卖墙"}
              </h1>
              <span className="asset-label">{asset} / 美元</span>
              <strong className="headline-price">
                {frame?.price ? "$" + price(frame.price) : "等待行情"}
              </strong>
              <div className="segmented asset-switch">
                {(["BTC", "ETH"] as Asset[]).map((a) => (
                  <button
                    key={a}
                    className={asset === a ? "selected" : ""}
                    onClick={() => changeAsset(a)}
                  >
                    {a}
                  </button>
                ))}
              </div>
              {fixture && (
                <span className="demo-badge">演示数据 · 仅本地预览</span>
              )}
            </div>
            <p className="subtitle">
              {view === "liquidity"
                ? "柱子越长，当前挂单金额越大"
                : view === "whales"
                  ? "看清已监控大仓位的均价与动态清算位置"
                  : view === "flow"
                    ? "主动买卖与真实成交，验证挂单是否得到响应"
                    : view === "derivatives"
                      ? "OI、资金费率与已发生清算，提供合约背景"
                      : view === "health"
                        ? "覆盖、延迟与存储预算，一处查看"
                        : "先观察现货，再结合成交与合约背景"}
            </p>
          </div>
        </section>
        {(error || stale) && (
          <div className="notice danger">
            <WarningCircle size={18} />
            {error || "实时连接已中断，当前数字为最后一次有效观察。"}
          </div>
        )}
        {!fixture && frame && valid.length < frame.coverage.length && (
          <div className="notice">
            <WarningCircle size={17} />
            {valid.length}/{frame.coverage.length}{" "}
            个现货盘口有效；汇总仅含有效来源。
            <button onClick={() => go("health")}>查看覆盖</button>
          </div>
        )}
        {view === "liquidity" || view === "overview" ? (
          <>
            {view === "overview" && !fixture && (
              <OverviewSummary asset={asset} onNavigate={go} />
            )}
            <div className="liquidity-layout">
              <section className="book-area">
                <div className="toolbar">
                  <label>
                    价格精度{" "}
                    <select
                      value={step}
                      onChange={(e) => setStep(+e.target.value)}
                    >
                      {(asset === "BTC"
                        ? [25, 100, 250, 500, 1000, 2500]
                        : [1, 5, 10, 25, 50, 100]
                      ).map((s) => (
                        <option key={s} value={s}>
                          ${s}
                        </option>
                      ))}
                    </select>
                  </label>
                  <label>
                    价格范围{" "}
                    <select
                      value={span}
                      onChange={(e) => setSpan(+e.target.value)}
                    >
                      {[0.5, 1, 2, 5, 10, 1000].map((n) => (
                        <option key={n} value={n}>
                          {n === 1000
                            ? asset === "BTC"
                              ? "$1万–$20万全景"
                              : "全部已覆盖范围"
                            : `±${n}%`}
                        </option>
                      ))}
                    </select>
                  </label>
                  <label>
                    大额状态最低持续时间{" "}
                    <select
                      value={minAge}
                      onChange={(e) => setMinAge(+e.target.value)}
                    >
                      {[
                        [0, "全部时长"],
                        [60, "≥1分钟"],
                        [300, "≥5分钟"],
                        [900, "≥15分钟"],
                        [3600, "≥1小时"],
                        [86400, "≥1天"],
                      ].map(([v, t]) => (
                        <option key={v} value={v}>
                          {t}
                        </option>
                      ))}
                    </select>
                  </label>
                  <span className="toolbar-caption">
                    {expanded ? "全部价位" : "重点价位"} · 每档{step}美元
                  </span>
                  <button
                    className="text-button"
                    onClick={() => setExpanded((x) => !x)}
                  >
                    {expanded ? "收起" : "展开全部"}
                  </button>
                </div>
                <div className="range-summary" aria-label="所选价格范围汇总">
                  <div>
                    <span>所选范围已覆盖买单</span>
                    <strong className="buy">
                      {frame?.summary?.hasData
                        ? amount(frame.summary.bidCents)
                        : "—"}
                      <small> USD</small>
                    </strong>
                  </div>
                  <div>
                    <span>所选范围已覆盖卖单</span>
                    <strong className="sell">
                      {frame?.summary?.hasData
                        ? amount(frame.summary.askCents)
                        : "—"}
                      <small> USD</small>
                    </strong>
                  </div>
                </div>
                <p className="book-definition">
                  当前挂单存量 · 每档{step}美元 · 1分钟粒度 · 约2分钟采集
                  <br />
                  {frame?.summary?.oldestSourceAt
                    ? `来源快照 ${clock(frame.summary.oldestSourceAt)}–${clock(frame.summary.newestSourceAt!)}（北京时间）`
                    : "等待有效来源快照"}{" "}
                  · 上游返回的盘口切片，非全量实时L2
                  <br />
                  总额覆盖所选价格范围内全部已返回价位，不随重点列表或持续筛选变化。
                  {expanded
                    ? "已展开当前筛选价位。"
                    : "下方仅展示部分重点价位。"}
                </p>
                <div className="book-table" aria-label="按价格排列的现货挂单">
                  <div className="book-grid table-heading">
                    <span>
                      价格区间 <small>(USD)</small>
                    </span>
                    <DollarAxis scale={scale} />
                    <span>本档挂单金额</span>
                    <span>大额持续</span>
                    <span>成交证据</span>
                  </div>
                  <h2 className="side-label sell">上方卖单 · 阻力候选</h2>
                  <ZoneRows
                    currentPrice={frame?.price ?? 0}
                    rows={visible.filter((z) => z.side === "ask")}
                    selected={selected}
                    scale={scale}
                    onSelect={(z) => setSelectedKey(`${z.side}/${z.price}`)}
                  />
                  <div className="current-price">
                    <strong>
                      当前价格　{frame?.price ? "$" + price(frame.price) : "—"}
                    </strong>
                    <span />
                    <small>
                      {connected ? "价格实时 · 盘口定时快照" : "本地缓存"}
                    </small>
                  </div>
                  <h2 className="side-label buy">下方买单 · 支撑候选</h2>
                  <ZoneRows
                    currentPrice={frame?.price ?? 0}
                    rows={visible.filter((z) => z.side === "bid")}
                    selected={selected}
                    scale={scale}
                    onSelect={(z) => setSelectedKey(`${z.side}/${z.price}`)}
                  />
                </div>
                {zones.length === 0 && (
                  <Empty
                    text={
                      frame?.price
                        ? "当前筛选下暂无价位。刚开始采集时，可把持续时间改为“全部时长”。"
                        : "正在建立有效盘口，请稍候。"
                    }
                  />
                )}
                <div className="book-footer">
                  <span>买卖双方共用金额比例尺</span>
                  <span>
                    {expanded ? zones.length : visible.length} / {zones.length}{" "}
                    档 · 盘口来源{" "}
                    {frame?.coverage
                      .filter((c) => c.valid && c.observedAt)
                      .map((c) => c.observedAt)
                      .sort()
                      .at(-1)
                      ? clock(
                          frame.coverage
                            .filter((c) => c.valid && c.observedAt)
                            .map((c) => c.observedAt)
                            .sort()
                            .at(-1)!,
                        )
                      : "—"}
                  </span>
                </div>
              </section>
              <aside className="inspector">
                {selected ? (
                  <>
                    <h2>所选价位</h2>
                    <strong
                      className={`selected-price ${selected.side === "bid" ? "buy" : "sell"}`}
                    >
                      ${price(selected.price, 0)}{" "}
                      <span>
                        – {price(selected.price + selected.step - 1, 0)}
                      </span>
                    </strong>
                    <strong className="selected-amount">
                      {amount(selected.usdCents)} <small>USD</small>
                    </strong>
                    <dl className="evidence">
                      <div>
                        <dt>金额</dt>
                        <dd>
                          {selected.grade}{" "}
                          <small>
                            {selected.percentile != null
                              ? `约P${selected.percentile.toFixed(0)} · ${selected.samples}个同类价位样本`
                              : "需7天、500个同类有效样本"}
                          </small>
                        </dd>
                      </div>
                      <div>
                        <dt>持续</dt>
                        <dd>
                          {selected.seconds > 0
                            ? age(selected.seconds)
                            : "待积累"}
                          {!fixture && (
                            <small>
                              近30分钟大额出现{" "}
                              {(selected.occupancy * 100).toFixed(0)}%
                            </small>
                          )}
                        </dd>
                      </div>
                      <div>
                        <dt>成交</dt>
                        <dd>{selected.evidence}</dd>
                      </div>
                    </dl>
                    <div className="sources">
                      <h3>
                        来源分布{" "}
                        <small>
                          （{Object.keys(selected.sources).length}家现货）
                        </small>
                      </h3>
                      {Object.entries(selected.sources)
                        .sort((a, b) => b[1] - a[1])
                        .map(([v, n]) => (
                          <div className="source-row" key={v}>
                            <span>{venue(v)}</span>
                            <meter min={0} max={selected.usdCents} value={n} />
                            <span>
                              {((n / selected.usdCents) * 100).toFixed(0)}%
                            </span>
                          </div>
                        ))}
                    </div>
                    <details className="source-details">
                      <summary>核对各所金额、报价与时间</summary>
                      {(frame?.coverage ?? []).map((c) => {
                        const n = selected.sources[c.venue.toLowerCase()];
                        const inRange = selected.covered?.includes(
                          c.venue.toLowerCase(),
                        );
                        return (
                          <div key={c.venue}>
                            <strong>
                              {c.venue} · {n != null ? amount(n) + " USD" : "—"}
                            </strong>
                            <span>
                              {!c.valid
                                ? c.reason || "数据不可用"
                                : n != null
                                  ? "本档有返回金额"
                                  : inRange
                                    ? "处于返回范围内，本档未返回记录"
                                    : "本档未覆盖"}
                            </span>
                            <span>
                              {c.symbol} · {c.quote}兑USD {c.rate || "不可用"}
                            </span>
                            <span>
                              来源{" "}
                              {c.observedAt ? clock(c.observedAt) : "时间未知"}{" "}
                              · 获取 {c.fetchedAt ? clock(c.fetchedAt) : "未知"}
                              {c.fxAt ? ` · 汇率 ${clock(c.fxAt)}` : ""}
                            </span>
                          </div>
                        );
                      })}
                    </details>
                    <button className="primary full" onClick={() => go("flow")}>
                      查看成交明细 <ArrowRight size={19} />
                    </button>
                    <p className="helper">
                      挂单金额、持续时间、成交证据分别判断。疑似撤走不等于已确认撤单。
                      {selected.sampled &&
                        " 盘口约2分钟更新，持续指价位采样稳定程度。"}
                    </p>
                  </>
                ) : (
                  <Empty text="选择一个有效价格区间，查看来源与成交证据。" />
                )}
              </aside>
            </div>
            {!fixture && <LargeOrders asset={asset} panorama={span === 1000} />}
            <section className="wall-history">
              <div className="section-heading">
                <h2>
                  {historyMode ? "历史挂单热力图" : "这堵墙是怎样变化的？"}
                </h2>
                <span>
                  {selected
                    ? `${price(selected.price, 0)}–${price(selected.price + selected.step - 1, 0)}`
                    : "—"}
                  　挂单金额
                </span>
                <button
                  className="text-button push-right"
                  onClick={() => setAnnotationOpen(true)}
                >
                  标注开仓 / 止盈 / 止损
                </button>
                <label>
                  回看时间{" "}
                  <select
                    value={hours}
                    onChange={(e) => setHours(+e.target.value)}
                  >
                    {[1, 4, 24, 24 * 7, 24 * 30, 24 * 90].map((h) => (
                      <option value={h} key={h}>
                        {h < 24 ? h + "小时" : h / 24 + "天"}
                      </option>
                    ))}
                  </select>
                </label>
                <button
                  className="secondary"
                  onClick={() => setHistoryMode((x) => !x)}
                >
                  {historyMode ? "返回金额变化" : "查看历史热力图"}{" "}
                  <ArrowRight size={16} />
                </button>
              </div>
              <HistoryChart
                history={history.data}
                heat={historyMode}
                fixture={fixture}
              />
              {history.error && <p className="helper sell">{history.error}</p>}
              <p className="helper">
                {fixture
                  ? "演示数据；上线后从实际采集时刻开始形成历史。"
                  : `${history.data?.note ?? "空白表示未采集或未覆盖。"} 实际精度：${history.data?.resolution ?? "等待数据"}`}
              </p>
            </section>
          </>
        ) : fixture ? (
          <Empty text="这是本地布局演示。退出演示参数并登录后查看实时数据页面。" />
        ) : (
          <MarketPages
            view={view}
            asset={asset}
            frame={frame}
            health={health.data}
            onNavigate={go}
          />
        )}
        <footer className="page-footer">
          <span>未覆盖的价格范围显示“未覆盖” · 挂单可能随时撤走</span>
          <a
            href="https://github.com/linlea666/tidal-crypto-intelligence"
            target="_blank"
            rel="noreferrer"
          >
            TIDAL {health.data?.version ?? "V2"} <ArrowSquareOut size={13} />
          </a>
        </footer>
      </main>
      {annotationOpen && (
        <AnnotationModal
          asset={asset}
          initial={frame?.price ?? 0}
          onClose={() => setAnnotationOpen(false)}
        />
      )}
    </>
  );
}
function ZoneRows({
  currentPrice,
  rows,
  scale,
  selected,
  onSelect,
}: {
  currentPrice: number;
  rows: Zone[];
  scale: number;
  selected?: Zone;
  onSelect: (z: Zone) => void;
}) {
  return (
    <>
      {rows.map((z) => (
        <button
          className={`book-grid zone-row ${z.side} ${selected?.price === z.price && selected.side === z.side ? "is-selected" : ""}`}
          key={`${z.side}/${z.price}`}
          onClick={() => onSelect(z)}
          title={`价格 ≥ ${z.price} 且 < ${z.price + z.step}；${z.evidence}；${z.updatedAt ? clock(z.updatedAt) : "未知时间"}来源快照`}
          aria-pressed={selected?.price === z.price && selected.side === z.side}
        >
          <span className="row-price">
            ${price(z.price, 0)} <span>– {price(z.price + z.step - 1, 0)}</span>
            <small className="row-distance">
              {currentPrice > 0
                ? ((z.price / currentPrice - 1) * 100).toFixed(2) + "%"
                : "—"}
            </small>
          </span>
          <span className="bar-track">
            <meter
              aria-label={`${z.side === "bid" ? "买单" : "卖单"} ${amount(z.usdCents)}美元`}
              min={0}
              max={scale}
              value={z.usdCents}
            />
          </span>
          <strong>
            {amount(z.usdCents)}
            <small className={`grade-badge ${z.strong ? "strong" : ""}`}>
              {z.grade} · {Object.keys(z.sources).length}家贡献
            </small>
          </strong>
          <span>{z.seconds > 0 ? age(z.seconds) : "待积累"}</span>
          <span className="row-evidence">{z.evidence}</span>
        </button>
      ))}
    </>
  );
}
export function Empty({ text }: { text: string }) {
  return (
    <div className="empty">
      <ChartBar size={28} weight="light" />
      <p>{text}</p>
    </div>
  );
}
function Login({ onLogin }: { onLogin: () => void }) {
  const [password, setPassword] = useState("");
  const [err, setErr] = useState("");
  const [busy, setBusy] = useState(false);
  return (
    <div className="login-page">
      <div className="login-panel">
        <span className="wordmark">
          TIDAL <small>潮汐</small>
        </span>
        <h1>看清每一层流动性</h1>
        <p>现货挂单、成交证据与公开巨鲸持仓。</p>
        <form
          onSubmit={async (e) => {
            e.preventDefault();
            setBusy(true);
            try {
              await api("login", {
                method: "POST",
                body: JSON.stringify({ password }),
              });
              onLogin();
            } catch (e) {
              setErr((e as Error).message);
            } finally {
              setBusy(false);
            }
          }}
        >
          <label htmlFor="password">看板访问密码</label>
          <input
            id="password"
            type="password"
            autoComplete="current-password"
            autoFocus
            required
            value={password}
            onChange={(e) => setPassword(e.target.value)}
          />
          {err && (
            <p className="sell" role="alert">
              {err}
            </p>
          )}
          <button className="primary full" disabled={busy}>
            {busy ? <ArrowsClockwise size={19} /> : <ShieldCheck size={19} />}{" "}
            {busy ? "正在登录…" : "进入情报台"}
          </button>
        </form>
        <small>个人数据看板 · 仅用于观察市场</small>
      </div>
    </div>
  );
}
function HistoryChart({
  history,
  heat,
  fixture,
}: {
  history: History | null;
  heat: boolean;
  fixture: boolean;
}) {
  const option = useMemo(() => {
    if (fixture)
      return {
        grid: { left: 75, right: 30, top: 18, bottom: 35 },
        xAxis: {
          type: "category",
          data: [
            "12:45",
            "12:50",
            "12:55",
            "13:00",
            "13:05",
            "13:10",
            "13:15",
            "13:20",
            "13:27",
          ],
          boundaryGap: false,
          axisLine: { lineStyle: { color: "#43504d" } },
        },
        yAxis: {
          type: "value",
          axisLabel: { formatter: (v: number) => amount(v) },
          splitLine: { lineStyle: { color: "#27312f" } },
        },
        tooltip: {
          trigger: "axis",
          valueFormatter: (v: number) => amount(v) + " USD",
        },
        series: [
          {
            type: "line",
            step: "end",
            symbol: "none",
            lineStyle: { color: "#7deba9", width: 2 },
            areaStyle: { color: "#7deba9", opacity: 0.06 },
            data: [3200, 3600, 5400, 6400, 6400, 8000, 8000, 12800, 12800].map(
              (n) => n * 1000000,
            ),
          },
        ],
      };
    if (!history?.points.length) return null;
    const points = history.points;
    if (heat) {
      const prices = [
        ...new Set(points.flatMap((p) => (p.zones ?? []).map((z) => z.price))),
      ].sort((a, b) => a - b);
      const data = points.flatMap((p, x) =>
        (p.zones ?? []).map((z) => [
          x,
          prices.indexOf(z.price),
          z.usdCents,
          z.side,
        ]),
      );
      const max = Math.max(1, ...data.map((d) => Number(d[2])));
      return {
        grid: { left: 80, right: 75, top: 15, bottom: 40 },
        xAxis: {
          type: "category",
          data: points.map((p) => clock(p.time)),
          axisLabel: { interval: "auto" },
        },
        yAxis: {
          type: "category",
          data: prices.map((p) => price(p, 0)),
          axisLabel: { interval: "auto" },
        },
        tooltip: {
          formatter: (p: { data: number[] }) =>
            `${prices[p.data[1]]}: ${amount(p.data[2])} USD`,
        },
        visualMap: {
          min: 0,
          max,
          orient: "vertical",
          right: 0,
          text: ["大", "小"],
          inRange: { color: ["#121b19", "#234a3a", "#79e9a5"] },
          formatter: (v: number) => amount(v),
        },
        series: [
          {
            type: "heatmap",
            data: data.map((d) => d.slice(0, 3)),
            progressive: 5000,
          },
        ],
      };
    }
    return {
      grid: { left: 78, right: 24, top: 15, bottom: 35 },
      xAxis: {
        type: "time",
        axisLabel: { formatter: (t: number) => clock(t / 1000) },
        splitLine: { show: false },
      },
      yAxis: {
        type: "value",
        axisLabel: { formatter: (v: number) => amount(v) },
        splitLine: { lineStyle: { color: "#27312f" } },
      },
      tooltip: {
        trigger: "axis",
        valueFormatter: (v: number) => amount(v) + " USD",
      },
      series: [
        {
          type: "line",
          step: "end",
          symbol: "none",
          connectNulls: false,
          lineStyle: { color: "#7deba9", width: 2 },
          areaStyle: { color: "#7deba9", opacity: 0.05 },
          data: points.map((p) => [p.time * 1000, p.usdCents ?? null]),
        },
      ],
    };
  }, [history, heat, fixture]);
  return option ? (
    <Chart
      label={heat ? "历史挂单金额热力图" : "所选价位历史挂单金额"}
      height={heat ? 370 : 125}
      option={option}
    />
  ) : (
    <Empty text="历史正在积累，采集后会显示这堵墙的金额变化。" />
  );
}
function AnnotationModal({
  asset,
  initial,
  onClose,
}: {
  asset: Asset;
  initial: number;
  onClose: () => void;
}) {
  const [entry, setEntry] = useState(initial.toFixed(2));
  const [stop, setStop] = useState("");
  const [target, setTarget] = useState("");
  const [side, setSide] = useState("long");
  const [msg, setMsg] = useState("");
  const risk = Math.abs(+entry - +stop),
    reward = Math.abs(+target - +entry);
  const modal = useRef<HTMLElement>(null);
  useEffect(() => {
    const previous = document.activeElement as HTMLElement | null;
    modal.current?.querySelector<HTMLButtonElement>("button")?.focus();
    return () => previous?.focus();
  }, []);
  return (
    <div className="modal-shade" onClick={onClose}>
      <section
        className="modal"
        ref={modal}
        role="dialog"
        aria-modal="true"
        aria-labelledby="annotation-title"
        onClick={(e) => e.stopPropagation()}
        onKeyDown={(e) => {
          if (e.key === "Escape") {
            e.preventDefault();
            onClose();
          }
          if (e.key !== "Tab") return;
          const items = modal.current?.querySelectorAll<HTMLElement>(
            "button, input, select, [tabindex='0']",
          );
          if (!items?.length) return;
          const first = items[0],
            last = items[items.length - 1];
          if (e.shiftKey && document.activeElement === first) {
            e.preventDefault();
            last.focus();
          } else if (!e.shiftKey && document.activeElement === last) {
            e.preventDefault();
            first.focus();
          }
        }}
      >
        <button
          className="modal-close icon-button"
          aria-label="关闭"
          onClick={onClose}
        >
          <X size={22} />
        </button>
        <h2 id="annotation-title">手工价格标注 · {asset}</h2>
        <p className="helper">
          记录自己的观察计划，不会提交交易订单。价格以美元计；实际执行需核对交易平台报价。
        </p>
        <form
          onSubmit={async (e) => {
            e.preventDefault();
            try {
              await api("annotations", {
                method: "POST",
                body: JSON.stringify({
                  asset,
                  entry: +entry,
                  stop: +stop,
                  target: +target,
                  side,
                }),
              });
              setMsg("标注已保存");
            } catch (e) {
              setMsg((e as Error).message);
            }
          }}
        >
          <label>
            方向
            <select value={side} onChange={(e) => setSide(e.target.value)}>
              <option value="long">多头</option>
              <option value="short">空头</option>
            </select>
          </label>
          {[
            ["开仓", entry, setEntry],
            ["止损", stop, setStop],
            ["止盈", target, setTarget],
          ].map(([label, value, setter]) => (
            <label key={String(label)}>
              {String(label)}
              <input
                type="number"
                step="any"
                min="0"
                required
                value={String(value)}
                onChange={(e) =>
                  (setter as (v: string) => void)(e.target.value)
                }
              />
            </label>
          ))}
          <p>
            盈亏比{" "}
            <strong>
              {risk > 0 && target && stop
                ? (reward / risk).toFixed(2) + " : 1"
                : "—"}
            </strong>
          </p>
          <button className="primary full">保存标注</button>
          {msg && <p role="status">{msg}</p>}
        </form>
      </section>
    </div>
  );
}

function DollarAxis({ scale }: { scale: number }) {
  const ref = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(0);
  useEffect(() => {
    if (!ref.current) return;
    const ro = new ResizeObserver(([e]) => setWidth(e.contentRect.width));
    ro.observe(ref.current);
    return () => ro.disconnect();
  }, []);
  const count = width >= 350 ? 5 : width >= 200 ? 3 : 2;
  return (
    <div ref={ref} className="axis-labels dollar-axis">
      {Array.from({ length: count }, (_, i) => {
        const r = i / (count - 1);
        return (
          <span
            key={i}
            style={{
              left: `${r * 100}%`,
              transform:
                i === 0
                  ? "none"
                  : i === count - 1
                    ? "translateX(-100%)"
                    : "translateX(-50%)",
            }}
          >
            {r === 0 ? "0" : amount(scale * r)}
          </span>
        );
      })}
    </div>
  );
}
