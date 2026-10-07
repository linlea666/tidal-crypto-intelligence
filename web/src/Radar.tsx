import { useState } from "react";
import { api, amount, useAPI } from "./data";
import "./radar.css";

type Event = {
  id: string;
  address: string;
  asset: string;
  side: string;
  level: string;
  age: string;
  positionOpenedAt: string;
  detectedAt: string;
  verifiedAt: string;
  dataThrough: string;
  thresholdAt: string | null;
  closedAt: string | null;
  size: string;
  openedNotionalUSDC: string;
  notionalUSDC: string;
  usdCents: number | null;
  entry: string;
  liquidation: string | null;
  configuredLeverage: number;
  effectiveLeverage: string | null;
  concentration: string | null;
  depositCents: number | null;
  rapidFunding: boolean;
  concentrated: boolean;
  historyComplete: boolean;
  verified: boolean;
  liveOpening: boolean;
  groupId?: string;
  members: string[];
  reasons: string[];
  context: string[];
  fx: string;
  fxAt: string | null;
};
type Settings = {
  emailEnabled: boolean;
  configured: boolean;
  receiptVerified: boolean;
  offline: boolean;
  limitPerHour: number;
  totalLimitPerHour: number;
  existingLimitPerHour: number;
};
type Notice = {
  id: string;
  eventId: string;
  kind: string;
  status: string;
  attemptedAt: string | null;
  completedAt: string | null;
  error: string;
};
type Health = {
  offline: boolean;
  storageBytes: number;
  storageBudget: number;
  storagePaused: boolean;
  note: string;
  health: {
    gapFrom: string | null;
    gapThrough: string | null;
    connected: boolean;
    lastMessage: string | null;
    lastVerified: string | null;
    lastError: string;
    lastErrorAt: string | null;
    gaps: number;
    dropped: number;
    queued: number;
    candidates: number;
    weightLastMinute: number;
    websocketUsers: number;
  };
};
type Response = {
  events: Event[];
  next: string;
  notifications: Notice[];
  settings: Settings;
  status: Health;
  available?: boolean;
  error?: string;
};
type Wallet = {
  wallet: {
    firstSeenLocal: string;
    earliestActivityObserved: string | null;
    historyFrom: string;
    historyThrough: string;
    historyComplete: boolean;
    historyNote: string;
    role: string;
    master?: string;
    ledger: {
      time: number;
      hash: string;
      delta: { type: string; usdc: string; user: string; destination: string };
    }[];
  } | null;
  fills: {
    coin: string;
    time: number;
    side: string;
    px: string;
    sz: string;
    startPosition: string;
    tid: number;
  }[];
  note?: string;
};
type Outcome = {
  minutes: number;
  state: string;
  returnPercent: string | null;
  mfePercent: string | null;
  maePercent: string | null;
  observedBars: number;
  expectedBars: number;
};
type Metric = {
  minutes: number;
  population: string;
  events: number;
  complete: number;
  missing: number;
  pending: number;
  meanReturnPercent: string | null;
  meanMaePercent: string | null;
};
type Study = {
  truncated: boolean;
  metrics: Metric[];
  origin: string;
  days: number;
  independentEvents: number;
  completeEvents: number;
  reviewReady: boolean;
  note: string;
  items: {
    id: string;
    asset: string;
    side: string;
    at: string;
    alert: boolean;
    groupId: string;
    outcomes: Outcome[];
  }[];
};
const stamp = (s: string | number | null | undefined) =>
  s
    ? new Date(s).toLocaleString("zh-CN", {
        timeZone: "Asia/Shanghai",
        hour12: false,
      })
    : "未知";
const usd = (v: number | null | undefined) =>
  v == null ? "未知" : `$${amount(v)}`;
const num = (v: string | null | undefined, suffix = "") =>
  v == null || v === ""
    ? "未知"
    : `${Number(v).toLocaleString("zh-CN", { maximumFractionDigits: 4 })}${suffix}`;
const ages: Record<string, string> = {
  recent: "查询范围内新近活跃",
  unknown: "新发现 · 年龄未核实",
  established: "已观察到较早活动",
};
const levels: Record<string, string> = {
  candidate: "候选观察",
  opening: "百万级建仓",
  priority: "重点异常",
};
const mails: Record<string, string> = {
  pending: "排队中",
  sent: "SMTP 已接受",
  sending: "提交中",
  delivery_unknown: "结果不确定 · 不重发",
  unknown_after_restart: "重启后结果未知",
  suppressed_restart: "重启积压未补发",
  suppressed_expired: "超过5分钟未发送",
  disabled: "邮件关闭",
  unconfigured: "收件验收未完成",
  suppressed_closed: "发送前已平仓",
  failed_before_submission: "提交前失败",
  rejected: "服务器拒绝",
};
export function RadarPage() {
  const [asset, setAsset] = useState("");
  const [side, setSide] = useState("");
  const [cursor, setCursor] = useState("");
  const [past, setPast] = useState<string[]>([]);
  const [selected, setSelected] = useState("");
  const [message, setMessage] = useState("");
  const [saving, setSaving] = useState(false);
  const [override, setOverride] = useState<Settings | null>(null);
  const [studyOpen, setStudyOpen] = useState(false);
  const [receiptCode, setReceiptCode] = useState("");
  const q = useAPI<Response>(
    `hl-radar/events?asset=${asset}&side=${side}&limit=25&before=${encodeURIComponent(cursor)}`,
    10000,
  );
  const d = q.data;
  const wallet = useAPI<Wallet>(
    selected ? `hl-radar/wallet?address=${selected}` : null,
    15000,
  );
  const study = useAPI<Study>(studyOpen ? "hl-radar/study" : null, 60000);
  const settings = override ?? d?.settings;
  const status = d?.status;
  const health = status?.health;
  const reset = () => {
    setCursor("");
    setPast([]);
    setSelected("");
  };
  async function acceptMail(action: "test" | "confirm") {
    setSaving(true);
    setMessage("");
    try {
      if (action === "test") {
        const result = await api<{ message: string }>("hl-radar/mail-test", {
          method: "POST",
          body: JSON.stringify({ action }),
        });
        setMessage(result.message);
      } else {
        const result = await api<Settings>("hl-radar/mail-test", {
          method: "POST",
          body: JSON.stringify({ action, code: receiptCode }),
        });
        setOverride(result);
        setReceiptCode("");
        setMessage("真实收件已确认，可以开启雷达邮件");
      }
    } catch (e) {
      setMessage((e as Error).message);
    } finally {
      setSaving(false);
    }
  }
  async function toggle() {
    if (!settings) return;
    setSaving(true);
    setMessage("");
    try {
      const s = await api<Settings>("hl-radar/settings", {
        method: "PUT",
        body: JSON.stringify({ emailEnabled: !settings.emailEnabled }),
      });
      setOverride(s);
      setMessage(
        s.emailEnabled
          ? "已启用；仅发送启用后新事件"
          : "已关闭，旧事件不会补发",
      );
    } catch (e) {
      setMessage((e as Error).message);
    } finally {
      setSaving(false);
    }
  }
  return (
    <div className="radar-page">
      <div className="radar-intro">
        <span>HYPERLIQUID · BTC / ETH 原生永续</span>
        <p>
          先核验新建仓，再检查入金、账户敞口与同步行为。异常程度与证据完整度分别展示。
        </p>
      </div>
      {(q.error || d?.error) && (
        <p role="alert" className="sell">
          {q.error || d?.error}
        </p>
      )}
      <section className="radar-summary" aria-label="雷达状态">
        <div>
          <small>公开成交发现</small>
          <strong>
            {status?.offline
              ? "离线验收"
              : health?.connected
                ? "已连接"
                : "连接未确认"}
          </strong>
          <span>最近成交 {stamp(health?.lastMessage)}</span>
        </div>
        <div>
          <small>核验队列</small>
          <strong>{health ? `${health.queued} / 128` : "—"}</strong>
          <span>
            {health?.candidates ?? "—"} 个候选 · 最近核验{" "}
            {stamp(health?.lastVerified)}
          </span>
        </div>
        <div>
          <small>邮件</small>
          <strong>{settings?.emailEnabled ? "已启用" : "关闭"}</strong>
          <span>雷达 6 次／小时 · 全站 12 次</span>
        </div>
      </section>
      <section className="radar-controls" aria-label="事件筛选">
        <label>
          币种{" "}
          <select
            value={asset}
            onChange={(e) => {
              setAsset(e.target.value);
              reset();
            }}
          >
            <option value="">BTC 与 ETH</option>
            <option>BTC</option>
            <option>ETH</option>
          </select>
        </label>
        <label>
          方向{" "}
          <select
            value={side}
            onChange={(e) => {
              setSide(e.target.value);
              reset();
            }}
          >
            <option value="">双向</option>
            <option value="long">多头</option>
            <option value="short">空头</option>
          </select>
        </label>
        <button
          disabled={
            saving ||
            !settings ||
            (!settings.emailEnabled &&
              (!settings.receiptVerified || settings.offline))
          }
          onClick={toggle}
        >
          {saving
            ? "保存中…"
            : settings?.emailEnabled
              ? "关闭雷达邮件"
              : "开启雷达邮件"}
        </button>
        {!settings?.receiptVerified && <small>真实收件验收待完成</small>}
        <span role="status">{message}</span>
      </section>
      {!settings?.receiptVerified && (
        <section className="radar-receipt" aria-label="收件验收">
          <p>
            先向已配置的收件箱发送测试，再输入邮件里的验收码。每次测试计入发送额度，不自动重发。
          </p>
          <div className="radar-controls">
            <button
              disabled={saving || !settings?.configured || settings.offline}
              onClick={() => acceptMail("test")}
            >
              发送收件测试
            </button>
            <label>
              收件验收码{" "}
              <input
                aria-label="收件验收码"
                autoComplete="off"
                value={receiptCode}
                maxLength={32}
                onChange={(e) => setReceiptCode(e.target.value)}
              />
            </label>
            <button
              disabled={
                saving ||
                receiptCode.trim().length !== 32 ||
                !settings?.configured ||
                settings.offline
              }
              onClick={() => acceptMail("confirm")}
            >
              确认实际收件
            </button>
          </div>
        </section>
      )}
      <p className="helper">
        100 万美元名义仓位起提醒；25
        万美元起核验。仅部分覆盖，不承诺零漏报。数据过期时历史金额保留，当前判断暂停。
      </p>
      {health?.lastError && (
        <details className="radar-health">
          <summary>
            采集缺口与限制 · 累计 {health.gaps} 次异常 / {health.dropped}{" "}
            次丢弃或拒绝
          </summary>
          <p>
            {health.lastError} · {stamp(health.lastErrorAt)}
          </p>
          {health.gapFrom && (
            <p>
              最近公开成交断流：{stamp(health.gapFrom)} —{" "}
              {health.gapThrough ? stamp(health.gapThrough) : "仍未恢复"}
              。恢复后核对已知活跃地址；未知地址覆盖不能追认。
            </p>
          )}
          <p>
            REST权重 {health.weightLastMinute}/600 · 用户订阅{" "}
            {health.websocketUsers}/8 · 存储{" "}
            {((status?.storageBytes ?? 0) / 1048576).toFixed(1)}/512 MiB
            {status?.storagePaused ? " · 容量保护" : ""}
          </p>
          <p>{status?.note}</p>
        </details>
      )}
      {!d && <p role="status">正在读取本地雷达记录…</p>}
      {d && !d.events.length && (
        <div className="radar-empty">
          <strong>尚无符合采集条件的已核验建仓</strong>
          <p>系统会从公开成交中发现地址。没有记录不代表市场没有大额建仓。</p>
        </div>
      )}
      <div className="radar-events">
        {d?.events.map((e) => {
          const fresh =
            e.verified &&
            Date.now() - Date.parse(e.dataThrough) <= 90000 &&
            !q.error;
          const notes = d.notifications.filter((n) => n.eventId === e.id);
          return (
            <article
              key={e.id}
              className={`radar-event ${fresh ? e.side : "radar-stale"}`}
            >
              <header>
                <div>
                  <span className={e.side === "long" ? "buy" : "sell"}>
                    {e.asset} · {e.side === "long" ? "多头" : "空头"}
                  </span>
                  <h2>{levels[e.level] ?? e.level}</h2>
                </div>
                <strong>{usd(e.usdCents)}</strong>
              </header>
              <p className="radar-address">{e.address}</p>
              <div className="radar-tags">
                <span>{ages[e.age] ?? e.age}</span>
                <span>
                  {e.historyComplete ? "请求范围无已知截断" : "历史证据不完整"}
                </span>
                <span>
                  {e.closedAt
                    ? "已核验平仓"
                    : fresh
                      ? "当前仓位已核验"
                      : "历史快照 · 当前未知"}
                </span>
              </div>
              <dl className="radar-numbers">
                <div>
                  <dt>入金窗口外部资金</dt>
                  <dd>{usd(e.depositCents)}</dd>
                </div>
                <div>
                  <dt>账户实际敞口倍数</dt>
                  <dd>{num(e.effectiveLeverage, "×")}</dd>
                </div>
                <div>
                  <dt>该币持仓占比</dt>
                  <dd>
                    {e.concentration == null
                      ? "未知"
                      : `${(Number(e.concentration) * 100).toFixed(1)}%`}
                  </dd>
                </div>
              </dl>
              <p>
                {e.reasons.length
                  ? e.reasons.join("；")
                  : e.liveOpening
                    ? "候选建仓，尚未满足全部邮件条件"
                    : "历史建仓记录，不追认提醒"}
              </p>
              <p className="helper">
                开仓 {stamp(e.positionOpenedAt)} · 发现 {stamp(e.detectedAt)}
              </p>
              <details>
                <summary>查看建仓与同步证据</summary>
                <dl className="radar-detail">
                  <dt>门槛时间</dt>
                  <dd>{stamp(e.thresholdAt)}</dd>
                  <dt>最近核验／数据截止</dt>
                  <dd>
                    {stamp(e.verifiedAt)} / {stamp(e.dataThrough)}
                  </dd>
                  <dt>仓位／本轮开仓成交</dt>
                  <dd>
                    {num(e.size)} {e.asset} / {num(e.openedNotionalUSDC)} USDC
                  </dd>
                  <dt>均价／清算参考价</dt>
                  <dd>
                    {num(e.entry)} / {num(e.liquidation)} USDC
                  </dd>
                  <dt>设置杠杆</dt>
                  <dd>{e.configuredLeverage}×（不等于实际敞口倍数）</dd>
                  <dt>换汇证据</dt>
                  <dd>
                    {num(e.fx)} USD/USDC · {stamp(e.fxAt)}
                  </dd>
                </dl>
                <ul>
                  {e.context.map((x, i) => (
                    <li key={i}>{x}</li>
                  ))}
                </ul>
                {!!e.members.length && (
                  <div>
                    <strong>同步地址 · 控制关系未知</strong>
                    <ul>
                      {e.members.map((a) => (
                        <li className="radar-address" key={a}>
                          {a}
                        </li>
                      ))}
                    </ul>
                  </div>
                )}
              </details>
              <div className="radar-mail">
                {notes.length ? (
                  notes.map((n) => (
                    <p key={n.id}>
                      {n.kind.endsWith("priority") ? "升级" : "建仓"}邮件：
                      {mails[n.status] ?? n.status} ·{" "}
                      {stamp(n.completedAt ?? n.attemptedAt)}
                      {n.error && ` · ${n.error}`}
                    </p>
                  ))
                ) : (
                  <p>尚无邮件记录</p>
                )}
              </div>
              <button
                aria-expanded={selected === e.address}
                onClick={() =>
                  setSelected(selected === e.address ? "" : e.address)
                }
              >
                公开钱包时间线{selected === e.address ? " −" : " ＋"}
              </button>
              {selected === e.address && (
                <div className="radar-wallet">
                  {wallet.error && <p role="alert">{wallet.error}</p>}
                  {!wallet.data ? (
                    <p>读取本地证据…</p>
                  ) : wallet.data.wallet ? (
                    <>
                      <p>
                        本地首次发现：{stamp(wallet.data.wallet.firstSeenLocal)}
                        <br />
                        可见历史最早活动：
                        {stamp(wallet.data.wallet.earliestActivityObserved)}
                      </p>
                      <p>
                        角色：{wallet.data.wallet.role || "未知"} · 主账户：
                        {wallet.data.wallet.master || "未取得"}
                      </p>
                      <p>{wallet.data.wallet.historyNote}</p>
                      <p>
                        历史范围 {stamp(wallet.data.wallet.historyFrom)} —{" "}
                        {stamp(wallet.data.wallet.historyThrough)}
                      </p>
                      <h3>近期已核验成交</h3>
                      <div className="radar-table">
                        <table>
                          <thead>
                            <tr>
                              <th>时间</th>
                              <th>币种</th>
                              <th>买卖</th>
                              <th>成交前仓位</th>
                              <th>成交数量</th>
                            </tr>
                          </thead>
                          <tbody>
                            {wallet.data.fills.map((f) => (
                              <tr key={`${f.coin}/${f.time}/${f.tid}`}>
                                <td>{stamp(f.time)}</td>
                                <td>{f.coin}</td>
                                <td>{f.side === "B" ? "买" : "卖"}</td>
                                <td>{num(f.startPosition)}</td>
                                <td>{num(f.sz)}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                      <h3>资金事件</h3>
                      <ul>
                        {wallet.data.wallet.ledger
                          .slice(-20)
                          .reverse()
                          .map((l, i) => (
                            <li key={i}>
                              {stamp(l.time)} · {l.delta.type} ·{" "}
                              {num(l.delta.usdc)} USDC
                              {l.delta.user && (
                                <span className="radar-address">
                                  {" "}
                                  来源 {l.delta.user}
                                </span>
                              )}
                            </li>
                          ))}
                      </ul>
                      <small>
                        内部划转与外部入金分列；共同公共通道不是共同控制证据。
                      </small>
                    </>
                  ) : (
                    <p>{wallet.data.note}</p>
                  )}
                </div>
              )}
            </article>
          );
        })}
      </div>
      <div className="radar-pagination">
        <button
          disabled={!past.length}
          onClick={() => {
            setCursor(past[past.length - 1]);
            setPast(past.slice(0, -1));
            setSelected("");
          }}
        >
          上一页
        </button>
        <span>第 {past.length + 1} 页</span>
        <button
          disabled={!d?.next}
          onClick={() => {
            setPast([...past, cursor]);
            setCursor(d?.next ?? "");
            setSelected("");
          }}
        >
          下一页
        </button>
      </div>
      <section className="radar-method">
        <h2>规则与验证</h2>
        <p>
          快速部署：开仓前30分钟外部入金≥10万美元。敞口集中：该币占账户总名义仓位≥80%，总名义／正权益≥5倍。同步建仓：15分钟至少3个候选地址同币同向，各≥25万美元、合计≥300万美元。
        </p>
        <p>
          每轮最多一封建仓、一封升级；5分钟后过期，重启或历史补采不补发。SMTP接受不代表收件箱签收。其他平台对冲及跨链原始资金来源未知，公开行为不能认定内幕交易。
        </p>
        <button
          aria-expanded={studyOpen}
          onClick={() => setStudyOpen(!studyOpen)}
        >
          {studyOpen ? "收起" : "查看"}独立前向复盘
        </button>
        {studyOpen && (
          <div>
            {study.error && <p role="alert">{study.error}</p>}
            {study.data ? (
              <>
                <p>
                  {study.data.days.toFixed(1)}天 ·{" "}
                  {study.data.independentEvents}个独立观察 ·{" "}
                  {study.data.completeEvents}个完整事件 ·{" "}
                  {study.data.reviewReady
                    ? "达到首轮审查样本门槛"
                    : "样本积累中，不展示胜率"}
                </p>
                <p>{study.data.note}</p>
                {study.data.truncated && (
                  <p role="alert">
                    观察超过500条，当前为部分汇总，不能认定全量审查完成。
                  </p>
                )}
                <div className="radar-table">
                  <table>
                    <thead>
                      <tr>
                        <th>样本组</th>
                        <th>窗口</th>
                        <th>独立事件／完整／缺失／待观察</th>
                        <th>平均方向变化／最大不利</th>
                      </tr>
                    </thead>
                    <tbody>
                      {study.data.metrics?.map((m) => (
                        <tr key={`${m.population}/${m.minutes}`}>
                          <td>
                            {m.population === "radar_selected"
                              ? "雷达入选子集"
                              : "全部大额建仓基线"}
                          </td>
                          <td>{m.minutes}分钟</td>
                          <td>
                            {m.events} / {m.complete} / {m.missing} /{" "}
                            {m.pending}
                          </td>
                          <td>
                            {num(m.meanReturnPercent, "%")}/
                            {num(m.meanMaePercent, "%")}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                <div className="radar-table">
                  <table>
                    <thead>
                      <tr>
                        <th>发现时间</th>
                        <th>分组</th>
                        <th>窗口</th>
                        <th>方向变化</th>
                        <th>最大有利／不利</th>
                        <th>有效K线</th>
                      </tr>
                    </thead>
                    <tbody>
                      {study.data.items.slice(0, 50).flatMap((t) =>
                        t.outcomes.map((o) => (
                          <tr key={`${t.id}/${o.minutes}`}>
                            <td>{stamp(t.at)}</td>
                            <td>
                              {t.asset} {t.alert ? "雷达提醒" : "大额基线"}
                            </td>
                            <td>{o.minutes}分钟</td>
                            <td>{num(o.returnPercent, "%")}</td>
                            <td>
                              {num(o.mfePercent, "%")}/{num(o.maePercent, "%")}
                            </td>
                            <td>
                              {o.observedBars}/{o.expectedBars} ·{" "}
                              {o.state === "complete" ? "完整" : "缺口"}
                            </td>
                          </tr>
                        )),
                      )}
                    </tbody>
                  </table>
                </div>
              </>
            ) : (
              <p>读取前向记录…</p>
            )}
          </div>
        )}
      </section>
    </div>
  );
}
