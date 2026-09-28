import { useEffect, useMemo, useState } from "react";
import {
  ArrowSquareOut,
  Bell,
  Clock,
  WarningCircle,
} from "@phosphor-icons/react";
import { api, age, price, useAPI } from "./data";
import { Chart } from "./Chart";
import "./VIX.css";

type Point = { at: string; value: string; tradingDate: string };
type Daily = {
  date: string;
  open: string;
  high: string;
  low: string;
  close: string;
};
type Feed = {
  fetchedAt: string | null;
  attemptedAt: string | null;
  error: string;
  lastDate?: string;
};
type Settings = {
  emailEnabled: boolean;
  configured: boolean;
  offline: boolean;
  recipient: string;
  limitPerHour: number;
  cycle: {
    cycleId: string;
    cycleStartedAt: string | null;
    lowSince: string | null;
    watchRecorded: boolean;
    priorityRecorded: boolean;
  };
};
type Current = {
  latest: Point | null;
  fetchedAt: string | null;
  status: string;
  reason: string;
  level: string | null;
  nextThresholdDistance: string | null;
  sourceAgeSeconds: number | null;
  settings: Settings;
  now: string;
  mailError: { at: string; error: string } | null;
};
type History = {
  source: string;
  points?: Point[];
  daily?: Daily[];
  feed: Feed;
};
type Event = {
  id: string;
  level: string;
  value: string;
  observedAt: string;
  detectedAt: string;
  tradingDate: string;
  initial: boolean;
  status: string;
};
type Events = { items: Event[]; hasMore: boolean; nextBefore: string | null };
const levels: Record<string, string> = {
  low: "低波动",
  elevated: "波动升高",
  watch: "买入观察",
  priority: "重点买入观察",
};
export const vixStatuses: Record<string, string> = {
  delayed: "延时行情",
  closed: "非发布时段",
  stale: "行情过期",
  missing: "等待数据",
  error: "来源异常",
};
const delivery: Record<string, string> = {
  pending: "等待发送 · 受总限额约束",
  sending: "发送中",
  sent: "邮件服务器已接受",
  delivery_unknown: "发送结果不确定 · 不自动重发",
  failed_before_submission: "提交前失败",
  rejected: "邮件服务器明确拒绝",
  unknown_after_restart: "重启前结果不确定 · 不重发",
  disabled: "邮件关闭 · 不补发",
  unconfigured: "邮箱未配置 · 不补发",
  offline: "离线记录 · 未发送",
  suppressed_restart: "重启积压 · 不补发",
  suppressed_stale: "行情过期／周期结束 · 未发送",
  suppressed_recovered: "行情已回落 · 未发送",
  superseded: "已升级或来源修正 · 未发送",
  unknown: "发送记录不可用",
};
const stamp = (s?: string | null) =>
  s
    ? new Date(s).toLocaleString("zh-CN", {
        timeZone: "Asia/Shanghai",
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        hour12: false,
      })
    : "—";

export function VIXPage() {
  const current = useAPI<Current>("vix", 15000);
  const [range, setRange] = useState("day");
  const history = useAPI<History>(`vix/history?range=${range}`, 60000);
  const [cursor, setCursor] = useState<string | null>(null);
  const latestEvents = useAPI<Events>("vix/alerts?limit=20", 15000);
  const olderEvents = useAPI<Events>(
    cursor ? `vix/alerts?limit=20&before=${encodeURIComponent(cursor)}` : null,
    15000,
  );
  const events = cursor ? olderEvents : latestEvents;
  const [saving, setSaving] = useState(false);
  const [message, setMessage] = useState("");
  const [saved, setSaved] = useState<Settings | null>(null);
  const data = current.data;
  useEffect(() => {
    setSaved(null);
  }, [data]);
  const settings = saved ?? data?.settings;
  const level = data?.level ?? "";
  const lastEvent = latestEvents.data?.items[0];
  const option = useMemo(() => {
    const daily = range !== "day";
    const points: [number, number | null][] = [];
    let previous = 0;
    for (const p of history.data?.points ?? []) {
      const at = new Date(p.at).getTime();
      if (previous && at - previous > 60000)
        points.push([previous + 60000, null]);
      points.push([at, +p.value]);
      previous = at;
    }
    const rows = history.data?.daily ?? [];
    // A missing weekday may be a holiday or a coverage gap. Keep it unknown;
    // only ordinary weekends are omitted from the daily axis.
    const dailyPoints: { date: string; close: number | null }[] = [];
    const byDate = new Map(rows.map((r) => [r.date, +r.close]));
    if (rows.length) {
      for (
        let d = Date.parse(rows[0].date);
        d <= Date.parse(rows[rows.length - 1].date);
        d += 86400000
      ) {
        const date = new Date(d).toISOString().slice(0, 10);
        const weekday = new Date(d).getUTCDay();
        if (byDate.has(date) || (weekday !== 0 && weekday !== 6))
          dailyPoints.push({ date, close: byDate.get(date) ?? null });
      }
    }
    const max = Math.max(
      45,
      ...rows.map((r) => +r.high),
      ...points.map((p) => p[1] ?? 0),
    );
    return {
      grid: { left: 44, right: 28, top: 32, bottom: 42 },
      tooltip: {
        trigger: "axis",
        renderMode: "richText",
        formatter: (
          items: {
            axisValue: string | number;
            value: number | [number, number | null];
            seriesName: string;
          }[],
        ) => {
          const point = items[0];
          if (!point) return "未覆盖";
          const value = Array.isArray(point.value)
            ? point.value[1]
            : point.value;
          const date = daily
            ? `${point.axisValue} · 美东交易日`
            : `${stamp(new Date(Number(point.axisValue)).toISOString())} · 北京时间`;
          return `${date}\n${point.seriesName}  ${typeof value === "number" ? price(value) : "未覆盖"}`;
        },
      },
      xAxis: daily
        ? {
            type: "category",
            data: dailyPoints.map((r) => r.date),
            boundaryGap: false,
            axisLabel: { formatter: (v: string) => v.slice(5) },
          }
        : {
            type: "time",
            axisLabel: {
              formatter: (v: number) =>
                new Date(v).toLocaleTimeString("zh-CN", {
                  timeZone: "Asia/Shanghai",
                  hour: "2-digit",
                  minute: "2-digit",
                  hour12: false,
                }),
            },
            axisPointer: {
              label: {
                formatter: (p: { value: number }) =>
                  stamp(new Date(p.value).toISOString()),
              },
            },
          },
      yAxis: {
        type: "value",
        min: 0,
        max: Math.ceil(max / 5) * 5,
        splitLine: { lineStyle: { color: "#2b3833" } },
      },
      series: [
        {
          name: daily ? "Cboe官方日收盘" : "新浪分时观察",
          type: "line",
          showSymbol: false,
          connectNulls: false,
          data: daily ? dailyPoints.map((r) => r.close) : points,
          lineStyle: { color: "#dce8e1", width: 2 },
          itemStyle: { color: "#dce8e1" },
          markLine: {
            silent: true,
            symbol: "none",
            data: [
              {
                yAxis: 20,
                lineStyle: { color: "#74897d", type: "dashed" },
                label: {
                  formatter: "20",
                  color: "#a0aeaa",
                  position: "insideEndTop",
                },
              },
              {
                yAxis: 30,
                lineStyle: { color: "#e5be74", type: "dashed" },
                label: {
                  formatter: "30 · 买入观察",
                  color: "#e5be74",
                  position: "insideEndTop",
                },
              },
              {
                yAxis: 40,
                lineStyle: { color: "#f17369", type: "dashed" },
                label: {
                  formatter: "40 · 超过后重点观察",
                  color: "#f17369",
                  position: "insideEndTop",
                },
              },
            ],
          },
        },
      ],
    };
  }, [history.data, range]);
  const hasHistory = !!(range === "day"
    ? history.data?.points?.length
    : history.data?.daily?.length);
  async function toggle() {
    if (!settings || saving) return;
    setSaving(true);
    setMessage("");
    try {
      const result = await api<Settings>("vix/settings", {
        method: "PUT",
        body: JSON.stringify({ emailEnabled: !settings.emailEnabled }),
      });
      setSaved(result);
      setMessage(
        result.emailEnabled
          ? "邮件开关已开启；仅有效行情可触发，关闭期间的记录不补发。"
          : "邮件已关闭；站内观察继续记录，已进入发送的邮件无法撤回。",
      );
      current.refresh();
      events.refresh();
    } catch (e) {
      setMessage((e as Error).message);
    } finally {
      setSaving(false);
    }
  }
  return (
    <div className="vix-page">
      <div
        className={`vix-source-status ${data?.status === "error" || data?.status === "stale" ? "sell" : ""}`}
        role="status"
      >
        <Clock size={17} />
        <strong>{vixStatuses[data?.status ?? "missing"]}</strong>
        <span>{current.error || data?.reason || "正在读取本地行情…"}</span>
      </div>
      <section className="vix-metrics" aria-label="VIX摘要">
        <div>
          <span>分时最新 · 指数点</span>
          <strong className="vix-value">
            {data?.latest ? price(+data.latest.value) : "—"}
          </strong>
          <small>新浪 · {data?.latest?.tradingDate ?? "暂无"} 交易日</small>
        </div>
        <div>
          <span>恐慌等级</span>
          <strong
            className={
              level === "priority" ? "sell" : level === "watch" ? "amber" : ""
            }
          >
            {levels[level] ?? "未判定"}
          </strong>
          <small>美股观察，不表示底部已确认</small>
        </div>
        <div>
          <span>{level === "watch" ? "距离重点观察" : "距离买入观察"}</span>
          <strong>
            {level === "priority"
              ? "已超过 40"
              : data?.nextThresholdDistance != null
                ? `${price(+data.nextThresholdDistance)} 点`
                : "—"}
          </strong>
          <small>
            {level === "priority"
              ? "本轮重点档仅提醒一次"
              : level === "watch"
                ? "超过 40 后升级；等于 40 仍属普通档"
                : "达到 30 后进入观察区间"}
          </small>
        </div>
        <div>
          <span>行情时间 · 北京时间</span>
          <strong className="vix-time">{stamp(data?.latest?.at)}</strong>
          <small>
            {data?.sourceAgeSeconds != null
              ? `${age(data.sourceAgeSeconds)}前的行情`
              : "来源时间缺失"}{" "}
            · 获取 {stamp(data?.fetchedAt)}
          </small>
        </div>
      </section>
      <div className="vix-layout">
        <section className="vix-chart-panel">
          <div className="vix-section-heading">
            <div>
              <h2>恐慌走到哪里了？</h2>
              <p className="helper">
                {range === "day"
                  ? "新浪分时 · 北京时间 · 来源声明至少延迟15分钟"
                  : "Cboe官方日收盘 · 美东交易日期 · 缺失工作日保留断线"}
              </p>
            </div>
            <div className="segmented" aria-label="VIX历史范围">
              {[
                ["day", "分时"],
                ["1m", "1个月"],
                ["3m", "3个月"],
                ["1y", "1年"],
              ].map(([id, label]) => (
                <button
                  key={id}
                  aria-pressed={range === id}
                  className={range === id ? "selected" : ""}
                  onClick={() => setRange(id)}
                >
                  {label}
                </button>
              ))}
            </div>
          </div>
          {hasHistory ? (
            <Chart
              option={option}
              height={360}
              label={
                range === "day"
                  ? "VIX分时曲线与20、30、40阈值，缺失采样不连接"
                  : "VIX官方日收盘曲线与20、30、40阈值"
              }
            />
          ) : (
            <div className="vix-empty">
              {history.loading
                ? "正在读取历史…"
                : "暂无可用历史，后台采集后自动显示。"}
            </div>
          )}
          {(history.error || history.data?.feed.error) && (
            <p role="alert" className="sell">
              {history.error || history.data?.feed.error}；保留已有历史。
            </p>
          )}
          <p className="helper">
            {range === "day"
              ? "分时末值不等同官方收盘；不使用未核实字段计算昨收、涨跌幅或成交量。"
              : `官方数据截至 ${history.data?.feed.lastDate || "—"}；获取时间 ${stamp(history.data?.feed.fetchedAt)}。日线仅供回看，不触发盘中邮件。`}
          </p>
          <div className="vix-bands" aria-label="阈值说明">
            <div>
              <b>&lt;20</b>
              <span>低波动</span>
            </div>
            <div>
              <b>20–&lt;30</b>
              <span>波动升高</span>
            </div>
            <div className="amber">
              <b>30–40</b>
              <span>买入观察</span>
            </div>
            <div className="sell">
              <b>&gt;40</b>
              <span>重点买入观察</span>
            </div>
          </div>
        </section>
        <aside className="vix-alert-panel">
          <div className="vix-section-heading">
            <h2>
              <Bell size={20} /> 邮件提醒
            </h2>
            <span className="vix-chip">美股观察</span>
          </div>
          <label className="vix-toggle">
            <span>
              阈值提醒
              <strong>{settings?.emailEnabled ? "已开启" : "已关闭"}</strong>
            </span>
            <input
              type="checkbox"
              role="switch"
              aria-label="VIX邮件提醒"
              checked={settings?.emailEnabled ?? false}
              disabled={saving || !settings}
              onChange={toggle}
            />
          </label>
          <p className="helper">接收邮箱：{settings?.recipient ?? "读取中"}</p>
          {settings?.offline && (
            <p className="amber">当前为离线模式，不采集、不发送邮件。</p>
          )}
          {settings && !settings.configured && (
            <p className="amber">邮件通道未配置，仅记录站内事件。</p>
          )}
          {data?.mailError?.error && (
            <p className="sell" role="alert">
              最近邮件错误：{data.mailError.error}（{stamp(data.mailError.at)}）
            </p>
          )}
          <div className="vix-rule">
            <b className="amber">30 ≤ VIX ≤ 40</b>
            <strong>买入观察</strong>
            <p>检测到达标即提醒，本轮一次。</p>
          </div>
          <div className="vix-rule">
            <b className="sell">VIX &gt; 40</b>
            <strong>重点买入观察</strong>
            <p>升级时追加一次；直接跳到此档只发重点提醒。</p>
          </div>
          <dl className="vix-cycle">
            <dt>当前周期</dt>
            <dd>
              {settings?.cycle.cycleId ? "恐慌观察中" : "等待进入观察区间"}
            </dd>
            {settings?.cycle.cycleStartedAt && (
              <>
                <dt>本轮行情起点</dt>
                <dd>{stamp(settings.cycle.cycleStartedAt)}</dd>
                <dt>本轮已覆盖</dt>
                <dd>
                  {settings.cycle.priorityRecorded
                    ? "普通档与重点档"
                    : "普通档"}
                </dd>
              </>
            )}
            <dt>恢复条件</dt>
            <dd>低于 30 连续观察 30 分钟</dd>
            {settings?.cycle.lowSince && (
              <>
                <dt>开始恢复观察</dt>
                <dd>{stamp(settings.cycle.lowSince)}</dd>
              </>
            )}
            <dt>最近通知</dt>
            <dd>
              {lastEvent
                ? (delivery[lastEvent.status] ?? lastEvent.status)
                : "暂无新提醒"}
            </dd>
          </dl>
          <p className="helper">
            与原有提醒共用每小时 6
            次总限额。异常或过期行情不触发，休市不重置周期。
          </p>
          <p className="vix-save-message" role="status">
            {saving ? "正在保存…" : message}
          </p>
        </aside>
      </div>
      <section className="data-section">
        <div className="vix-section-heading">
          <div>
            <h2>提醒记录</h2>
            <p className="helper">保留 90 天；发送状态与市场观察分开记录。</p>
          </div>
          <div className="segmented">
            <button disabled={!cursor} onClick={() => setCursor(null)}>
              最新记录
            </button>
            <button
              disabled={!events.data?.hasMore}
              onClick={() => setCursor(events.data?.nextBefore ?? null)}
            >
              更早记录
            </button>
          </div>
        </div>
        {events.error && (
          <p className="sell" role="alert">
            {events.error}
          </p>
        )}
        {events.data?.items.length ? (
          <div className="vix-events">
            {events.data.items.map((e) => (
              <article key={e.id} className="vix-event">
                <div>
                  <strong className={e.level === "priority" ? "sell" : "amber"}>
                    {levels[e.level]} · {price(+e.value)}
                  </strong>
                  <small>
                    {e.initial ? "首次启用时已达标" : "本轮首次检测到达标"}
                  </small>
                </div>
                <div>
                  <span>行情 {stamp(e.observedAt)}</span>
                  <small>发现 {stamp(e.detectedAt)} · 北京时间</small>
                </div>
                <span>{delivery[e.status] ?? e.status}</span>
              </article>
            ))}
          </div>
        ) : (
          <div className="vix-empty compact">
            {events.loading
              ? "正在读取记录…"
              : "尚无提醒。只有有效行情进入阈值，才会产生真实观察记录。"}
          </div>
        )}
      </section>
      <section className="vix-explainer">
        <WarningCircle size={22} />
        <div>
          <h2>高 VIX 是观察起点，不能单独确认底部</h2>
          <p>
            VIX 衡量标普 500 未来约 30 天的预期波动。2022 年盘中最高 38.94；2020
            年收盘从 40.11 继续升至
            82.69。这两档是用户自定观察规则，未验证收益，不自动下单，也不推导
            BTC/ETH 买点。
          </p>
          <div className="vix-links">
            <a
              href="https://www.cboe.com/tradable-products/vix/vix-historical-data"
              target="_blank"
              rel="noreferrer"
            >
              Cboe历史数据 <ArrowSquareOut size={14} />
            </a>
            <a
              href="https://finance.sina.com.cn/stock/globalindex/quotes/VIX"
              target="_blank"
              rel="noreferrer"
            >
              新浪行情与延时说明 <ArrowSquareOut size={14} />
            </a>
          </div>
        </div>
      </section>
    </div>
  );
}
