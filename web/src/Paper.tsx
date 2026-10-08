import { useState } from "react";
import { Chart } from "./Chart";
import { age, price, useAPI } from "./data";
import { Metric, lineOption } from "./Pages";

type D = string | null;
type Signal = {
  id: string;
  direction: string;
  level?: string;
  at: string;
  referencePrice: number;
  rulesVersion: string;
};
type Trade = {
  id: string;
  group: string;
  signal: Signal;
  actionableAt: string;
  enteredAt: string;
  exitedAt: string | null;
  entryPrice: string;
  quantity: string;
  remaining: string;
  atr: string;
  atrThrough: string;
  stop: D;
  target: D;
  grossPnl: string;
  fees: string;
  funding: string;
  mfePerUnit: string;
  maePerUnit: string;
  mfeAt: string;
  maeAt: string;
  quality: string[];
  exitReason: string;
  exitEligibleAfter: string;
};
type TradeView = {
  trade: Trade;
  netPnl: D;
  notionalReturnPercent: D;
  fundingPending: boolean;
};
type Stats = {
  closed: number;
  complete: number;
  pathComplete: number;
  pathLongs: number;
  pathShorts: number;
  pendingFunding: number;
  abnormal: number;
  wins: number;
  longs: number;
  shorts: number;
  net: D;
  expectancy: D;
  winRate: D;
  payoffRatio: D;
  profitFactor: D;
  averageHoldSeconds: D;
  notionalReturn: D;
  doubleCostNet: D;
  eligible: boolean;
  confidenceLow: D;
  confidenceHigh: D;
  gate: string;
};
type Account = {
  account: {
    group: string;
    grossPnl: string;
    fees: string;
    funding: string;
    position: Trade | null;
    pending: { signal: Signal } | null;
  };
  initialCapital: string;
  unrealizedPnl: D;
  equity: D;
  accountReturnPercent: D;
  fundingPending: boolean;
  all: Stats;
  clean: Stats;
  commonEntries: Stats;
  byDirection: Record<string, Stats>;
  byPublishedLevel: Record<string, Stats>;
  curve: {
    points: { at: string; value: D }[];
    maximumObservedDrawdown: D;
    drawdownLowerBound: D;
    drawdownLowerBoundPercent: D;
    completeSampledDrawdown: D;
    drawdownComplete: boolean;
    maximumObservedDrawdownPercent: D;
    exposedSeconds: number;
    gapSamples: number;
    resolutionSeconds: number;
  };
};
type Quote = {
  id: number;
  receivedAt: string;
  eventAt: string;
  bid: string;
  ask: string;
  bidQty: string;
  askQty: string;
};
type Paper = {
  enabled: boolean;
  mode: string;
  error?: string;
  contract: string;
  state?: {
    version: string;
    generation: string;
    origin: string | null;
    at: string;
    pauseReason: string;
    gap: boolean;
    lastFailure: string;
    lastFailureAt: string | null;
    fundingThrough: string;
    observedSeconds: number;
    coveredSeconds: number;
  };
  feed?: {
    quote: Quote | null;
    markAt: string | null;
    atr: D;
    atrThrough: string | null;
    bytes: number;
    error: string;
  };
  coveragePercent?: number;
  accounts: Account[];
  events?: { at: string; kind: string; reason: string }[];
  quality: {
    id: string;
    group: string;
    signalId: string;
    at: string;
    state: string;
    reason: string;
  }[];
  intakeCounts: Record<string, number>;
  at: string;
};
type Fill = {
  id: string;
  kind: string;
  side: string;
  at: string;
  price: string;
  quantity: string;
  fee: string;
  quote: Quote;
};
type Detail = TradeView & {
  fills: Fill[];
  fundingEntries: {
    tradeId: string;
    amount: string;
    quantity: string;
    settlement: {
      settledAt: string;
      acquiredAt: string;
      rate: string;
      markPrice: string;
    };
  }[];
};
const groupName = (s: string) =>
  s === "opposite" ? "A · 反向信号退出" : "B · 固定风控退出";
const direction = (s: string) => (s === "buy" ? "做多" : "做空");
const fmt = (v: D | undefined, digits = 2) =>
  v == null ? "—" : price(Number(v), digits);
const stamp = (s: string | null | undefined) =>
  !s || s.startsWith("0001")
    ? "—"
    : new Date(s).toLocaleString("zh-CN", {
        timeZone: "Asia/Shanghai",
        hour12: false,
      });
const labels: Record<string, string> = {
  off: "未启用",
  collect: "公共行情验证中",
  run: "模拟实验已启用",
  warming_up: "行情预热中",
  collection_only: "仅采集公共行情",
  storage_reserve: "容量保护：停止新开仓",
  storage_error: "账本写入异常",
  market_gap: "行情缺口",
  quote_gap: "报价断流",
  stream_disconnect: "行情连接中断",
  processing_delay: "处理延迟",
  recovering: "等待连续30秒正常行情",
  restart_gap: "进程重启恢复",
  source_restore_boundary: "数据恢复边界",
  disabled: "模拟未运行",
  signal_expired: "信号超过30秒",
  ineligible_signal: "非正式首次发布信号",
  contract_unavailable: "合约状态不可用",
  atr_missing: "完整小时ATR缺失",
  quote_stale: "报价已过期",
  mark_stale: "标记价格已过期",
  before_experiment: "早于实验起点",
  same_direction: "同向关联，不加仓",
  superseded_by_new_event: "被更新事件替代",
  quantity_filter: "不满足合约数量规则",
  top_size_insufficient: "对手一档数量不足",
  cash_insufficient: "可用资金不足",
  funding_unsettled: "资金费尚待核实",
  opposite_signal: "正式反向信号",
  stop_loss: "1 ATR止损",
  take_profit: "2 ATR止盈",
  time_limit: "持仓达到4小时",
  data_gap: "数据中断退出",
  seen: "已读取",
  skipped: "跳过",
  associated: "持仓关联",
  opened: "已开仓",
  first_publication: "正式首次发布",
  buy: "做多",
  sell: "做空",
  anomaly: "初始异动",
  strong: "强异动",
  unknown: "发布等级未知",
};
Object.assign(labels, {
  ordinary: "普通异动",
  large: "大额异动",
  supported: "背景支持",
  gap: "行情中断",
  recovered: "恢复完成",
  activated: "实验启用",
  failure: "账本异常",
  fetch_failure: "采集失败",
  continuous_30s: "已连续30秒收到正常行情",
});
const label = (s: string) => labels[s] || s || "—";
function StatTable({ rows }: { rows: { name: string; stats: Stats }[] }) {
  return (
    <div className="paper-table-wrap">
      <table>
        <thead>
          <tr>
            <th>统计对象</th>
            <th>已结算 / 已平仓</th>
            <th>净期望 / 笔</th>
            <th>净胜率</th>
            <th>盈亏比 / 利润因子</th>
            <th>平均持仓</th>
          </tr>
        </thead>
        <tbody>
          {rows.map(({ name, stats: s }) => (
            <tr key={name}>
              <th>{name}</th>
              <td>
                {s.complete} / {s.closed}
              </td>
              <td>{fmt(s.expectancy)} USDT</td>
              <td>{fmt(s.winRate)}%</td>
              <td>
                {fmt(s.payoffRatio)} / {fmt(s.profitFactor)}
              </td>
              <td>
                {s.averageHoldSeconds == null
                  ? "—"
                  : age(Number(s.averageHoldSeconds))}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
function equityOption(points: { at: string; value: D }[]) {
  const base = lineOption(
    points.map((p) => [
      new Date(p.at).getTime(),
      p.value == null ? null : Number(p.value),
    ]),
    "账户净值",
  );
  return {
    ...base,
    yAxis: {
      ...base.yAxis,
      axisLabel: { color: "#9aad9f", formatter: (v: number) => price(v, 2) },
    },
    tooltip: {
      trigger: "axis",
      valueFormatter: (v: unknown) => `${price(Number(v), 2)} USDT`,
    },
  };
}
function PaperAccount({
  data: a,
  onTrade,
}: {
  data: Account;
  onTrade: (id: string) => void;
}) {
  const p = a.account.position;
  return (
    <article className="paper-account">
      <div className="paper-account-heading">
        <h3>{groupName(a.account.group)}</h3>
        <span>
          {p
            ? `${direction(p.signal.direction)} · ${p.exitReason ? "待平仓" : "持仓中"}`
            : "空仓"}
        </span>
      </div>
      <p className="paper-rule">
        {a.account.group === "opposite"
          ? "新反向正式事件退出；无策略止损或时间上限。"
          : "反向事件、1 ATR止损、2 ATR止盈或4小时退出；阈值入场后冻结。"}
      </p>
      <div className="paper-metrics">
        <Metric title="账户净值" value={fmt(a.equity)} unit="USDT" />
        <Metric
          title="账户收益率"
          value={fmt(a.accountReturnPercent)}
          unit="%"
        />
        <Metric title="未实现盈亏" value={fmt(a.unrealizedPnl)} unit="USDT" />
        <Metric
          title="已实现毛盈亏"
          value={fmt(a.account.grossPnl)}
          unit="USDT"
        />
        <Metric title="累计手续费" value={fmt(a.account.fees, 4)} unit="USDT" />
        <Metric
          title="已入账资金费"
          value={fmt(a.account.funding, 4)}
          unit="USDT"
        />
      </div>
      <p className="paper-muted">
        独立初始资金 {fmt(a.initialCapital)} USDT。
        {a.fundingPending
          ? "资金费待核实，完整净值保持未知。"
          : "净值包含已入账资金费与未实现盈亏。"}
      </p>
      {p ? (
        <button
          className={`paper-position ${p.signal.direction}`}
          onClick={() => onTrade(p.id)}
        >
          <strong>
            {direction(p.signal.direction)} {fmt(p.remaining, 6)} BTC
          </strong>
          <span>
            入场 {fmt(p.entryPrice)} USDT · {stamp(p.enteredAt)}
          </span>
          <span>
            {p.exitReason
              ? `退出已触发：${label(p.exitReason)}`
              : "查看成交依据与单笔复盘 →"}
          </span>
        </button>
      ) : (
        <p>
          等待新的正式事件。
          {a.account.pending ? "已有事件进入至少1秒的成交等待期。" : ""}
        </p>
      )}
      <div className="paper-gate">
        <strong>
          {a.all.eligible
            ? "达到阶段审查门槛"
            : "样本不足 · 暂不判断策略有效性"}
        </strong>
        <p>{a.all.gate}</p>
        <p>
          路径与账务完整平仓 {a.all.pathComplete} / 100 · 做多 {a.all.pathLongs}{" "}
          / 30 · 做空 {a.all.pathShorts} / 30
        </p>
        {a.all.eligible && (
          <p>
            按UTC日分块重采样，95%净期望区间：{fmt(a.all.confidenceLow)}～
            {fmt(a.all.confidenceHigh)} USDT / 笔。
          </p>
        )}
      </div>
      <StatTable
        rows={[
          { name: "全部已结算交易", stats: a.all },
          { name: "路径完整子集", stats: a.clean },
          { name: "共同入场事件", stats: a.commonEntries },
        ]}
      />
      <div className="paper-small-metrics">
        <p>
          名义本金收益率 <strong>{fmt(a.all.notionalReturn)}%</strong>
        </p>
        <p>
          费用加倍净结果 <strong>{fmt(a.all.doubleCostNet)} USDT</strong>
        </p>
        <p>
          连续可见片段回撤{" "}
          <strong>
            {fmt(a.curve.maximumObservedDrawdown)} USDT /{" "}
            {fmt(a.curve.maximumObservedDrawdownPercent)}%
          </strong>
        </p>
        <p>
          已知净值点回撤下界 <strong>{fmt(a.curve.drawdownLowerBound)} USDT / {fmt(a.curve.drawdownLowerBoundPercent)}%</strong>
        </p>
        <p>完整采样回撤 <strong>{a.curve.drawdownComplete ? `${fmt(a.curve.completeSampledDrawdown)} USDT` : "未知 · 存在路径缺口"}</strong></p>
        <p>
          市场暴露时间 <strong>{age(a.curve.exposedSeconds)}</strong>
        </p>
      </div>
      <p className="paper-muted">
        异常平仓 {a.all.abnormal} 笔仍计入总账；待结算 {a.all.pendingFunding}{" "}
        笔不进入净胜率分母。盈亏比=平均盈利/平均亏损绝对值；利润因子=盈利总额/亏损绝对值，无亏损分母时保持未知。费用敏感性将手续费和不利滑点加倍，资金费与成交路径保持相同。
      </p>
      <details>
        <summary>多空及首次发布等级分组</summary>
        <StatTable
          rows={[
            ...Object.entries(a.byDirection),
            ...Object.entries(a.byPublishedLevel),
          ].map(([name, stats]) => ({ name: label(name), stats }))}
        />
        <p className="paper-muted">
          仅使用发布时已知的等级，后续价格确认不作为入场条件。
        </p>
      </details>
      <h4>账户净值 · 最近24小时</h4>
      {a.curve.points.length ? (
        <Chart
          label={`${groupName(a.account.group)}最近24小时净值（USDT）`}
          option={equityOption(a.curve.points)}
          height={220}
        />
      ) : (
        <p className="paper-empty">尚无可展示的前向净值序列。</p>
      )}
      <p className="paper-muted">
        每{a.curve.resolutionSeconds}
        秒采样，包含未实现盈亏；缺口不连线。回撤下界保留缺口前已知峰值；连续片段回撤单列，均不代表缺失区间极值。
        {a.curve.gapSamples > 0
          ? `${a.curve.gapSamples}个缺失采样使完整最大回撤不可确认；`
          : ""}
        不是逐笔价格路径的极值。
      </p>
    </article>
  );
}
function PaperDetail({ id, onClose }: { id: string; onClose: () => void }) {
  const { data, error, loading } = useAPI<Detail>(
    `paper/trade?id=${encodeURIComponent(id)}`,
    15000,
  );
  return (
    <section className="paper-detail" aria-label="单笔模拟复盘">
      <div className="paper-account-heading">
        <h3>单笔模拟复盘</h3>
        <button onClick={onClose}>关闭详情</button>
      </div>
      {error && <p role="alert">{error}</p>}
      {loading && !data && <p>读取账本…</p>}
      {data && (
        <>
          <h4>
            {groupName(data.trade.group)} ·{" "}
            {direction(data.trade.signal.direction)}
          </h4>
          <dl>
            <dt>原始事件</dt>
            <dd>{data.trade.signal.id}</dd>
            <dt>正式信号生成</dt>
            <dd>{stamp(data.trade.signal.at)}</dd>
            <dt>引擎首次读到</dt>
            <dd>{stamp(data.trade.actionableAt)}</dd>
            <dt>模拟开仓 / 最终平仓</dt>
            <dd>
              {stamp(data.trade.enteredAt)} / {stamp(data.trade.exitedAt)}
            </dd>
            <dt>实际开仓价 / 数量</dt>
            <dd>
              {fmt(data.trade.entryPrice, 4)} USDT /{" "}
              {fmt(data.trade.quantity, 6)} BTC
            </dd>
            <dt>冻结ATR</dt>
            <dd>
              {fmt(data.trade.atr, 4)} USDT · 完整小时截止{" "}
              {stamp(data.trade.atrThrough)}
            </dd>
            <dt>止损 / 止盈触发线</dt>
            <dd>
              {fmt(data.trade.stop)} / {fmt(data.trade.target)}{" "}
              USDT；触发线不保证成交价。
            </dd>
            <dt>退出原因</dt>
            <dd>{label(data.trade.exitReason)}</dd>
            <dt>最大有利 / 不利波动</dt>
            <dd>
              {fmt(data.trade.mfePerUnit, 4)} / {fmt(data.trade.maePerUnit, 4)}{" "}
              USDT / BTC
              <br />
              {stamp(data.trade.mfeAt)} / {stamp(data.trade.maeAt)}
            </dd>
            <dt>净收益 / 名义本金收益率</dt>
            <dd>
              {fmt(data.netPnl, 4)} USDT / {fmt(data.notionalReturnPercent)}%{" "}
              {data.fundingPending ? "（未平仓或资金费待结算）" : ""}
            </dd>
            <dt>路径质量</dt>
            <dd>
              {data.trade.quality.length
                ? data.trade.quality.map(label).join("、")
                : "目前无已知中断"}
            </dd>
          </dl>
          <div className="paper-table-wrap">
            <table>
              <thead>
                <tr>
                  <th>成交时间 / 类型</th>
                  <th>价格 / BTC数量</th>
                  <th>手续费</th>
                  <th>报价依据</th>
                </tr>
              </thead>
              <tbody>
                {data.fills.map((f) => (
                  <tr key={f.id}>
                    <td>
                      {stamp(f.at)}
                      <br />
                      {f.kind === "open" ? "开仓" : "平仓"} ·{" "}
                      {f.side === "buy" ? "买入" : "卖出"}
                    </td>
                    <td>
                      {fmt(f.price, 4)} / {fmt(f.quantity, 6)}
                    </td>
                    <td>{fmt(f.fee, 6)} USDT</td>
                    <td>
                      #{f.quote.id}
                      <br />
                      买一 {fmt(f.quote.bid)} × {fmt(f.quote.bidQty, 6)}
                      <br />
                      卖一 {fmt(f.quote.ask)} × {fmt(f.quote.askQty, 6)}
                      <br />
                      收到 {stamp(f.quote.receivedAt)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <h4>资金费账务</h4>
          {data.fundingEntries.length ? (
            data.fundingEntries.map((f, i) => (
              <p key={i}>
                {stamp(f.settlement.settledAt)} · 费率{" "}
                {fmt(String(Number(f.settlement.rate) * 100), 6)}% · 标记价{" "}
                {fmt(f.settlement.markPrice)} · 持仓 {fmt(f.quantity, 6)} BTC ·
                入账 {fmt(f.amount, 6)} USDT
                <br />
                <small>实际取得：{stamp(f.settlement.acquiredAt)}</small>
              </p>
            ))
          ) : (
            <p>暂无已入账结算；是否仍待核实以净收益状态为准。</p>
          )}
        </>
      )}
    </section>
  );
}
export function PaperPage() {
  const { data, error, loading } = useAPI<Paper>("paper", 5000);
  const fresh =
    !!data?.feed?.quote &&
    !!data.feed.markAt &&
    !!data.state &&
    Date.now() - new Date(data.feed.quote.receivedAt).getTime() <= 5000 &&
    Date.now() - new Date(data.feed.markAt).getTime() <= 5000 &&
    Date.now() - new Date(data.state.at).getTime() <= 5000;
  const [group, setGroup] = useState("");
  const [offset, setOffset] = useState(0);
  const [selected, setSelected] = useState<string | null>(null);
  const list = useAPI<{ items: TradeView[]; more: boolean }>(
    data?.state
      ? `paper/trades?group=${group}&offset=${offset}&limit=25`
      : null,
    15000,
  );
  return (
    <div className="research-page paper-page">
      <div className="research-heading">
        <div>
          <small>仅公共数据 · 不连接交易账户</small>
          <h2>永续合约模拟仓位</h2>
          <p>用从启用之后真实收到的信号与行情，比较两种退出方式。</p>
        </div>
        <span className="research-badge">BTCUSDT · 纸面模拟</span>
      </div>
      {error && (
        <p role="alert" className="paper-gate">
          读取模拟状态失败：{error}。状态未知，不能视为正常运行。
        </p>
      )}
      {loading && !data && <p>读取模拟状态…</p>}
      {data && (
        <>
          <section className="paper-status">
            <h3>{label(data.mode)}</h3>
            <p>
              {data.contract} · {data.state?.version || "等待实验初始化"}
            </p>
            {data.error && <p role="alert">{data.error}</p>}
            {data.state ? (
              <>
                <p>
                  实验起点：{stamp(data.state.origin)} · 数据覆盖{" "}
                  {data.coveragePercent?.toFixed(2) ?? "—"}% · 已观察{" "}
                  {age(data.state.observedSeconds)}
                </p>
                <p>
                  {data.state.pauseReason
                    ? `暂停原因：${label(data.state.pauseReason)}`
                    : !fresh
                      ? "行情或引擎状态未知／已过期，不能视为正常运行。"
                      : data.state.gap
                        ? "行情恢复观察中，停止新开仓。"
                        : "按冻结规则前向累计。"}
                </p>
                <p>
                  报价收到：{stamp(data.feed?.quote?.receivedAt)} · 标记价收到：
                  {stamp(data.feed?.markAt)}
                  <br />
                  账本更新：{stamp(data.state.at)} · 资金费核实截止：
                  {stamp(data.state.fundingThrough)}
                </p>
                {data.state.lastFailure && (
                  <p className="paper-muted">
                    最近失败：{stamp(data.state.lastFailureAt)} ·{" "}
                    {label(data.state.lastFailure)}（后续成功保留此记录）
                  </p>
                )}
              </>
            ) : (
              <p>
                服务配置尚未启用公共合约采集。历史信号不会填入前向模拟账户。
              </p>
            )}
          </section>
          <p className="paper-assumptions">
            两组各10,000 USDT；每次固定1,000
            USDT名义金额并按数量步长向下取整。至少等待1秒，买卖按对手一档再加2
            bp不利滑点，开平仓各5
            bp手续费。费用为实验假设，非真实账户费率；不模拟杠杆或强平。
          </p>
          <div className="paper-accounts">
            {data.accounts.map((a) => (
              <PaperAccount
                key={a.account.group}
                data={a}
                onTrade={setSelected}
              />
            ))}
          </div>
          <section className="paper-trades">
            <h3>模拟流水</h3>
            <p className="paper-muted">
              共同入场仅比较两组都已平仓且资金费结算完整的同一正式事件；风控组可以提前空仓，独立账户成绩与共同事件成绩分别展示。
            </p>
            <div className="paper-controls">
              <label>
                实验组{" "}
                <select
                  value={group}
                  onChange={(e) => {
                    setGroup(e.target.value);
                    setOffset(0);
                  }}
                >
                  <option value="">全部</option>
                  <option value="opposite">A · 反向信号退出</option>
                  <option value="risk">B · 固定风控退出</option>
                </select>
              </label>
              <button
                disabled={offset === 0}
                onClick={() => setOffset(Math.max(0, offset - 25))}
              >
                上一页
              </button>
              <button
                disabled={!list.data?.more}
                onClick={() => setOffset(offset + 25)}
              >
                下一页
              </button>
              <span>第{offset / 25 + 1}页</span>
            </div>
            {list.error && <p role="alert">{list.error}</p>}
            {list.loading && !list.data ? (
              <p>正在读取模拟流水…</p>
            ) : list.error && !list.data ? (
              <p>流水状态未知。</p>
            ) : list.data?.items.length ? (
              <div className="paper-table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>开仓 / 实验组</th>
                      <th>方向 / 入场</th>
                      <th>净收益 / 名义收益率</th>
                      <th>退出</th>
                      <th>依据</th>
                    </tr>
                  </thead>
                  <tbody>
                    {list.data.items.map((v) => (
                      <tr key={v.trade.id}>
                        <td>
                          {stamp(v.trade.enteredAt)}
                          <br />
                          {groupName(v.trade.group)}
                        </td>
                        <td className={v.trade.signal.direction}>
                          {direction(v.trade.signal.direction)}
                          <br />
                          {fmt(v.trade.entryPrice)} USDT
                        </td>
                        <td>
                          {fmt(v.netPnl)} USDT / {fmt(v.notionalReturnPercent)}%
                          {v.fundingPending && <small> · 未完成结算</small>}
                        </td>
                        <td>
                          {v.trade.exitedAt
                            ? label(v.trade.exitReason)
                            : v.trade.exitReason
                              ? `待平仓 · ${label(v.trade.exitReason)}`
                              : "持仓中"}
                          {v.trade.quality.length > 0 && (
                            <small> · 路径有缺口</small>
                          )}
                        </td>
                        <td>
                          <button
                            onClick={() => setSelected(v.trade.id)}
                            aria-label={`复盘${groupName(v.trade.group)}${stamp(v.trade.enteredAt)}`}
                          >
                            查看复盘
                          </button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <p className="paper-empty">
                暂无模拟成交。等待新的正式首次发布事件，不补造历史交易。
              </p>
            )}
          </section>
          {selected && (
            <PaperDetail id={selected} onClose={() => setSelected(null)} />
          )}
          <section className="paper-quality">
            <h3>数据质量与信号处理</h3>
            <p>
              累计读取 {data.intakeCounts?.seen || 0} · 开仓{" "}
              {data.intakeCounts?.opened || 0} · 跳过{" "}
              {data.intakeCounts?.skipped || 0} · 同向关联{" "}
              {data.intakeCounts?.associated || 0}（两组分别计数）
            </p>
            <p className="paper-muted">
              断流和重启期间不推测止盈止损；恢复后退出原仓位，连续30秒正常行情后只接受新事件。最新100条处理记录如下，完整记录保留在账本。
            </p>
            {data.events?.length ? (
              <details>
                <summary>恢复与采集记录（最新100条）</summary>
                {data.events.map((e, i) => (
                  <p key={i}>
                    {stamp(e.at)} · {label(e.kind)} · {label(e.reason)}
                  </p>
                ))}
              </details>
            ) : null}
            {data.quality?.length ? (
              <div className="paper-table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>时间 / 实验组</th>
                      <th>处理</th>
                      <th>原因 / 原事件</th>
                    </tr>
                  </thead>
                  <tbody>
                    {data.quality.map((v) => (
                      <tr key={v.id}>
                        <td>
                          {stamp(v.at)}
                          <br />
                          {groupName(v.group)}
                        </td>
                        <td>{label(v.state)}</td>
                        <td>
                          {label(v.reason)}
                          <br />
                          <small>{v.signalId}</small>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <p>尚无处理记录。</p>
            )}
          </section>
        </>
      )}
    </div>
  );
}
