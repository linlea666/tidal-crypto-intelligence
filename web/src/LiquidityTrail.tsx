import type { LiquidityChange } from "./types";
import { clock, price } from "./data";

const labels: Record<string, string> = {
  decrease: "采样间挂单减少",
  observed: "两端均有挂单",
  unreturned: "本次未再返回",
  uncomparable: "暂不可比较",
};
export function LiquidityTrail({
  events,
  compact = false,
}: {
  events: LiquidityChange[];
  compact?: boolean;
}) {
  return (
    <div className="liquidity-trail">
      {compact && <h3>临近时，挂单有没有变化？</h3>}
      {!events.length && (
        <p className="helper">
          尚无可比变化记录。没有记录不能证明未触及、未成交或未撤走。
        </p>
      )}
      {events.map((e) => (
        <article key={e.key}>
          <div className="liquidity-event-title">
            <strong>
              {labels[e.kind] || "观察记录"}
              {e.decreasePercent != null && e.decreasePercent > 0
                ? ` ${e.decreasePercent.toFixed(1)}%`
                : ""}
            </strong>
            <time>{clock(e.at)}</time>
          </div>
          <p>
            {e.venue} · {e.side === "bid" ? "买方挂单" : "卖方挂单"} ·{" "}
            {price(e.quoteLow)}–{price(e.quoteLow + e.step)} {e.quote}
          </p>
          {e.Before != null && e.After != null && (
            <p>
              原币数量 {e.Before} → {e.After} ·{" "}
              {e.from ? clock(e.from) : "时间未知"}至{clock(e.at)}
            </p>
          )}
          <p className="helper">{e.note}</p>
          <p className="helper">
            {e.tradeNote ||
              "尚无匹配成交佐证；五分钟足迹无法还原两分钟内的逐单过程。"}
            {e.tradeKnownAt ? `（${clock(e.tradeKnownAt)}获取）` : ""}
          </p>
        </article>
      ))}
    </div>
  );
}
