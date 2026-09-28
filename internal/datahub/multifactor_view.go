package datahub

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

func unixTimeOrNil(ts int64) *time.Time {
	if ts <= 0 {
		return nil
	}
	return flowPtr(time.Unix(ts, 0).UTC())
}

type SignalMailResult struct {
	ID        string     `json:"id"`
	SignalID  string     `json:"signalId"`
	Kind      string     `json:"kind"`
	Status    string     `json:"status"`
	Created   time.Time  `json:"createdAt"`
	Attempted *time.Time `json:"attemptedAt"`
	Completed *time.Time `json:"completedAt"`
	Error     string     `json:"error,omitempty"`
}

func (h *Hub) signalMailResults(ctx context.Context, ids []string) ([]SignalMailResult, error) {
	out := []SignalMailResult{}
	if len(ids) == 0 {
		return out, nil
	}
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	rows, e := h.Store.research.QueryContext(ctx, `SELECT n.id,n.signal_id,n.kind,n.status,n.created,n.attempted,coalesce(r.completed,0),coalesce(r.error,'') FROM notices n LEFT JOIN mail_batch_items i ON i.notice_id=n.id LEFT JOIN mail_results r ON r.batch_id=i.batch_id WHERE n.signal_id IN (`+strings.TrimRight(strings.Repeat("?,", len(ids)), ",")+`) ORDER BY n.created,n.id`, args...)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	for rows.Next() {
		var r SignalMailResult
		var created, attempted, completed int64
		if e = rows.Scan(&r.ID, &r.SignalID, &r.Kind, &r.Status, &created, &attempted, &completed, &r.Error); e != nil {
			return nil, e
		}
		r.Created = time.Unix(created, 0).UTC()
		r.Attempted = unixTimeOrNil(attempted)
		if completed > 0 {
			r.Completed = flowPtr(time.Unix(0, completed).UTC())
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

type SignalPricePoint struct {
	At    time.Time `json:"at"`
	Close *float64  `json:"closeUsdt"`
}

func (h *Hub) signalsExtra(ctx context.Context, a string, rows []json.RawMessage) (map[string]any, error) {
	now := time.Now().UTC()
	var current FlowSnapshot
	var currentValue any
	if h.Store.LoadState("signals/current/"+a, &current) {
		current.Fresh = current.Fresh && !current.DataThrough.After(now) && now.Sub(current.DataThrough) <= 12*time.Minute
		current.explain("")
		currentValue = current
	}
	ids := []string{}
	for _, b := range rows {
		ids = append(ids, decodeSignal(b).ID)
	}
	mail, e := h.signalMailResults(ctx, ids)
	if e != nil {
		return nil, e
	}
	prices := []SignalPricePoint{}
	byTime := map[int64]float64{}
	// Warehouse reads only. No page request can trigger upstream collection.
	e = h.Store.FactsAsOf(ctx, ID("candles", a, "Binance", "spot"), now.Add(-24*time.Hour), now, now, func(o Observation) error {
		t := recordTime(o)
		if o.Quality == "valid" && o.Resolution == 300 && o.Payload.Candle != nil && t.Unix()%300 == 0 && !t.Add(5*time.Minute).After(now) {
			byTime[t.Add(5*time.Minute).Unix()] = o.Payload.Candle.Close
		}
		return nil
	})
	if e != nil {
		return nil, e
	}
	for t := now.Add(-24 * time.Hour).Truncate(5 * time.Minute).Add(5 * time.Minute); !t.After(now); t = t.Add(5 * time.Minute) {
		p := SignalPricePoint{At: t}
		if v, ok := byTime[t.Unix()]; ok {
			p.Close = flowPtr(v)
		}
		prices = append(prices, p)
	}
	var live any
	if d, ok := h.Dataset(ID("price", a, "Binance", "spot")); ok {
		if p, exists := h.Store.Latest(d.ID); exists && p.Quality == "valid" && p.Fresh(d, now) && p.Payload.Price != nil {
			live = map[string]any{"valueUsdt": num(p.Payload.Price.Value), "at": p.Time()}
		}
	}
	return map[string]any{"current": currentValue, "notificationResults": mail, "prices": prices, "currentPrice": live}, nil
}
func flowNoticeTitle(v noticePayload) string {
	name, confirm := "买盘", "突破"
	if v.Direction == "sell" {
		name, confirm = "卖压", "跌破"
	}
	if v.Kind == "confirmed" {
		tail := "资金仍同向"
		if v.Signal != nil && v.Signal.ConfirmationSnapshot != nil {
			for _, e := range v.Signal.ConfirmationSnapshot.Evidence {
				if e.Factor == "futures" && e.State == "conflict" {
					tail = "资金同向，合约存在分歧"
				}
			}
		}
		return "[TIDAL] BTC " + confirm + "确认｜" + tail
	}
	if v.Signal != nil && v.Signal.Multifactor != nil {
		s := v.Signal.Multifactor
		w := s.Spot["60"]
		if v.Signal.Pattern == "fast" {
			w = s.Spot["15"]
		}
		if w.VolumeRatio != nil {
			return fmt.Sprintf("[TIDAL] BTC %s异动｜放量 %.1f 倍，价格尚未确认", name, *w.VolumeRatio)
		}
	}
	return "[TIDAL] BTC " + name + "异动｜价格尚未确认"
}
func multifactorNoticeBody(v noticePayload, dashboard string) string {
	s := v.Signal.Multifactor
	if v.Kind == "confirmed" && v.Signal.ConfirmationSnapshot != nil {
		s = v.Signal.ConfirmationSnapshot
	}
	local := time.FixedZone("UTC+8", 8*3600)
	b := flowNoticeTitle(v) + "\r\n" + s.Headline + "\r\n规则效果验证中。"
	for _, m := range []string{"15", "60", "240"} {
		w := s.Spot[m]
		if w.Net == nil {
			b += "\r\n" + m + "分钟：数据缺失"
			continue
		}
		b += fmt.Sprintf("\r\n%d分钟主动净买卖：%+.2f万美元", w.Minutes, float64(*w.Net)/1e6)
		if w.VolumeRatio != nil {
			b += fmt.Sprintf("；量比 %.2f", *w.VolumeRatio)
		}
		share := w.BuyShare
		name := "买入"
		if v.Direction == "sell" {
			share = w.SellShare
			name = "卖出"
		}
		if share != nil {
			b += fmt.Sprintf("；%s占比 %.1f%%", name, *share)
		}
	}
	states := map[string]string{"support": "支持", "conflict": "分歧", "neutral": "背景", "missing": "缺失/降级"}
	for _, e := range s.Evidence {
		b += "\r\n" + states[e.State] + "：" + e.Text
	}
	if v.Kind == "confirmed" {
		b += "\r\n两根连续已闭合五分钟收盘已越过原固定区间，最近15分钟资金仍同向；后续继续观察是否收回。"
	} else {
		b += "\r\n价格尚未确认；需要两根连续已闭合五分钟收盘越过固定区间，且最近15分钟资金同向。"
	}
	line := v.Signal.FrozenHigh
	if v.Direction == "sell" {
		line = v.Signal.FrozenLow
	}
	b += fmt.Sprintf("\r\n固定观察线：%.2f USDT（Binance BTC/USDT）\r\n发现时间：%s\r\n成交数据截止：%s\r\n当前阶段计算：%s（北京时间）", line, v.At.In(local).Format("01-02 15:04:05"), s.DataThrough.In(local).Format("01-02 15:04:05"), s.At.In(local).Format("01-02 15:04:05"))
	if s.Context.OI.Coin1H != nil {
		b += fmt.Sprintf("\r\n币计价OI：1小时 %+.2f%%", *s.Context.OI.Coin1H)
	}
	if s.Context.OI.USD1H != nil {
		b += fmt.Sprintf("；美元OI %+.2f%%（含价格影响）", *s.Context.OI.USD1H)
	}
	for _, f := range s.Context.Funding {
		period := "周期未知"
		if f.Hours != nil {
			period = fmt.Sprintf("%g小时", *f.Hours)
		}
		b += fmt.Sprintf("\r\nFunding %s / %s：%s%% / %s / 类型%s；获取于%s", f.Venue, f.Margin, f.RatePercent, period, f.RateKind, f.FetchedAt.In(local).Format("15:04:05"))
	}
	b += "\r\n现货成交覆盖五家，合约成交和清算覆盖三家，OI为CoinGlass聚合，市场范围不同。主动净买卖不等于充值提现；确认不保证后续延续。"
	if dashboard != "" {
		b += "\r\n看板：" + dashboard
	}
	return b
}
