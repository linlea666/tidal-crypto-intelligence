package datahub

import (
	"context"
	"math"
	"sort"
	"time"
)

type windowFlow struct {
	Buy        int64            `json:"buyCents"`
	Sell       int64            `json:"sellCents"`
	Rows       int              `json:"rows"`
	Coverage   float64          `json:"coverage"`
	Boundaries bool             `json:"boundaries"`
	Partial    bool             `json:"partial"`
	Series     []map[string]any `json:"series"`
}

func (h *Hub) flowWindow(ctx context.Context, d Dataset, from, to, anchor time.Time, res int) (windowFlow, error) {
	out := windowFlow{Series: []map[string]any{}}
	var first, last, previous time.Time
	cvd := int64(0)
	validSeconds := 0
	err := h.Store.Visit(ctx, d, res, minTime(from, anchor), to, func(o Observation) error {
		if o.Payload.Flow == nil {
			return nil
		}
		at := recordTime(o)
		duration := max(60, o.Resolution)
		if at.Before(from) && at.Before(anchor) {
			return nil
		}
		if at.Add(time.Duration(duration) * time.Second).After(to) {
			return nil
		}
		if !previous.IsZero() && at.Sub(previous) > time.Duration(res)*time.Second {
			out.Series = append(out.Series, map[string]any{"time": previous.Add(time.Duration(res) * time.Second).Unix(), "cvdCents": nil, "gap": true})
			out.Partial = true
		}
		b, s := money(o.Payload.Flow.Buy), money(o.Payload.Flow.Sell)
		if !at.Before(from) {
			out.Buy += b
			out.Sell += s
			out.Rows++
			if first.IsZero() {
				first = at
			}
			last = at.Add(time.Duration(duration) * time.Second)
			if o.Quality != "partial" && o.Quality != "missing" {
				validSeconds += duration
			} else {
				out.Partial = true
			}
		}
		if !at.Before(anchor) {
			cvd += b - s
			out.Series = append(out.Series, map[string]any{"time": at.Unix(), "buyCents": b, "sellCents": s, "cvdCents": cvd, "quality": o.Quality})
		}
		previous = at
		return nil
	})
	out.Coverage = math.Min(1, float64(validSeconds)/to.Sub(from).Seconds())
	out.Boundaries = first.Equal(from) && last.Equal(to)
	out.Partial = out.Partial || out.Coverage < 1 || !out.Boundaries
	return out, err
}
func activityBias(s windowFlow, fresh bool, priceOK bool) (string, *float64) {
	total := s.Buy + s.Sell
	if total <= 0 {
		return "证据不足", nil
	}
	share := float64(s.Buy) / float64(total) * 100
	if !fresh || !priceOK || s.Coverage < .9 || !s.Boundaries {
		return "证据不足", &share
	}
	if share >= 55 {
		return "买方较主动", &share
	}
	if share <= 45 {
		return "卖方较主动", &share
	}
	return "相对均衡", &share
}
func (h *Hub) ActivityView(ctx context.Context, a string, hours int, span float64) (any, error) {
	now := time.Now().UTC()
	fd, _ := h.Dataset(ID("flow", a, "", "spot"))
	flow, flowOK := h.Store.Latest(fd.ID)
	cd, _ := h.Dataset(ID("candles", a, "Binance", "spot"))
	candle, candleOK := h.Store.Latest(cd.ID)
	to := now.Truncate(5 * time.Minute)
	if flowOK {
		to = minTime(to, flow.Time().Add(time.Minute).Truncate(5*time.Minute))
	}
	if candleOK {
		to = minTime(to, candle.Time().Add(5*time.Minute))
	}
	from := to.Add(-time.Duration(hours) * time.Hour)
	stats, err := h.flowWindow(ctx, fd, from, to, from, 60)
	if err != nil {
		return nil, err
	}
	candles := []map[string]any{}
	first, last := 0.0, 0.0
	startOK, endOK := false, false
	count := 0
	err = h.Store.Visit(ctx, cd, 300, from, to, func(o Observation) error {
		if o.Payload.Candle == nil || (o.Quality == "missing" || o.Quality == "partial") {
			return nil
		}
		c := o.Payload.Candle
		t := recordTime(o)
		if t.Equal(from) {
			first = c.Open
			startOK = true
		}
		if t.Add(5 * time.Minute).Equal(to) {
			last = c.Close
			endOK = true
		}
		count++
		candles = append(candles, map[string]any{"time": t.Unix(), "open": c.Open, "close": c.Close, "low": c.Low, "high": c.High})
		return nil
	})
	if err != nil {
		return nil, err
	}
	priceOK := startOK && endOK && first > 0 && last > 0 && count*10 >= hours*12*9 && candleOK && candle.Fresh(cd, now)
	var pct *float64
	reaction := "等待价格窗口补齐"
	if priceOK {
		p := (last/first - 1) * 100
		pct = &p
		reaction = "小幅波动"
		if p > .05 {
			reaction = "价格上涨"
		}
		if p < -.05 {
			reaction = "价格下跌"
		}
	}
	_, _, currentPriceOK := h.CurrentPrice(a, now)
	bias, share := activityBias(stats, flowOK && flow.Fresh(fd, now) && currentPriceOK, priceOK)
	perpD, _ := h.Dataset(ID("flow", a, "", "futures"))
	perp, err := h.flowWindow(ctx, perpD, from, to, from, 60)
	if err != nil {
		return nil, err
	}
	perpLatest, perpOK := h.Store.Latest(perpD.ID)
	derivatives, err := h.derivativesAt(ctx, a, hours, to)
	if err != nil {
		return nil, err
	}
	// Footprint remains a distinct (USDT) three-venue dataset. Only common
	// venues may validate a spot wall; it is not added to five-venue flow.
	feet, err := h.flowViewAt(ctx, a, "spot", hours, from.Format(time.RFC3339), to)
	if err != nil {
		return nil, err
	}
	feetMap := feet.(map[string]any)
	matched := []string{}
	for _, m := range feetMap["footprintSources"].([]map[string]any) {
		if (m["venue"] == "Binance" || m["venue"] == "OKX") && m["status"] == "fresh" {
			matched = append(matched, m["venue"].(string))
		}
	}
	reason := "按已覆盖主动成交描述买卖压力，不代表主力身份或未来涨跌"
	if bias == "证据不足" {
		reason = "需要≥90%有效分钟、完整起止边界及新鲜价格/成交；缺失数据不补零"
	}
	return map[string]any{
		"orderHistoryGap": false, "interpretation": activityInterpretation(bias, pct), "asset": a, "from": from, "to": to, "hours": hours, "bias": bias, "reason": reason, "buyShare": share, "flow": stats, "flowMeta": metadata(fd, flow, flowOK),
		"pressureReaction": pressureReaction(bias, pct), "priceChangePercent": pct, "priceReaction": reaction, "priceQuote": "USDT · 币安5分钟K线", "candles": candles,
		"perpFlow": perp, "perpMeta": metadata(perpD, perpLatest, perpOK), "derivatives": derivatives,
		"footprint": feetMap["footprint"], "footprintVenues": feetMap["footprintVenues"], "footprintPartial": feetMap["footprintPartial"], "footprintQuote": "USDT", "matchedFootprintVenues": matched,
		"orders": []any{}, "orderCount": 0, "orderHasData": false, "orderBidCents": 0, "orderAskCents": 0, "orderSources": []any{},
		"events": []any{}, "eventsHasMore": false, "historyStatus": []any{}, "ordersMovedTo": "large-orders", "rulesVersion": RulesVersion,
		"tradeFeedAvailable": false, "tradeFeedNote": "逐笔大额成交暂未接入；累计成交变化不等于逐笔成交",
		"at": now, "storage": h.Store.Status(),
	}, nil
}
func (h *Hub) orderHistoryStatus(a string) []map[string]any {
	h.Scheduler.mu.Lock()
	defer h.Scheduler.mu.Unlock()
	out := []map[string]any{}
	for _, j := range h.Scheduler.jobs {
		if j.Dataset.Kind == "large-history" && j.Dataset.Asset == a {
			out = append(out, map[string]any{"venue": j.Dataset.Venue, "state": j.Dataset.Params["state"], "from": j.OrderFirst, "through": j.OrderThrough, "pendingPages": len(j.OrderRanges), "gaps": j.OrderGaps, "error": j.Error})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i]["venue"].(string)+out[i]["state"].(string) < out[j]["venue"].(string)+out[j]["state"].(string)
	})
	return out
}

func (h *Hub) LargeOrdersPage(ctx context.Context, a string, history bool, limit, offset int) (any, error) {
	if !history {
		v := h.LargeView(a, false).(map[string]any)
		items := v["items"].([]map[string]any)
		end := min(len(items), offset+limit)
		v["total"] = len(items)
		v["hasMore"] = end < len(items)
		v["offset"] = offset
		v["items"] = items[min(offset, len(items)):end]
		return v, nil
	}
	rows, more, err := h.Store.EndedOrders(ctx, a, limit, offset)
	if err != nil {
		return nil, err
	}
	items := []map[string]any{}
	for _, t := range rows {
		r := t.Order
		end := t.Seen
		if r.End != nil {
			end = *r.End
		}
		start, basis := r.Start, "source_created"
		if start == nil && !t.FirstSeen.IsZero() {
			start = &t.FirstSeen
			basis = "local_observed"
		}
		var duration any
		if start != nil && !end.Before(*start) {
			duration = end.Sub(*start).Seconds()
		}
		items = append(items, map[string]any{"durationThrough": end, "durationBasis": basis, "durationSeconds": duration, "id": t.Key, "venue": t.Venue, "side": r.Side, "price": r.Price, "quote": t.Quote, "quantity": r.Quantity, "usdCents": nil, "reportedUsd": r.ReportedUSD, "executedUsd": r.ExecutedUSD, "state": r.State, "rawState": r.RawState, "startAt": r.Start, "changedAt": r.Changed, "endAt": r.End, "fetchedAt": t.Seen, "valid": true, "historical": true, "trades": r.Trades, "initialQuantity": r.InitialQuantity, "initialUsd": r.InitialUSD, "executedQuantity": r.ExecutedQuantity})
	}
	return map[string]any{"items": items, "hasMore": more, "offset": offset, "historyStatus": h.orderHistoryStatus(a), "note": "结束记录按上游标记展示；残留数量不计当前挂单，累计成交不归入获取时段。历史窗口未完整时不声称没有订单。"}, nil
}

func pressureReaction(bias string, pct *float64) string {
	if pct == nil || bias == "证据不足" {
		return "价格与成交证据尚未齐全"
	}
	if math.Abs(*pct) <= .05 {
		return "价格小幅波动，未形成明显跟随"
	}
	if bias == "相对均衡" {
		return "买卖成交相对均衡，价格另有方向"
	}
	if (bias == "买方较主动" && *pct > 0) || (bias == "卖方较主动" && *pct < 0) {
		return "价格与主动成交方向一致"
	}
	return "价格与主动成交方向背离，单一信号不充分"
}

func activityInterpretation(bias string, pct *float64) map[string]string {
	flow := bias
	if bias == "相对均衡" {
		flow = "买卖力量接近"
	}
	reaction := "等待完整价格窗口"
	if pct != nil {
		switch {
		case bias == "证据不足":
			reaction = "数据不足，暂不判断配合"
		case math.Abs(*pct) <= .05:
			reaction = "价格变化小"
		case bias == "相对均衡":
			reaction = "买卖接近，价格另有方向"
		case bias == "买方较主动" && *pct > 0 || bias == "卖方较主动" && *pct < 0:
			reaction = "成交与价格方向一致"
		default:
			reaction = "成交与价格方向不同"
		}
	}
	return map[string]string{"flow": flow, "price": reaction}
}
