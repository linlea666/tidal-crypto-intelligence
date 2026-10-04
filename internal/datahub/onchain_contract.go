package datahub

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"time"
)

func costClose(day string) time.Time { t, _ := costDay(day); return t.AddDate(0, 0, 1) }
func costDistributionRevision(f CostFrame) string {
	return costHash(struct {
		Method   string
		STH, LTH CostCohort
	}{f.Method, f.STH, f.LTH})
}
func (s *costStore) price(ctx context.Context, day string, asOf time.Time) (*CostPrice, error) {
	var p CostPrice
	var seen int64
	e := s.db.QueryRowContext(ctx, `SELECT day,revision,first_seen,value FROM prices WHERE day=? AND first_seen<=? ORDER BY first_seen DESC,revision DESC LIMIT 1`, day, asOf.UnixNano()).Scan(&p.Date, &p.Revision, &seen, &p.Value)
	if e == sql.ErrNoRows {
		return nil, nil
	}
	if e != nil {
		return nil, e
	}
	p.FirstSeen = time.Unix(0, seen).UTC()
	end := costClose(day)
	p.IntervalEnd = &end
	p.Role = "daily_close"
	p.Source = onchainSource
	p.Completion = "completed"
	var raw []byte
	e = s.db.QueryRowContext(ctx, "SELECT payload FROM price_meta WHERE day=? AND revision=?", p.Date, p.Revision).Scan(&raw)
	if e == nil {
		e = json.Unmarshal(raw, &p)
	}
	if e != nil && e != sql.ErrNoRows {
		return nil, e
	}
	return &p, nil
}
func costContract() any {
	return map[string]any{
		"version": onchainMethod, "description": onchainSourceDescription, "units": "BTC", "bucketInterval": "[low,high)",
		"cohorts":          "最近155个仍有正余额的取得日分组 / 更早分组；来源声明互斥分组",
		"entityAdjustment": nil, "specialAddressExclusions": nil, "denominator": "同日来源声明STH与LTH总量之和，独立核对原始桶总量",
		"completedDayLabel": "UTC日初日期标签；日收盘发生于次日00:00；已完成不等于不可修订",
		"ranks":             "此前52个周日，至少42点；经验CDF使用≤，当前点排除；集中度下限排名=count(历史上限≤当前下限)/N，上限排名=count(历史下限≤当前上限)/N",
		"priceRoles":        []string{"snapshot_reference", "daily_close", "market_reference"},
	}
}
func (s *costStore) storageDetail(ctx context.Context, now time.Time) any {
	sizes := map[string]int64{}
	if s == nil {
		return map[string]any{"files": sizes}
	}
	for _, x := range []struct{ key, suffix string }{{"databaseBytes", ""}, {"walBytes", "-wal"}, {"shmBytes", "-shm"}} {
		if st, e := os.Stat(s.path + x.suffix); e == nil {
			sizes[x.key] = st.Size()
		}
	}
	if s.available() != nil {
		return map[string]any{"files": sizes}
	}
	var baseline struct {
		At    time.Time `json:"at"`
		Bytes int64     `json:"bytes"`
	}
	_ = costLoad(ctx, s.db, "growth-baseline", &baseline)
	var daily, days *float64
	if !baseline.At.IsZero() && now.Sub(baseline.At) >= 24*time.Hour {
		d := float64(s.bytes()-baseline.Bytes) / (now.Sub(baseline.At).Hours() / 24)
		daily = &d
		if d > 0 {
			n := float64(onchainBudget*95/100-s.bytes()) / d
			days = &n
		}
	}
	return map[string]any{"files": sizes, "controlReserveBytes": onchainBudget * 5 / 100, "growthBytesPerDay": daily, "estimatedDaysToPause": days, "estimateNote": "按实际净增长估算，不保证固定保留年限；不足24小时或无增长时不外推"}
}

func costSubtractBounds(a, b CostBounds) CostBounds {
	return CostBounds{dec(a.Lower).Sub(dec(b.Upper)).String(), dec(a.Upper).Sub(dec(b.Lower)).String()}
}
func (h *Hub) costUpgradeView(ctx context.Context, f CostFrame, state costState, historical bool, now time.Time) any {
	asOf := now
	if historical {
		asOf = costClose(f.Date).Add(6 * time.Hour)
		if asOf.After(now) {
			asOf = now
		}
	}
	priceDay := f.Date
	if !historical {
		priceDay = costDate(now.AddDate(0, 0, -1))
	}
	daily, _ := h.Store.onchain.price(ctx, priceDay, asOf)
	var market any
	var four costReference
	if !historical {
		if p, at, ok := h.CurrentPrice("BTC", now); ok {
			market = map[string]any{"value": fmtCostFloat(p), "at": at, "source": "Binance BTCUSDT × 当前有效Kraken USDT/USD", "role": "market_reference"}
		}
		_ = costLoad(ctx, h.Store.onchain.db, "four-hour-reference", &four)
	}
	cases := []CostCase{}
	currentDaily, _ := h.Store.onchain.price(ctx, costDate(now.AddDate(0, 0, -1)), now)
	for _, c := range state.Cases {
		if !c.Shadow {
			c.Quality = "unknown"
			if c.State == "watching" && c.LastDate == "" && now.Before(costClose(c.From).Add(6*time.Hour)) {
				c.Quality = "waiting"
			}
			if currentDaily != nil && c.LastDate == currentDaily.Date {
				c.Quality = "available"
			}
			cases = append(cases, c)
		}
	}
	var decomposition any
	previous, e := h.Store.onchain.frame(ctx, costDayAdd(f.Date, -1), asOf)
	if e == nil && previous != nil && previous.Method == f.Method {
		a := costConcentration(*previous, "5")
		shifted := *previous
		shifted.Price = f.Price
		b := costConcentration(shifted, "5")
		c := costConcentration(f, "5")
		decomposition = map[string]any{"from": previous.Date, "to": f.Date, "windowEffect": costSubtractBounds(b, a), "distributionEffect": costSubtractBounds(c, b), "total": costSubtractBounds(c, a), "note": "百分点；分布效应包含分母变化。上下限为保守范围；两项共享中间项，不能逐项相加当成精确值；不代表买卖。"}
	}
	var capabilities any = map[string]any{"structure": false, "dailyPrice": daily != nil, "fourHourPrice": false, "note": "历史模式不附加今天的判断能力"}
	if !historical {
		capabilities = h.costCapabilities(ctx, now)
	}
	return map[string]any{"contract": costContract(), "capabilities": capabilities, "dailyPrice": daily, "marketPrice": market, "fourHourReference": four, "cases": cases, "decomposition": decomposition, "nextAction": "接近边界关注后续收盘；待确认不等于趋势；连续两日条件成立后仍需检查偏离距离、缺失证据和返回区间风险。"}
}

func (h *Hub) costMailReadiness(ctx context.Context, now time.Time) any {
	reason := ""
	switch {
	case h.mail == nil:
		reason = "SMTP尚未配置"
	case h.offline:
		reason = "本地离线模式，不发送邮件"
	case h.onchainDisabled || h.onchainEventsDisabled:
		reason = "链上采集或事件任务已关闭"
	case h.Store.onchain.writable() != nil:
		reason = "链上存储暂停写入，暂停新邮件"
	default:
		p, e := h.Store.onchain.price(ctx, costDate(now.AddDate(0, 0, -1)), now)
		f, fe := h.costFeed(ctx)
		status, _ := costFeedStatus(f, now)
		if (e != nil || p == nil) && (fe != nil || status != "fresh" || f.LastDate != costDate(now.AddDate(0, 0, -1))) {
			reason = "当前所需日线输入不足，等待合格价格或结构"
		}
	}
	ready := reason == ""
	if ready {
		reason = "发送能力可用；仍需新事件和发送前核验，共享每小时6次限额"
	}
	return map[string]any{"ready": ready, "reason": reason}
}
