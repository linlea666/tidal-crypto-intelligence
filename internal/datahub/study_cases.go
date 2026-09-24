package datahub

import (
	"context"
	"time"
)

// Case windows are user-selected Chinese calendar dates, not retrospectively
// selected exact breakout starts. Missing minute history still permits display
// of daily wallet observations, without calling them a minute-level backtest.
func (h *Hub) studyCases(ctx context.Context, s Study, bars map[int64]FlowBar, candles map[int64]Candle, now time.Time) []map[string]any {
	out := []map[string]any{}
	for _, date := range []string{"2026-08-19", "2026-09-03", "2026-09-18"} {
		d, _ := time.ParseInLocation("2006-01-02", date, time.FixedZone("CST", 8*3600))
		from, to := d.Add(-48*time.Hour), d.Add(72*time.Hour)
		c := map[string]any{"date": date, "from": from, "to": to, "observations": nil, "independentValidation": false, "wallet": []BalanceChange{}, "hourly": []map[string]any{}, "note": "自然日观察窗口；尚未定义精确拉升起点。未知历史发布时间，只作关联。"}
		if from.Before(s.From) || to.After(s.To) {
			c["note"] = "所选研究未覆盖完整案例窗口"
			out = append(out, c)
			continue
		}
		balances := []Observation{}
		e := h.Store.FactsAsOf(ctx, ID("balance-history", s.Asset, "", "chain"), from.Add(-48*time.Hour), to, now, func(o Observation) error { balances = append(balances, o); return nil })
		if e != nil {
			c["walletError"] = "钱包事实暂不可读取"
		}
		changes := []BalanceChange{}
		for i := 1; i < len(balances); i++ {
			if balances[i].Time().Before(from) {
				continue
			}
			changes = append(changes, comparableBalances(balances[i-1], balances[i]))
		}
		c["wallet"] = changes
		hourly := []map[string]any{}
		for end := from.Add(time.Hour); !end.After(to); end = end.Add(time.Hour) {
			v, ok := sumBars(bars, end, 12)
			var net *int64
			if ok {
				n := v.Net()
				net = &n
			}
			var change *float64
			start, ok1 := candles[end.Add(-time.Hour).Unix()]
			last, ok2 := candles[end.Add(-5*time.Minute).Unix()]
			if ok1 && ok2 && start.Open > 0 {
				p := (last.Close/start.Open - 1) * 100
				change = &p
			}
			hourly = append(hourly, map[string]any{"end": end, "netCents": net, "priceChange": change, "flowComplete": ok})
		}
		c["hourly"] = hourly
		out = append(out, c)
	}
	return out
}
