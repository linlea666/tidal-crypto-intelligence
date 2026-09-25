package datahub

import (
	"context"
	"time"
)

// Cases use closed hourly intervals. An hourly observation is never counted as
// twelve five-minute samples; hourly and strict strategy coverage stay separate.
func (h *Hub) studyCases(ctx context.Context, s Study, bars map[int64]FlowBar, candles map[int64]Candle, now time.Time) []map[string]any {
	out := []map[string]any{}
	for _, date := range []string{"2026-08-19", "2026-09-03", "2026-09-18"} {
		d, _ := time.ParseInLocation("2006-01-02", date, time.FixedZone("CST", 8*3600))
		from, to := d.Add(-48*time.Hour), d.Add(72*time.Hour)
		c := map[string]any{"date": date, "from": from, "to": to, "observations": nil, "independentValidation": false, "wallet": []BalanceChange{}, "hourly": []map[string]any{}, "state": "unavailable", "note": "北京时间自然日观察窗口；没有指定精确启动时刻。历史发布时间未知，仅分析关联。"}
		if from.Before(s.From) || to.After(s.To) {
			c["note"] = "所选研究未覆盖完整案例窗口"
			out = append(out, c)
			continue
		}
		hourlyFlow := map[int64]FlowBar{}
		oi := map[int64]float64{}
		premium := map[int64]float64{}
		errs := []string{}
		for _, kind := range []string{"flow", "oi-history", "premium"} {
			market, venue := "spot", ""
			if kind == "oi-history" {
				market = "futures"
			}
			if kind == "premium" {
				venue = "Coinbase"
			}
			e := h.Store.FactsAsOf(ctx, ID(kind, s.Asset, venue, market), from.Add(-time.Hour), to, now, func(o Observation) error {
				if o.Resolution != 3600 || o.Quality != "valid" || recordTime(o).Unix()%3600 != 0 {
					return nil
				}
				at := recordTime(o).Unix()
				switch kind {
				case "flow":
					if o.Payload.Flow != nil {
						hourlyFlow[at] = FlowBar{recordTime(o), money(o.Payload.Flow.Buy), money(o.Payload.Flow.Sell)}
					}
				case "oi-history":
					if len(o.Payload.OI) > 0 {
						oi[at] = num(o.Payload.OI[0].USD)
					}
				case "premium":
					if o.Payload.Premium != nil {
						premium[at] = num(o.Payload.Premium.USD)
					}
				}
				return nil
			})
			if e != nil {
				errs = append(errs, kind+": 本地事实读取失败")
			}
		}
		balances := []Observation{}
		if e := h.Store.FactsAsOf(ctx, ID("balance-history", s.Asset, "", "chain"), from.Add(-48*time.Hour), to, now, func(o Observation) error { balances = append(balances, o); return nil }); e != nil {
			errs = append(errs, "钱包事实读取失败")
		}
		changes := []BalanceChange{}
		for i := 1; i < len(balances); i++ {
			if !balances[i].Time().Before(from) {
				changes = append(changes, comparableBalances(balances[i-1], balances[i]))
			}
		}
		c["wallet"] = changes
		hourly := []map[string]any{}
		nf, np, no, nv, fine := 0, 0, 0, 0, 0
		for end := from.Add(time.Hour); !end.After(to); end = end.Add(time.Hour) {
			start := end.Add(-time.Hour)
			key := start.Unix()
			flow, ok := sumBars(bars, end, 12)
			res := 300
			if !ok {
				flow, ok = hourlyFlow[key]
				res = 3600
			} else {
				fine++
			}
			var net *int64
			if ok {
				n := flow.Net()
				net = &n
				nf++
			}
			var change *float64
			first, ok1 := candles[key]
			last, ok2 := candles[end.Add(-5*time.Minute).Unix()]
			complete := ok1 && ok2 && first.Open > 0
			for at := start; at.Before(end) && complete; at = at.Add(5 * time.Minute) {
				_, complete = candles[at.Unix()]
			}
			if complete {
				v := (last.Close/first.Open - 1) * 100
				change = &v
				np++
			}
			var delta, prem *float64
			x, xok := oi[key]
			y, yok := oi[start.Add(-time.Hour).Unix()]
			if xok && yok && y > 0 {
				v := (x/y - 1) * 100
				delta = &v
				no++
			}
			if v, yes := premium[key]; yes {
				prem = &v
				nv++
			}
			hourly = append(hourly, map[string]any{"end": end, "netCents": net, "priceChange": change, "priceClose": func() any {
				if complete {
					return last.Close
				}
				return nil
			}(), "flowComplete": ok, "flowResolutionSeconds": res, "oiChange": delta, "premiumUsd": prem})
		}
		c["hourly"], c["errors"] = hourly, errs
		expected := len(hourly)
		c["coverage"] = map[string]any{"expectedHours": expected, "flowHours": nf, "fineFlowHours": fine, "priceHours": np, "oiHours": no, "premiumHours": nv}
		if nf == expected && np == expected {
			c["state"] = "hourly_available"
		} else if nf+np+len(changes) > 0 {
			c["state"] = "partial"
		}
		if nf == 0 {
			c["flowNote"] = "此窗口尚无完整小时成交；请查看下方补采状态。价格和钱包可独立查看。"
		} else if fine < nf {
			c["flowNote"] = "部分或全部成交使用上游小时数据，只作小时关联分析，不计入五分钟策略覆盖。"
		} else {
			c["flowNote"] = "小时成交由完整五分钟记录汇总。"
		}
		out = append(out, c)
	}
	return out
}
