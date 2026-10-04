package datahub

import (
	"context"
	"database/sql"
	"encoding/json"
	"github.com/shopspring/decimal"
	"time"
)

type CostEvidence struct {
	Source   string    `json:"source"`
	Kind     string    `json:"kind"`
	Title    string    `json:"title"`
	Status   string    `json:"status"`
	From     string    `json:"from"`
	To       string    `json:"to"`
	AsOf     time.Time `json:"asOf"`
	Coverage *float64  `json:"coverage"`
	Data     any       `json:"data"`
	Note     string    `json:"note"`
}

func (h *Hub) costEvidence(ctx context.Context, date string, asOf time.Time) []CostEvidence {
	day, e := costDay(date)
	if e != nil {
		return []CostEvidence{}
	}
	end := day.AddDate(0, 0, 1)
	out := []CostEvidence{}
	// Each result remains independent; missing futures/ETF cannot invent a spot fact.
	for _, spec := range []struct{ kind, title, id string }{
		{"flow", "已完成日现货主动成交", ID("flow", "BTC", "", "spot")},
		{"oi", "BTC折算OI变化", ID("oi-coin-history", "BTC", "", "futures")},
		{"oi_usd", "OI美元名义价值变化", ID("oi-history", "BTC", "", "futures")},
		{"funding", "资金费率（按类型和周期）", ID("funding", "ALL", "", "futures")},
		{"liquidations", "已发生清算", ID("liquidations", "BTC", "", "futures")},
		{"etf", "ETF实际报告日", ID("etf", "BTC", "", "fund")},
	} {
		v := CostEvidence{Source: "CoinGlass · 本地实际留存", Kind: spec.kind, Title: spec.title, Status: "missing", From: date, To: costDate(end), AsOf: asOf, Note: "此日期在当时可得的本地证据不足"}
		acc := newFlowAccumulator(300)
		var oiStart, oiEnd *string
		var funding []Funding
		var fundingAt *time.Time
		var etf *ETFRecord
		from := day
		if spec.kind == "oi" || spec.kind == "oi_usd" {
			from = day.Add(-5 * time.Minute)
		}
		err := h.Store.FactsAsOf(ctx, spec.id, from, end, asOf, func(o Observation) error {
			if o.Quality != "valid" {
				return nil
			}
			at := recordTime(o)
			through := at.Add(time.Duration(o.Resolution) * time.Second)
			switch spec.kind {
			case "flow":
				if !at.Before(day) && !through.After(end) {
					if o.Payload.Flow != nil && (dec(o.Payload.Flow.Buy).Abs().GreaterThan(dec("90000000000000")) || dec(o.Payload.Flow.Sell).Abs().GreaterThan(dec("90000000000000"))) {
						return nil
					}
					acc.add(o)
				}
			case "liquidations":
				if o.Payload.Liquidation != nil && !at.Before(day) && !through.After(end) {
					if dec(o.Payload.Liquidation.Long).Abs().GreaterThan(dec("90000000000000")) || dec(o.Payload.Liquidation.Short).Abs().GreaterThan(dec("90000000000000")) {
						return nil
					}
					o.Payload.Flow = &Flow{Buy: o.Payload.Liquidation.Long, Sell: o.Payload.Liquidation.Short}
					acc.add(o)
				}
			case "oi", "oi_usd":
				if o.Resolution == 300 && len(o.Payload.OI) == 1 {
					x := o.Payload.OI[0].USD
					if spec.kind == "oi" {
						if o.Payload.OI[0].USD != "" {
							return nil
						}
						x = o.Payload.OI[0].Base
					}
					if x == "" {
						return nil
					}
					if through.Equal(day) {
						oiStart = &x
					}
					if through.Equal(end) {
						oiEnd = &x
					}
				}
			case "funding":
				if o.ObservedAt != nil && !o.ObservedAt.Before(day) && o.ObservedAt.Before(end) && (fundingAt == nil || o.ObservedAt.After(*fundingAt)) {
					funding = []Funding{}
					for _, f := range o.Payload.Funding {
						if f.Asset == "BTC" {
							funding = append(funding, f)
						}
					}
					t := *o.ObservedAt
					fundingAt = &t
				}
			case "etf":
				if o.Payload.ETF != nil && !at.Before(day) && at.Before(end) {
					etf = o.Payload.ETF
				}
			}
			return nil
		})
		if err != nil {
			v.Note = "证据读取未完成，未使用部分计算结果"
			out = append(out, v)
			continue
		}
		switch spec.kind {
		case "flow", "liquidations":
			bars := acc.finish()
			buy, sell := decimal.Zero, decimal.Zero
			covered := 0
			for ts, b := range bars {
				if ts >= day.Unix() && ts < end.Unix() {
					buy = buy.Add(decimal.NewFromInt(b.Buy))
					sell = sell.Add(decimal.NewFromInt(b.Sell))
					covered++
				}
			}
			longest, gap := 0, 0
			for ts := day.Unix(); ts < end.Unix(); ts += 300 {
				if _, ok := bars[ts]; !ok {
					gap++
					if gap > longest {
						longest = gap
					}
				} else {
					gap = 0
				}
			}
			coverage := float64(covered) / 288
			v.Coverage = &coverage
			if covered > 0 {
				v.Status = "partial"
				v.Data = map[string]any{"buyUsd": buy.Div(dec("100")).String(), "sellUsd": sell.Div(dec("100")).String(), "netUsd": buy.Sub(sell).Div(dec("100")).String(), "timeCoverage": coverage, "longestGapMinutes": longest * 5, "actualSources": nil, "sourceCoverage": nil, "messageCompleteness": nil, "sourceSetChanged": nil}
				if coverage >= .95 {
					v.Status = "available"
				}
				v.Note = "已覆盖来源的主动成交，不是链上充值/提现；完整5分钟格覆盖率"
				if spec.kind == "flow" {
					// This aggregate contract names five configured exchanges but has
					// no per-exchange completeness field. Temporal coverage alone
					// cannot establish the required common-source 95% coverage.
					v.Status = "partial"
					v.Note = "聚合主动成交，配置为Binance/OKX/Coinbase/Kraken/Bitfinex；展示时间格覆盖率，实际共同来源覆盖未核实，不能作为完整方向证据；不是充值/提现"
				}
				if spec.kind == "liquidations" {
					v.Data = map[string]any{"longUsd": buy.Div(dec("100")).String(), "shortUsd": sell.Div(dec("100")).String()}
					v.Note = "已发生清算美元金额；与模型强度分离"
				}
			}
		case "oi", "oi_usd":
			if oiStart != nil && oiEnd != nil {
				v.Status = "available"
				v.Data = map[string]any{"startBTC": *oiStart, "endBTC": *oiEnd, "changeBTC": dec(*oiEnd).Sub(dec(*oiStart)).String(), "nativeContracts": nil, "contractType": nil, "contractSize": nil, "unit": "BTC-equivalent"}
				v.Note = "BTC折算OI；原生合约数量、合约类型与面值未核实，不能单独推断增减仓；两日来源可比性仅限此聚合序列"
				if spec.kind == "oi_usd" {
					v.Data = map[string]any{"startUsd": *oiStart, "endUsd": *oiEnd, "changeUsd": dec(*oiEnd).Sub(dec(*oiStart)).String()}
					v.Note = "美元名义价值同时受价格和合约数量影响，独立展示，不作为BTC增减仓证据"
				}
			}
		case "funding":
			if len(funding) > 0 {
				v.Status = "partial"
				v.Data = map[string]any{"items": funding, "observedAt": fundingAt}
				v.Note = "保留原始计费周期与类型；未知类型或周期不作可比判断，不跨交易所平均"
				all := true
				for _, f := range funding {
					if fundingKey(f) == "" {
						all = false
					}
				}
				if all {
					v.Status = "available"
				}
			}
		case "etf":
			if etf != nil {
				v.Status = "partial"
				if etf.Reconciled && etf.ExplicitFinal && etf.USD != nil {
					v.Status = "available"
				}
				v.Data = etf
				v.Note = "按实际报告日及来源完整状态展示；非交易日不填零"
			}
		}
		out = append(out, v)
	}
	return out
}
func (h *Hub) costFrozenEvidence(ctx context.Context, date string, asOf time.Time) ([]CostEvidence, error) {
	var b []byte
	e := h.Store.onchain.db.QueryRowContext(ctx, "SELECT payload FROM evidence WHERE day=? AND as_of<=? ORDER BY as_of DESC LIMIT 1", date, asOf.UnixNano()).Scan(&b)
	if e == nil {
		var out []CostEvidence
		e = json.Unmarshal(b, &out)
		return out, e
	}
	if e != sql.ErrNoRows {
		return nil, e
	}
	// Historical reads may compute from facts actually available at the selected cutoff,
	// but GET never persists and never uses today's facts for an old date.
	return h.costEvidence(ctx, date, asOf), nil
}
