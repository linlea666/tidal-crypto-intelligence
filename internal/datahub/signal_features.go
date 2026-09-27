package datahub

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

const CandidateRules = "flow-candidate-2.5.0"
const EvaluationVersion = "events-2.5.0"

// Features are computed only from closed intervals and frozen at discovery.
// All amounts are integer cents; unavailable evidence remains nil.
type SignalFeatures struct {
	Funding          []Funding    `json:"funding"`
	FundingAt        *time.Time   `json:"fundingAt"`
	Liquidation      *Liquidation `json:"realizedLiquidation1m"`
	LiquidationAt    *time.Time   `json:"liquidationAt"`
	Net1H            int64        `json:"net1hCents"`
	Net4H            int64        `json:"net4hCents"`
	Volume1H         int64        `json:"volume1hCents"`
	BuyShare         float64      `json:"buyShare"`
	VolumeRatio      float64      `json:"volumeRatio"`
	PositiveQuarters int          `json:"positiveQuarters"`
	Return1H         float64      `json:"return1h"`
	PriorATR         float64      `json:"priorAtr1h"`
	Stage            string       `json:"stage"`
	Extended         bool         `json:"extended"`
	FuturesNet1H     *int64       `json:"futuresNet1hCents"`
	OIChange         *float64     `json:"oiChangeUsdPercent"`
	Premium          *float64     `json:"premiumUsd"`
}

type SignalUpgrade struct {
	DetectionPrice *float64       `json:"detectionPriceUsdt"`
	At             time.Time      `json:"at"`
	DataThrough    time.Time      `json:"dataThrough"`
	Features       SignalFeatures `json:"features"`
	Baseline       SignalBaseline `json:"baseline"`
}
type LifecycleRepair struct {
	At            time.Time `json:"at"`
	PreviousState string    `json:"previousState"`
	Reason        string    `json:"reason"`
}

func candidateFeatures(bars map[int64]FlowBar, candles map[int64]Candle, end time.Time, baseline SignalBaseline) (*SignalFeatures, bool) {
	one, ok1 := sumBars(bars, end, 12)
	four, ok4 := sumBars(bars, end, 48)
	atr := hourlyATR(candles, end.Add(-time.Hour))
	prev, pok := candles[end.Add(-time.Hour-5*time.Minute).Unix()]
	last, lok := candles[end.Add(-5*time.Minute).Unix()]
	_, _, _, priceOK := candleBounds(candles, end)
	if !ok1 || !ok4 || !pok || !lok || !priceOK || prev.Close <= 0 || atr == nil || *atr <= 0 || !baseline.Valid || baseline.P95Hour == nil || baseline.Median60 <= 0 {
		return nil, false
	}
	f := &SignalFeatures{Net1H: one.Net(), Net4H: four.Net(), Volume1H: one.Buy + one.Sell, BuyShare: one.Share() * 100, VolumeRatio: float64(one.Buy+one.Sell) / baseline.Median60, Return1H: (last.Close/prev.Close - 1) * 100, PriorATR: *atr, Stage: "range"}
	for i := 0; i < 4; i++ {
		v, _ := sumBars(bars, end.Add(-time.Duration(i)*15*time.Minute), 3)
		if v.Net() > 0 {
			f.PositiveQuarters++
		}
	}
	f.Extended = last.Close-prev.Close > 1.5*(*atr)
	// A breakout already observed within this event horizon is following evidence.
	for t := end.Add(-4 * time.Hour); !t.After(end); t = t.Add(5 * time.Minute) {
		if priceCross(candles, t, "buy") {
			f.Stage = "following"
			break
		}
	}
	return f, true
}
func candidateLevel(f *SignalFeatures, b SignalBaseline, strongCents int64) string {
	if f == nil || b.P95Hour == nil || !b.Valid || f.Net1H < 3_000_000_000 || float64(f.Net1H) < b.P90 || f.BuyShare < 55 || f.VolumeRatio < 1.5 || f.PositiveQuarters < 3 || f.Net4H <= 0 {
		return ""
	}
	if !f.Extended && f.Net1H >= strongCents && float64(f.Net1H) >= *b.P95Hour {
		return "strong"
	}
	return "observe"
}

// Scalar five-minute series cannot be overwritten by a native hourly close.
// Keys are interval END timestamps, so an unfinished interval cannot be read.
func (h *Hub) closedScalars(ctx context.Context, id string, from, to, asOf time.Time) (map[int64]float64, error) {
	out := map[int64]float64{}
	e := h.Store.FactsAsOf(ctx, id, from, to, asOf, func(o Observation) error {
		at := recordTime(o)
		end := at.Add(5 * time.Minute)
		if o.Resolution != 300 || o.Quality != "valid" || at.Unix()%300 != 0 || end.After(to) || end.After(asOf) {
			return nil
		}
		if len(o.Payload.OI) > 0 {
			out[end.Unix()] = num(o.Payload.OI[0].USD)
		}
		if o.Payload.Premium != nil {
			out[end.Unix()] = num(o.Payload.Premium.USD)
		}
		return nil
	})
	return out, e
}
func scalarChange(values map[int64]float64, end time.Time) *float64 {
	x, xok := values[end.Unix()]
	y, yok := values[end.Add(-time.Hour).Unix()]
	if !xok || !yok || y <= 0 {
		return nil
	}
	v := (x/y - 1) * 100
	return &v
}
func (h *Hub) candidateContext(ctx context.Context, a string, end, now time.Time, f *SignalFeatures) error {
	acc := newFlowAccumulator(300)
	if e := h.Store.FactsAsOf(ctx, ID("flow", a, "", "futures"), end.Add(-time.Hour), end, now, func(o Observation) error { acc.add(o); return nil }); e != nil {
		return e
	}
	if v, ok := sumBars(acc.finish(), end, 12); ok {
		n := v.Net()
		f.FuturesNet1H = &n
	}
	oi, e := h.closedScalars(ctx, ID("oi-history", a, "", "futures"), end.Add(-65*time.Minute), end, now)
	if e != nil {
		return e
	}
	f.OIChange = scalarChange(oi, end)
	prem, e := h.closedScalars(ctx, ID("premium", a, "Coinbase", "spot"), end.Add(-5*time.Minute), end, now)
	if e != nil {
		return e
	}
	if v, ok := prem[end.Unix()]; ok {
		f.Premium = &v
	}
	fd, _ := h.Dataset(ID("funding", "ALL", "", "futures"))
	if o, ok := h.Store.Latest(fd.ID); ok && o.Quality == "valid" && o.Fresh(fd, now) {
		at := o.Time()
		f.FundingAt = &at
		for _, r := range o.Payload.Funding {
			if r.Asset == a {
				f.Funding = append(f.Funding, r)
			}
		}
	}
	ld, _ := h.Dataset(ID("liquidations", a, "", "futures"))
	if o, ok := h.Store.Latest(ld.ID); ok && o.Quality == "valid" && o.Resolution == 60 && o.Fresh(ld, now) && !o.Time().Add(time.Minute).After(now) && o.Payload.Liquidation != nil {
		at := o.Time()
		f.LiquidationAt = &at
		v := *o.Payload.Liquidation
		f.Liquidation = &v
	}
	return nil
}
func (h *Hub) processCandidate(ctx context.Context, a string, state *signalState, bars map[int64]FlowBar, candles map[int64]Candle, baseline SignalBaseline, end, now time.Time, updates *[]Signal) error {
	f, ok := candidateFeatures(bars, candles, end, baseline)
	if !ok {
		return nil
	}
	level := candidateLevel(f, baseline, 5_000_000_000)
	if level == "" {
		return nil
	}
	var existing Signal
	id := state.Active["candidate-buy"]
	if id != "" {
		if e := h.Store.document(ctx, "signal", id, &existing); e != nil && e != sql.ErrNoRows {
			return e
		}
		// Lifecycle and upgrade updates must compose in the same transaction.
		for _, s := range *updates {
			if s.ID == id {
				existing = s
			}
		}
		if now.Before(existing.Expires) {
			if level == "strong" && existing.Upgrade == nil {
				if e := h.candidateContext(ctx, a, end, now, f); e != nil {
					return e
				}
				existing.Level = "strong"
				existing.Updated = now
				existing.Upgrade = &SignalUpgrade{At: now, DataThrough: end, Features: *f, Baseline: baseline, DetectionPrice: h.signalDetectionPrice(a, now)}
				for i, s := range *updates {
					if s.ID == id {
						(*updates)[i] = existing
						return nil
					}
				}
				*updates = append(*updates, existing)
			}
			return nil
		}
	}
	if e := h.candidateContext(ctx, a, end, now, f); e != nil {
		return e
	}
	hi, lo, price, _ := candleBounds(candles, end)
	v, _ := sumBars(bars, end, 3)
	delay := now.Sub(end).Seconds()
	s := Signal{ID: fmt.Sprintf("%s-candidate-buy-%d", a, end.Unix()), Asset: a, Direction: "buy", Rules: CandidateRules, Pattern: "candidate", State: "anomaly", Level: level, At: now, Updated: now, DataThrough: end, Expires: now.Add(4 * time.Hour), FrozenHigh: hi, FrozenLow: lo, ReferencePrice: price, ATR: &f.PriorATR, Net15: v.Net(), BuyShare: f.BuyShare, Baseline: baseline, Features: f, DetectionDelaySeconds: &delay, Evaluation: EvaluationVersion, Evidence: []string{"美元主动净买入、历史分位、量比、四段持续性同时满足固定候选规则"}, Conflicts: []string{"站内观察；尚未证明能够预测未来1–4小时持续拉升"}, Missing: []string{"合约成交与美元OI仅作背景；美元OI变化包含价格影响，不能单独解释为开多", "资金费率、已发生清算见独立合约背景；不重复计分"}}
	if f.Stage == "following" {
		s.Conflicts = append(s.Conflicts, "已发生区间突破，属于跟随，不能称启动前预警")
	}
	if f.Extended {
		s.Conflicts = append(s.Conflicts, "一小时位移超过此前小时ATR的1.5倍，不升为强候选")
	}
	if level == "strong" {
		s.Upgrade = &SignalUpgrade{At: now, DataThrough: end, Features: *f, Baseline: baseline, DetectionPrice: h.signalDetectionPrice(a, now)}
	}
	pd, _ := h.Dataset(ID("price", a, "Binance", "spot"))
	if p, ok := h.Store.Latest(pd.ID); ok && p.Fresh(pd, now) && p.Payload.Price != nil {
		v := num(p.Payload.Price.Value)
		s.DetectionPrice = &v
	}
	state.Active["candidate-buy"] = s.ID
	*updates = append(*updates, s)
	return nil
}

func (h *Hub) signalDetectionPrice(a string, now time.Time) *float64 {
	d, _ := h.Dataset(ID("price", a, "Binance", "spot"))
	if o, ok := h.Store.Latest(d.ID); ok && o.Fresh(d, now) && o.Payload.Price != nil {
		v := num(o.Payload.Price.Value)
		if v > 0 {
			return &v
		}
	}
	return nil
}
