package datahub

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"
)

type OIContext struct {
	Coin1H *float64 `json:"coinChange1h"`
	Coin4H *float64 `json:"coinChange4h"`
	USD1H  *float64 `json:"usdChange1h"`
	USD4H  *float64 `json:"usdChange4h"`
	AbsP75 *float64 `json:"absChangeP75"`
	Regime string   `json:"regime"`
}
type FundingPoint struct {
	Funding
	ObservedAt *time.Time `json:"observedAt"`
	FetchedAt  time.Time  `json:"fetchedAt"`
	P95        *float64   `json:"p95"`
	P05        *float64   `json:"p05"`
	Change     *float64   `json:"changePercentagePoints"`
	Comparable bool       `json:"comparable"`
	Samples    int        `json:"samples"`
}
type LiquidationWindow struct {
	From     time.Time `json:"from"`
	To       time.Time `json:"to"`
	Coverage float64   `json:"coverage"`
	Long     *int64    `json:"longCents"`
	Short    *int64    `json:"shortCents"`
	LongP95  *float64  `json:"longP95Cents"`
	ShortP95 *float64  `json:"shortP95Cents"`
}
type DerivativeContext struct {
	Futures      map[string]FlowWindow        `json:"futures"`
	OI           OIContext                    `json:"oi"`
	Funding      []FundingPoint               `json:"funding"`
	Liquidations map[string]LiquidationWindow `json:"liquidations"`
}

func (c DerivativeContext) fundingWarning(side string) (known, crowded bool) {
	venues, high := map[string]bool{}, map[string]bool{}
	for _, f := range c.Funding {
		if !f.Comparable || f.P95 == nil || f.P05 == nil || (f.Venue != "Binance" && f.Venue != "OKX" && f.Venue != "Bybit") {
			continue
		}
		venues[f.Venue] = true
		rate := num(f.RatePercent)
		if side == "buy" && rate > 0 && rate > *f.P95 || side == "sell" && rate < 0 && rate < *f.P05 {
			high[f.Venue] = true
		}
	}
	return len(venues) >= 2, len(high) >= 2
}

type fundingBaseline struct {
	P95     *float64 `json:"p95"`
	P05     *float64 `json:"p05"`
	Samples int      `json:"samples"`
	Valid   bool     `json:"valid"`
}
type contextBaseline struct {
	To           time.Time                    `json:"to"`
	Version      string                       `json:"version"`
	Futures      SignalBaseline               `json:"futures"`
	OIAbsP75     *float64                     `json:"oiAbsP75"`
	Funding      map[string]fundingBaseline   `json:"funding"`
	Liquidations map[string]LiquidationWindow `json:"liquidations"`
}
type contextSeries struct {
	Futures      map[int64]FlowBar
	Coin, USD    map[int64]float64
	Liquidations map[int64]FlowBar // Buy = realized long liquidation; Sell = realized short.
	Funding      []Observation
}

func (h *Hub) contextSeries(ctx context.Context, a string, from, to, asOf time.Time) (contextSeries, error) {
	s := contextSeries{Futures: map[int64]FlowBar{}, Coin: map[int64]float64{}, USD: map[int64]float64{}, Liquidations: map[int64]FlowBar{}, Funding: []Observation{}}
	for _, kind := range []string{"flow", "liquidations", "oi-history", "oi-coin-history", "funding"} {
		asset := a
		if kind == "funding" {
			asset = "ALL"
		}
		acc := newFlowAccumulator(300)
		err := h.Store.FactsAsOf(ctx, ID(kind, asset, "", "futures"), from, to, asOf, func(o Observation) error {
			at := recordTime(o)
			if o.Quality != "valid" {
				return nil
			}
			switch kind {
			case "flow":
				if !at.Add(time.Duration(o.Resolution) * time.Second).After(to) {
					acc.add(o)
				}
			case "liquidations":
				if o.Payload.Liquidation != nil && !at.Add(time.Duration(o.Resolution)*time.Second).After(to) {
					o.Payload.Flow = &Flow{Buy: o.Payload.Liquidation.Long, Sell: o.Payload.Liquidation.Short}
					acc.add(o)
				}
			case "oi-history", "oi-coin-history":
				end := at.Add(5 * time.Minute)
				if o.Resolution != 300 || at.Unix()%300 != 0 || end.After(to) || end.After(asOf) || len(o.Payload.OI) != 1 {
					return nil
				}
				if kind == "oi-history" && o.Payload.OI[0].USD != "" {
					s.USD[end.Unix()] = num(o.Payload.OI[0].USD)
				}
				if kind == "oi-coin-history" && o.Payload.OI[0].Base != "" && o.Payload.OI[0].USD == "" {
					s.Coin[end.Unix()] = num(o.Payload.OI[0].Base)
				}
			case "funding":
				if !o.FetchedAt.After(asOf) {
					// Keep only the six possible comparison series in working memory.
					// The warehouse retains the complete original observation.
					selected := []Funding{}
					for _, f := range o.Payload.Funding {
						if f.Asset == a && (f.Venue == "Binance" || f.Venue == "OKX" || f.Venue == "Bybit") {
							selected = append(selected, f)
						}
					}
					o.Payload = Payload{Funding: selected}
					s.Funding = append(s.Funding, o)
				}
			}
			return nil
		})
		if err != nil {
			return s, err
		}
		if kind == "flow" {
			s.Futures = acc.finish()
		}
		if kind == "liquidations" {
			s.Liquidations = acc.finish()
		}
	}
	return s, nil
}
func fundingKey(f Funding) string {
	if f.Hours == nil || *f.Hours <= 0 || (f.RateKind != "settled" && f.RateKind != "predicted") {
		return ""
	}
	return fmt.Sprintf("%s/%s/%s/%s/%g", f.Asset, f.Venue, f.Margin, f.RateKind, *f.Hours)
}
func changeOver(values map[int64]float64, end time.Time, hours int) *float64 {
	x, xok := values[end.Unix()]
	y, yok := values[end.Add(-time.Duration(hours)*time.Hour).Unix()]
	if !xok || !yok || y <= 0 {
		return nil
	}
	return flowPtr((x/y - 1) * 100)
}
func makeContextBaseline(series contextSeries, from, to, now time.Time) contextBaseline {
	b := contextBaseline{To: to, Futures: baselineFromBars(series.Futures, from, to, now), Funding: map[string]fundingBaseline{}, Liquidations: map[string]LiquidationWindow{}}
	oi := []float64{}
	dates := map[string]int{}
	for t := from.Add(time.Hour); !t.After(to); t = t.Add(5 * time.Minute) {
		if v := changeOver(series.Coin, t, 1); v != nil {
			oi = append(oi, math.Abs(*v))
			dates[t.Format("2006-01-02")]++
		}
	}
	validDays := 0
	for _, n := range dates {
		if n >= 274 {
			validDays++
		}
	}
	expected := int(to.Sub(from.Add(time.Hour))/(5*time.Minute)) + 1
	if expected > 0 && float64(len(oi))/float64(expected) >= .95 && validDays >= 21 {
		b.OIAbsP75 = flowPtr(percentile(oi, .75))
	}
	for _, m := range []int{15, 60} {
		long, short := []float64{}, []float64{}
		days := map[string]int{}
		for t := from.Add(time.Duration(m) * time.Minute); !t.After(to); t = t.Add(5 * time.Minute) {
			if x, ok := sumBars(series.Liquidations, t, m/5); ok {
				long = append(long, float64(x.Buy))
				short = append(short, float64(x.Sell))
				days[t.Format("2006-01-02")]++
			}
		}
		validDays = 0
		for _, n := range days {
			if n >= 274 {
				validDays++
			}
		}
		expected = int((to.Sub(from)-time.Duration(m)*time.Minute)/(5*time.Minute)) + 1
		w := LiquidationWindow{}
		if expected > 0 && float64(len(long))/float64(expected) >= .95 && validDays >= 21 {
			w.LongP95 = flowPtr(percentile(long, .95))
			w.ShortP95 = flowPtr(percentile(short, .95))
		}
		b.Liquidations[fmt.Sprint(m)] = w
	}
	// One genuine observation per ten-minute sampling bucket; no forward fill.
	groups := map[string]map[int64]float64{}
	for _, o := range series.Funding {
		if o.FetchedAt.Before(from) || !o.FetchedAt.Before(to) {
			continue
		}
		for _, f := range o.Payload.Funding {
			key := fundingKey(f)
			if f.Asset != "BTC" || key == "" {
				continue
			}
			if groups[key] == nil {
				groups[key] = map[int64]float64{}
			}
			groups[key][o.FetchedAt.Truncate(10*time.Minute).Unix()] = num(f.RatePercent)
		}
	}
	for key, g := range groups {
		values := []float64{}
		days := map[int64]int{}
		for t, v := range g {
			values = append(values, v)
			days[t/86400]++
		}
		d := 0
		for _, n := range days {
			if n >= 137 {
				d++
			}
		}
		f := fundingBaseline{Samples: len(values), Valid: d >= 21 && float64(len(values))/to.Sub(from).Minutes()*10 >= .95}
		if f.Valid {
			f.P95 = flowPtr(percentile(values, .95))
			f.P05 = flowPtr(percentile(values, .05))
		}
		b.Funding[key] = f
	}
	return b
}
func buildDerivativeContext(series contextSeries, b contextBaseline, end, now time.Time) DerivativeContext {
	c := DerivativeContext{Futures: map[string]FlowWindow{}, Liquidations: map[string]LiquidationWindow{}, Funding: []FundingPoint{}}
	for _, m := range []int{15, 60} {
		median := 0.0
		if b.Futures.Valid {
			median = b.Futures.Median15
			if m == 60 {
				median = b.Futures.Median60
			}
		}
		c.Futures[fmt.Sprint(m)] = flowWindow(series.Futures, end, m, median)
		x := flowWindow(series.Liquidations, end, m, 0)
		bw := b.Liquidations[fmt.Sprint(m)]
		c.Liquidations[fmt.Sprint(m)] = LiquidationWindow{From: x.From, To: x.To, Coverage: x.Coverage, Long: x.Buy, Short: x.Sell, LongP95: bw.LongP95, ShortP95: bw.ShortP95}
	}
	c.OI = OIContext{Coin1H: changeOver(series.Coin, end, 1), Coin4H: changeOver(series.Coin, end, 4), USD1H: changeOver(series.USD, end, 1), USD4H: changeOver(series.USD, end, 4), AbsP75: b.OIAbsP75, Regime: "unknown"}
	if c.OI.Coin1H != nil && b.OIAbsP75 != nil {
		c.OI.Regime = "stable"
		if math.Abs(*c.OI.Coin1H) > *b.OIAbsP75 {
			c.OI.Regime = "expanding"
			if *c.OI.Coin1H < 0 {
				c.OI.Regime = "contracting"
			}
		}
	}
	sort.Slice(series.Funding, func(i, j int) bool { return series.Funding[i].FetchedAt.Before(series.Funding[j].FetchedAt) })
	if len(series.Funding) > 0 {
		last := series.Funding[len(series.Funding)-1]
		if !last.FetchedAt.After(now) && now.Sub(last.FetchedAt) <= 25*time.Minute && (last.ObservedAt == nil || (!last.ObservedAt.After(now) && now.Sub(*last.ObservedAt) <= 25*time.Minute)) {
			for _, f := range last.Payload.Funding {
				if f.Asset != "BTC" {
					continue
				}
				key := fundingKey(f)
				base := b.Funding[key]
				p := FundingPoint{Funding: f, ObservedAt: last.ObservedAt, FetchedAt: last.FetchedAt, P95: base.P95, P05: base.P05, Samples: base.Samples, Comparable: key != "" && base.Valid}
				// Compare only the immediately preceding snapshot of this venue/margin.
				// A period/type switch breaks the comparison, even if an older key matches.
				if len(series.Funding) > 1 && key != "" {
					prev := series.Funding[len(series.Funding)-2]
					if last.FetchedAt.Sub(prev.FetchedAt) <= 20*time.Minute {
						for _, q := range prev.Payload.Funding {
							if q.Asset == f.Asset && q.Venue == f.Venue && q.Margin == f.Margin && fundingKey(q) == key {
								p.Change = flowPtr(num(f.RatePercent) - num(q.RatePercent))
							}
						}
					}
				}
				c.Funding = append(c.Funding, p)
			}
		}
	}
	return c
}
func (h *Hub) currentDerivativeContext(ctx context.Context, a string, end, now time.Time) (DerivativeContext, error) {
	to := end.Truncate(time.Hour).Add(-time.Hour)
	from := to.Add(-30 * 24 * time.Hour)
	version := ""
	for _, kind := range []string{"flow", "liquidations", "oi-coin-history", "funding"} {
		asset := a
		if kind == "funding" {
			asset = "ALL"
		}
		v, err := h.Store.datasetRangeVersion(ctx, ID(kind, asset, "", "futures"), from, to)
		if err != nil {
			return DerivativeContext{}, err
		}
		version += v + "/"
	}
	var baseline contextBaseline
	key := "signals/context-baseline/" + a
	if !h.Store.LoadState(key, &baseline) || !baseline.To.Equal(to) || baseline.Version != version {
		series, e := h.contextSeries(ctx, a, from, to, now)
		if e != nil {
			return DerivativeContext{}, e
		}
		baseline = makeContextBaseline(series, from, to, now)
		baseline.Version = version
		if e = h.Store.SaveState(key, baseline); e != nil {
			return DerivativeContext{}, e
		}
	}
	series, e := h.contextSeries(ctx, a, end.Add(-4*time.Hour-5*time.Minute), now, now)
	if e != nil {
		return DerivativeContext{}, e
	}
	return buildDerivativeContext(series, baseline, end, now), nil
}
