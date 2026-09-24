package datahub

import (
	"context"
	"fmt"
	"hash/fnv"
	"math"
	"sort"
	"strings"
	"time"
)

type Coverage struct {
	Venue      string     `json:"venue"`
	Symbol     string     `json:"symbol"`
	Quote      string     `json:"quote"`
	Valid      bool       `json:"valid"`
	Reason     string     `json:"reason,omitempty"`
	ObservedAt *time.Time `json:"observedAt"`
	FetchedAt  time.Time  `json:"fetchedAt"`
	Low        float64    `json:"bidLow"`
	High       float64    `json:"askHigh"`
	Bid        float64    `json:"bid"`
	Ask        float64    `json:"ask"`
	Levels     int        `json:"levels"`
	Rate       string     `json:"rate"`
	FXAt       *time.Time `json:"fxAt"`
}
type Zone struct {
	UpdatedAt  time.Time        `json:"updatedAt"`
	Price      float64          `json:"price"`
	Step       float64          `json:"step"`
	Side       string           `json:"side"`
	USD        int64            `json:"usdCents"`
	Sources    map[string]int64 `json:"sources"`
	Since      time.Time        `json:"since"`
	Seconds    int64            `json:"seconds"`
	Occupancy  float64          `json:"occupancy"`
	Evidence   string           `json:"evidence"`
	Grade      string           `json:"grade"`
	Traded     int64            `json:"tradedCents"`
	Samples    int              `json:"samples"`
	Sampled    bool             `json:"sampled"`
	Percentile *float64         `json:"percentile"`
	Covered    []string         `json:"covered"`
	Strong     bool             `json:"strong"`
	Reason     string           `json:"reason"`
}
type RangeSummary struct {
	BidCents  int64      `json:"bidCents"`
	AskCents  int64      `json:"askCents"`
	ZoneCount int        `json:"zoneCount"`
	Oldest    *time.Time `json:"oldestSourceAt"`
	Newest    *time.Time `json:"newestSourceAt"`
	HasData   bool       `json:"hasData"`
}
type Frame struct {
	Summary      RangeSummary     `json:"summary"`
	CoverageKind string           `json:"coverageKind"`
	Asset        string           `json:"asset"`
	At           time.Time        `json:"at"`
	Price        float64          `json:"price"`
	PriceAt      *time.Time       `json:"priceAt"`
	PriceValid   bool             `json:"priceValid"`
	Step         float64          `json:"step"`
	Zones        []Zone           `json:"zones"`
	Coverage     []Coverage       `json:"coverage"`
	Rates        []map[string]any `json:"rates"`
	StartedAt    time.Time        `json:"startedAt"`
	Source       string           `json:"source"`
	Rules        string           `json:"rulesVersion"`
	Partial      bool             `json:"partial"`
	Note         string           `json:"note"`
}
type Baseline struct {
	Values []int64         `json:"values"`
	Count  int             `json:"count"`
	Days   map[string]bool `json:"days"`
	At     time.Time       `json:"at"`
}
type wallSample struct {
	At          time.Time `json:"at"`
	USD         int64     `json:"usd"`
	Large       bool      `json:"large"`
	Fingerprint string    `json:"fingerprint"`
}
type wallContinuity struct {
	Since time.Time `json:"since"`
	Last  time.Time `json:"last"`
}
type wallHistory map[string][]wallSample

func steps(a string) []float64 {
	if a == "ETH" {
		return []float64{1, 5, 10, 25, 50, 100}
	}
	return []float64{25, 100, 250, 500, 1000, 2500}
}
func baseStep(a string) float64 {
	if a == "ETH" {
		return 1
	}
	return 25
}
func distanceBand(distance float64) int {
	for i, x := range []float64{1, 3, 5, 10, 25, 50, 100} {
		if distance < x {
			return i
		}
	}
	return 7
}
func baselineKey(a string, z Zone, price float64) string {
	return fmt.Sprintf("%s/%s/%.0f/%d/%s", a, z.Side, z.Step, distanceBand(math.Abs(z.Price-price)/price*100), strings.Join(z.Covered, ","))
}
func wallKey(a string, z Zone) string {
	return fmt.Sprintf("%s/%s/%.0f/%.0f/%s", a, z.Side, z.Price, z.Step, strings.Join(z.Covered, ","))
}
func best(b *Book) (float64, float64) {
	bid, ask := 0.0, math.Inf(1)
	for _, l := range b.Bids {
		bid = max(bid, num(l.Price))
	}
	for _, l := range b.Asks {
		ask = min(ask, num(l.Price))
	}
	if math.IsInf(ask, 1) {
		ask = 0
	}
	return bid, ask
}
func makeZones(asset string, step, price float64, books map[string]Observation, registry map[string]Dataset, rates map[string]string, now time.Time, live bool) ([]Zone, []Coverage) {
	zones := map[string]*Zone{}
	coverage := []Coverage{}
	for id, o := range books {
		d := registry[id]
		b := o.Payload.Book
		if b == nil {
			continue
		}
		rate, fxOK := rates[d.Quote]
		valid := fxOK && (!live || o.Fresh(d, now))
		bid, ask := best(b)
		c := Coverage{Venue: d.Venue, Symbol: d.Symbol, Quote: d.Quote, Valid: valid, ObservedAt: o.ObservedAt, FetchedAt: o.FetchedAt, Levels: len(b.Bids) + len(b.Asks), Rate: rate}
		if fxOK {
			c.Low = num(multiply(fmt.Sprint(b.Low), rate))
			c.High = num(multiply(fmt.Sprint(b.High), rate))
			c.Bid = num(multiply(fmt.Sprint(bid), rate))
			c.Ask = num(multiply(fmt.Sprint(ask), rate))
		}
		if !valid {
			if !fxOK {
				c.Reason = "美元汇率过期或缺失"
			} else {
				c.Reason = "盘口过期"
			}
		}
		coverage = append(coverage, c)
		if !valid {
			continue
		}
		for side, levels := range map[string][]Level{"bid": b.Bids, "ask": b.Asks} {
			for _, l := range levels {
				usdPrice := num(multiply(l.Price, rate))
				p := math.Floor(usdPrice/step) * step
				if p <= 0 {
					continue
				}
				key := fmt.Sprintf("%s/%.8f", side, p)
				z := zones[key]
				if z == nil {
					z = &Zone{Price: p, Step: step, Side: side, Sources: map[string]int64{}, Evidence: "尚未触及", Grade: "样本不足", Since: o.Time(), UpdatedAt: o.Time(), Sampled: true, Reason: "金额、持续和成交证据分别判断"}
					zones[key] = z
				}
				value := money(multiply(multiply(l.Price, l.Quantity), rate))
				z.USD += value
				z.Sources[strings.ToLower(d.Venue)] += value
				if o.Time().Before(z.Since) {
					z.Since = o.Time()
					z.UpdatedAt = o.Time()
				}
			}
		}
	}
	out := []Zone{}
	for _, z := range zones {
		for _, c := range coverage {
			if c.Valid && ((z.Side == "bid" && z.Price >= c.Low && z.Price+step <= c.Bid+step) || (z.Side == "ask" && z.Price+step <= c.High+step && z.Price >= c.Ask-step)) {
				z.Covered = append(z.Covered, strings.ToLower(c.Venue))
			}
		}
		sort.Strings(z.Covered)
		out = append(out, *z)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Price == out[j].Price {
			return out[i].Side < out[j].Side
		}
		return out[i].Price > out[j].Price
	})
	sort.Slice(coverage, func(i, j int) bool { return coverage[i].Venue < coverage[j].Venue })
	return out, coverage
}
func (h *Hub) rawFrame(asset string, step float64, now time.Time) Frame {
	price, priceAt, valid := h.CurrentPrice(asset, now)
	rates := map[string]string{"USD": "1"}
	displayRates := []map[string]any{}
	for _, q := range []string{"USD", "USDT", "USDC"} {
		r, at, ok := h.Rate(q, now)
		if ok {
			rates[q] = r
		}
		displayRates = append(displayRates, map[string]any{"quote": q, "usd": r, "observedAt": at, "valid": ok})
	}
	books := map[string]Observation{}
	registry := h.datasets()
	for id, d := range registry {
		if d.Kind == "book" && d.Asset == asset {
			if o, ok := h.Store.Latest(id); ok {
				books[id] = o
			}
		}
	}
	zones, cov := makeZones(asset, step, price, books, registry, rates, now, true)
	for id, d := range registry {
		if d.Kind == "book" && d.Asset == asset {
			if _, ok := books[id]; !ok {
				cov = append(cov, Coverage{Venue: d.Venue, Symbol: d.Symbol, Quote: d.Quote, Reason: "等待首次快照"})
			}
		}
	}
	for i := range cov {
		_, at, _ := h.Rate(cov[i].Quote, now)
		cov[i].FXAt = at
	}
	count := 0
	for _, c := range cov {
		if c.Valid {
			count++
		}
	}
	return Frame{Asset: asset, At: now, Price: price, PriceAt: priceAt, PriceValid: valid, Step: step, Zones: zones, Coverage: cov, Rates: displayRates, StartedAt: h.boot, Source: "CoinGlass 代理 · 五家现货", Rules: RulesVersion, Partial: count < 5 || !valid, Note: "按价格区间聚合；盘口约2分钟更新；未覆盖不等于零。"}
}
func (h *Hub) grade(a string, z *Zone, p float64) {
	if p <= 0 || len(z.Covered) == 0 {
		return
	}
	key := baselineKey(a, *z, p)
	h.baselineMu.RLock()
	b, ok := h.baselines[key]
	h.baselineMu.RUnlock()
	if !ok || b.Count < 500 || len(b.Values) < 500 || len(b.Days) < 7 || time.Since(b.At) > 2*time.Hour {
		return
	}
	x := float64(sort.Search(len(b.Values), func(i int) bool { return b.Values[i] > z.USD })) / float64(len(b.Values)) * 100
	z.Percentile = &x
	z.Samples = b.Count
	switch {
	case x >= 99:
		z.Grade = "异常大"
	case x >= 95:
		z.Grade = "很大"
	case x >= 80:
		z.Grade = "较大"
	default:
		z.Grade = "常见金额"
	}
}
func (h *Hub) Overview(ctx context.Context, asset string, step, span float64, minAge int64) Frame {
	now := time.Now().UTC()
	f := h.rawFrame(asset, step, now)
	if span < 1000 && !f.PriceValid {
		f.Zones = []Zone{}
		f.Note = "当前美元价格不可用，无法计算相对价格范围"
		return f
	}
	h.wallMu.RLock()
	histories := h.walls
	continuity := h.continuity
	h.wallMu.RUnlock()
	var candles []Observation
	cd, _ := h.Dataset(ID("candles", asset, "Binance", "spot"))
	_ = h.Store.Visit(ctx, cd, 300, now.Add(-40*time.Minute), now, func(o Observation) error { candles = append(candles, o); return nil })
	historicalFX := map[int64]string{}
	fd, _ := h.Dataset("fx.usd.kraken")
	_ = h.Store.Visit(ctx, fd, 60, now.Add(-40*time.Minute), now, func(o Observation) error {
		for _, r := range o.Payload.Rates {
			if r.Quote == "USDT" {
				historicalFX[recordTime(o).Truncate(time.Minute).Unix()] = r.USD
			}
		}
		return nil
	})
	feet := map[string][]Observation{}
	for _, v := range []string{"Binance", "OKX"} {
		d, _ := h.Dataset(ID("footprint", asset, v, "spot"))
		latest, ok := h.Store.Latest(d.ID)
		if ok && latest.Fresh(d, now) {
			_ = h.Store.Visit(ctx, d, 300, now.Add(-35*time.Minute), now, func(o Observation) error {
				if o.Quality != "missing" {
					feet[v] = append(feet[v], o)
				}
				return nil
			})
		}
	}
	result := []Zone{}
	for _, zone := range f.Zones {
		z := zone
		if f.Price > 0 && span < 1000 && math.Abs(z.Price-f.Price)/f.Price*100 > span {
			continue
		}
		if span >= 1000 && asset == "BTC" && (z.Price < 10000 || z.Price > 200000) {
			continue
		}
		f.Summary.ZoneCount++
		if z.Side == "bid" {
			f.Summary.BidCents += z.USD
		} else {
			f.Summary.AskCents += z.USD
		}
		h.grade(asset, &z, f.Price)
		key := wallKey(asset, z)
		samples := histories[key]
		z.Occupancy, z.Seconds, z.Since = persistence(samples, now)
		if c, ok := continuity[key]; ok && now.Sub(c.Last) <= 3*time.Minute {
			z.Seconds = int64(now.Sub(c.Since).Seconds())
			z.Since = c.Since
		}
		if z.Percentile == nil || *z.Percentile < 95 {
			z.Seconds = 0
		}

		if z.Percentile != nil && *z.Percentile >= 80 {
			if z.Side == "bid" {
				z.Evidence = "大额买墙·待验证"
			} else {
				z.Evidence = "大额卖墙·待验证"
			}
		}
		if !f.PriceValid {
			z.Evidence = "当前价格或汇率过期"
			z.Reason = "暂停触及与强度判断"
		} else if (z.Side == "bid" && f.Price < z.Price) || (z.Side == "ask" && f.Price >= z.Price+z.Step) {
			z.Evidence = "等待盘口更新"
			z.Reason = "价格已越过上次快照价位"
		} else {
			evidence, traded, strong := h.evidence(asset, z, candles, feet, historicalFX, now)
			z.Traded = traded
			if evidence != "" {
				z.Evidence = evidence
			}
			if strong && z.Percentile != nil && *z.Percentile >= 95 && z.Occupancy >= .8 && len(z.Sources) >= 3 && z.Seconds >= 30*60 {
				z.Strong = true
				z.Evidence = "证据较强"
				z.Reason = "历史P95、持续大额、三家贡献、匹配足迹和随后两根K线共同支持"
			}
		}
		if z.Seconds >= minAge {
			result = append(result, z)
		}
	}
	f.Zones = result
	f.CoverageKind = "上游返回的盘口价格切片，非全量实时L2"
	for _, c := range f.Coverage {
		if c.Valid && c.ObservedAt != nil {
			f.Summary.HasData = true
			if f.Summary.Oldest == nil || c.ObservedAt.Before(*f.Summary.Oldest) {
				f.Summary.Oldest = c.ObservedAt
			}
			if f.Summary.Newest == nil || c.ObservedAt.After(*f.Summary.Newest) {
				f.Summary.Newest = c.ObservedAt
			}
		}
	}
	return f
}
func (h *Hub) evidence(asset string, z Zone, candles []Observation, feet map[string][]Observation, historicalFX map[int64]string, now time.Time) (string, int64, bool) {
	_, _, ok := h.Rate("USDT", now)
	if !ok {
		return "", 0, false
	}
	total := int64(0)
	touched := false
	var touch time.Time
	for _, venue := range []string{"Binance", "OKX"} {
		if z.Sources[strings.ToLower(venue)] <= 0 {
			continue
		}
		for _, o := range feet[venue] {
			rate := historicalFX[o.Time().Truncate(time.Minute).Unix()]
			if rate == "" {
				continue
			}
			// We must have observed this wall before the matched trade interval.
			if z.Since.IsZero() || o.Time().Before(z.Since) || o.Time().Add(5*time.Minute).After(now) {
				continue
			}
			for _, f := range o.Payload.Foot {
				low, high := num(multiply(f.Low, rate)), num(multiply(f.High, rate))
				if high <= z.Price || low >= z.Price+z.Step {
					continue
				}
				opposite := f.SellUSDT
				if z.Side == "ask" {
					opposite = f.BuyUSDT
				}
				value := money(multiply(opposite, rate))
				if value > 0 {
					total += value
					touched = true
					if o.Time().After(touch) {
						touch = o.Time()
					}
				}
			}
		}
	}
	if !touched {
		return "", 0, false
	}
	evidence := "触及并记录到成交"
	favorable := 0
	for _, o := range candles {
		if o.Payload.Candle == nil || !o.Time().After(touch) || o.Time().After(touch.Add(10*time.Minute)) {
			continue
		}
		rate := historicalFX[o.Time().Add(5*time.Minute-time.Second).Truncate(time.Minute).Unix()]
		if rate == "" {
			continue
		}
		close := o.Payload.Candle.Close * num(rate)
		if (z.Side == "bid" && close >= z.Price+z.Step) || (z.Side == "ask" && close < z.Price) {
			favorable++
		}
	}
	// Explicit significance threshold, not a fitted probability: opposite taker
	// notional is at least 5% of the current wall and $100k BTC / $50k ETH.
	minimum := int64(10_000_000)
	if asset == "ETH" {
		minimum = 5_000_000
	}
	significant := total >= max(minimum, z.USD/20)
	if favorable > 0 && significant {
		if z.Side == "bid" {
			evidence = "出现承接迹象"
		} else {
			evidence = "出现抛压迹象"
		}
	}
	return evidence, total, significant && favorable >= 2
}
func (h *Hub) SampleWalls(now time.Time) {
	var histories wallHistory
	if !h.Store.LoadState("wallHistory", &histories) {
		histories = wallHistory{}
	}
	continuity := map[string]wallContinuity{}
	h.Store.LoadState("wallContinuity", &continuity)
	for _, a := range Assets() {
		for _, step := range steps(a) {
			f := h.rawFrame(a, step, now)
			if !f.PriceValid {
				continue
			}
			finger := ""
			for _, c := range f.Coverage {
				if c.Valid && c.ObservedAt != nil {
					finger += c.Venue + c.ObservedAt.String()
				}
			}
			for _, z := range f.Zones {
				if math.Abs(z.Price-f.Price)/f.Price > .10 {
					continue
				}
				h.grade(a, &z, f.Price)
				key := wallKey(a, z)
				samples := histories[key]
				if len(samples) > 0 && samples[len(samples)-1].Fingerprint == finger {
					continue
				}
				large := z.Percentile != nil && *z.Percentile >= 95
				if large {
					c, ok := continuity[key]
					if !ok || now.Sub(c.Last) > 3*time.Minute {
						c.Since = now
					}
					c.Last = now
					continuity[key] = c
				} else {
					delete(continuity, key)
				}
				histories[key] = append(samples, wallSample{now, z.USD, large, finger})
			}
		}
	}
	for key, samples := range histories {
		keep := samples[:0]
		for _, s := range samples {
			if s.At.After(now.Add(-35 * time.Minute)) {
				keep = append(keep, s)
			}
		}
		if len(keep) == 0 {
			delete(histories, key)
		} else {
			histories[key] = keep
		}
	}
	for k, c := range continuity {
		if now.Sub(c.Last) > 3*time.Minute {
			delete(continuity, k)
		}
	}
	_ = h.Store.SaveState("wallContinuity", continuity)
	_ = h.Store.SaveState("wallHistory", histories)
	h.wallMu.Lock()
	h.walls = histories
	h.continuity = continuity
	h.wallMu.Unlock()
}
func (h *Hub) BuildBaselines(ctx context.Context) error {
	now := time.Now().UTC()
	from := now.Add(-30 * 24 * time.Hour)
	registry := h.datasets()
	result := map[string]Baseline{}
	totalValues := 0
	// Hourly FX history is required for USDT samples. Missing historical FX never
	// becomes a 1:1 assumption; those source cohorts remain unavailable.
	fx := map[int64]map[string]string{}
	fd, _ := h.Dataset("fx.usd.kraken")
	_ = h.Store.Visit(ctx, fd, 3600, from, now, func(o Observation) error {
		rates := map[string]string{"USD": "1"}
		for _, r := range o.Payload.Rates {
			rates[r.Quote] = r.USD
		}
		fx[recordTime(o).Truncate(time.Hour).Unix()] = rates
		return nil
	})
	for _, a := range Assets() {
		hourBooks := map[int64]map[string]Observation{}
		for id, d := range registry {
			if d.Kind != "book" || d.Asset != a {
				continue
			}
			if e := h.Store.Visit(ctx, d, 3600, from, now, func(o Observation) error {
				if o.Quality == "missing" || o.Payload.Book == nil {
					return nil
				}
				key := recordTime(o).Truncate(time.Hour).Unix()
				if hourBooks[key] == nil {
					hourBooks[key] = map[string]Observation{}
				}
				hourBooks[key][id] = o
				return nil
			}); e != nil {
				return e
			}
		}
		times := make([]int64, 0, len(hourBooks))
		for ts := range hourBooks {
			times = append(times, ts)
		}
		sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
		for _, ts := range times {
			books := hourBooks[ts]
			if ctx.Err() != nil {
				return ctx.Err()
			}
			rates := fx[ts]
			if rates == nil {
				rates = map[string]string{"USD": "1"}
			}
			midpoints := []float64{}
			for id, o := range books {
				rate, ok := rates[registry[id].Quote]
				if !ok {
					continue
				}
				b, a := best(o.Payload.Book)
				if b > 0 && a > 0 {
					midpoints = append(midpoints, (a+b)/2*num(rate))
				}
			}
			if len(midpoints) == 0 {
				continue
			}
			sort.Float64s(midpoints)
			p := midpoints[len(midpoints)/2]
			for _, step := range steps(a) {
				zones, _ := makeZones(a, step, p, books, registry, rates, now, false)
				for _, z := range zones {
					key := baselineKey(a, z, p)
					b := result[key]
					if b.Days == nil {
						b.Days = map[string]bool{}
					}
					b.Count++
					b.Days[time.Unix(ts, 0).UTC().Format("2006-01-02")] = true
					b.At = now
					if len(b.Values) < 1024 && totalValues < 4_000_000 {
						b.Values = append(b.Values, z.USD)
						totalValues++
					} else if len(b.Values) > 0 {
						hash := fnv.New64a()
						fmt.Fprintf(hash, "%s/%d/%.8f", key, ts, z.Price)
						idx := int64(hash.Sum64() % uint64(b.Count))
						if idx < int64(len(b.Values)) {
							b.Values[idx] = z.USD
						}
					}
					result[key] = b
				}
			}
		}
	}
	for k, b := range result {
		sort.Slice(b.Values, func(i, j int) bool { return b.Values[i] < b.Values[j] })
		result[k] = b
	}
	h.baselineMu.Lock()
	h.baselines = result
	h.baselineMu.Unlock()
	return h.Store.SaveState("baselines", result)
}

func persistence(samples []wallSample, now time.Time) (float64, int64, time.Time) {
	start := now.Add(-30 * time.Minute)
	occupied := time.Duration(0)
	since := now
	continuous := false
	for i, s := range samples {
		end := now
		if i+1 < len(samples) {
			end = samples[i+1].At
		}
		end = minTime(end, s.At.Add(3*time.Minute))
		if s.Large {
			from := s.At
			if from.Before(start) {
				from = start
			}
			if end.After(from) {
				occupied += end.Sub(from)
			}
		}
	}
	previous := now
	for i := len(samples) - 1; i >= 0; i-- {
		s := samples[i]
		if !s.Large || previous.Sub(s.At) > 3*time.Minute {
			break
		}
		since = s.At
		continuous = true
		previous = s.At
	}
	seconds := int64(0)
	if continuous {
		seconds = int64(now.Sub(since).Seconds())
	}
	return math.Min(1, occupied.Seconds()/1800), seconds, since
}
