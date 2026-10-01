package datahub

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/shopspring/decimal"
)

// Compact cells keep the bounded historical response below the shared view cache
// budget. [USD center, bid=0/ask=1, cents, coin quantity, venue mask, coverage mask].
type zoneHistorySource struct {
	Venue    string     `json:"venue"`
	At       time.Time  `json:"at"`
	Low      float64    `json:"low"`
	High     float64    `json:"high"`
	BestBid  float64    `json:"bestBid"`
	BestAsk  float64    `json:"bestAsk"`
	Partial  bool       `json:"partial"`
	FXAt     *time.Time `json:"fxAt"`
	HourlyFX bool       `json:"hourlyFX"`
}
type zoneHistoryPoint struct {
	Time    int64               `json:"time"`
	Cells   [][]any             `json:"cells"`
	Sources []zoneHistorySource `json:"sources"`
	Partial bool                `json:"partial"`
}
type zoneTrackPoint struct {
	Time     int64    `json:"time"`
	Price    *float64 `json:"price"`
	Quantity *string  `json:"quantity"`
	Cents    *int64   `json:"usdCents"`
	State    string   `json:"state"`
}
type zoneHistoryCandle struct {
	Time        int64   `json:"time"`
	Open        float64 `json:"open"`
	Close       float64 `json:"close"`
	High        float64 `json:"high"`
	Low         float64 `json:"low"`
	Approximate bool    `json:"hourlyFX"`
}
type orderZoneHistoryView struct {
	Layer          string              `json:"layer"`
	Asset          string              `json:"asset"`
	From           time.Time           `json:"from"`
	To             time.Time           `json:"to"`
	ActualFrom     *time.Time          `json:"actualFrom"`
	ActualTo       *time.Time          `json:"actualTo"`
	Reference      *float64            `json:"referencePrice"`
	Step           float64             `json:"step"`
	SampleSeconds  int                 `json:"sampleSeconds"`
	DisplaySeconds int                 `json:"displaySeconds"`
	CandleSeconds  int                 `json:"candleSeconds"`
	Points         []zoneHistoryPoint  `json:"points"`
	Candles        []zoneHistoryCandle `json:"candles"`
	Track          []zoneTrackPoint    `json:"track"`
	Order          string              `json:"order"`
	Venues         []string            `json:"venues"`
	ExpectedSlots  int                 `json:"expectedSlots"`
	CoveredSlots   int                 `json:"coveredSlots"`
	Partial        bool                `json:"partial"`
	MissingFX      int                 `json:"missingFX"`
	StorageBytes   int64               `json:"storageBytes"`
	StorageLimit   int64               `json:"storageLimit"`
	CapacityGapAt  *time.Time          `json:"capacityGapAt"`
	Note           string              `json:"note"`
}
type zoneHistoryKey struct {
	center float64
	side   string
}
type zoneHistoryCell struct {
	center   float64
	side     string
	usd      int64
	quantity decimal.Decimal
	mask     int
}
type zoneHistorySlice struct {
	source zoneHistorySource
	cells  map[zoneHistoryKey]zoneHistoryCell
}

func (h *Hub) orderZoneHistory(ctx context.Context, a string, q url.Values, now time.Time) (orderZoneHistoryView, error) {
	step, span, _, err := orderZoneParams(a, q)
	if err != nil {
		return orderZoneHistoryView{}, err
	}
	hours := 168
	if q.Get("period") == "24h" {
		hours = 24
	} else if q.Get("period") != "" && q.Get("period") != "7d" {
		return orderZoneHistoryView{}, fmt.Errorf("历史仅支持24小时或7天")
	}
	layer := q.Get("layer")
	if layer == "" {
		layer = "book"
	}
	if layer != "book" && layer != "orders" {
		return orderZoneHistoryView{}, fmt.Errorf("无效历史图层")
	}
	display, candle := 1800, 14400
	if hours == 24 {
		display, candle = 300, 900
	}
	out := orderZoneHistoryView{Asset: a, Layer: layer, From: now.Add(-time.Duration(hours) * time.Hour), To: now, Step: step, SampleSeconds: 300, DisplaySeconds: display, CandleSeconds: candle, Venues: []string{"Binance", "OKX", "Coinbase", "Kraken", "Bitfinex"}, Points: []zoneHistoryPoint{}, Candles: []zoneHistoryCandle{}, Track: []zoneTrackPoint{}, StorageLimit: orderZoneLimit, Order: q.Get("order"), Note: "每个显示时段仅取各来源最后一次实际采样，不跨时点累加、不填补缺口。盘口与大单分别统计；来源数仅指返回区域，非全市场完整深度。"}
	p, _, ok := h.CurrentPrice(a, now)
	if !ok {
		out.Partial = true
		return out, nil
	}
	out.Reference = &p
	if 2*p*span/100/step > 400 {
		return out, fmt.Errorf("价位超过400档，请增大精度或缩小范围")
	}
	fx, err := h.orderZoneFX(ctx, out.From, now)
	if err != nil {
		return out, err
	}
	type preparedPrice struct {
		value  decimal.Decimal
		center float64
		inside bool
	}
	type priceCache struct {
		rate   string
		values map[string]preparedPrice
	}
	caches := map[string]*priceCache{}
	stepDecimal := decimal.NewFromFloat(step)
	half := decimal.NewFromFloat(step / 2)
	type nativePrice struct {
		value  decimal.Decimal
		approx float64
	}
	nativePrices := map[string]nativePrice{}
	frames := map[int64]map[string]zoneHistorySlice{}
	retained := 0
	consume := func(d Dataset, o Observation, at time.Time, rate string, fxAt *time.Time, approx bool) error {
		if o.Quality == "missing" {
			return nil
		}
		if rate == "" {
			out.MissingFX++
			return nil
		}
		key := at.Unix() / int64(display) * int64(display)
		sources := frames[key]
		if sources == nil {
			sources = map[string]zoneHistorySlice{}
			frames[key] = sources
		}
		if old, exists := sources[d.Venue]; exists {
			if !at.After(old.source.At) {
				return nil
			}
			retained -= len(old.cells)
		}
		s := zoneHistorySlice{source: zoneHistorySource{Venue: d.Venue, At: at, Partial: o.Quality != "valid", FXAt: fxAt, HourlyFX: approx}, cells: map[zoneHistoryKey]zoneHistoryCell{}}
		cache := caches[d.Venue]
		if cache == nil || cache.rate != rate {
			cache = &priceCache{rate: rate, values: map[string]preparedPrice{}}
			caches[d.Venue] = cache
		}
		rd := dec(rate)
		rateFloat := num(rate)
		add := func(side, raw, qty string) {
			pp, exists := cache.values[raw]
			if !exists {
				native, exists := nativePrices[raw]
				if !exists {
					native = nativePrice{dec(raw), num(raw)}
					if len(nativePrices) < 8192 {
						nativePrices[raw] = native
					}
				}
				pd := native.value.Mul(rd)
				lp := native.approx * rateFloat
				center := historyOrderCenter(pd, lp, step, stepDecimal, half)
				pp = preparedPrice{pd, center, math.Abs(lp-p)/p*100 <= span}
				if len(cache.values) < 8192 {
					cache.values[raw] = pp
				}
			}
			if !pp.inside {
				return
			}
			k := zoneHistoryKey{pp.center, side}
			q := dec(qty)
			c := s.cells[k]
			c.center = pp.center
			c.side = side
			c.usd += moneyDecimal(pp.value.Mul(q))
			c.quantity = c.quantity.Add(q)
			s.cells[k] = c
		}

		if layer == "book" {
			if b := o.Payload.Book; b != nil {
				s.source.Low = b.Low * num(rate)
				s.source.High = b.High * num(rate)
				bid, ask := best(b)
				s.source.BestBid = bid * num(rate)
				s.source.BestAsk = ask * num(rate)
				for _, r := range b.Bids {
					add("bid", r.Price, r.Quantity)
				}
				for _, r := range b.Asks {
					add("ask", r.Price, r.Quantity)
				}
			}
		} else {
			for _, r := range o.Payload.Large {
				if r.RawState == 1 {
					add(r.Side, r.Price, r.Quantity)
				}
			}
		}
		sources[d.Venue] = s
		retained += len(s.cells)
		if retained > 100000 {
			return fmt.Errorf("历史单元超过查询预算，请增大精度或缩小范围")
		}
		if out.ActualFrom == nil || at.Before(*out.ActualFrom) {
			t := at
			out.ActualFrom = &t
		}
		if out.ActualTo == nil || at.After(*out.ActualTo) {
			t := at
			out.ActualTo = &t
		}
		return nil
	}
	if layer == "book" {
		for _, d := range Registry() {
			if d.Kind != "book" || d.Asset != a {
				continue
			}
			err = h.Store.visitSampled(ctx, d, 300, out.From, now, display, func(o Observation) error {
				at := recordTime(o)
				rate, approx := fx.rate(d.Quote, at)
				return consume(d, o, at, rate, nil, approx)
			})
			if err != nil {
				return out, err
			}
		}
	} else {
		// No lifecycle event is used to backfill the new observation series.
		var lastTrack time.Time
		err = h.Store.visitOrderZoneSamples(ctx, a, out.From, now, func(o Observation) error {
			d, _ := h.Dataset(o.Dataset)
			rate := ""
			for _, r := range o.Payload.Rates {
				if r.Quote == d.Quote {
					rate = r.USD
				}
			}
			var fa *time.Time
			if t := timestamp(o.Dependencies["fxAt"]); t != nil {
				fa = t
			}
			if out.Order != "" {
				matched := false
				for _, r := range o.Payload.Large {
					if orderKey(d, r) != out.Order {
						continue
					}
					matched = true
					at := o.FetchedAt
					if !lastTrack.IsZero() && at.Sub(lastTrack) > 450*time.Second {
						out.Track = append(out.Track, zoneTrackPoint{Time: lastTrack.Add(5 * time.Minute).Unix(), State: "gap"})
					}
					tp := zoneTrackPoint{Time: at.Unix(), Quantity: &r.Quantity, State: "observed"}
					if rate != "" {
						price := num(multiply(r.Price, rate))
						c := money(multiply(multiply(r.Price, r.Quantity), rate))
						tp.Price = &price
						tp.Cents = &c
					} else {
						tp.State = "missing_fx"
					}
					out.Track = append(out.Track, tp)
					lastTrack = at
				}
				if !matched && strings.HasPrefix(out.Order, "coinglass:spot:"+d.Venue+":"+d.Symbol+":") && !lastTrack.IsZero() {
					out.Track = append(out.Track, zoneTrackPoint{Time: o.FetchedAt.Unix(), State: "not_returned"})
					lastTrack = time.Time{}
				}
			}
			return consume(d, o, o.FetchedAt, rate, fa, false)
		})
		if err != nil {
			return out, err
		}
		if !lastTrack.IsZero() && now.Sub(lastTrack) > 450*time.Second {
			out.Track = append(out.Track, zoneTrackPoint{Time: lastTrack.Add(5 * time.Minute).Unix(), State: "gap"})
		}
		if err = h.Store.db.QueryRowContext(ctx, "SELECT bytes FROM order_zone_storage WHERE id=1").Scan(&out.StorageBytes); err != nil {
			return out, err
		}
		h.Store.LoadState("orderZoneGapAt", &out.CapacityGapAt)
	}
	first := out.From.Unix() / int64(display) * int64(display)
	for t := first; t <= now.Unix()/int64(display)*int64(display); t += int64(display) {
		point := zoneHistoryPoint{Time: t, Cells: [][]any{}, Sources: []zoneHistorySource{}}
		cells := map[zoneHistoryKey]zoneHistoryCell{}
		slices := frames[t]
		for i, v := range out.Venues {
			s, exists := slices[v]
			if !exists {
				continue
			}
			point.Sources = append(point.Sources, s.source)
			for k, c := range s.cells {
				sum := cells[k]
				sum.center = c.center
				sum.side = c.side
				sum.usd += c.usd
				sum.quantity = sum.quantity.Add(c.quantity)
				sum.mask |= 1 << i
				cells[k] = sum
			}
		}
		point.Partial = len(point.Sources) < len(out.Venues)
		for _, s := range point.Sources {
			point.Partial = point.Partial || s.Partial
		}
		if len(point.Sources) > 0 {
			out.CoveredSlots++
		}
		out.ExpectedSlots++
		out.Partial = out.Partial || point.Partial
		keys := []zoneHistoryKey{}
		for k := range cells {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool {
			if keys[i].center == keys[j].center {
				return keys[i].side < keys[j].side
			}
			return keys[i].center < keys[j].center
		})
		for _, k := range keys {
			c := cells[k]
			coverage := 0
			for i, v := range out.Venues {
				s, exists := slices[v]
				if !exists || s.source.Partial {
					continue
				}
				lo, hi := c.center-step/2, c.center+step/2
				if layer == "orders" || (lo >= s.source.Low && hi <= s.source.High && ((c.side == "bid" && hi <= s.source.BestBid+step) || (c.side == "ask" && lo >= s.source.BestAsk-step))) {
					coverage |= 1 << i
				}
			}
			side := 0
			if c.side == "ask" {
				side = 1
			}
			point.Cells = append(point.Cells, []any{c.center, side, c.usd, c.quantity.String(), c.mask, coverage})
		}
		out.Points = append(out.Points, point)
	}
	out.Partial = out.Partial || out.MissingFX > 0
	out.Candles, err = h.orderZoneCandles(ctx, a, out.From, now, candle, fx)
	return out, err
}
func (h *Hub) orderZoneCandles(ctx context.Context, a string, from, to time.Time, seconds int, fx zoneFX) ([]zoneHistoryCandle, error) {
	d, _ := h.Dataset(ID("candles", a, "Binance", "spot"))
	type group struct {
		c           zoneHistoryCandle
		seen        map[int64]bool
		first, last int64
	}
	groups := map[int64]*group{}
	err := h.Store.Visit(ctx, d, 300, from, to, func(o Observation) error {
		at := recordTime(o)
		if o.Quality != "valid" || o.Payload.Candle == nil || at.Add(5*time.Minute).After(to) {
			return nil
		}
		rate, approx := fx.rate(d.Quote, at)
		if rate == "" {
			return nil
		}
		k := at.Unix() / int64(seconds) * int64(seconds)
		g := groups[k]
		c := o.Payload.Candle
		r := num(rate)
		if g == nil {
			g = &group{c: zoneHistoryCandle{Time: k, Open: c.Open * r, Close: c.Close * r, High: c.High * r, Low: c.Low * r}, seen: map[int64]bool{}, first: at.Unix(), last: at.Unix()}
			groups[k] = g
		}
		if at.Unix() < g.first {
			g.first = at.Unix()
			g.c.Open = c.Open * r
		}
		if at.Unix() >= g.last {
			g.last = at.Unix()
			g.c.Close = c.Close * r
		}
		g.c.High = math.Max(g.c.High, c.High*r)
		g.c.Low = math.Min(g.c.Low, c.Low*r)
		g.seen[at.Unix()] = true
		g.c.Approximate = g.c.Approximate || approx
		return nil
	})
	out := []zoneHistoryCandle{}
	for _, g := range groups {
		if len(g.seen) == seconds/300 && g.first == g.c.Time {
			out = append(out, g.c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Time < out[j].Time })
	return out, err
}

// Only the display index takes a floating fast path. Near any bucket boundary
// (with a guard much wider than IEEE multiplication error), use exact decimals.
// All notional, quantity and cent calculations remain decimal.
func historyOrderCenter(exact decimal.Decimal, approx, step float64, stepD, half decimal.Decimal) float64 {
	index := approx/step + 0.5
	if math.Abs(index-math.Round(index)) <= math.Max(1, math.Abs(index))*1e-12 {
		return centeredOrderPrice(exact, stepD, half)
	}
	return math.Floor(index) * step
}
