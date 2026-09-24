package tidal

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

type zoneTrack struct {
	Since, Last time.Time
	LastAmount  int64
	Seen, Valid int64
	LastSources string
	LastFX      string
	Price       float64
	Side        string
}
type dedupEntry struct {
	Key string
	At  time.Time
}
type Engine struct {
	whaleTimes     map[string]time.Time
	BootAt         time.Time
	mu             sync.RWMutex
	Books          map[string]*Book
	rates          map[string]Rate
	rateHistory    map[string][]Rate
	frames         map[string]Frame
	tracks         map[string]map[string]*zoneTrack
	flows          map[string]*Flow
	deriv          map[string]Derivative
	liquidations   []Liquidation
	whales         map[string][]Whale
	whaleCore      map[string]bool
	health         map[string]Health
	tradeIDs       map[string]bool
	intervalTrades map[string]map[int64][2]int64
	tradeQueue     []dedupEntry
	candles        map[string]Candle
	Started        time.Time
	store          *Store
}

func NewEngine() *Engine {
	e := &Engine{whaleTimes: map[string]time.Time{}, BootAt: time.Now().UTC(), intervalTrades: map[string]map[int64][2]int64{}, Books: map[string]*Book{}, rates: map[string]Rate{}, rateHistory: map[string][]Rate{}, frames: map[string]Frame{}, tracks: map[string]map[string]*zoneTrack{}, flows: map[string]*Flow{}, deriv: map[string]Derivative{}, whales: map[string][]Whale{}, whaleCore: map[string]bool{}, health: map[string]Health{}, tradeIDs: map[string]bool{}, candles: map[string]Candle{}, Started: time.Now().UTC()}
	for _, i := range Instruments() {
		e.Books[i.Key()] = newBook(i)
	}
	return e
}
func (e *Engine) SetHealth(k string, ok bool, detail string) {
	e.mu.Lock()
	h := e.health[k]
	h.Component = k
	h.At = time.Now().UTC()
	h.OK = ok
	h.Detail = detail
	if !ok {
		h.Errors++
	}
	e.health[k] = h
	e.mu.Unlock()
}
func (e *Engine) SetRate(q, s string, at time.Time) {
	f := number(s)
	if !finite(f) || f < 0.5 || f > 1.5 {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	r := Rate{q, s, at, f}
	e.rates[q] = r
	rs := append(e.rateHistory[q], r)
	cut := at.Add(-24 * time.Hour)
	j := 0
	for j < len(rs) && rs[j].ObservedAt.Before(cut) {
		j++
	}
	e.rateHistory[q] = rs[j:]
}
func (e *Engine) rateLocked(q string, at time.Time) (Rate, bool) {
	if q == "USD" {
		return Rate{"USD", "1", at, 1}, true
	}
	r, ok := e.rates[q]
	return r, ok && at.Sub(r.ObservedAt) <= 30*time.Second && at.Sub(r.ObservedAt) >= -time.Second
}
func (e *Engine) Rate(q string) (Rate, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.rateLocked(q, time.Now())
}
func (e *Engine) historicalRateLocked(q string, at time.Time) (float64, bool) {
	if q == "USD" {
		return 1, true
	}
	rs := e.rateHistory[q]
	i := sort.Search(len(rs), func(i int) bool { return rs[i].ObservedAt.After(at.Add(time.Second)) }) - 1
	if i < 0 {
		return 0, false
	}
	r := rs[i]
	return r.Value, at.Sub(r.ObservedAt) < 30*time.Second
}
func (e *Engine) Trade(i Instrument, id, side, p, q string, at time.Time, multiplier float64) {
	if multiplier == 0 {
		multiplier = 1
	}
	price, qty := number(p), number(q)*multiplier
	if (side != "buy" && side != "sell") || price <= 0 || qty <= 0 || !finite(price*qty) || at.After(time.Now().Add(time.Minute)) || at.Before(e.BootAt) {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	key := i.Key() + ":" + id
	if id != "" && e.tradeIDs[key] {
		return
	}
	fx, ok := e.historicalRateLocked(i.Quote, at)
	if !ok {
		e.markPartialLocked(i.Venue, i.Asset, i.Market, at)
		return
	}
	if id != "" {
		e.tradeIDs[key] = true
		e.tradeQueue = append(e.tradeQueue, dedupEntry{key, time.Now()})
	}
	m := at.UTC().Truncate(time.Minute).Unix()
	fk := fmt.Sprintf("%s/%s/%s/%d", i.Venue, i.Asset, i.Market, m)
	f := e.flows[fk]
	if f == nil {
		f = &Flow{Venue: i.Venue, Asset: i.Asset, Market: i.Market, Minute: m, PriceBins: map[int64][2]int64{}}
		e.flows[fk] = f
	}
	value := cents(price * qty * fx)
	bucket := int64(math.Floor(price * fx / Step(i.Asset)))
	b := f.PriceBins[bucket]
	if side == "buy" {
		f.BuyCents += value
		b[0] += value
	} else {
		f.SellCents += value
		b[1] += value
	}
	f.PriceBins[bucket] = b
	if i.Market == "spot" && time.Since(at) <= 3*time.Second {
		if e.intervalTrades[i.Asset] == nil {
			e.intervalTrades[i.Asset] = map[int64][2]int64{}
		}
		x := e.intervalTrades[i.Asset][bucket]
		if side == "buy" {
			x[0] += value
		} else {
			x[1] += value
		}
		e.intervalTrades[i.Asset][bucket] = x
	}
	f.BaseQty += qty
	f.USDQty += price * qty * fx
	f.Trades++
}
func (e *Engine) Tick(now time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, a := range []string{"BTC", "ETH"} {
		frame := Frame{Asset: a, At: now, Step: Step(a), StartedAt: e.Started, Zones: []Zone{}, Coverage: []Coverage{}, Rates: []Rate{}}
		for _, q := range []string{"USD", "USDT", "USDC"} {
			r, _ := e.rateLocked(q, now)
			frame.Rates = append(frame.Rates, r)
		}
		mids := []float64{}
		for _, b := range e.Books {
			if b.Instrument.Asset != a {
				continue
			}
			b.mu.RLock()
			fx, rateOK := e.rateLocked(b.Instrument.Quote, now)
			c := Coverage{Venue: b.Instrument.Venue, Symbol: b.Instrument.Symbol, Quote: b.Instrument.Quote, ObservedAt: b.Seen, Reason: b.Error, Levels: len(b.Bids) + len(b.Asks), Resyncs: b.Resyncs}
			c.LiveBidLow, c.LiveAskHigh = b.LowBid*fx.Value, b.HighAsk*fx.Value
			c.Valid = b.Valid && now.Sub(b.Seen) < 30*time.Second && rateOK
			bid, ask := 0.0, math.Inf(1)
			for p := range b.Bids {
				if p > bid {
					bid = p
				}
			}
			for p := range b.Asks {
				if p < ask {
					ask = p
				}
			}
			if !math.IsInf(ask, 1) && rateOK {
				c.Bid = bid * fx.Value
				c.Ask = ask * fx.Value
				c.BidLow = b.LowBid * fx.Value
				c.AskHigh = b.HighAsk * fx.Value
				if now.Sub(b.DeepAt) < 30*time.Second && !b.DeepAt.IsZero() {
					lo, hi := bounds(b.DeepBids, b.DeepAsks)
					c.BidLow = math.Min(c.BidLow, lo*fx.Value)
					c.AskHigh = math.Max(c.AskHigh, hi*fx.Value)
					c.DeepAt = b.DeepAt
					c.Levels += len(b.DeepBids) + len(b.DeepAsks)
				}
			}
			if !rateOK {
				c.Reason = "美元汇率过期或尚未就绪"
			} else if now.Sub(b.Seen) >= 30*time.Second {
				c.Reason = "盘口数据过期"
			}
			if c.Valid && c.Bid > 0 && (c.Ask-c.Bid)/c.Bid < 0.005 {
				mids = append(mids, (c.Ask+c.Bid)/2)
			}
			frame.Coverage = append(frame.Coverage, c)
			b.mu.RUnlock()
		}
		sort.Float64s(mids)
		if len(mids) > 0 {
			frame.Price = mids[len(mids)/2]
		}
		sort.Slice(frame.Coverage, func(i, j int) bool {
			return frame.Coverage[i].Venue+frame.Coverage[i].Symbol < frame.Coverage[j].Venue+frame.Coverage[j].Symbol
		})
		zs := map[string]*Zone{}
		for _, b := range e.Books {
			if b.Instrument.Asset != a {
				continue
			}
			b.mu.RLock()
			fx, ok := e.rateLocked(b.Instrument.Quote, now)
			if !ok || !b.Valid || now.Sub(b.Seen) >= 30*time.Second || frame.Price == 0 {
				b.mu.RUnlock()
				continue
			}
			add := func(side string, l RawLevel, sampled bool) {
				p := l.P * fx.Value
				if p < frame.Price*.9 || p > frame.Price*1.1 {
					return
				}
				bucket := math.Floor((p+1e-9)/frame.Step) * frame.Step
				k := fmt.Sprintf("%s/%.4f", side, bucket)
				z := zs[k]
				if z == nil {
					z = &Zone{Price: bucket, Step: frame.Step, Side: side, Sources: map[string]int64{}}
					zs[k] = z
				}
				amount := cents(l.Value * fx.Value)
				z.USDCents += amount
				z.Sources[b.Instrument.Venue] += amount
				z.Sampled = z.Sampled || sampled
			}
			for _, l := range b.Bids {
				if l.P >= b.LowBid {
					add("bid", l, false)
				}
			}
			for _, l := range b.Asks {
				if l.P <= b.HighAsk {
					add("ask", l, false)
				}
			}
			if now.Sub(b.DeepAt) < 30*time.Second && !b.DeepAt.IsZero() {
				for _, l := range b.DeepBids {
					if l.P < b.LowBid {
						add("bid", l, true)
					}
				}
				for _, l := range b.DeepAsks {
					if l.P > b.HighAsk {
						add("ask", l, true)
					}
				}
			}
			b.mu.RUnlock()
		}
		if e.tracks[a] == nil {
			e.tracks[a] = map[string]*zoneTrack{}
		}
		tracks := e.tracks[a]
		recent := map[int64][2]int64{}
		for _, f := range e.flows {
			if f.Asset == a && f.Market == "spot" && f.Minute >= now.Add(-2*time.Minute).Unix() {
				for k, b := range f.PriceBins {
					x := recent[k]
					x[0] += b[0]
					x[1] += b[1]
					recent[k] = x
				}
			}
		}
		fxKey := ""
		for _, r := range frame.Rates {
			fxKey += r.Quote + ":" + r.USD + ";"
		}
		for k, z := range zs {
			tr := tracks[k]
			if tr == nil {
				tr = &zoneTrack{Since: now, Price: z.Price, Side: z.Side}
				tracks[k] = tr
			}
			src := coverageKey(frame, z.Price, z.Step, z.Side)
			gap := !tr.Last.IsZero() && now.Sub(tr.Last) > 3*time.Second
			changed := tr.LastSources != "" && tr.LastSources != src
			fxChanged := tr.LastFX != "" && tr.LastFX != fxKey
			if gap || changed {
				tr.Since = now
			}
			tr.Valid++
			tr.Seen++
			z.Since = tr.Since
			z.Seconds = int64(now.Sub(tr.Since).Seconds())
			z.Samples = tr.Valid
			z.Occupancy = float64(tr.Seen) / float64(tr.Valid)
			idx := 0
			if z.Side == "bid" {
				idx = 1
			}
			bucket := int64(math.Round(z.Price / frame.Step))
			z.TradedCents = recent[bucket][idx]
			z.Evidence = "尚未触及"
			if z.Seconds < 60 {
				z.Evidence = "新增挂单"
			}
			if z.TradedCents > 0 {
				if z.Side == "bid" {
					z.Evidence = "出现承接"
				} else {
					z.Evidence = "出现抛压"
				}
			}
			if gap || changed {
				z.Evidence = "覆盖变化，重新观察"
			} else if !fxChanged && tr.LastAmount > 0 {
				z.ChangeCents = z.USDCents - tr.LastAmount
				if z.USDCents < tr.LastAmount*7/10 {
					if z.Sampled {
						z.Evidence = "深度采样金额下降"
					} else if e.intervalTrades[a][bucket][idx] >= tr.LastAmount-z.USDCents {
						z.Evidence = "成交消耗迹象"
					} else {
						z.Evidence = "疑似撤走（估计）"
					}
				}
			}
			tr.Last = now
			tr.LastAmount = z.USDCents
			tr.LastSources = src
			tr.LastFX = fxKey
			frame.Zones = append(frame.Zones, *z)
		}
		for k, tr := range tracks {
			if _, ok := zs[k]; !ok {
				if covered(frame, tr.Price, tr.Side) {
					tr.Valid++
				}
				if now.Sub(tr.Last) > 24*time.Hour {
					delete(tracks, k)
				}
			}
		}
		if len(tracks) > 20000 {
			for k, tr := range tracks {
				if now.Sub(tr.Last) > time.Minute {
					delete(tracks, k)
				}
			}
		}
		e.intervalTrades[a] = map[int64][2]int64{}

		gradeZones(frame.Zones, frame.Price)
		sort.Slice(frame.Zones, func(i, j int) bool { return frame.Zones[i].Price > frame.Zones[j].Price })
		minute := now.Truncate(time.Minute).Unix()
		c := e.candles[a]
		if c.Time != minute || c.Open == 0 {
			c = Candle{minute, frame.Price, frame.Price, frame.Price, frame.Price}
		} else if frame.Price > 0 {
			c.High = math.Max(c.High, frame.Price)
			if c.Low == 0 {
				c.Low = frame.Price
			} else {
				c.Low = math.Min(c.Low, frame.Price)
			}
			c.Close = frame.Price
		}
		e.candles[a] = c
		frame.Candle = c
		e.frames[a] = frame
	}
	cut := now.Add(-2 * time.Hour).Unix()
	for k, f := range e.flows {
		if f.Minute < cut {
			delete(e.flows, k)
		}
	}
	n := 0
	for n < len(e.tradeQueue) && (now.Sub(e.tradeQueue[n].At) > 2*time.Hour || len(e.tradeQueue)-n > 200000) {
		delete(e.tradeIDs, e.tradeQueue[n].Key)
		n++
	}
	if n > 0 {
		e.tradeQueue = append([]dedupEntry(nil), e.tradeQueue[n:]...)
	}
	if len(e.liquidations) > 5000 {
		e.liquidations = e.liquidations[len(e.liquidations)-5000:]
	}
}
func sourceKey(m map[string]int64) string {
	ss := make([]string, 0, len(m))
	for k := range m {
		ss = append(ss, k)
	}
	sort.Strings(ss)
	return fmt.Sprint(ss)
}
func gradeZones(zs []Zone, price float64) {
	groups := map[string][]int{}
	for i, z := range zs {
		band := int(math.Abs(z.Price-price) / math.Max(price, 1) * 100)
		k := fmt.Sprintf("%s/%d", z.Side, band)
		groups[k] = append(groups[k], i)
	}
	for _, is := range groups {
		sort.Slice(is, func(a, b int) bool { return zs[is[a]].USDCents < zs[is[b]].USDCents })
		for rank, i := range is {
			zs[i].Grade = "普通"
			if len(is) < 10 {
				zs[i].Grade = "样本不足"
			} else if float64(rank+1)/float64(len(is)) >= .9 {
				zs[i].Grade = "较大"
			} else if float64(rank+1)/float64(len(is)) >= .7 {
				zs[i].Grade = "中等"
			}
		}
	}
}
func (e *Engine) Frame(a string) Frame { e.mu.RLock(); defer e.mu.RUnlock(); return e.frames[a] }
func (e *Engine) Flows(a, market string, since time.Time) []Flow {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := []Flow{}
	for _, f := range e.flows {
		if f.Asset == a && f.Market == market && f.Minute >= since.Unix() {
			cp := *f
			cp.PriceBins = map[int64][2]int64{}
			for k, b := range f.PriceBins {
				cp.PriceBins[k] = b
			}
			out = append(out, cp)
		}
	}
	return out
}
func (e *Engine) Derivatives(a string) []Derivative {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := []Derivative{}
	for _, d := range e.deriv {
		if d.Asset == a {
			_, fxOK := e.rateLocked(d.Quote, time.Now())
			d.Valid = d.Valid && time.Since(d.At) < 90*time.Second && fxOK
			out = append(out, d)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Venue < out[j].Venue })
	return out
}
func (e *Engine) SetDerivative(d Derivative) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.deriv[d.Venue+":"+d.Asset] = d
}
func (e *Engine) Liquidation(l Liquidation) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, old := range e.liquidations[max(0, len(e.liquidations)-200):] {
		if old.Venue == l.Venue && old.Asset == l.Asset && old.At.Equal(l.At) && old.USDCents == l.USDCents && old.Side == l.Side {
			return
		}
	}
	e.liquidations = append(e.liquidations, l)
}
func (e *Engine) Liquidations(a string) []Liquidation {
	e.mu.RLock()
	defer e.mu.RUnlock()
	ls := []Liquidation{}
	for _, l := range e.liquidations {
		if l.Asset == a && time.Since(l.At) < 24*time.Hour {
			ls = append(ls, l)
		}
	}
	return ls
}
func (e *Engine) Health() []Health {
	e.mu.RLock()
	defer e.mu.RUnlock()
	hs := []Health{}
	for _, h := range e.health {
		hs = append(hs, h)
	}
	sort.Slice(hs, func(i, j int) bool { return hs[i].Component < hs[j].Component })
	return hs
}
func (e *Engine) Whales(a string) []Whale {
	e.mu.RLock()
	defer e.mu.RUnlock()
	ws := []Whale{}
	_, fxOK := e.rateLocked("USDC", time.Now())
	for address, ps := range e.whales {
		if !e.whaleCore[address] {
			continue
		}
		for _, w := range ps {
			if w.Asset == a {
				w.Valid = time.Since(w.At) <= 90*time.Second && fxOK
				ws = append(ws, w)
			}
		}
	}
	sort.Slice(ws, func(i, j int) bool { return ws[i].USDCents > ws[j].USDCents })
	return ws
}

// Coverage changes, reconnects and exchange boundaries are observation changes, never withdrawals.
func coverageKey(f Frame, price, step float64, side string) string {
	out := ""
	for _, c := range f.Coverage {
		if c.Valid && ((side == "bid" && price+step > c.BidLow && price <= c.Bid) || (side == "ask" && price+step > c.Ask && price <= c.AskHigh)) {
			out += fmt.Sprintf("%s/%s/%d;", c.Venue, c.Symbol, c.Resyncs)
		}
	}
	return out
}

func (e *Engine) markPartialLocked(venue, asset, market string, at time.Time) {
	m := at.Truncate(time.Minute).Unix()
	k := fmt.Sprintf("%s/%s/%s/%d", venue, asset, market, m)
	f := e.flows[k]
	if f == nil {
		f = &Flow{Venue: venue, Asset: asset, Market: market, Minute: m, PriceBins: map[int64][2]int64{}}
		e.flows[k] = f
	}
	f.Partial = true
}
func (e *Engine) MarkGap(venue, market string, from, to time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	from = from.Truncate(time.Minute)
	if from.Before(to.Add(-2 * time.Hour)) {
		from = to.Add(-2 * time.Hour).Truncate(time.Minute)
	}
	for t := from; !t.After(to); t = t.Add(time.Minute) {
		for _, a := range []string{"BTC", "ETH"} {
			e.markPartialLocked(venue, a, market, t)
		}
	}
}
