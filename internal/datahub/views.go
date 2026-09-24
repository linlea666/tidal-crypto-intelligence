package datahub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"sort"
	"strings"
	"time"
)

func metadata(d Dataset, o Observation, ok bool) map[string]any {
	status := "missing"
	if ok {
		status = o.Status(d, time.Now())
	}
	expires := o.Time().Add(time.Duration(d.TTL) * time.Second)
	return map[string]any{"dataset": d.ID, "source": d.Source, "asset": d.Asset, "market": d.Market, "venue": d.Venue, "symbol": d.Symbol, "quote": d.Quote, "unit": d.Unit, "observedAt": o.ObservedAt, "fetchedAt": o.FetchedAt, "timeBasis": o.TimeBasis, "expiresAt": expires, "resolutionSeconds": o.Resolution, "revision": o.Revision, "status": status, "coverageRequested": d.Params["exchange_list"]}
}
func (h *Hub) historyWindow(ctx context.Context, d Dataset, hours int, fn func(Observation) error) (int, error) {
	res := nativeRes(d)
	if hours > 24 {
		res = 900
	}
	if hours > 720 {
		res = 3600
	}
	if d.Kind == "book" && hours > 24 {
		res = 300
	}
	if d.Kind == "book" && hours > 720 {
		res = 3600
	}
	now := time.Now().UTC().Truncate(time.Minute)
	from := now.Add(-time.Duration(hours) * time.Hour)
	return res, h.Store.Visit(ctx, d, res, from, now, fn)
}
func (h *Hub) FlowView(ctx context.Context, a, market string, hours int, anchorText string) (any, error) {
	return h.flowViewAt(ctx, a, market, hours, anchorText, time.Now().UTC().Truncate(time.Minute))
}
func (h *Hub) flowViewAt(ctx context.Context, a, market string, hours int, anchorText string, now time.Time) (any, error) {
	d, _ := h.Dataset(ID("flow", a, "", market))
	from := now.Add(-time.Duration(hours) * time.Hour)
	anchor := from
	if anchorText != "" {
		var err error
		anchor, err = time.Parse(time.RFC3339, anchorText)
		if err != nil || anchor.After(now) || anchor.Before(now.Add(-91*24*time.Hour)) {
			return nil, errors.New("无效CVD起算点")
		}
	}
	res := nativeRes(d)
	if hours > 24 {
		res = 900
	}
	if hours > 720 {
		res = 3600
	}
	stats, e := h.flowWindow(ctx, d, from, now, anchor, res)
	if e != nil {
		return nil, e
	}
	buy, sell, n, partial, series := stats.Buy, stats.Sell, stats.Rows, stats.Partial, stats.Series
	foot := map[string][2]int64{}
	venues := []string{}
	baseTotal, quoteTotal := dec("0"), dec("0")
	footPartial := false
	footMeta := []map[string]any{}
	for _, v := range []string{"Binance", "OKX", "Bybit"} {
		fd, _ := h.Dataset(ID("footprint", a, v, market))
		rows := 0
		validSeconds := 0
		fo, fok := h.Store.Latest(fd.ID)
		footMeta = append(footMeta, metadata(fd, fo, fok))
		if !fok || !fo.Fresh(fd, time.Now()) {
			footPartial = true
		}
		footRes := max(300, res)
		e = h.Store.Visit(ctx, fd, footRes, from, now, func(o Observation) error {
			if recordTime(o).Add(time.Duration(max(300, o.Resolution)) * time.Second).After(now) {
				return nil
			}
			if o.Quality == "missing" {
				footPartial = true
				return nil
			}
			rows++
			if o.Quality != "partial" {
				validSeconds += max(300, o.Resolution)
			}
			for _, f := range o.Payload.Foot {
				key := f.Low + ":" + f.High
				b := foot[key]
				b[0] += money(f.BuyUSDT)
				b[1] += money(f.SellUSDT)
				foot[key] = b
				baseTotal = baseTotal.Add(dec(f.BuyBase)).Add(dec(f.SellBase))
				quoteTotal = quoteTotal.Add(dec(f.BuyQuote)).Add(dec(f.SellQuote))
			}
			return nil
		})
		if e != nil {
			return nil, e
		}
		if validSeconds < int(now.Sub(from).Seconds()) {
			footPartial = true
		}
		if rows > 0 {
			venues = append(venues, v)
		} else {
			footPartial = true
		}
	}
	vwap := 0.0
	if baseTotal.IsPositive() {
		vwap = num(quoteTotal.Div(baseTotal).String())
	}
	latest, ok := h.Store.Latest(d.ID)
	return map[string]any{"buyCents": optionalAmount(buy, n > 0), "sellCents": optionalAmount(sell, n > 0), "netCents": optionalAmount(buy-sell, n > 0), "hasData": n > 0, "series": series, "footprint": foot, "footprintQuote": "USDT", "footprintVenues": venues, "footprintPartial": footPartial, "footprintSources": footMeta, "vwap": vwap, "vwapQuote": "USDT", "partial": partial, "market": market, "resolution": fmt.Sprintf("%ds", res), "step": baseStep(a), "from": from, "to": now, "startedAt": h.boot, "anchor": anchor, "definition": "主动买入额减主动卖出额；不是充值提现。足迹金额保留原始USDT，VWAP按原始报价计算。", "meta": metadata(d, latest, ok)}, nil
}
func (h *Hub) DerivativesView(ctx context.Context, a string, hours int) (any, error) {
	return h.derivativesAt(ctx, a, hours, time.Now().UTC().Truncate(time.Minute))
}
func (h *Hub) derivativesAt(ctx context.Context, a string, hours int, to time.Time) (any, error) {
	from := to.Add(-time.Duration(hours) * time.Hour)
	d, _ := h.Dataset(ID("oi", a, "", "futures"))
	o, ok := h.Store.Latest(d.ID)
	fd, _ := h.Dataset(ID("funding", "ALL", "", "futures"))
	f, fok := h.Store.Latest(fd.ID)
	items := []map[string]any{}
	var total *int64
	for _, r := range o.Payload.OI {
		if strings.EqualFold(r.Venue, "all") {
			n := money(r.USD)
			total = &n
			continue
		}
		items = append(items, map[string]any{"venue": r.Venue, "oiUsdCents": money(r.USD), "oiBase": num(r.Base), "valid": o.Fresh(d, time.Now())})
	}
	funding := []Funding{}
	for _, r := range f.Payload.Funding {
		if r.Asset == a {
			funding = append(funding, r)
		}
	}
	series := []map[string]any{}
	res := nativeRes(d)
	if hours > 24 {
		res = 900
	}
	if hours > 720 {
		res = 3600
	}
	e := h.Store.Visit(ctx, d, res, from, to, func(o Observation) error {
		for _, r := range o.Payload.OI {
			if strings.EqualFold(r.Venue, "all") {
				series = append(series, map[string]any{"time": recordTime(o).Unix(), "oiUsdCents": money(r.USD)})
			}
		}
		return nil
	})
	if e != nil {
		return nil, e
	}
	ld, _ := h.Dataset(ID("liquidations", a, "", "futures"))
	res = nativeRes(ld)
	if hours > 24 {
		res = 900
	}
	if hours > 720 {
		res = 3600
	}
	long, short := int64(0), int64(0)
	liqSeries := []map[string]any{}
	e = h.Store.Visit(ctx, ld, res, from, to, func(o Observation) error {
		if recordTime(o).Add(time.Duration(max(60, o.Resolution)) * time.Second).After(to) {
			return nil
		}
		if r := o.Payload.Liquidation; r != nil {
			l, s := money(r.Long), money(r.Short)
			long += l
			short += s
			liqSeries = append(liqSeries, map[string]any{"time": recordTime(o).Unix(), "longCents": l, "shortCents": s})
		}
		return nil
	})
	if e != nil {
		return nil, e
	}
	lo, lok := h.Store.Latest(ld.ID)
	var oiChange *int64
	if len(series) > 1 {
		first, last := series[0], series[len(series)-1]
		if first["time"].(int64) <= from.Add(5*time.Minute).Unix() && last["time"].(int64) >= to.Add(-10*time.Minute).Unix() {
			n := last["oiUsdCents"].(int64) - first["oiUsdCents"].(int64)
			oiChange = &n
		}
	}
	return map[string]any{"from": from, "to": to, "oiChangeCents": oiChange, "liquidationMeta": metadata(ld, lo, lok), "totalOiCents": total, "items": items, "series": series, "funding": funding, "meta": metadata(d, o, ok), "fundingMeta": metadata(fd, f, fok), "longLiquidationCents": optionalAmount(long, len(liqSeries) > 0), "shortLiquidationCents": optionalAmount(short, len(liqSeries) > 0), "liquidationSeries": liqSeries, "coverage": "OI为上游覆盖交易所；All汇总单独展示，不与分所相加。资金费率按原始结算周期显示。"}, nil
}
func (h *Hub) WhalesView(ctx context.Context, a, side, order string, limit int, step float64) (any, error) {
	d, _ := h.Dataset(ID("whales", "ALL", "Hyperliquid", "futures"))
	o, ok := h.Store.Latest(d.ID)
	now := time.Now().UTC()
	previous := map[string]Whale{}
	var history []Observation
	_ = h.Store.Visit(ctx, d, 300, now.Add(-20*time.Minute), now.Add(-5*time.Minute), func(r Observation) error { history = append(history, r); return nil })
	if len(history) > 0 {
		for _, w := range history[len(history)-1].Payload.Whales {
			if w.Asset == a {
				previous[w.Address] = w
			}
		}
	}
	items := []map[string]any{}
	bucketMap := map[string]map[string]int64{}
	fresh, observed := 0, 0
	long, short, near := int64(0), int64(0), int64(0)
	for _, w := range o.Payload.Whales {
		if w.Asset != a {
			continue
		}
		observed++
		direction := "long"
		if dec(w.Size).IsNegative() {
			direction = "short"
		}
		valid := ok && o.Fresh(d, now) && freshWhale(w, now)
		if valid {
			fresh++
		}
		var distance *float64
		if w.Liquidation != nil && num(w.Mark) > 0 {
			n := math.Abs(num(*w.Liquidation)-num(w.Mark)) / num(w.Mark) * 100
			distance = &n
		}
		var change *string
		if prev, exists := previous[w.Address]; exists {
			v := dec(w.Size).Sub(dec(prev.Size)).String()
			change = &v
		}
		item := map[string]any{"address": w.Address, "asset": a, "side": direction, "size": w.Size, "entry": w.Entry, "mark": num(w.Mark), "usdCents": money(w.USD), "leverage": num(w.Leverage), "margin": w.Margin, "liquidation": w.Liquidation, "distance": distance, "unrealizedCents": money(w.PnL), "fundingFeeCents": money(w.FundingFee), "marginBalanceCents": money(w.MarginBalance), "at": w.At, "firstSeen": w.Created, "changeSize": change, "valid": valid, "quote": "USD", "rate": "1"}
		if side == "all" || side == direction {
			items = append(items, item)
		}
		if !valid {
			continue
		}
		amount := money(w.USD)
		if direction == "long" {
			long += amount
		} else {
			short += amount
		}
		if distance != nil && *distance < 3 {
			near += amount
		}
		for _, kind := range []string{"entry", "liquidation"} {
			value := num(w.Entry)
			if kind == "liquidation" {
				if w.Liquidation == nil {
					continue
				}
				value = num(*w.Liquidation)
			}
			if value <= 0 {
				continue
			}
			p := math.Floor(value/step) * step
			k := fmt.Sprintf("%s/%s/%.8f", kind, direction, p)
			if bucketMap[k] == nil {
				bucketMap[k] = map[string]int64{}
			}
			bucketMap[k][w.Address] += amount
		}
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i]["valid"].(bool) != items[j]["valid"].(bool) {
			return items[i]["valid"].(bool)
		}
		if order == "distance" {
			a, b := items[i]["distance"].(*float64), items[j]["distance"].(*float64)
			if a == nil {
				return false
			}
			if b == nil {
				return true
			}
			return *a < *b
		}
		return items[i]["usdCents"].(int64) > items[j]["usdCents"].(int64)
	})
	buckets := []map[string]any{}
	for key, addresses := range bucketMap {
		parts := strings.Split(key, "/")
		sum, largest := int64(0), int64(0)
		for _, n := range addresses {
			sum += n
			largest = max(largest, n)
		}
		buckets = append(buckets, map[string]any{"kind": parts[0], "side": parts[1], "price": num(parts[2]), "usdCents": sum, "addresses": len(addresses), "largestShare": float64(largest) / float64(max(1, sum))})
	}
	monitor := map[string]any{"candidates": observed, "fresh": fresh, "scope": "CoinGlass覆盖的Hyperliquid百万美元级持仓，不代表全市场", "refreshSeconds": WhaleRefreshSeconds, "ttlSeconds": WhaleTTLSeconds, "pinned": []string{}, "websocketUsers": 0, "coreLimit": 100, "limit": 100}
	return map[string]any{"items": items[:min(limit, len(items))], "count": len(items), "buckets": buckets, "monitor": monitor, "at": now, "longCents": optionalAmount(long, fresh > 0), "shortCents": optionalAmount(short, fresh > 0), "nearLiquidationCents": optionalAmount(near, fresh > 0), "hasData": fresh > 0, "meta": metadata(d, o, ok), "distributionScope": "全部有效已覆盖大仓，不受榜单前50/100及方向筛选影响"}, nil
}
func (h *Hub) LargeView(a string, history bool) any {
	kind := "large"
	if history {
		kind = "large-history"
	}
	items := []map[string]any{}
	sources := []map[string]any{}
	now := time.Now()
	for _, d := range Registry() {
		if d.Kind != kind || d.Asset != a {
			continue
		}
		o, ok := h.Store.Latest(d.ID)
		sources = append(sources, metadata(d, o, ok))
		rate, fxAt, fx := h.Rate(d.Quote, now)
		for _, r := range o.Payload.Large {
			var usd, priceUSD any
			if fx {
				usd = money(multiply(multiply(r.Price, r.Quantity), rate))
				priceUSD = num(multiply(r.Price, rate))
			}
			items = append(items, map[string]any{"id": d.ID + ":" + r.ID, "venue": d.Venue, "side": r.Side, "price": r.Price, "quote": d.Quote, "priceUsd": priceUSD, "quantity": r.Quantity, "usdCents": usd, "reportedUsd": r.ReportedUSD, "executedUsd": r.ExecutedUSD, "state": r.State, "startAt": r.Start, "changedAt": r.Changed, "fetchedAt": o.FetchedAt, "valid": fx && o.Fresh(d, now) && (r.RawState == 1 || r.RawState == 0), "fxAt": fxAt, "trades": r.Trades, "rawState": r.RawState, "endAt": r.End, "initialQuantity": r.InitialQuantity, "initialUsd": r.InitialUSD, "executedQuantity": r.ExecutedQuantity})
		}
	}
	sort.Slice(items, func(i, j int) bool { return num(items[i]["price"]) > num(items[j]["price"]) })
	return map[string]any{"items": items, "sources": sources, "note": "大额挂单是筛选出的跟踪记录，与盘口金额可能重叠，不重复相加；远处发现大单不代表完整盘口覆盖。"}
}
func (h *Hub) LiquidationView(a, r string) any {
	result := map[string]any{}
	for _, kind := range []string{"map", "heatmap"} {
		d, _ := h.Dataset(ID(kind, a, "", "futures"))
		id := d.ID
		if r != "24h" {
			id += "@" + r
			d.ID = id
		}
		o, ok := h.Store.Latest(id)
		result[kind] = map[string]any{"data": o.Payload.Model, "meta": metadata(d, o, ok)}
	}
	result["note"] = "模型强度为相对估计，不是必然爆仓金额；实际清算与账户参考清算价分别展示。"
	return result
}
func (h *Hub) HistoryView(ctx context.Context, a string, hours int, price, step, span float64, heat bool) (any, error) {
	now := time.Now().UTC()
	from := now.Add(-time.Duration(hours) * time.Hour)
	res := 60
	if hours > 24 || heat {
		res = 300
	}
	if hours > 720 || heat && hours > 168 {
		res = 3600
	}
	// Saved FX is used at its own actual granularity. Hourly historical FX is an
	// explicit approximation, never an invented dollar peg.
	fx := map[int64]map[string]string{}
	fineFX := map[int64]map[string]string{}
	fd, _ := h.Dataset("fx.usd.kraken")
	for _, r := range []int{3600, 60} {
		if err := h.Store.Visit(ctx, fd, r, from.Add(-time.Hour), now, func(o Observation) error {
			rates := map[string]string{}
			for _, q := range o.Payload.Rates {
				rates[q.Quote] = q.USD
			}
			if r == 3600 {
				fx[recordTime(o).Truncate(time.Hour).Unix()] = rates
			} else {
				fineFX[recordTime(o).Truncate(time.Minute).Unix()] = rates
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}
	type point struct {
		time        int64
		usd         int64
		covered     int
		partial     bool
		approximate bool
		zones       map[string]Zone
	}
	points := map[int64]*point{}
	center, _, _ := h.CurrentPrice(a, now)
	for _, d := range Registry() {
		if d.Kind != "book" || d.Asset != a {
			continue
		}
		err := h.Store.Visit(ctx, d, res, from, now, func(o Observation) error {
			if o.Payload.Book == nil || o.Quality == "missing" {
				return nil
			}
			t := recordTime(o)
			key := t.Truncate(time.Duration(res) * time.Second).Unix()
			p := points[key]
			if p == nil {
				p = &point{time: key, zones: map[string]Zone{}}
				points[key] = p
			}
			rate := "1"
			if d.Quote != "USD" {
				rate = fineFX[t.Truncate(time.Minute).Unix()][d.Quote]
				if rate == "" {
					rate = fx[t.Truncate(time.Hour).Unix()][d.Quote]
					p.approximate = true
				}
				if rate == "" {
					p.partial = true
					return nil
				}
			}
			book := o.Payload.Book
			bid, ask := best(book)
			r := num(rate)
			if price > 0 && ((price >= book.Low*r && price+step <= bid*r+step) || (price >= ask*r-step && price+step <= book.High*r+step)) {
				p.covered++
			}
			if o.Quality == "partial" {
				p.partial = true
			}
			for side, levels := range map[string][]Level{"bid": book.Bids, "ask": book.Asks} {
				for _, l := range levels {
					lp := num(multiply(l.Price, rate))
					usd := money(multiply(multiply(l.Price, l.Quantity), rate))
					if lp >= price && lp < price+step {
						p.usd += usd
					}
					if heat && center > 0 && math.Abs(lp-center)/center*100 <= math.Min(span, 30) {
						low := math.Floor(lp/step) * step
						k := fmt.Sprintf("%s/%.8f", side, low)
						z := p.zones[k]
						z.Price = low
						z.Step = step
						z.Side = side
						z.USD += usd
						p.zones[k] = z
					}
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	out := []map[string]any{}
	keys := []int64{}
	for t := range points {
		keys = append(keys, t)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	stride := max(1, (len(keys)+719)/720)
	for i, t := range keys {
		if i%stride != 0 {
			continue
		}
		p := points[t]
		zs := []Zone{}
		for _, z := range p.zones {
			zs = append(zs, z)
		}
		sort.Slice(zs, func(i, j int) bool { return zs[i].Price < zs[j].Price })
		if len(zs) > 400 {
			return nil, errors.New("历史价位过多，请增大价格精度或缩小范围")
		}
		out = append(out, map[string]any{"time": p.time, "price": price, "usdCents": optionalAmount(p.usd, p.covered > 0), "sources": p.covered, "zones": zs, "partial": p.partial || p.covered < 5, "hourlyFX": p.approximate})
	}
	return map[string]any{"points": out, "resolution": fmt.Sprintf("%ds", res), "sampleSeconds": res * stride, "from": from, "to": now, "startedAt": h.boot, "note": "来源独立保存；历史FX按实际分钟或小时精度换算。未覆盖不补零；显示抽样不改变原始保存精度。"}, nil
}
func (h *Hub) CandleView(ctx context.Context, a string, hours int) (any, error) {
	d, _ := h.Dataset(ID("candles", a, "Binance", "spot"))
	points := []map[string]any{}
	res, e := h.historyWindow(ctx, d, hours, func(o Observation) error {
		if c := o.Payload.Candle; c != nil {
			points = append(points, map[string]any{"time": recordTime(o).Unix(), "open": c.Open, "high": c.High, "low": c.Low, "close": c.Close, "volume": c.Volume})
		}
		return nil
	})
	return map[string]any{"points": points, "resolution": fmt.Sprintf("%ds", res), "quote": "USDT", "source": "Binance"}, e
}
func (h *Hub) Status() any {
	return map[string]any{"datasets": h.Catalog(), "scheduler": h.Scheduler.State(), "storage": h.Store.Status(), "startedAt": h.boot, "at": time.Now().UTC(), "rulesVersion": RulesVersion, "legacyCollectorsRunning": false}
}

// Read never schedules or fetches. Even a completely empty local query is pure.
func (h *Hub) Read(ctx context.Context, path string, q url.Values) (json.RawMessage, error) {
	a := q.Get("asset")
	if a == "" {
		a = "BTC"
	}
	if !ValidAsset(a) {
		return nil, errors.New("仅支持BTC或ETH")
	}
	hours := parseInt(q, "hours", 24, 1, 2160)
	market := q.Get("market")
	if market == "" || market == "spot" {
		market = "spot"
	} else if market == "perp" || market == "futures" {
		market = "futures"
	} else {
		return nil, errors.New("无效市场")
	}
	step := parseFloat(q, "step", baseStep(a)*4, baseStep(a), baseStep(a)*100)
	span := parseFloat(q, "range", 10, .1, 1000)
	ttl := 10 * time.Second
	if path == "overview" || path == "levels" {
		ttl = time.Second
	}
	key := path + "?" + q.Encode()
	return h.cached(ctx, key, ttl, func() (any, error) {
		switch path {
		case "overview", "levels":
			return h.Overview(ctx, a, step, span, int64(parseInt(q, "minAge", 0, 0, 86400*30))), nil
		case "flow":
			return h.FlowView(ctx, a, market, hours, q.Get("anchor"))
		case "derivatives":
			return h.DerivativesView(ctx, a, hours)
		case "whales":
			side := q.Get("side")
			if side == "" {
				side = "all"
			}
			return h.WhalesView(ctx, a, side, q.Get("sort"), parseInt(q, "limit", 50, 1, 100), step)
		case "activity":
			hours = parseInt(q, "hours", 1, 1, 24)
			if hours != 1 && hours != 4 && hours != 24 {
				return nil, errors.New("动向窗口仅支持1、4、24小时")
			}
			activitySpan := parseFloat(q, "range", 5, .1, 1000)
			return h.ActivityView(ctx, a, hours, activitySpan)
		case "large-orders":
			return h.LargeOrdersPage(ctx, a, q.Get("history") == "1", parseInt(q, "limit", 100, 1, 300), parseInt(q, "offset", 0, 0, 10000))
		case "liquidations":
			r := q.Get("period")
			if r == "" {
				r = "24h"
			}
			if r != "24h" && r != "7d" && r != "30d" {
				return nil, errors.New("无效清算周期")
			}
			return h.LiquidationView(a, r), nil
		case "history":
			return h.HistoryView(ctx, a, hours, parseFloat(q, "price", 0, 0, 1e7), step, span, q.Get("heatmap") == "1")
		case "candles":
			return h.CandleView(ctx, a, hours)
		case "data/catalog":
			return map[string]any{"datasets": h.Catalog()}, nil
		case "data-status":
			return h.Status(), nil
		default:
			if strings.HasPrefix(path, "data/") {
				id := strings.TrimPrefix(path, "data/")
				d, ok := h.Dataset(id)
				if !ok {
					return nil, errors.New("未知数据集")
				}
				o, exists := h.Store.Latest(id)
				if q.Get("from") == "" {
					return map[string]any{"meta": metadata(d, o, exists), "data": o.Payload}, nil
				}
				from, e := time.Parse(time.RFC3339, q.Get("from"))
				if e != nil {
					return nil, errors.New("无效起始时间")
				}
				to, e := time.Parse(time.RFC3339, q.Get("to"))
				if e != nil || !from.Before(to) || to.Sub(from) > 90*24*time.Hour || from.Before(time.Now().Add(-91*24*time.Hour)) {
					return nil, errors.New("历史查询限制90天")
				}
				res := parseInt(q, "resolution", nativeRes(d), 60, 3600)
				valid := res == nativeRes(d) || res == 300 || res == 900 || res == 3600
				if !valid {
					return nil, errors.New("无效保存精度")
				}
				rows, more, e := h.Store.Query(ctx, d, res, from, to, parseInt(q, "limit", 100, 1, 500))
				var cursor *time.Time
				if more && len(rows) > 0 {
					t := recordTime(rows[len(rows)-1]).Add(time.Second)
					cursor = &t
				}
				return map[string]any{"dataset": d, "rows": rows, "hasMore": more, "nextFrom": cursor, "resolutionSeconds": res, "missing": len(rows) == 0}, e
			}
			return nil, errors.New("未知V2接口")
		}
	})
}

func optionalAmount(n int64, ok bool) any {
	if !ok {
		return nil
	}
	return n
}
