package datahub

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/shopspring/decimal"
)

const OrderZoneRules = "large-order-zones-1"

type OrderZoneSource struct {
	Venue    string `json:"venue"`
	Quantity string `json:"quantity"`
	USD      int64  `json:"usdCents"`
	Orders   int    `json:"orders"`
}
type OrderZone struct {
	ID                        string            `json:"id"`
	Side                      string            `json:"side"`
	Center                    float64           `json:"center"`
	Low                       float64           `json:"low"`
	High                      float64           `json:"high"`
	USD                       int64             `json:"usdCents"`
	Quantity                  string            `json:"quantity"`
	Rank                      int               `json:"rank"`
	Focus                     bool              `json:"focus"`
	Distance                  float64           `json:"distancePercent"`
	Longest                   *float64          `json:"longestSeconds"`
	LocalFirst                *time.Time        `json:"localFirstAt"`
	Sources                   []OrderZoneSource `json:"sources"`
	LargestShare              float64           `json:"largestSourcePercent"`
	Items                     []map[string]any  `json:"items"`
	Evidence                  string            `json:"evidence"`
	ChangeQuantity            *string           `json:"changeQuantity"`
	ChangeFrom                *time.Time        `json:"changeFrom"`
	ChangeTo                  *time.Time        `json:"changeTo"`
	ExecutedQuantity          *string           `json:"executedQuantity"`
	ExecutedCents             *int64            `json:"executedCents"`
	UncertainExecutedQuantity *string           `json:"uncertainExecutedQuantity"`
}
type ZoneTrade struct {
	Center      float64  `json:"center"`
	Low         float64  `json:"low"`
	High        float64  `json:"high"`
	Buy         string   `json:"buyQuantity"`
	Sell        string   `json:"sellQuantity"`
	BuyCents    int64    `json:"buyCents"`
	SellCents   int64    `json:"sellCents"`
	Sources     []string `json:"sources"`
	Approximate bool     `json:"approximate"`
}
type ZoneTradeCoverage struct {
	Venue     string     `json:"venue"`
	Samples   int        `json:"samples"`
	Expected  int        `json:"expectedSamples"`
	MissingFX int        `json:"missingFX"`
	Through   *time.Time `json:"through"`
}
type ZoneTrades struct {
	From     time.Time           `json:"from"`
	To       time.Time           `json:"to"`
	Zones    []ZoneTrade         `json:"zones"`
	Coverage []ZoneTradeCoverage `json:"coverage"`
	Partial  bool                `json:"partial"`
	Note     string              `json:"note"`
}
type OrderZoneView struct {
	Rules         string           `json:"rulesVersion"`
	Asset         string           `json:"asset"`
	Version       string           `json:"version"`
	At            time.Time        `json:"at"`
	Expires       time.Time        `json:"expiresAt"`
	Reference     *float64         `json:"referencePrice"`
	PriceAt       *time.Time       `json:"priceAt"`
	Step          float64          `json:"step"`
	Range         float64          `json:"range"`
	Hours         int              `json:"hours"`
	Zones         []OrderZone      `json:"zones"`
	Sources       []map[string]any `json:"sources"`
	Scale         int64            `json:"scaleMaxCents"`
	Bid           *int64           `json:"bidCents"`
	Ask           *int64           `json:"askCents"`
	Fetched       int              `json:"fetchedCount"`
	Valid         int              `json:"validCount"`
	Excluded      int              `json:"excludedCount"`
	Partial       bool             `json:"partial"`
	Support       string           `json:"supportId"`
	Resistance    string           `json:"resistanceId"`
	Trades        ZoneTrades       `json:"trades"`
	EventsPartial bool             `json:"eventsPartial"`
	HistoryStatus []map[string]any `json:"historyStatus"`
	Note          string           `json:"note"`
}

// Centered bins are an independent display contract. Existing floor-based book
// cohorts and their grades remain unchanged. Use decimal arithmetic at boundaries.
func orderZoneCenter(p string, step float64) float64 {
	return centeredOrderPrice(dec(p), decimal.NewFromFloat(step), decimal.NewFromFloat(step/2))
}

// QuoRem avoids finite-precision division rounding across a half-open boundary.
func centeredOrderPrice(p, step, half decimal.Decimal) float64 {
	q, _ := p.Add(half).QuoRem(step, 0)
	return q.Mul(step).InexactFloat64()
}
func orderZoneID(side string, center float64) string { return fmt.Sprintf("%s/%.8f", side, center) }
func orderZoneParams(a string, q url.Values) (float64, float64, int, error) {
	step := 250.0
	if a == "ETH" {
		step = 10
	}
	if q.Has("step") {
		step = parseFloat(q, "step", step, 0, 1e6)
	}
	allowed := false
	for _, s := range steps(a) {
		if step == s {
			allowed = true
		}
	}
	if !allowed {
		return 0, 0, 0, fmt.Errorf("不支持的价格精度")
	}
	span := parseFloat(q, "range", 10, 1, 30)
	hours := parseInt(q, "hours", 4, 1, 24)
	if hours != 1 && hours != 4 && hours != 24 {
		return 0, 0, 0, fmt.Errorf("成交窗口仅支持1、4、24小时")
	}
	return step, span, hours, nil
}
func (h *Hub) orderZones(ctx context.Context, a string, q url.Values, now time.Time) (OrderZoneView, error) {
	step, span, hours, err := orderZoneParams(a, q)
	if err != nil {
		return OrderZoneView{}, err
	}
	out := OrderZoneView{Rules: OrderZoneRules, Asset: a, At: now, Expires: now.Add(30 * time.Second), Step: step, Range: span, Hours: hours, Zones: []OrderZone{}, Sources: []map[string]any{}, Note: "仅已获取的现货大单；金额排名不是反弹概率。来源创建跨度不代表当前数量一直存在。"}
	p, pat, priceOK, rows, sources, err := h.orderZoneSnapshot(ctx, a, now)
	if err != nil {
		return out, err
	}
	out.PriceAt = pat
	out.Sources = sources
	for _, s := range sources {
		if at, ok := s["fxAt"].(*time.Time); ok && at != nil {
			out.Expires = minTime(out.Expires, at.Add(30*time.Second))
		}
	}
	if priceOK {
		out.Reference = &p
		if pat != nil {
			out.Expires = minTime(out.Expires, pat.Add(15*time.Second))
		}
	}
	byKey := map[string]*OrderZone{}
	for _, r := range rows {
		out.Fetched++
		if r["valid"] != true || !priceOK || !dec(str(r["quantity"])).IsPositive() || num(r["usdCents"]) <= 0 {
			out.Excluded++
			continue
		}
		lp := num(r["priceUsd"])
		if math.Abs(lp-p)/p*100 > span {
			continue
		}
		side := str(r["side"])
		// A quote that crosses the reference is still a quote, but cannot be a
		// lower support/upper resistance candidate against this reference.
		if side == "bid" && lp >= p || side == "ask" && lp <= p {
			out.Excluded++
			continue
		}
		center := orderZoneCenter(multiply(str(r["price"]), str(r["fxRate"])), step)
		key := orderZoneID(side, center)
		z := byKey[key]
		if z == nil {
			z = &OrderZone{ID: key, Side: side, Center: center, Low: center - step/2, High: center + step/2, Quantity: "0", Sources: []OrderZoneSource{}, Items: []map[string]any{}, Distance: (center/p - 1) * 100, Evidence: "尚无同来源匹配成交证据"}
			byKey[key] = z
		}
		r["durationSeconds"] = nil
		end := r["fetchedAt"].(time.Time)
		if t, ok := r["observedAt"].(*time.Time); ok && t != nil {
			end = *t
		}
		start, _ := r["startAt"].(*time.Time)
		if start != nil && !start.After(end) {
			v := end.Sub(*start).Seconds()
			r["durationSeconds"] = v
			if z.Longest == nil || v > *z.Longest {
				z.Longest = &v
			}
		}
		if t, ok := r["localFirstAt"].(time.Time); ok && (z.LocalFirst == nil || t.Before(*z.LocalFirst)) {
			z.LocalFirst = &t
		}
		cents := int64(num(r["usdCents"]))
		qty := str(r["quantity"])
		v := str(r["venue"])
		z.USD += cents
		z.Quantity = dec(z.Quantity).Add(dec(qty)).String()
		z.Items = append(z.Items, r)
		found := false
		for i := range z.Sources {
			if z.Sources[i].Venue == v {
				z.Sources[i].USD += cents
				z.Sources[i].Quantity = dec(z.Sources[i].Quantity).Add(dec(qty)).String()
				z.Sources[i].Orders++
				found = true
				break
			}
		}
		if !found {
			z.Sources = append(z.Sources, OrderZoneSource{v, qty, cents, 1})
		}
		out.Valid++
		if t, ok := r["expiresAt"].(time.Time); ok {
			out.Expires = minTime(out.Expires, t)
		}
	}
	out.Partial = !priceOK
	anyFresh := false
	for _, m := range out.Sources {
		fresh := m["usable"] == true
		anyFresh = anyFresh || fresh
		if !fresh {
			out.Partial = true
		}
	}
	var bid, ask int64
	for _, z := range byKey {
		sort.Slice(z.Sources, func(i, j int) bool { return z.Sources[i].USD > z.Sources[j].USD })
		if z.USD > 0 {
			z.LargestShare = float64(z.Sources[0].USD) / float64(z.USD) * 100
		}
		if z.Side == "bid" {
			bid += z.USD
		} else {
			ask += z.USD
		}
		if z.USD > out.Scale {
			out.Scale = z.USD
		}
		out.Zones = append(out.Zones, *z)
	}
	if anyFresh && priceOK {
		out.Bid = &bid
		out.Ask = &ask
	}
	rankOrderZones(out.Zones, p)
	sort.Slice(out.Zones, func(i, j int) bool {
		if out.Zones[i].Center == out.Zones[j].Center {
			return out.Zones[i].Side < out.Zones[j].Side
		}
		return out.Zones[i].Center > out.Zones[j].Center
	})
	for _, z := range out.Zones {
		if z.Rank == 1 {
			if z.Side == "bid" {
				out.Support = z.ID
			} else {
				out.Resistance = z.ID
			}
		}
	}
	out.Trades, err = h.orderZoneTrades(ctx, a, hours, step, span, p, priceOK, now, out.Zones)
	if err != nil {
		return out, err
	}
	if err = h.orderZoneEvents(ctx, a, step, out.Trades.From, out.Trades.To, &out); err != nil {
		return out, err
	}
	out.HistoryStatus = h.orderHistoryStatus(a)
	for _, s := range out.HistoryStatus {
		if num(s["gaps"]) > 0 || str(s["error"]) != "" {
			out.EventsPartial = true
		}
		if t, ok := s["through"].(*time.Time); !ok || t == nil || t.Before(out.Trades.To) {
			out.EventsPartial = true
		}
	}
	b, _ := json.Marshal(out)
	sum := sha256.Sum256(b)
	out.Version = fmt.Sprintf("%x", sum[:12])
	return out, ctx.Err()
}
func rankOrderZones(zs []OrderZone, p float64) {
	for _, side := range []string{"bid", "ask"} {
		ix := []int{}
		for i, z := range zs {
			if z.Side == side {
				ix = append(ix, i)
			}
		}
		sort.Slice(ix, func(i, j int) bool {
			x, y := zs[ix[i]], zs[ix[j]]
			if x.USD == y.USD {
				return x.ID < y.ID
			}
			return x.USD > y.USD
		})
		for n, i := range ix {
			zs[i].Rank = n + 1
			if n < 3 {
				zs[i].Focus = true
			}
		}
		sort.Slice(ix, func(i, j int) bool {
			x, y := zs[ix[i]], zs[ix[j]]
			dx, dy := math.Abs(x.Center-p), math.Abs(y.Center-p)
			if dx == dy {
				return x.ID < y.ID
			}
			return dx < dy
		})
		for _, i := range ix[:min(3, len(ix))] {
			zs[i].Focus = true
		}
	}
}

type zoneFX struct{ fine, hour map[int64]map[string]string }

func (h *Hub) orderZoneFX(ctx context.Context, from, to time.Time) (zoneFX, error) {
	f := zoneFX{map[int64]map[string]string{}, map[int64]map[string]string{}}
	d, _ := h.Dataset("fx.usd.kraken")
	for _, res := range []int{60, 3600} {
		err := h.Store.Visit(ctx, d, res, from.Add(-time.Hour), to, func(o Observation) error {
			if o.Quality == "missing" {
				return nil
			}
			m := map[string]string{}
			for _, r := range o.Payload.Rates {
				if dec(r.USD).IsPositive() {
					m[r.Quote] = r.USD
				}
			}
			if res == 60 {
				f.fine[recordTime(o).Truncate(time.Minute).Unix()] = m
			} else {
				f.hour[recordTime(o).Truncate(time.Hour).Unix()] = m
			}
			return nil
		})
		if err != nil {
			return f, err
		}
	}
	return f, nil
}
func (f zoneFX) rate(quote string, at time.Time) (string, bool) {
	if quote == "USD" {
		return "1", false
	}
	if r := f.fine[at.Truncate(time.Minute).Unix()][quote]; r != "" {
		return r, false
	}
	r := f.hour[at.Truncate(time.Hour).Unix()][quote]
	return r, r != ""
}
func (h *Hub) orderZoneTrades(ctx context.Context, a string, hours int, step, span, p float64, priceOK bool, now time.Time, zs []OrderZone) (ZoneTrades, error) {
	to := now.Truncate(5 * time.Minute)
	from := to.Add(-time.Duration(hours) * time.Hour)
	out := ZoneTrades{From: from, To: to, Zones: []ZoneTrade{}, Coverage: []ZoneTradeCoverage{}, Note: "已闭合5分钟足迹，按价格区间中点归档；跨档位或小时汇率换算标为近似。Bybit为独立成交背景；买挂单成交属于被动承接。"}
	fx, err := h.orderZoneFX(ctx, from, to)
	if err != nil {
		return out, err
	}
	type match struct {
		price    float64
		observed time.Time
		zone     int
	}
	matches := map[string][]match{}
	for i, z := range zs {
		for _, r := range z.Items {
			at, _ := r["localFirstAt"].(time.Time)
			v := str(r["venue"])
			if !at.IsZero() {
				matches[v] = append(matches[v], match{num(r["price"]), at, i})
			}
		}
	}
	for v := range matches {
		sort.Slice(matches[v], func(i, j int) bool { return matches[v][i].price < matches[v][j].price })
	}
	bins := map[float64]*ZoneTrade{}
	for _, v := range []string{"Binance", "OKX", "Bybit"} {
		d, _ := h.Dataset(ID("footprint", a, v, "spot"))
		c := ZoneTradeCoverage{Venue: v, Expected: hours * 12}
		err = h.Store.Visit(ctx, d, 300, from, to, func(o Observation) error {
			at := recordTime(o)
			end := at.Add(5 * time.Minute)
			if end.After(to) || o.Quality == "missing" {
				return nil
			}
			if o.Quality != "partial" {
				c.Samples++
			}
			c.Through = &end
			rate, approx := fx.rate(d.Quote, at)
			if rate == "" {
				c.MissingFX++
				return nil
			}
			for _, f := range o.Payload.Foot {
				low, high := num(multiply(f.Low, rate)), num(multiply(f.High, rate))
				mid := multiply(dec(f.Low).Add(dec(f.High)).Div(dec("2")).String(), rate)
				if !priceOK || math.Abs(num(mid)-p)/p*100 > span {
					continue
				}
				center := orderZoneCenter(mid, step)
				z := bins[center]
				if z == nil {
					z = &ZoneTrade{Center: center, Low: center - step/2, High: center + step/2, Buy: "0", Sell: "0", Sources: []string{}}
					bins[center] = z
				}
				z.Buy = dec(z.Buy).Add(dec(f.BuyBase)).String()
				z.Sell = dec(z.Sell).Add(dec(f.SellBase)).String()
				z.BuyCents += money(multiply(f.BuyQuote, rate))
				z.SellCents += money(multiply(f.SellQuote, rate))
				z.Approximate = z.Approximate || approx || low < z.Low || high > z.High
				if !contains(z.Sources, v) {
					z.Sources = append(z.Sources, v)
				}
				// Background is only called matched when venue, native price overlap,
				// and an actually observed earlier order agree. It is not a fill claim.
				if v == "Binance" || v == "OKX" {
					ms := matches[v]
					lowNative, highNative := num(f.Low), num(f.High)
					for i := sort.Search(len(ms), func(i int) bool { return ms[i].price >= lowNative }); i < len(ms) && ms[i].price < highNative; i++ {
						m := ms[i]
						if m.observed.After(at) {
							continue
						}
						contra := f.SellBase
						if zs[m.zone].Side == "ask" {
							contra = f.BuyBase
						}
						if dec(contra).IsPositive() {
							zs[m.zone].Evidence = "有同所价位成交背景 · 未确认本单成交"
						}
					}
				}
			}
			return nil
		})
		if err != nil {
			return out, err
		}
		if c.Samples < c.Expected || c.MissingFX > 0 {
			out.Partial = true
		}
		out.Coverage = append(out.Coverage, c)
	}
	for _, z := range bins {
		out.Zones = append(out.Zones, *z)
	}
	sort.Slice(out.Zones, func(i, j int) bool {
		x, y := out.Zones[i], out.Zones[j]
		if x.BuyCents+x.SellCents == y.BuyCents+y.SellCents {
			return x.Center < y.Center
		}
		return x.BuyCents+x.SellCents > y.BuyCents+y.SellCents
	})
	return out, nil
}
func contains(xs []string, v string) bool {
	for _, x := range xs {
		if x == v {
			return true
		}
	}
	return false
}
func (h *Hub) orderZoneEvents(ctx context.Context, a string, step float64, from, to time.Time, out *OrderZoneView) error {
	events, more, err := h.Store.OrderEvents(ctx, a, from, to, 2000, 0)
	if err != nil {
		return err
	}
	out.EventsPartial = more
	// Changes use native quantity, never FX movement. Current-zone membership is
	// explicitly frozen; only unchanged native price and known before/after count.
	keys := map[string]int{}
	for i, z := range out.Zones {
		for _, r := range z.Items {
			keys[str(r["key"])] = i
		}
	}
	seen := map[string]bool{}
	for _, e := range events {
		key := e.Key
		if pos := strings.LastIndex(key, ":"); pos >= 0 {
			key = key[:pos]
		}
		i, ok := keys[key]
		if !ok {
			continue
		}
		z := &out.Zones[i]
		if e.Kind == "correction" {
			out.EventsPartial = true
			continue
		}
		if e.ExecutedQuantityDelta != nil && dec(*e.ExecutedQuantityDelta).IsPositive() {
			target := &z.ExecutedQuantity
			if e.From == nil || e.From.Before(from) {
				target = &z.UncertainExecutedQuantity
				out.EventsPartial = true
			}
			s := "0"
			if *target != nil {
				s = **target
			}
			s = dec(s).Add(dec(*e.ExecutedQuantityDelta)).String()
			*target = &s
			if target == &z.ExecutedQuantity && e.ExecutedDelta != nil {
				n := money(*e.ExecutedDelta)
				if z.ExecutedCents == nil {
					z.ExecutedCents = &n
				} else {
					*z.ExecutedCents += n
				}
			}
		}
		if !seen[key] && e.Before != nil && e.After != nil && e.From != nil && e.From.After(from) && e.Before.Price == e.After.Price && e.Kind == "change" && e.QuantityDelta != nil {
			seen[key] = true
			s := "0"
			if z.ChangeQuantity != nil {
				s = *z.ChangeQuantity
			}
			s = dec(s).Add(dec(*e.QuantityDelta)).String()
			z.ChangeQuantity = &s
			if z.ChangeFrom == nil || e.From.Before(*z.ChangeFrom) {
				z.ChangeFrom = e.From
			}
			if z.ChangeTo == nil || e.At.After(*z.ChangeTo) {
				t := e.At
				z.ChangeTo = &t
			}
		}
	}
	return nil
}

// Hold the existing writer only while capturing current facts. Slow historical
// scans happen after release, and cannot partially change this snapshot's rows.
func (h *Hub) orderZoneSnapshot(ctx context.Context, a string, now time.Time) (float64, *time.Time, bool, []map[string]any, []map[string]any, error) {
	wait := time.NewTicker(5 * time.Millisecond)
	defer wait.Stop()
	for !h.Store.write.TryLock() {
		select {
		case <-ctx.Done():
			return 0, nil, false, nil, nil, ctx.Err()
		case <-wait.C:
		}
	}
	defer h.Store.write.Unlock()
	p, pat, ok := h.CurrentPrice(a, now)
	ok = ok && p > 0
	raw := h.largeViewAt(a, false, now).(map[string]any)
	rows := raw["items"].([]map[string]any)
	sources := raw["sources"].([]map[string]any)
	if len(rows) > 5000 {
		return 0, nil, false, nil, nil, fmt.Errorf("大单记录超过有界查询预算")
	}
	// Identity duplicates are not additional liquidity. Conflicting duplicate
	// facts invalidate that source snapshot rather than selecting an arbitrary row.
	unique := make([]map[string]any, 0, len(rows))
	identities := map[string]map[string]any{}
	conflicts := map[string]bool{}
	for _, r := range rows {
		k := str(r["key"])
		if prev, seen := identities[k]; seen {
			if str(prev["price"]) != str(r["price"]) || str(prev["quantity"]) != str(r["quantity"]) || str(prev["side"]) != str(r["side"]) || str(prev["rawState"]) != str(r["rawState"]) {
				conflicts[str(r["venue"])] = true
			}
			continue
		}
		identities[k] = r
		unique = append(unique, r)
	}
	rows = unique
	all := make([]any, len(rows))
	for i, r := range rows {
		all[i] = r
	}
	if err := h.reconcileOrderRows(ctx, a, all, now); err != nil {
		return 0, nil, false, nil, nil, err
	}
	for _, r := range rows {
		var b []byte
		var tracked TrackedOrder
		if err := h.Store.db.QueryRowContext(ctx, "SELECT payload FROM tracked_orders WHERE k=?", r["key"]).Scan(&b); err == nil && json.Unmarshal(b, &tracked) == nil {
			r["localFirstAt"] = tracked.FirstSeen
		}
	}
	for _, m := range sources {
		d, _ := h.Dataset(str(m["dataset"]))
		o, exists := h.Store.Latest(d.ID)
		rate, at, fx := h.Rate(d.Quote, now)
		m["conflictingDuplicates"] = conflicts[d.Venue]
		m["usable"] = !conflicts[d.Venue] && exists && o.Quality == "valid" && o.Fresh(d, now) && fx && dec(rate).IsPositive()
		m["fxAt"] = at
		m["fxRate"] = rate
		if m["usable"] != true {
			for _, r := range rows {
				if str(r["venue"]) == d.Venue {
					r["valid"] = false
				}
			}
		}
	}
	return p, pat, ok, rows, sources, ctx.Err()
}
