package datahub

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/url"
	"regexp"
	"time"
)

type LiquidationReference struct {
	At      time.Time  `json:"at"`
	Native  *float64   `json:"native"`
	Quote   string     `json:"quote"`
	PriceAt *time.Time `json:"priceAt"`
	USD     *float64   `json:"usd"`
	FX      string     `json:"fx"`
	FXAt    *time.Time `json:"fxAt"`
	FXValid bool       `json:"fxValid"`
	ATR     *float64   `json:"atr"`
}

func (h *Hub) liquidationReference(ctx context.Context, a, quote string, now time.Time) LiquidationReference {
	r := LiquidationReference{At: now, Quote: quote}
	pd, _ := h.Dataset(ID("price", a, "Binance", "spot"))
	o, ok := h.Store.Latest(pd.ID)
	if ok && o.Payload.Price != nil && o.Payload.Price.Quote == quote && o.Fresh(pd, now) {
		v := num(o.Payload.Price.Value)
		if v > 0 {
			r.Native = &v
			t := o.Time()
			r.PriceAt = &t
		}
	}
	r.FX, r.FXAt, r.FXValid = h.Rate(quote, now)
	if num(r.FX) <= 0 {
		r.FXValid = false
	}
	if r.FXValid && r.Native != nil {
		v := *r.Native * num(r.FX)
		r.USD = &v
	}
	if quote == "USDT" {
		if c, e := h.liquidationCandleSeries(ctx, a, now.Add(-15*time.Hour), now, now, false); e == nil {
			r.ATR = hourlyATR(c, now)
		}
	}
	return r
}
func (h *Hub) liquidationRealized(ctx context.Context, a string, now time.Time) (LiquidationWindow, error) {
	end := now.Truncate(5 * time.Minute)
	from := end.Add(-time.Hour)
	d, _ := h.Dataset(ID("liquidations", a, "", "futures"))
	acc := newFlowAccumulator(300)
	e := h.Store.Visit(ctx, d, 60, from, end, func(o Observation) error {
		if o.Payload.Liquidation != nil && o.Quality == "valid" {
			o.Payload.Flow = &Flow{Buy: o.Payload.Liquidation.Long, Sell: o.Payload.Liquidation.Short}
			acc.add(o)
		}
		return nil
	})
	if e != nil {
		return LiquidationWindow{From: from, To: end}, e
	}
	x := flowWindow(acc.finish(), end, 60, 0)
	return LiquidationWindow{From: from, To: end, Coverage: x.Coverage, Long: x.Buy, Short: x.Sell}, nil
}

type liquidationWhaleCoverage struct {
	Covered         *int   `json:"covered"`
	ExcludedNull    *int   `json:"excludedNull"`
	ExcludedStale   *int   `json:"excludedStale"`
	ExcludedInvalid *int   `json:"excludedInvalid"`
	Available       bool   `json:"available"`
	Reason          string `json:"reason"`
}

func (h *Hub) liquidationWhales(zones []LiquidationZone, asset string, now time.Time) liquidationWhaleCoverage {
	c := liquidationWhaleCoverage{}
	d, _ := h.Dataset(ID("whales", "ALL", "Hyperliquid", "futures"))
	o, ok := h.Store.Latest(d.ID)
	// Registry identities are authoritative; tolerate no guessed fallback feed.
	if !ok {
		for _, x := range h.datasets() {
			if x.Kind == "whales" {
				d = x
				o, ok = h.Store.Latest(x.ID)
				break
			}
		}
	}
	if !ok || !o.Fresh(d, now) {
		c.Reason = "持仓来源缺失或过期"
		return c
	}
	c.Available = true
	c.Covered, c.ExcludedNull, c.ExcludedStale, c.ExcludedInvalid = flowPtr(0), flowPtr(0), flowPtr(0), flowPtr(0)
	seen := map[string]bool{}
	for i := range zones {
		if zones[i].LowUSD != nil {
			v := int64(0)
			zones[i].WhaleCents = &v
			zones[i].WhaleCount = flowPtr(0)
		}
	}
	for _, w := range o.Payload.Whales {
		if w.Asset != asset {
			continue
		}
		key := w.Address + "/" + w.Asset
		if seen[key] {
			continue
		}
		seen[key] = true
		if !freshWhale(w, now) {
			*c.ExcludedStale++
			continue
		}
		if w.Liquidation == nil {
			*c.ExcludedNull++
			continue
		}
		lp, le := validNumber(*w.Liquidation, false)
		usd, ue := validNumber(w.USD, false)
		if le != nil || ue != nil || num(lp) <= 0 || num(w.Size) == 0 || dec(usd).Mul(dec("100")).Round(0).GreaterThan(dec("9000000000000000")) {
			*c.ExcludedInvalid++
			continue
		}
		*c.Covered++
		side := "long"
		if dec(w.Size).IsNegative() {
			side = "short"
		}
		for i := range zones {
			z := &zones[i]
			if z.Side == side && z.LowUSD != nil && z.HighUSD != nil && num(lp) >= *z.LowUSD && num(lp) < *z.HighUSD {
				if z.WhaleCents != nil {
					amount := money(usd)
					if *z.WhaleCents > 9000000000000000-amount {
						z.WhaleCents = nil
						c.Reason = "区间美元金额超出安全整数范围"
					} else {
						*z.WhaleCents += amount
					}
				}
				*z.WhaleCount++
			}
		}
	}
	return c
}
func (h *Hub) liquidationRiskView(ctx context.Context, a, period string, now time.Time) (any, error) {
	d, _ := h.Dataset(ID("map", a, "", "futures"))
	if period != "24h" {
		d.ID += "@" + period
	}
	o, ok := h.Store.Latest(d.ID)
	var s LiquidationMapSnapshot
	e := h.Store.liquidationLoad(ctx, "state", d.ID, &s)
	if e != nil && e != sql.ErrNoRows {
		return nil, e
	}
	fresh := ok && o.Payload.Model != nil && o.Fresh(d, now)
	// A new snapshot awaiting background processing is browse-only; never show
	// old zones with a newer source timestamp or a newly flipped direction.
	if ok && o.Payload.Model != nil && (s.Dataset == "" || !s.Fetched.Equal(o.FetchedAt)) {
		s, e = makeLiquidationMap(d, o, time.Time{})
		if e != nil {
			return nil, e
		}
		s.Complete = false
		if s.Reason == "" {
			if fresh {
				s.Reason = "新快照等待后台区域验证"
			} else {
				s.Reason = "过期历史模型，退出当前重点判断"
			}
		}
	}
	ref := h.liquidationReference(ctx, a, s.Quote, now)
	for i := range s.Zones {
		z := &s.Zones[i]
		z.Eligible = fresh && s.Complete && z.Missing == 0 && z.Touch == nil && z.Cross == nil && z.Uncertain == "" && z.Side != "neutral"
		if !fresh {
			z.State = "stale"
		}
		if ref.FXValid && z.Quote == ref.Quote {
			lo, hi := z.Low*num(ref.FX), z.High*num(ref.FX)
			z.LowUSD, z.HighUSD = &lo, &hi
		}
		if ref.Native != nil && z.Quote == ref.Quote {
			dist := zoneDistance(*z, *ref.Native)
			pct := dist / *ref.Native * 100
			z.Distance, z.DistanceNative = &pct, &dist
			if z.Eligible && ref.ATR != nil && dist <= .25**ref.ATR {
				z.State = "approaching"
			}
			if z.Side != "neutral" && !liquidationAhead(*z, *ref.Native) {
				z.Eligible = false
			}
		}
	}
	whale := h.liquidationWhales(s.Zones, a, now)
	realized, re := h.liquidationRealized(ctx, a, now)
	realizedError := ""
	if re != nil {
		realizedError = "已发生清算读取失败，金额未知"
	}
	evidence := map[string][]LiquidationEvidence{}
	if a == "BTC" {
		for _, side := range []string{"short", "long"} {
			evidence[side] = h.liquidationEvidence(side, now)
		}
	}
	var gap liquidationGap
	h.Store.LoadState("liquidation/gap", &gap)
	return map[string]any{"snapshot": s, "reference": ref, "fresh": fresh, "unitLabel": "模型强度·非美元", "unitBasis": "CoinGlass定义为清算强度，美元单位未经确认；不可与USD金额相加", "whaleCoverage": whale, "realized": realized, "realizedError": realizedError, "realizedVenues": []string{"Binance", "OKX", "Bybit"}, "evidence": evidence, "gap": gap}, nil
}

var liquidationZoneID = regexp.MustCompile(`^[a-f0-9]{24}$`)

type liquidationCursor struct {
	At    int64  `json:"at"`
	ID    string `json:"id"`
	Until int64  `json:"until"`
}

func (h *Hub) liquidationHistory(ctx context.Context, a string, q url.Values, now time.Time) (any, error) {
	id := q.Get("zoneId")
	if !liquidationZoneID.MatchString(id) {
		return nil, errors.New("无效区域ID")
	}
	hours := parseInt(q, "hours", 24, 1, 168)
	limit := parseInt(q, "limit", 200, 1, 500)
	until := now.UnixNano() + 1
	after := now.Add(-time.Duration(hours) * time.Hour).UnixNano()
	afterID := ""
	if raw := q.Get("cursor"); raw != "" {
		var c liquidationCursor
		b, e := base64.RawURLEncoding.DecodeString(raw)
		if e != nil || len(b) > 512 || json.Unmarshal(b, &c) != nil || c.Until > until || c.Until < now.Add(-7*24*time.Hour).UnixNano() {
			return nil, errors.New("无效分页游标")
		}
		after, afterID, until = c.At, c.ID, c.Until
	}
	rows, e := h.Store.research.QueryContext(ctx, `SELECT kind,id,at,payload FROM lz_records WHERE kind IN ('history','transition') AND asset=? AND at>=? AND at<? AND (at>? OR (at=? AND id>?)) ORDER BY at,id LIMIT ?`, a, now.Add(-time.Duration(hours)*time.Hour).UnixNano(), until, after, after, afterID, limit+1)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	items := []json.RawMessage{}
	next := ""
	var last liquidationCursor
	scanned := 0
	for rows.Next() {
		var kind, rid string
		var at int64
		var b []byte
		if e = rows.Scan(&kind, &rid, &at, &b); e != nil {
			return nil, e
		}
		if scanned == limit {
			b, _ := json.Marshal(last)
			next = base64.RawURLEncoding.EncodeToString(b)
			break
		}
		scanned++
		last = liquidationCursor{at, rid, until}
		var zones []LiquidationZone
		if kind == "history" {
			zones, e = decodeLiquidationHistory(b)
			if e != nil {
				return nil, e
			}
		} else {
			var zone LiquidationZone
			if e = liquidationDecode(b, &zone); e != nil {
				return nil, e
			}
			zones = []LiquidationZone{zone}
		}
		for _, zone := range zones {
			if zone.ID == id {
				v := struct {
					LiquidationZone
					RecordedAt time.Time `json:"recordedAt"`
				}{zone, time.Unix(0, at).UTC()}
				raw, e := json.Marshal(v)
				if e != nil {
					return nil, e
				}
				items = append(items, raw)
				break
			}
		}
	}
	return map[string]any{"items": items, "nextCursor": next, "hours": hours, "rulesVersion": LiquidationRules}, rows.Err()
}
