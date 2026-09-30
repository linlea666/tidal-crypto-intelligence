package datahub

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"sort"
	"time"
)

type LiquidationEvidence struct {
	Name     string         `json:"name"`
	State    string         `json:"state"`
	Detail   string         `json:"detail"`
	At       *time.Time     `json:"at"`
	NetCents *int64         `json:"netCents"`
	Funding  []FundingPoint `json:"funding,omitempty"`
}

func (h *Hub) liquidationEvidence(side string, asOf time.Time) []LiquidationEvidence {
	names := []string{"15分钟现货资金", "1小时现货资金", "币计价OI", "Funding", "已发生清算"}
	out := []LiquidationEvidence{}
	for _, n := range names {
		out = append(out, LiquidationEvidence{Name: n, State: "unknown", Detail: "缺少当时有效证据"})
	}
	var s FlowSnapshot
	if !h.Store.LoadState("signals/current/BTC", &s) || !s.Fresh || s.At.After(asOf) || asOf.Sub(s.At) > 15*time.Minute {
		return out
	}
	sign := int64(1)
	if side == "long" {
		sign = -1
	}
	for i, k := range []string{"15", "60"} {
		w := s.Spot[k]
		if w.Net != nil && w.Coverage >= 1 {
			out[i].State = "conflict"
			if *w.Net*sign > 0 {
				out[i].State = "support"
			}
			out[i].NetCents = w.Net
			if *w.Net == 0 {
				out[i].State = "unknown"
			}
			out[i].Detail = "净资金方向作为独立证据；零值无方向"
			out[i].At = &s.At
		}
	}
	if s.Context.OI.Coin1H != nil {
		out[2].Detail = fmt.Sprintf("1小时币计价OI变化 %.2f%%；OI本身不区分多空", *s.Context.OI.Coin1H)
		out[2].At = &s.At
	}
	out[3].Funding = s.Context.Funding
	fundingKnown, crowded := s.Context.fundingWarning(map[string]string{"long": "buy", "short": "sell"}[side])
	if fundingKnown {
		out[3].State = "conflict"
		if crowded {
			out[3].State = "support"
		}
		out[3].Detail = "按现有同口径Funding基线检查该侧拥挤；不等于价格方向"
		out[3].At = &s.At
	}
	liq := s.Context.Liquidations["60"]
	if liq.Long != nil && liq.Short != nil && liq.Coverage >= 1 {
		out[4].State = "conflict"
		if (side == "long" && *liq.Long > *liq.Short) || (side == "short" && *liq.Short > *liq.Long) {
			out[4].State = "support"
		}
		if *liq.Long == *liq.Short {
			out[4].State = "unknown"
		}
		out[4].Detail = "过去1小时已观察同侧与对侧清算比较；不预测后续"
		out[4].At = &s.At
	}
	return out
}

type LiquidationOutcome struct {
	Hours     int      `json:"hours"`
	State     string   `json:"state"`
	Coverage  float64  `json:"coverage"`
	Hit       *bool    `json:"hit"`
	Minutes   *float64 `json:"minutesToTouch"`
	Crossed   *bool    `json:"crossed"`
	Reclaimed *bool    `json:"reclaimed"`
	MFE       *float64 `json:"mfeNative"`
	MAE       *float64 `json:"maeNative"`
	Reason    string   `json:"reason"`
}
type LiquidationStudyEvent struct {
	ID              string                        `json:"id"`
	Side            string                        `json:"side"`
	Rule            string                        `json:"rulesVersion"`
	Selected        time.Time                     `json:"selectedAt"`
	Start           time.Time                     `json:"start"`
	Zone            LiquidationZone               `json:"zone"`
	Control         *LiquidationZone              `json:"control"`
	Unmatched       string                        `json:"unmatchedReason"`
	Price           float64                       `json:"price"`
	ATR             float64                       `json:"atr"`
	Distance        float64                       `json:"distanceATR"`
	Model           string                        `json:"modelRevision"`
	ModelAvailable  time.Time                     `json:"modelAvailableAt"`
	ModelFetched    time.Time                     `json:"modelFetchedAt"`
	ModelContract   string                        `json:"modelContract"`
	ModelCoverage   []string                      `json:"modelCoverage"`
	Evidence        []LiquidationEvidence         `json:"evidence"`
	Outcomes        map[string]LiquidationOutcome `json:"outcomes"`
	ControlOutcomes map[string]LiquidationOutcome `json:"controlOutcomes"`
}

func liquidationAhead(z LiquidationZone, price float64) bool {
	return z.Side == "short" && price < z.Low || z.Side == "long" && price > z.High
}
func chooseLiquidationZone(zones []LiquidationZone, side string, price float64) *LiquidationZone {
	candidates := []LiquidationZone{}
	for _, z := range zones {
		if z.Side == side && liquidationAhead(z, price) && z.Relative >= 50 && z.Samples >= 3 && z.Last.Sub(z.Continuous) >= 30*time.Minute && z.Touch == nil && z.Cross == nil && z.Missing == 0 && z.Uncertain == "" {
			candidates = append(candidates, z)
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Relative == candidates[j].Relative {
			return zoneDistance(candidates[i], price) < zoneDistance(candidates[j], price)
		}
		return candidates[i].Relative > candidates[j].Relative
	})
	if len(candidates) == 0 {
		return nil
	}
	return &candidates[0]
}
func matchLiquidationControl(zones []LiquidationZone, z LiquidationZone, price, atr float64) *LiquidationZone {
	if atr <= 0 {
		return nil
	}
	bin := math.Floor(zoneDistance(z, price) / atr)
	var best *LiquidationZone
	for _, c := range zones {
		if c.Side != z.Side || !liquidationAhead(c, price) || c.Relative >= 50 || c.Touch != nil || c.Cross != nil || c.Missing > 0 || c.Uncertain != "" || math.Floor(zoneDistance(c, price)/atr) != bin {
			continue
		}
		if best == nil || math.Abs(zoneDistance(c, price)-zoneDistance(z, price)) < math.Abs(zoneDistance(*best, price)-zoneDistance(z, price)) {
			v := c
			best = &v
		}
	}
	return best
}
func (h *Hub) selectLiquidationStudy(ctx context.Context, s LiquidationMapSnapshot) error {
	return h.selectLiquidationStudyAt(ctx, s, s.Available)
}
func (h *Hub) selectCurrentLiquidationStudy(ctx context.Context, now time.Time) error {
	id := ID("map", "BTC", "", "futures")
	var s LiquidationMapSnapshot
	if e := h.Store.liquidationLoad(ctx, "state", id, &s); e == sql.ErrNoRows {
		return nil
	} else if e != nil {
		return e
	}
	d, _ := h.Dataset(id)
	o, ok := h.Store.Latest(id)
	if !ok || !o.Fresh(d, now) || !s.Fetched.Equal(o.FetchedAt) {
		return nil
	}
	pd, _ := h.Dataset(ID("price", "BTC", "Binance", "spot"))
	p, ok := h.Store.Latest(pd.ID)
	if !ok || !p.Fresh(pd, now) || p.Payload.Price == nil || p.Payload.Price.Quote != s.Quote {
		return nil
	}
	value := num(p.Payload.Price.Value)
	if value <= 0 {
		return nil
	}
	s.MarketPrice = &value
	at := p.Time()
	s.MarketAt = &at
	return h.selectLiquidationStudyAt(ctx, s, now)
}
func (h *Hub) selectLiquidationStudyAt(ctx context.Context, s LiquidationMapSnapshot, asOf time.Time) error {
	if s.MarketPrice == nil || s.Quote != "USDT" || asOf.Sub(s.Available) > 35*time.Minute {
		return nil
	}
	candles, e := h.liquidationCandleSeries(ctx, "BTC", asOf.Add(-15*time.Hour), asOf, asOf, false)
	if e != nil {
		return e
	}
	atr := hourlyATR(candles, asOf)
	if atr == nil || *atr <= 0 {
		return nil
	}
	candles, e = h.liquidationCandles(ctx, "BTC", asOf.Add(-15*time.Hour), asOf, asOf)
	if e != nil {
		return e
	}
	for i := range s.Zones {
		z := &s.Zones[i]
		start := z.First.Truncate(5 * time.Minute)
		if z.LastBar.After(start) {
			start = z.LastBar.Add(5 * time.Minute)
		}
		if start.Before(asOf.Add(-15 * time.Hour)) {
			start = asOf.Add(-15 * time.Hour).Truncate(5 * time.Minute)
		}
		for t := start; !t.Add(5 * time.Minute).After(asOf); t = t.Add(5 * time.Minute) {
			if t.Before(z.First) || !t.After(z.LastBar) {
				continue
			}
			if c, ok := candles[t.Unix()]; ok {
				applyLiquidationCandle(z, t, c)
			} else {
				z.Uncertain = "首次可见后的价格窗口不完整"
				break
			}
		}
	}
	for _, side := range []string{"long", "short"} {
		z := chooseLiquidationZone(s.Zones, side, *s.MarketPrice)
		if z == nil {
			continue
		}
		var previous LiquidationStudyEvent
		e = h.Store.liquidationLoad(ctx, "episode", side, &previous)
		if e != nil && e != sql.ErrNoRows {
			return e
		}
		if previous.Zone.ID == z.ID {
			continue
		}
		if !previous.Selected.IsZero() && asOf.Sub(previous.Selected) < 4*time.Hour {
			continue
		}
		// Eligibility becomes known at this snapshot. Counting the preceding 30m
		// survival window as a prediction would leak the selection criterion.
		start := asOf.Truncate(5 * time.Minute).Add(5 * time.Minute)
		event := LiquidationStudyEvent{ID: liquidationHash(side + "/" + asOf.Format(time.RFC3339Nano)), Side: side, Rule: LiquidationRules, Selected: asOf, Start: start, Zone: *z, Price: *s.MarketPrice, ATR: *atr, Distance: zoneDistance(*z, *s.MarketPrice) / *atr, Model: s.Revision, ModelAvailable: s.Available, ModelFetched: s.Fetched, ModelContract: s.Contract, ModelCoverage: s.Coverage, Evidence: h.liquidationEvidence(side, asOf), Outcomes: map[string]LiquidationOutcome{}, ControlOutcomes: map[string]LiquidationOutcome{}}
		event.Control = matchLiquidationControl(s.Zones, *z, event.Price, event.ATR)
		if event.Control == nil {
			event.Unmatched = "同侧、同ATR距离档没有未触及的低强度区域"
		}
		for _, hours := range []int{1, 4} {
			key := fmt.Sprint(hours)
			event.Outcomes[key] = LiquidationOutcome{Hours: hours, State: "observing"}
			if event.Control != nil {
				event.ControlOutcomes[key] = LiquidationOutcome{Hours: hours, State: "observing"}
			}
		}
		// Episode and immutable input are committed together, preventing restart
		// duplication. Outcomes only transition from observing to a frozen result.
		b, e := liquidationJSON(event)
		if e != nil {
			return e
		}
		tx, e := h.Store.research.BeginTx(ctx, nil)
		if e != nil {
			return e
		}
		_, e = tx.ExecContext(ctx, "INSERT OR IGNORE INTO lz_records(kind,id,asset,at,payload) VALUES('event',?,'BTC',?,?)", event.ID, event.Selected.UnixNano(), b)
		if e == nil {
			_, e = tx.ExecContext(ctx, `INSERT INTO lz_records(kind,id,asset,at,payload) VALUES('episode',?,'BTC',?,?) ON CONFLICT(kind,id) DO UPDATE SET at=excluded.at,payload=excluded.payload`, side, event.Selected.UnixNano(), b)
		}
		if e != nil {
			tx.Rollback()
			return e
		}
		if e = tx.Commit(); e != nil {
			return e
		}
	}
	return nil
}
func evaluateLiquidationOutcome(event LiquidationStudyEvent, z LiquidationZone, hours int, candles map[int64]Candle) LiquidationOutcome {
	out := LiquidationOutcome{Hours: hours, State: "complete"}
	z.First = event.Start
	z.Touch, z.Cross, z.Reclaim = nil, nil, nil
	z.LastBar = time.Time{}
	z.PreviousClose = event.Price
	z.FarCloses, z.NearCloses = 0, 0
	z.Uncertain = ""
	count := 0
	mfe, mae := 0.0, 0.0
	for t := event.Start; t.Before(event.Start.Add(time.Duration(hours) * time.Hour)); t = t.Add(5 * time.Minute) {
		c, ok := candles[t.Unix()]
		if !ok {
			continue
		}
		count++
		applyLiquidationCandle(&z, t, c)
		f, a := c.High-event.Price, event.Price-c.Low
		if z.Side == "long" {
			f, a = event.Price-c.Low, c.High-event.Price
		}
		mfe = math.Max(mfe, f)
		mae = math.Max(mae, a)
	}
	out.Coverage = float64(count) / float64(hours*12)
	if out.Coverage < 1 {
		out.State = "incomplete"
		out.Reason = "窗口K线不完整或非当时可见数据"
		return out
	}
	if z.Uncertain != "" {
		out.State = "uncertain"
		out.Reason = z.Uncertain
		return out
	}
	hit, cross, reclaim := z.Touch != nil, z.Cross != nil, z.Reclaim != nil
	out.Hit, out.Crossed, out.Reclaimed = &hit, &cross, &reclaim
	out.MFE, out.MAE = &mfe, &mae
	if z.Touch != nil {
		m := z.Touch.Sub(event.Start).Minutes()
		out.Minutes = &m
	}
	return out
}
func (h *Hub) advanceLiquidationStudy(ctx context.Context, now time.Time) error {
	// At most four unfinished episodes per tick, independent of retained history.
	rows, e := h.Store.research.QueryContext(ctx, "SELECT payload FROM lz_records WHERE kind='event' AND done=0 ORDER BY at LIMIT 4")
	if e != nil {
		return e
	}
	events := []LiquidationStudyEvent{}
	for rows.Next() {
		var b []byte
		var ev LiquidationStudyEvent
		if e = rows.Scan(&b); e != nil {
			break
		}
		if e = liquidationDecode(b, &ev); e != nil {
			break
		}
		events = append(events, ev)
	}
	re := rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	if re != nil {
		return re
	}
	for _, ev := range events {
		changed := false
		for _, hours := range []int{1, 4} {
			key := fmt.Sprint(hours)
			end := ev.Start.Add(time.Duration(hours) * time.Hour)
			if ev.Outcomes[key].State != "observing" || now.Before(end.Add(15*time.Minute)) {
				continue
			}
			// Fixed as-of deadline: late historical fills/corrections cannot improve a
			// missing research window, even if the worker resumes much later.
			c, e := h.liquidationCandles(ctx, "BTC", ev.Start, end, end.Add(15*time.Minute))
			if e != nil {
				return e
			}
			ev.Outcomes[key] = evaluateLiquidationOutcome(ev, ev.Zone, hours, c)
			if ev.Control != nil {
				ev.ControlOutcomes[key] = evaluateLiquidationOutcome(ev, *ev.Control, hours, c)
			}
			changed = true
		}
		if changed {
			if e = h.Store.liquidationPut(ctx, "event", ev.ID, "BTC", ev.Selected, ev); e != nil {
				return e
			}
			if ev.Outcomes["4"].State != "observing" {
				if _, e = h.Store.research.ExecContext(ctx, "UPDATE lz_records SET done=1 WHERE kind='event' AND id=?", ev.ID); e != nil {
					return e
				}
			}
		}
	}
	return nil
}
func liquidationWilson(hit, n int) [2]float64 {
	if n == 0 {
		return [2]float64{}
	}
	p := float64(hit) / float64(n)
	z := 1.96
	den := 1 + z*z/float64(n)
	center := (p + z*z/(2*float64(n))) / den
	half := z * math.Sqrt(p*(1-p)/float64(n)+z*z/(4*float64(n*n))) / den
	return [2]float64{center - half, center + half}
}

// Coverage counts elapsed time covered by a actually available complete map,
// including quiet periods with no qualifying event. Missing episodes cannot
// improve the denominator by simply disappearing from the sample list.
func (h *Hub) liquidationModelCoverage(ctx context.Context, from, to time.Time) (float64, error) {
	if !to.After(from) {
		return 0, nil
	}
	rows, e := h.Store.research.QueryContext(ctx, "SELECT at FROM lz_records WHERE kind='history' AND asset='BTC' AND id LIKE 'map.btc..futures/%' AND at>=? AND at<? ORDER BY at", from.Add(-35*time.Minute).UnixNano(), to.UnixNano())
	if e != nil {
		return 0, e
	}
	defer rows.Close()
	covered := time.Duration(0)
	end := from
	for rows.Next() {
		var at int64
		if e = rows.Scan(&at); e != nil {
			return 0, e
		}
		start := time.Unix(0, at)
		stop := minTime(start.Add(35*time.Minute), to)
		if start.Before(end) {
			start = end
		}
		if stop.After(start) {
			covered += stop.Sub(start)
			end = stop
		}
	}
	return math.Min(1, float64(covered)/float64(to.Sub(from))), rows.Err()
}

type liquidationEvidenceCount struct {
	Name, State                       string
	Events, Complete, Observing, Hits int
	First                             time.Time
}

func (h *Hub) liquidationStudyView(ctx context.Context, asset string, now time.Time) (any, error) {
	if asset != "BTC" {
		return map[string]any{"supported": false, "reason": "完整多因素前向研究首版仅支持BTC"}, nil
	}
	records, e := h.Store.liquidationRows(ctx, "event", "BTC", now.Add(-30*24*time.Hour).UnixNano(), now.UnixNano()+1, 500)
	if e != nil {
		return nil, e
	}
	var origin time.Time
	if e = h.Store.liquidationLoad(ctx, "origin", LiquidationRules, &origin); e != nil {
		return nil, e
	}
	var gap liquidationGap
	h.Store.LoadState("liquidation/gap", &gap)
	groups := []map[string]any{}
	recent := []LiquidationStudyEvent{}
	for _, side := range []string{"short", "long"} {
		for _, hours := range []int{1, 4} {
			total, complete, observing, hits, uncertain, unmatched, matched, controlHits, matchedHits := 0, 0, 0, 0, 0, 0, 0, 0, 0
			facets := map[string]*liquidationEvidenceCount{}
			first := now
			for _, r := range records {
				var ev LiquidationStudyEvent
				if e = liquidationDecode(r.Payload, &ev); e != nil {
					return nil, e
				}
				if ev.Side != side {
					continue
				}
				total++
				if ev.Selected.Before(first) {
					first = ev.Selected
				}
				o := ev.Outcomes[fmt.Sprint(hours)]
				for _, evidence := range ev.Evidence {
					key := evidence.Name + "/" + evidence.State
					g := facets[key]
					if g == nil {
						g = &liquidationEvidenceCount{Name: evidence.Name, State: evidence.State, First: ev.Selected}
						facets[key] = g
					}
					g.Events++
					if ev.Selected.Before(g.First) {
						g.First = ev.Selected
					}
					if o.State == "observing" {
						g.Observing++
					} else if o.State == "complete" {
						g.Complete++
						if o.Hit != nil && *o.Hit {
							g.Hits++
						}
					}
				}
				if o.State == "complete" {
					complete++
					if o.Hit != nil && *o.Hit {
						hits++
					}
				} else if o.State == "observing" {
					observing++
				} else {
					uncertain++
				}
				if ev.Control == nil {
					unmatched++
				} else {
					c := ev.ControlOutcomes[fmt.Sprint(hours)]
					if c.State == "complete" && o.State == "complete" {
						matched++
						if c.Hit != nil && *c.Hit {
							controlHits++
						}
					}
				}
			}
			coverage := 0.0
			if total > observing {
				coverage = float64(complete) / float64(total-observing)
			}
			days := 0.0
			if total > 0 {
				days = now.Sub(first).Hours() / 24
			}
			modelCoverage, e := h.liquidationModelCoverage(ctx, first, now)
			if e != nil {
				return nil, e
			}
			ready := days >= 14 && coverage >= .95 && modelCoverage >= .95 && complete >= 30 && !gap.Paused
			g := map[string]any{"side": side, "hours": hours, "events": total, "complete": complete, "observing": observing, "uncertain": uncertain, "unmatched": unmatched, "matched": matched, "coverage": coverage, "modelCoverage": modelCoverage, "days": days, "ready": ready, "touchRate": nil, "confidenceInterval": nil, "controlTouchRate": nil, "controlConfidenceInterval": nil, "matchedTouchRate": nil, "matchedConfidenceInterval": nil}
			if ready {
				g["touchRate"] = float64(hits) / float64(complete)
				g["confidenceInterval"] = liquidationWilson(hits, complete)
				if matched >= 30 {
					g["matchedTouchRate"] = float64(matchedHits) / float64(matched)
					g["matchedConfidenceInterval"] = liquidationWilson(matchedHits, matched)
					g["controlTouchRate"] = float64(controlHits) / float64(matched)
					g["controlConfidenceInterval"] = liquidationWilson(controlHits, matched)
				}
			}
			evidenceGroups := []map[string]any{}
			keys := []string{}
			for k := range facets {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				f := facets[k]
				cov := 0.0
				if f.Events > f.Observing {
					cov = float64(f.Complete) / float64(f.Events-f.Observing)
				}
				item := map[string]any{"name": f.Name, "state": f.State, "events": f.Events, "complete": f.Complete, "observing": f.Observing, "coverage": cov, "touchRate": nil, "confidenceInterval": nil}
				if ready && f.Complete >= 30 && cov >= .95 && now.Sub(f.First) >= 14*24*time.Hour {
					item["touchRate"] = float64(f.Hits) / float64(f.Complete)
					item["confidenceInterval"] = liquidationWilson(f.Hits, f.Complete)
				}
				evidenceGroups = append(evidenceGroups, item)
			}
			g["evidenceGroups"] = evidenceGroups
			groups = append(groups, g)
		}
	}
	for i := len(records) - 1; i >= 0 && len(recent) < 20; i-- {
		var ev LiquidationStudyEvent
		if e = liquidationDecode(records[i].Payload, &ev); e != nil {
			return nil, e
		}
		recent = append(recent, ev)
	}
	return map[string]any{"supported": true, "rulesVersion": LiquidationRules, "startedAt": origin, "groups": groups, "recent": recent, "gap": gap, "status": "效果验证中", "note": "满足持续条件并实际可见后，下一根完整5分钟K线开始观察；14天、95%覆盖、30个独立事件后才展示带Wilson 95%区间的样本触达率。仅研究公开现货价格，不是强平确认、交易胜率或未来概率。"}, nil
}
