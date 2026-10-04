package datahub

import (
	"context"
	"encoding/json"
	"math"
	"sort"
	"time"
)

type CostResult struct {
	EventID     string    `json:"eventId"`
	Kind        string    `json:"kind"`
	Horizon     int       `json:"horizon"`
	From        string    `json:"from"`
	Through     string    `json:"through"`
	Status      string    `json:"status"`
	Return      *float64  `json:"return"`
	PathMax     *float64  `json:"pathMax"`
	PathMin     *float64  `json:"pathMin"`
	Volatility  *float64  `json:"volatility"`
	FinalizedAt time.Time `json:"finalizedAt"`
	Cutoff      time.Time `json:"cutoff"`
}

func costOutcome(event CostEvent, horizon int, prices map[string]string, now time.Time) CostResult {
	anchor := event.DetectedAt.UTC().Truncate(24 * time.Hour)
	end := anchor.AddDate(0, 0, horizon)
	cutoff := end.AddDate(0, 0, 1).Add(6 * time.Hour)
	r := CostResult{EventID: event.ID, Kind: event.Kind, Horizon: horizon, From: costDate(anchor.AddDate(0, 0, 1)), Through: costDate(end), Status: "waiting", Cutoff: cutoff}
	if now.Before(cutoff) {
		return r
	}
	r.FinalizedAt = now
	r.Status = "missing"
	base, ok := prices[costDate(anchor)]
	if !ok || !dec(base).IsPositive() {
		return r
	}
	b := dec(base).InexactFloat64()
	prev := b
	lo, hi := 0., 0.
	returns := []float64{}
	last := 0.
	for i := 1; i <= horizon; i++ {
		v, ok := prices[costDate(anchor.AddDate(0, 0, i))]
		if !ok || !dec(v).IsPositive() {
			return r
		}
		p := dec(v).InexactFloat64()
		last = (p/b - 1) * 100
		lo = math.Min(lo, last)
		hi = math.Max(hi, last)
		returns = append(returns, math.Log(p/prev))
		prev = p
	}
	mean := 0.
	for _, v := range returns {
		mean += v
	}
	mean /= float64(horizon)
	variance := 0.
	for _, v := range returns {
		variance += (v - mean) * (v - mean)
	}
	vol := math.Sqrt(variance/float64(horizon-1)) * math.Sqrt(365) * 100
	r.Status = "complete"
	r.Return = &last
	r.PathMax = &hi
	r.PathMin = &lo
	r.Volatility = &vol
	return r
}
func (h *Hub) processCostResearch(ctx context.Context, now time.Time) error {
	s := h.Store.onchain
	if e := s.writable(); e != nil {
		return e
	}
	rows, e := s.db.QueryContext(ctx, `SELECT e.payload,h.n FROM events e CROSS JOIN (SELECT 7 n UNION ALL SELECT 14 UNION ALL SELECT 30 UNION ALL SELECT 60) h WHERE e.kind IN ('confirmed','concentrated') AND (e.detected / 86400000000000 + h.n + 1) * 86400000000000 + 21600000000000 <= ? AND NOT EXISTS(SELECT 1 FROM results r WHERE r.event_id=e.id AND r.horizon=h.n) ORDER BY e.detected + h.n * 86400000000000,h.n LIMIT 64`, now.UnixNano())
	if e != nil {
		return e
	}
	tasks := []struct {
		event CostEvent
		n     int
	}{}
	for rows.Next() {
		var b []byte
		var n int
		if e = rows.Scan(&b, &n); e != nil {
			rows.Close()
			return e
		}
		var event CostEvent
		if e = json.Unmarshal(b, &event); e != nil {
			rows.Close()
			return e
		}
		tasks = append(tasks, struct {
			event CostEvent
			n     int
		}{event, n})
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	for _, task := range tasks {
		r := costOutcome(task.event, task.n, nil, now)
		if r.Status == "waiting" {
			continue
		}
		prices, e := s.prices(ctx, r.Cutoff)
		if e != nil {
			return e
		}
		r = costOutcome(task.event, task.n, prices, now)
		b, _ := json.Marshal(r)
		if _, e = s.db.ExecContext(ctx, "INSERT OR IGNORE INTO results VALUES(?,?,?)", task.event.ID, task.n, b); e != nil {
			return e
		}
	}
	return nil
}

type costResearchGroup struct {
	Rules       string    `json:"rulesVersion"`
	Kind        string    `json:"kind"`
	Side        string    `json:"side"`
	Horizon     int       `json:"horizon"`
	Complete    int       `json:"complete"`
	Independent int       `json:"independent"`
	Waiting     int       `json:"waiting"`
	Missing     int       `json:"missing"`
	Overlapping int       `json:"overlapping"`
	RiseRate    *float64  `json:"riseRate"`
	Interval    []float64 `json:"confidenceInterval"`
}

func (h *Hub) costResearchView(ctx context.Context, now time.Time, mode string, offset, limit int) (any, error) {
	s := h.Store.onchain
	if mode == "historical" {
		summaries, e := s.summaries(ctx, now)
		if e != nil {
			return nil, e
		}
		prices, e := s.prices(ctx, now)
		if e != nil {
			return nil, e
		}
		out := []any{}
		sort.Slice(summaries, func(i, j int) bool { return summaries[i].Date > summaries[j].Date })
		end := min(len(summaries), offset+limit)
		if offset < len(summaries) {
			for _, sm := range summaries[offset:end] {
				day, _ := costDay(sm.Date)
				event := CostEvent{ID: "historical-" + sm.Date, Kind: "historical", DetectedAt: day}
				results := []CostResult{}
				for _, n := range []int{7, 14, 30, 60} {
					results = append(results, costOutcome(event, n, prices, now))
				}
				out = append(out, map[string]any{"date": sm.Date, "concentration": sm.Concentration["5"], "volatility": costVolatility(sm.Date, prices), "results": results})
			}
		}
		return map[string]any{"mode": "historical", "note": "事后可得数据分析：以快照日收盘为起点，非当时可交易回放；不计入前向样本。", "items": out, "total": len(summaries), "offset": offset, "nextOffset": func() any {
			if end < len(summaries) {
				return end
			}
			return nil
		}()}, nil
	}
	rows, e := s.db.QueryContext(ctx, "SELECT payload FROM events WHERE kind IN ('confirmed','concentrated') ORDER BY detected,id LIMIT 5000")
	if e != nil {
		return nil, e
	}
	events := []CostEvent{}
	for rows.Next() {
		var b []byte
		var v CostEvent
		if e = rows.Scan(&b); e != nil {
			rows.Close()
			return nil, e
		}
		if e = json.Unmarshal(b, &v); e != nil {
			rows.Close()
			return nil, e
		}
		events = append(events, v)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return nil, e
	}
	results := map[string]CostResult{}
	rows, e = s.db.QueryContext(ctx, "SELECT payload FROM results LIMIT 20000")
	if e != nil {
		return nil, e
	}
	for rows.Next() {
		var b []byte
		var v CostResult
		if e = rows.Scan(&b); e != nil {
			rows.Close()
			return nil, e
		}
		if e = json.Unmarshal(b, &v); e != nil {
			rows.Close()
			return nil, e
		}
		results[v.EventID+"/"+itoa(v.Horizon)] = v
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return nil, e
	}
	groups := map[string]*costResearchGroup{}
	lastThrough := map[string]string{}
	rises := map[string]int{}
	items := []CostResult{}
	for _, event := range events {
		side := "none"
		if event.Zone != nil {
			side = event.Zone.Side
		}
		for _, n := range []int{7, 14, 30, 60} {
			key := event.Rules + "/" + event.Kind + "/" + side + "/" + itoa(n)
			g := groups[key]
			if g == nil {
				g = &costResearchGroup{Rules: event.Rules, Kind: event.Kind, Side: side, Horizon: n, Interval: []float64{}}
				groups[key] = g
			}
			r, ok := results[event.ID+"/"+itoa(n)]
			if !ok {
				r = costOutcome(event, n, nil, now)
				r.Status = "waiting"
			}
			items = append(items, r)
			switch r.Status {
			case "complete":
				g.Complete++
				if r.From <= lastThrough[key] {
					g.Overlapping++
				} else {
					g.Independent++
					lastThrough[key] = r.Through
					if r.Return != nil && *r.Return > 0 {
						rises[key]++
					}
				}
			case "missing":
				g.Missing++
			default:
				g.Waiting++
			}
		}
	}
	keys := []string{}
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := []costResearchGroup{}
	for _, k := range keys {
		g := groups[k]
		if g.Independent >= 30 {
			v := float64(rises[k]) / float64(g.Independent)
			g.RiseRate = &v
			ci := liquidationWilson(rises[k], g.Independent)
			g.Interval = []float64{ci[0], ci[1]}
		}
		out = append(out, *g)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].From == items[j].From {
			return items[i].Horizon < items[j].Horizon
		}
		return items[i].From > items[j].From
	})
	end := min(len(items), offset+limit)
	page := []CostResult{}
	if offset < len(items) {
		page = items[offset:end]
	}
	return map[string]any{"mode": "forward", "groups": out, "items": page, "total": len(items), "offset": offset, "nextOffset": func() any {
		if end < len(items) {
			return end
		}
		return nil
	}(), "note": "发现后的首个完整UTC日起观察；每规则/方向/期限30个不重叠完整样本后显示收盘上涨占比与Wilson 95%区间，不是交易胜率。终点后6小时定稿，缺失不被迟到回补覆盖。"}, nil
}
