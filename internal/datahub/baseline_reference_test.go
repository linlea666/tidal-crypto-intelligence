package datahub

import (
	"context"
	"fmt"
	"hash/fnv"
	"sort"
	"time"
)

func (h *Hub) referenceBaselines(ctx context.Context) error {
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
				zones, _ := referenceZones(a, step, p, books, registry, rates, now, false)
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
