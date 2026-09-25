package datahub

import (
	"context"
	"fmt"
	"hash/fnv"
	"sort"
	"time"
)

// Only one day's decoded books are retained. Completed days are checkpointed;
// a timeout cannot turn the minute maintenance loop into a full-history retry.
type baselineBuild struct {
	From, To time.Time
	Asset    int
	Next     time.Time
	Result   map[string]Baseline
	Values   int
}

func (h *Hub) BuildBaselines(ctx context.Context) error {
	now := time.Now().UTC()
	var job baselineBuild
	if !h.Store.LoadState("baselineBuild.v24", &job) || job.Result == nil || now.Sub(job.To) > 2*time.Hour {
		job = baselineBuild{From: now.Add(-30 * 24 * time.Hour), To: now, Result: map[string]Baseline{}}
		job.Next = job.From
	}
	registry := h.datasets()
	for job.Asset < len(Assets()) {
		a := Assets()[job.Asset]
		end := job.Next.UTC().Truncate(24 * time.Hour).Add(24 * time.Hour)
		if end.After(job.To) {
			end = job.To
		}
		books := map[int64]map[string]Observation{}
		for id, d := range registry {
			if d.Kind != "book" || d.Asset != a {
				continue
			}
			if e := h.Store.Visit(ctx, d, 3600, job.Next, end, func(o Observation) error {
				if o.Quality == "missing" || o.Payload.Book == nil {
					return nil
				}
				key := recordTime(o).Truncate(time.Hour).Unix()
				if books[key] == nil {
					books[key] = map[string]Observation{}
				}
				books[key][id] = o
				return nil
			}); e != nil {
				return e
			}
		}
		fx := map[int64]map[string]string{}
		fd, _ := h.Dataset("fx.usd.kraken")
		if e := h.Store.Visit(ctx, fd, 3600, job.Next, end, func(o Observation) error {
			rates := map[string]string{"USD": "1"}
			for _, r := range o.Payload.Rates {
				rates[r.Quote] = r.USD
			}
			fx[recordTime(o).Truncate(time.Hour).Unix()] = rates
			return nil
		}); e != nil {
			return e
		}
		times := make([]int64, 0, len(books))
		for ts := range books {
			times = append(times, ts)
		}
		sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
		for _, ts := range times {
			if e := ctx.Err(); e != nil {
				return e
			}
			rates := fx[ts]
			if rates == nil {
				rates = map[string]string{"USD": "1"}
			}
			p := prepareBooks(books[ts], registry, rates, job.To, false)
			midpoints := []float64{}
			for id, o := range books[ts] {
				rate, ok := rates[registry[id].Quote]
				if !ok {
					continue
				}
				bid, ask := best(o.Payload.Book)
				if bid > 0 && ask > 0 {
					midpoints = append(midpoints, (bid+ask)/2*num(rate))
				}
			}
			if len(midpoints) == 0 {
				continue
			}
			sort.Float64s(midpoints)
			mid := midpoints[len(midpoints)/2]
			// Preserve the reference's order of asset, hour, bucket width and price.
			for _, step := range steps(a) {
				zones, _ := p.zones(step)
				for _, z := range zones {
					key := baselineKey(a, z, mid)
					b := job.Result[key]
					if b.Days == nil {
						b.Days = map[string]bool{}
					}
					b.Count++
					b.Days[time.Unix(ts, 0).UTC().Format("2006-01-02")] = true
					b.At = job.To
					if len(b.Values) < 1024 && job.Values < 4_000_000 {
						b.Values = append(b.Values, z.USD)
						job.Values++
					} else if len(b.Values) > 0 {
						hash := fnv.New64a()
						fmt.Fprintf(hash, "%s/%d/%.8f", key, ts, z.Price)
						idx := int64(hash.Sum64() % uint64(b.Count))
						if idx < int64(len(b.Values)) {
							b.Values[idx] = z.USD
						}
					}
					job.Result[key] = b
				}
			}
		}
		job.Next = end
		if !end.Before(job.To) {
			job.Asset++
			job.Next = job.From
		}
		if e := h.Store.SaveState("baselineBuild.v24", job); e != nil {
			return e
		}
	}
	for k, b := range job.Result {
		sort.Slice(b.Values, func(i, j int) bool { return b.Values[i] < b.Values[j] })
		job.Result[k] = b
	}
	if e := h.Store.SaveState("baselines", job.Result); e != nil {
		return e
	}
	h.baselineMu.Lock()
	h.baselines = job.Result
	h.baselineMu.Unlock()
	if e := h.Store.SaveState("baselineComputed", job.To); e != nil {
		return e
	}
	_, e := h.Store.db.Exec("DELETE FROM state WHERE key='baselineBuild.v24'")
	return e
}

// Read only small checkpoint metadata, never decode the full baseline for health.
func (h *Hub) baselineProgress() map[string]any {
	var next, through string
	var asset int
	_ = h.Store.db.QueryRow("SELECT json_extract(payload,'$.Next'),json_extract(payload,'$.To'),json_extract(payload,'$.Asset') FROM state WHERE key='baselineBuild.v24'").Scan(&next, &through, &asset)
	var completed time.Time
	h.Store.LoadState("baselineComputed", &completed)
	var err string
	h.Store.LoadState("baselineBuildError", &err)
	var liquidityError string
	h.Store.LoadState("liquidityError", &liquidityError)
	var gap time.Time
	h.Store.LoadState("liquidityGapAt", &gap)
	return map[string]any{"liquidityError": liquidityError, "next": next, "through": through, "assetIndex": asset, "completedAt": completed, "lastError": err, "liquidityGapAt": gap}
}
