package datahub

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

// Frozen V2.3 financial reference, test-only.
func referenceZones(asset string, step, price float64, books map[string]Observation, registry map[string]Dataset, rates map[string]string, now time.Time, live bool) ([]Zone, []Coverage) {
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
