package datahub

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

func distributions(rows []Whale, asset string, step float64, now time.Time) []Distribution {
	byPrice := map[string]map[string]int64{}
	for _, w := range rows {
		if w.Asset != asset || !freshWhale(w, now) {
			continue
		}
		side := "long"
		if dec(w.Size).IsNegative() {
			side = "short"
		}
		for _, kind := range []string{"entry", "liquidation"} {
			p := num(w.Entry)
			if kind == "liquidation" {
				if w.Liquidation == nil {
					continue
				}
				p = num(*w.Liquidation)
			}
			if p <= 0 {
				continue
			}
			key := fmt.Sprintf("%s/%s/%.0f", kind, side, math.Floor(p/step)*step)
			if byPrice[key] == nil {
				byPrice[key] = map[string]int64{}
			}
			byPrice[key][w.Address] += money(w.USD)
		}
	}
	out := []Distribution{}
	for k, addresses := range byPrice {
		parts := strings.Split(k, "/")
		sum, largest := int64(0), int64(0)
		for _, n := range addresses {
			sum += n
			largest = max(largest, n)
		}
		out = append(out, Distribution{num(parts[2]), parts[0], parts[1], sum, len(addresses), float64(largest) / float64(max(1, sum))})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Price == out[j].Price {
			return out[i].Kind+out[i].Side < out[j].Kind+out[j].Side
		}
		return out[i].Price > out[j].Price
	})
	return out
}
func (h *Hub) SampleDistributions(now time.Time) {
	d, _ := h.Dataset(ID("whales", "ALL", "Hyperliquid", "futures"))
	o, ok := h.Store.Latest(d.ID)
	if !ok || !o.Fresh(d, now) {
		return
	}
	for _, a := range Assets() {
		derived, _ := h.Dataset(ID("whale-distribution", a, "Hyperliquid", "futures"))
		sampleAt := now.Truncate(time.Minute)
		at := now
		valid := 0
		for _, w := range o.Payload.Whales {
			if w.Asset == a && freshWhale(w, now) {
				at = minTime(at, w.At)
				valid++
			}
		}
		quality := "valid"
		if valid == 0 {
			quality = "missing"
		}
		p := Payload{Distributions: distributions(o.Payload.Whales, a, baseStep(a)*4, now)}
		_, _ = h.Store.Ingest(derived, Observation{Dataset: derived.ID, Source: derived.Source, ObservedAt: &at, FetchedAt: o.FetchedAt, WindowStart: &sampleAt, TimeBasis: "derived", Resolution: 60, Quality: quality, Dependencies: map[string]string{d.ID: o.Revision, "rules": RulesVersion}, Payload: p})
	}
}
