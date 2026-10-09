package datahub

import (
	"context"
	"sort"
	"time"
)

const legacyCollection = "legacy-spot-cadence-v1"

func (h *Hub) collectionVersion() string {
	if h.bookFlowMode == "collect" || h.bookFlowMode == "run" {
		return BookFlowCollection
	}
	return legacyCollection
}
func collectionKey(v string) string {
	if v == "" {
		return "unrecorded"
	}
	return v
}

type collectionCohort struct {
	Version    string  `json:"collectionVersion"`
	Direction  string  `json:"direction"`
	Count      int     `json:"count"`
	Complete1H int     `json:"complete1h"`
	Complete4H int     `json:"complete4h"`
	Mean1H     *string `json:"mean1hPercent"`
	Mean4H     *string `json:"mean4hPercent"`
	sum1, sum4 float64
}
type collectionSample struct {
	Version string
	Trial   ShortTrial
}

func collectionCohorts(samples []collectionSample) []collectionCohort {
	by := map[string]*collectionCohort{}
	for _, x := range samples {
		version := collectionKey(x.Version)
		key := version + "/" + x.Trial.Direction
		if by[key] == nil {
			by[key] = &collectionCohort{Version: version, Direction: x.Trial.Direction}
		}
		g := by[key]
		g.Count++
		for _, o := range x.Trial.Outcomes {
			if o.State != "complete" || o.Return == nil {
				continue
			}
			if o.Minutes == 60 {
				g.Complete1H++
				g.sum1 += *o.Return
			}
			if o.Minutes == 240 {
				g.Complete4H++
				g.sum4 += *o.Return
			}
		}
	}
	keys := []string{}
	for k := range by {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := []collectionCohort{}
	for _, k := range keys {
		g := by[k]
		if g.Complete1H > 0 {
			g.Mean1H = finiteDecimal(flowPtr(g.sum1 / float64(g.Complete1H)))
		}
		if g.Complete4H > 0 {
			g.Mean4H = finiteDecimal(flowPtr(g.sum4 / float64(g.Complete4H)))
		}
		out = append(out, *g)
	}
	return out
}
func formalCollectionSample(s Signal, c map[int64]Candle) collectionSample {
	entry := s.At.Truncate(5 * time.Minute).Add(5 * time.Minute)
	o := outcomeAt(c, entry, s.Direction, c[entry.Unix()].Open, s.ATR)
	t := ShortTrial{Direction: s.Direction}
	for _, h := range []struct {
		m int
		r *float64
	}{{60, o.Return1H}, {240, o.Return4H}} {
		if completeCandles(c, entry, entry.Add(time.Duration(h.m)*time.Minute)) && h.r != nil {
			t.Outcomes = append(t.Outcomes, ShortOutcome{Minutes: h.m, State: "complete", Return: h.r})
		}
	}
	return collectionSample{Version: s.Collection, Trial: t}
}
func (h *Hub) layeredCollectionCohorts(ctx context.Context, from, to time.Time, side string) ([]collectionCohort, bool, error) {
	type row struct {
		Version string     `json:"version"`
		Trial   ShortTrial `json:"trial"`
	}
	rows, err := paperRows[row](ctx, h.Store.shortDB(), `SELECT json_object('version',json_extract(c.payload,'$.collectionVersion'),'trial',json(o.payload)) FROM alert_audit c JOIN alert_audit o ON o.kind='outcome' AND o.id=c.id WHERE c.kind='candidate' AND c.at>=? AND c.at<? AND (?='' OR json_extract(c.payload,'$.direction')=?) ORDER BY c.at,c.id LIMIT 2001`, from.Unix(), to.Unix(), side, side)
	if err != nil {
		return nil, false, err
	}
	truncated := len(rows) > 2000
	if truncated {
		rows = rows[:2000]
	}
	samples := make([]collectionSample, 0, len(rows))
	for _, r := range rows {
		samples = append(samples, collectionSample{r.Version, r.Trial})
	}
	return collectionCohorts(samples), truncated, nil
}
