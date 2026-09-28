package datahub

import (
	"fmt"
	"sort"
	"time"
)

// A direction can produce at most one event in each four-hour horizon. The
// anchor never moves when the breakout repeats, so history and live agree.
type PriceEpisode struct {
	DataThrough     time.Time  `json:"dataThrough"`
	ReconstructedAt *time.Time `json:"reconstructedAt,omitempty"`
	ID              string     `json:"id"`
	Direction       string     `json:"direction"`
	Start           time.Time  `json:"start"`
	Detected        time.Time  `json:"detectedAt"`
}
type EventMatch struct {
	EventID     string   `json:"eventId"`
	SignalID    string   `json:"signalId,omitempty"`
	Timing      string   `json:"timing"`
	LeadMinutes *float64 `json:"leadMinutes"`
}
type RuleEvaluation struct {
	Pending        int          `json:"pendingSignals"`
	Return1HMedian *float64     `json:"return1hMedian"`
	Return4HMedian *float64     `json:"return4hMedian"`
	MFE4HMedian    *float64     `json:"mfe4hMedian"`
	MAE4HMedian    *float64     `json:"mae4hMedian"`
	Positive4H     int          `json:"positive4h"`
	Resolved4H     int          `json:"resolved4h"`
	Rules          string       `json:"rulesVersion"`
	Signals        int          `json:"signals"`
	PriceEpisodes  int          `json:"priceEpisodes"`
	Early          int          `json:"early"`
	Following      int          `json:"following"`
	Missed         int          `json:"missed"`
	Unmatched      int          `json:"unmatchedSignals"`
	Matches        []EventMatch `json:"matches"`
	Outcomes       Experiment   `json:"outcomes"`
}

func priceCross(c map[int64]Candle, t time.Time, side string) bool {
	hi, lo, _, ok := candleBounds(c, t.Add(-10*time.Minute))
	if !ok {
		return false
	}
	c1, ok1 := c[t.Add(-10*time.Minute).Unix()]
	c2, ok2 := c[t.Add(-5*time.Minute).Unix()]
	if !ok1 || !ok2 {
		return false
	}
	if side == "sell" {
		return c1.Close < lo && c2.Close < lo
	}
	return c1.Close > hi && c2.Close > hi
}
func priceEpisodes(c map[int64]Candle, from, to time.Time) []PriceEpisode {
	out := []PriceEpisode{}
	last := map[string]time.Time{}
	for t := from; t.Before(to); t = t.Add(5 * time.Minute) {
		for _, side := range []string{"buy", "sell"} {
			if !last[side].IsZero() && t.Sub(last[side]) < 4*time.Hour {
				continue
			}
			if !priceCross(c, t, side) {
				continue
			}
			start := t.Add(-10 * time.Minute)
			last[side] = t
			out = append(out, PriceEpisode{ID: priceEventID(side, start), Direction: side, Start: start, Detected: t, DataThrough: t})
		}
	}
	return out
}

// Chronological one-to-one assignment: a signal spent on a following event can
// never receive early credit for a later event. Ties use stable IDs.
func matchEpisodes(episodes []PriceEpisode, signals []Signal, observedThrough ...time.Time) RuleEvaluation {
	r := RuleEvaluation{Signals: len(signals), PriceEpisodes: len(episodes), Matches: []EventMatch{}}
	signals = append([]Signal(nil), signals...)
	sort.Slice(signals, func(i, j int) bool {
		if signals[i].At.Equal(signals[j].At) {
			return signals[i].ID < signals[j].ID
		}
		return signals[i].At.Before(signals[j].At)
	})
	used := map[string]bool{}
	for _, e := range episodes {
		m := EventMatch{EventID: e.ID, Timing: "missed"}
		for _, s := range signals {
			if used[s.ID] || s.Direction != e.Direction || s.At.Before(e.Start.Add(-4*time.Hour)) || s.At.After(e.Start.Add(4*time.Hour)) {
				continue
			}
			used[s.ID] = true
			m.SignalID = s.ID
			m.Timing = "following"
			if s.At.Before(e.Start) && (s.Features == nil || s.Features.Stage != "following") && (s.Multifactor == nil || !s.Multifactor.Price.Following[s.Direction]) {
				m.Timing = "early"
				v := e.Start.Sub(s.At).Minutes()
				m.LeadMinutes = &v
				r.Early++
			} else {
				r.Following++
			}
			break
		}
		if m.Timing == "missed" {
			r.Missed++
		}
		r.Matches = append(r.Matches, m)
	}
	for _, sig := range signals {
		if used[sig.ID] {
			continue
		}
		if len(observedThrough) > 0 && sig.At.Add(8*time.Hour).After(observedThrough[0]) {
			r.Pending++
		} else {
			r.Unmatched++
		}
	}
	return r
}

func enrichEvaluation(r *RuleEvaluation, events []StudyEvent) {
	a, b, mf, ma := []float64{}, []float64{}, []float64{}, []float64{}
	for _, e := range events {
		o := e.Outcome
		if o.Return1H != nil {
			a = append(a, *o.Return1H)
		}
		if o.Return4H != nil {
			b = append(b, *o.Return4H)
			r.Resolved4H++
			if *o.Return4H > 0 {
				r.Positive4H++
			}
		}
		if o.MFE4H != nil {
			mf = append(mf, *o.MFE4H)
		}
		if o.MAE4H != nil {
			ma = append(ma, *o.MAE4H)
		}
	}
	med := func(v []float64) *float64 {
		if len(v) == 0 {
			return nil
		}
		n := percentile(v, .5)
		return &n
	}
	r.Return1HMedian = med(a)
	r.Return4HMedian = med(b)
	r.MFE4HMedian = med(mf)
	r.MAE4HMedian = med(ma)
}

func priceEventID(side string, start time.Time) string {
	return fmt.Sprintf("BTC-%s-%d", side, start.Unix())
}
