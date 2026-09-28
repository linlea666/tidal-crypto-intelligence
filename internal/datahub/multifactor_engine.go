package datahub

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

func snapshotForSide(current FlowSnapshot, side string) *FlowSnapshot {
	s := current
	s.Assessments = append([]FlowAssessment{}, current.Assessments...)
	s.explain(side)
	return &s
}
func (h *Hub) processMultifactor(ctx context.Context, a string, state *signalState, current FlowSnapshot, candles map[int64]Candle, newBar bool, now time.Time, updates *[]Signal, notices map[string]string) error {
	for _, assessment := range current.Assessments {
		side := assessment.Direction
		key := "multifactor-" + side
		// Data gaps interrupt the continuous 30-minute re-arm clock.
		valid := current.Fresh && current.Baseline.Valid && current.Spot["15"].Net != nil && current.Spot["60"].Net != nil && current.Price.High != nil
		pattern := assessment.pattern()
		if !valid {
			state.Clear[key] = nil
			continue
		}
		if id := state.Active[key]; id != "" {
			if pattern == "" {
				if state.Clear[key] == nil {
					v := now
					state.Clear[key] = &v
				}
				if now.Sub(*state.Clear[key]) >= 30*time.Minute {
					delete(state.Active, key)
					delete(state.Clear, key)
				}
			} else {
				state.Clear[key] = nil
				if newBar {
					var old Signal
					if e := h.Store.document(ctx, "signal", id, &old); e != nil {
						return e
					}
					index := -1
					for i, s := range *updates {
						if s.ID == id {
							old = s
							index = i
						}
					}
					snap := snapshotForSide(current, side)
					level := flowLevel(snap, side)
					rank := map[string]int{"ordinary": 0, "large": 1, "supported": 2}
					if now.Before(old.Expires) && rank[level] > rank[old.Level] {
						old.Level = level
						old.MultifactorUpgrade = snap
						if index >= 0 {
							(*updates)[index] = old
						} else {
							*updates = append(*updates, old)
						}
					}
				}
			}
			continue
		}
		if !newBar || pattern == "" {
			continue
		}
		high, low, reference, ok := candleBounds(candles, current.DataThrough)
		if !ok {
			continue
		}
		snapshot := snapshotForSide(current, side)
		level := flowLevel(snapshot, side)
		v := current.Spot["15"]
		s := Signal{ID: fmt.Sprintf("%s-mf1-%s-%d", a, side, current.DataThrough.Unix()), Asset: a, Direction: side, Pattern: pattern, State: "anomaly", Rules: MultifactorRules, At: now, DataThrough: current.DataThrough, Updated: now, Expires: now.Add(4 * time.Hour), FrozenHigh: high, FrozenLow: low, ReferencePrice: reference, DetectionPrice: h.signalDetectionPrice(a, now), ATR: current.Price.PriorATR, Net15: *v.Net, BuyShare: v.BuyShare, Baseline: current.Baseline, DetectionDelaySeconds: flowPtr(now.Sub(current.DataThrough).Seconds()), Evaluation: EvaluationVersion, Multifactor: snapshot, Level: level, Evidence: []string{}, Conflicts: []string{}, Missing: []string{}}
		for _, e := range snapshot.Evidence {
			switch e.State {
			case "support":
				s.Evidence = append(s.Evidence, e.Text)
			case "conflict":
				s.Conflicts = append(s.Conflicts, e.Text)
			case "missing":
				s.Missing = append(s.Missing, e.Text)
			}
		}
		*updates = append(*updates, s)
		state.Active[key] = s.ID
		notices[s.ID] = "anomaly"
	}
	return nil
}
func flowLevel(s *FlowSnapshot, side string) string {
	for _, a := range s.Assessments {
		if a.Direction == side {
			if a.Supported {
				return "supported"
			}
			if a.Large {
				return "large"
			}
		}
	}
	return "ordinary"
}

// History's confirmation remains immutable. A distinct progress snapshot follows
// active and recently confirmed events, including legacy events with known cutoffs.
func (h *Hub) updatePriceProgress(ctx context.Context, a string, bars map[int64]FlowBar, candles map[int64]Candle, end, now time.Time, fresh bool, updates *[]Signal) error {
	rows, e := h.Store.research.QueryContext(ctx, `SELECT payload FROM documents WHERE kind='signal' AND asset=? AND (json_extract(payload,'$.state') IN ('anomaly','weakened') OR (json_extract(payload,'$.confirmedDataThrough') IS NOT NULL AND (json_extract(payload,'$.priceProgress.status') IS NULL OR (json_extract(payload,'$.priceProgress.status') NOT LIKE 'completed_%' AND json_extract(payload,'$.priceProgress.status')!='ended_with_gap')))) ORDER BY at DESC LIMIT 512`, a)
	if e != nil {
		return e
	}
	tracked := []Signal{}
	for rows.Next() {
		var b []byte
		if e = rows.Scan(&b); e != nil {
			break
		}
		var s Signal
		if e = json.Unmarshal(b, &s); e != nil {
			break
		}
		tracked = append(tracked, s)
	}
	if e == nil {
		e = rows.Err()
	}
	rows.Close()
	if e != nil {
		return e
	}
	positions := map[string]int{}
	for i, s := range *updates {
		positions[s.ID] = i
	}
	for _, s := range tracked {
		if i, ok := positions[s.ID]; ok {
			s = (*updates)[i]
		}
		p := priceProgress(s, bars, candles, end, now, fresh)
		// Do not rewrite an expired archive on every worker tick.
		if s.Progress != nil && s.Progress.DataThrough.Equal(end) && s.Progress.Status == p.Status {
			continue
		}
		s.Progress = &p
		if i, ok := positions[s.ID]; ok {
			(*updates)[i] = s
		} else {
			positions[s.ID] = len(*updates)
			*updates = append(*updates, s)
		}
	}
	return nil
}
