package datahub

import (
	"context"
	"time"
)

const studyPipeline = "research-2.3"

func studyRequests(s Study, now time.Time) []DataRequest {
	out := []DataRequest{}
	add := func(kind, market, venue string, from, to time.Time, res int, purpose string) {
		if from.Before(now.Add(-90 * 24 * time.Hour)) {
			from = now.Add(-90 * 24 * time.Hour).Truncate(time.Hour).Add(time.Hour)
		}
		if !from.Before(to) {
			return
		}
		out = append(out, DataRequest{Dataset: ID(kind, "BTC", venue, market), From: &from, To: &to, Resolution: res, Purpose: purpose})
	}
	last := 0
	for _, days := range []int{1, 7, 14, 21, 30} {
		add("flow", "spot", "", maxTime(s.From, s.To.Add(-time.Duration(days)*24*time.Hour)), s.To.Add(-time.Duration(last)*24*time.Hour), 300, "signal_baseline")
		last = days
	}
	for _, date := range []string{"2026-08-19", "2026-09-03", "2026-09-18"} {
		d, _ := time.ParseInLocation("2006-01-02", date, time.FixedZone("CST", 8*3600))
		from, to := d.Add(-48*time.Hour), d.Add(72*time.Hour)
		if from.Before(s.From) || to.After(s.To) {
			continue
		}
		add("flow", "spot", "", from, to, 3600, "case")
		add("flow", "futures", "", from, to, 3600, "case")
		add("oi-history", "futures", "", from, to, 3600, "case")
		add("premium", "spot", "Coinbase", from, to, 3600, "case")
		add("flow", "spot", "", from, to, 300, "case")
	}
	add("flow", "spot", "", s.From, s.To, 300, "research")
	add("flow", "futures", "", s.From, s.To, 300, "research")
	add("oi-history", "futures", "", s.From, s.To, 300, "research")
	add("premium", "spot", "Coinbase", s.From, s.To, 300, "research")
	return out
}
func (h *Hub) queueStudy(s *Study, now time.Time) {
	requests := studyRequests(*s, s.Created)
	for s.QueueCursor < len(requests) {
		j, e := h.Request(requests[s.QueueCursor])
		if e != nil {
			s.Error = e.Error()
			s.State = "partial_queue"
			break
		}
		s.Jobs = append(s.Jobs, j.ID)
		s.QueueCursor++
	}
}
func (h *Hub) studyCoverage(s Study) []map[string]any {
	h.Scheduler.mu.Lock()
	defer h.Scheduler.mu.Unlock()
	out := []map[string]any{}
	for _, id := range s.Jobs {
		if j, ok := h.Scheduler.historyJobLocked(id); ok {
			state := "queued"
			if j.LastAttempt != nil {
				state = "collecting"
			}
			if j.Completed {
				state = "complete"
			}
			if len(j.Gaps) > 0 {
				state = "partial"
			}
			if j.Disabled {
				state = "unavailable"
			}
			out = append(out, map[string]any{"dataset": j.Dataset.ID, "resolutionSeconds": j.Dataset.Resolution, "from": j.RangeStart, "to": j.RangeEnd, "cursor": j.From, "state": state, "reason": j.Error, "errorKind": j.ErrorKind, "covered": append([]OrderRange{}, j.Covered...), "gaps": append([]HistoryGap{}, j.Gaps...), "purpose": j.Purpose})
		}
	}
	return out
}
func (h *Hub) studyInputVersion(ctx context.Context, s Study) string {
	return h.Store.researchVersion(ctx, s.Asset, s.From, s.To)
}
