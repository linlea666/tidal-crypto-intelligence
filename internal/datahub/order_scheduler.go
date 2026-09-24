package datahub

import (
	"context"
	"errors"
	"strconv"
	"time"
)

// One page uses exactly one reserved scheduler request. Splits are persisted;
// retries never bypass the global rolling ledger or perform hidden extra IO.
func (s *Scheduler) runOrderHistory(ctx context.Context, j Job) {
	now := time.Now().UTC()
	ranges := append([]OrderRange(nil), j.OrderRanges...)
	if len(ranges) == 0 {
		from := now.Add(-30 * time.Minute)
		if j.OrderThrough != nil {
			from = j.OrderThrough.Add(-time.Minute)
		}
		// Very old gaps are surfaced instead of requesting unbounded history.
		if from.Before(now.Add(-30 * 24 * time.Hour)) {
			from = now.Add(-30 * 24 * time.Hour)
			j.OrderGaps++
		}
		ranges = []OrderRange{{from, now}}
	}
	r := ranges[0]
	d := j.Dataset
	p := map[string]string{}
	for k, v := range d.Params {
		p[k] = v
	}
	p["start_time"] = strconv.FormatInt(r.From.UnixMilli(), 10)
	p["end_time"] = strconv.FormatInt(r.To.UnixMilli()-1, 10)
	d.Params = p
	raw, err := s.fetch(ctx, d)
	fetched := time.Now().UTC()
	var observations []Observation
	count := 0
	stateMismatch := false
	if err == nil {
		observations, err = Normalize(d, raw, fetched)
		for _, o := range observations {
			count += len(o.Payload.Large)
			for _, row := range o.Payload.Large {
				if row.End == nil || row.End.Before(time.UnixMilli(r.From.UnixMilli())) || !row.End.Before(r.To) || (row.RawState != 2 && row.RawState != 3) {
					err = errors.New("大单历史返回未满足结束时间/有效状态契约，暂停推进该窗口")
					break
				}
				// The proxy has returned state=2 rows for a state=3 query. Keep
				// the reported fact, never relabel it as revoked, and expose the
				// incomplete filter coverage without endlessly retrying one page.
				stateMismatch = stateMismatch || strconv.Itoa(row.RawState) != p["state"]
			}
		}
	}
	saturated := count >= 100
	if err == nil && !saturated {
		for _, o := range observations {
			if _, err = s.store.Ingest(d, o); err != nil {
				break
			}
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.jobs[j.ID]
	current.OrderGaps = max(current.OrderGaps, j.OrderGaps)
	current.InFlight = false
	s.inflight--
	if err != nil {
		current.Failures++
		current.Error = err.Error()
		current.Next = fetched.Add(time.Duration(30*(1<<min(current.Failures, 6))) * time.Second)
		current.OrderRanges = ranges
		var fe *FetchError
		if errors.As(err, &fe) {
			if fe.Status == 429 {
				s.quota.RateLimited++
				s.quota.Cooldown = fetched.Add(min(time.Hour, max(time.Minute, fe.RetryAfter)))
			}
			if fe.Status == 401 || fe.Status == 403 {
				s.quota.AuthFailed = true
			}
		}
	} else {
		current.Failures = 0
		current.Error = ""
		current.LastSuccess = &fetched
		if stateMismatch {
			current.OrderGaps++
			current.Error = "上游状态筛选与记录不一致；保留原始状态，该类历史覆盖不完整"
		}
		if saturated {
			if r.To.Sub(r.From) <= time.Millisecond || len(ranges) >= 64 {
				current.OrderGaps++
				current.Error = "历史窗口达到返回上限，存在无法补齐的缺口"
				t := r.To
				current.OrderThrough = &t
				ranges = ranges[1:]
			} else {
				mid := time.UnixMilli((r.From.UnixMilli() + r.To.UnixMilli()) / 2).UTC()
				ranges = append([]OrderRange{{r.From, mid}, {mid, r.To}}, ranges[1:]...)
			}
		} else {
			if current.OrderFirst == nil {
				t := r.From
				current.OrderFirst = &t
			}
			t := r.To
			current.OrderThrough = &t
			ranges = ranges[1:]
		}
		current.OrderRanges = ranges
		if len(ranges) > 0 {
			current.Next = fetched.Add(10 * time.Second)
		} else {
			current.Next = fetched.Add(time.Duration(d.Refresh) * time.Second)
		}
	}
	_ = s.persistLocked()
}
