package datahub

import (
	"sort"
	"time"
)

// Four sorted multisets hold only this window's features. Advancing the window
// replaces twelve hourly samples rather than rebuilding 30 days of slices.
type rollingBaseline struct {
	bars                       map[int64]FlowBar
	from, to                   time.Time
	dates                      map[int64]int
	valid                      int
	net15, vol15, net60, vol60 []float64
}

func sortedChange(a []float64, v float64, add bool) []float64 {
	i := sort.SearchFloat64s(a, v)
	if !add {
		if i < len(a) && a[i] == v {
			copy(a[i:], a[i+1:])
			a = a[:len(a)-1]
		}
		return a
	}
	a = append(a, 0)
	copy(a[i+1:], a[i:])
	a[i] = v
	return a
}
func sortedQuantile(a []float64, p float64) float64 {
	if len(a) == 0 {
		return 0
	}
	f := float64(len(a)-1) * p
	i := int(f)
	j := min(i+1, len(a)-1)
	return a[i] + (a[j]-a[i])*(f-float64(i))
}
func (r *rollingBaseline) bar(at time.Time, add bool) {
	if _, ok := r.bars[at.Unix()]; !ok {
		return
	}
	d := at.Unix() / 86400
	if add {
		r.valid++
		r.dates[d]++
	} else {
		r.valid--
		r.dates[d]--
		if r.dates[d] == 0 {
			delete(r.dates, d)
		}
	}
}
func (r *rollingBaseline) feature(end time.Time, n int, add bool) {
	b, ok := sumBars(r.bars, end, n)
	if !ok {
		return
	}
	if n == 3 {
		r.net15 = sortedChange(r.net15, float64(b.Net()), add)
		r.vol15 = sortedChange(r.vol15, float64(b.Buy+b.Sell), add)
	} else {
		r.net60 = sortedChange(r.net60, float64(b.Net()), add)
		r.vol60 = sortedChange(r.vol60, float64(b.Buy+b.Sell), add)
	}
}
func newRollingBaseline(bars map[int64]FlowBar, from, to time.Time) *rollingBaseline {
	r := &rollingBaseline{bars: bars, from: from, to: to, dates: map[int64]int{}}
	size := int(to.Sub(from)/(5*time.Minute)) + 1
	r.net15 = make([]float64, 0, size)
	r.vol15 = make([]float64, 0, size)
	r.net60 = make([]float64, 0, size)
	r.vol60 = make([]float64, 0, size)
	for at := from; at.Before(to); at = at.Add(5 * time.Minute) {
		r.bar(at, true)
		end := at.Add(5 * time.Minute)
		if end.Sub(from) >= 15*time.Minute {
			r.feature(end, 3, true)
		}
		if end.Sub(from) >= time.Hour {
			r.feature(end, 12, true)
		}
	}
	return r
}
func (r *rollingBaseline) advance(to time.Time) {
	for r.to.Before(to) {
		r.bar(r.from, false)
		r.feature(r.from.Add(15*time.Minute), 3, false)
		r.feature(r.from.Add(time.Hour), 12, false)
		r.bar(r.to, true)
		r.from = r.from.Add(5 * time.Minute)
		r.to = r.to.Add(5 * time.Minute)
		r.feature(r.to, 3, true)
		r.feature(r.to, 12, true)
	}
}
func (r *rollingBaseline) result(now time.Time) SignalBaseline {
	b := SignalBaseline{At: now, From: r.from, To: r.to, P95: sortedQuantile(r.net15, .95), P05: sortedQuantile(r.net15, .05), P90: sortedQuantile(r.net60, .9), P10: sortedQuantile(r.net60, .1), Median15: sortedQuantile(r.vol15, .5), Median60: sortedQuantile(r.vol60, .5)}
	expected := int(r.to.Sub(r.from) / (5 * time.Minute))
	if expected > 0 {
		b.Coverage = float64(r.valid) / float64(expected)
	}
	for _, n := range r.dates {
		if n >= 274 {
			b.Dates++
		}
	}
	b.Valid = b.Dates >= 21 && b.Coverage >= .95 && len(r.net15) > 0 && len(r.net60) > 0
	return b
}
