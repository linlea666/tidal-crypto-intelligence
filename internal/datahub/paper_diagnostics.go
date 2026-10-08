package datahub

import (
	"sync"
	"syscall"
	"time"
)

// Bounded, process-local telemetry. It never changes accounting or market time.
// A failure freezes its own snapshot before the consumer can drain the queue.
type paperLatency struct {
	Count   uint64    `json:"count"`
	LastMS  float64   `json:"lastMs"`
	MaxMS   float64   `json:"maxMs"`
	Buckets [7]uint64 `json:"buckets"` // <=1,5,20,100,500,1000ms, >1000ms
}
type paperRate struct {
	CPUSeconds float64 `json:"processCpuSeconds,omitempty"`
	Second     int64   `json:"second"`
	Received   uint64  `json:"received"`
	Processed  uint64  `json:"processed"`
}
type paperDiagnosticView struct {
	LastDequeuedAt     *time.Time              `json:"lastDequeuedAt,omitempty"`
	ArrivalThrough     *time.Time              `json:"arrivalThrough,omitempty"`
	ArrivalIntervalsUS []int64                 `json:"arrivalIntervalsUs,omitempty"`
	Started            time.Time               `json:"startedAt"`
	Received           uint64                  `json:"received"`
	Enqueued           uint64                  `json:"enqueued"`
	Processed          uint64                  `json:"processed"`
	Overflows          uint64                  `json:"overflows"`
	HighWater          int                     `json:"queueHighWater"`
	Active             string                  `json:"activeStage"`
	ActiveMS           float64                 `json:"activeMs"`
	Stages             map[string]paperLatency `json:"stages"`
	Rates              []paperRate             `json:"recentSeconds"`
}
type paperDiagnostics struct {
	mu           sync.Mutex
	view         paperDiagnosticView
	activeSince  time.Time
	rates        [120]paperRate
	arrivals     [512]int64
	arrivalCount uint64
	arrivalAt    time.Time
}

func (d *paperDiagnostics) init() {
	if d.view.Started.IsZero() {
		d.view.Started = time.Now().UTC()
		d.view.Stages = map[string]paperLatency{}
	}
}
func (d *paperDiagnostics) rate(at time.Time) *paperRate {
	i := at.Unix() % int64(len(d.rates))
	r := &d.rates[i]
	if r.Second != at.Unix() {
		*r = paperRate{Second: at.Unix()}
	}
	return r
}
func (d *paperDiagnostics) receive(at time.Time, depth int, accepted bool) {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.init()
	d.view.Received++
	observed := at
	if !d.arrivalAt.IsZero() {
		d.arrivals[d.arrivalCount%512] = max(int64(0), observed.Sub(d.arrivalAt).Microseconds())
		d.arrivalCount++
	}
	d.arrivalAt = observed
	d.rate(at).Received++
	if accepted {
		d.view.Enqueued++
	} else {
		d.view.Overflows++
	}
	d.view.HighWater = max(d.view.HighWater, depth)
}
func (d *paperDiagnostics) processed(at time.Time) {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.init()
	d.view.Processed++
	dequeued := time.Now().UTC()
	d.view.LastDequeuedAt = &dequeued
	d.rate(time.Now()).Processed++
	d.observe("queue_wait", time.Since(at))
}
func (d *paperDiagnostics) observe(stage string, elapsed time.Duration) {
	v := d.view.Stages[stage]
	v.Count++
	v.LastMS = float64(elapsed) / float64(time.Millisecond)
	v.MaxMS = max(v.MaxMS, v.LastMS)
	bucket := 6
	for i, ms := range []float64{1, 5, 20, 100, 500, 1000} {
		if v.LastMS <= ms {
			bucket = i
			break
		}
	}
	v.Buckets[bucket]++
	d.view.Stages[stage] = v
}
func (d *paperDiagnostics) stage(name string) func() {
	if d == nil {
		return func() {}
	}
	d.mu.Lock()
	d.init()
	old, oldAt := d.view.Active, d.activeSince
	start := time.Now()
	d.view.Active, d.activeSince = name, start
	d.mu.Unlock()
	return func() {
		d.mu.Lock()
		defer d.mu.Unlock()
		d.observe(name, time.Since(start))
		d.view.Active, d.activeSince = old, oldAt
	}
}
func (d *paperDiagnostics) snapshot() *paperDiagnosticView {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.init()
	v := d.view
	if !d.arrivalAt.IsZero() {
		at := d.arrivalAt
		v.ArrivalThrough = &at
	}
	v.ArrivalIntervalsUS = []int64{}
	first := uint64(0)
	if d.arrivalCount > 512 {
		first = d.arrivalCount - 512
	}
	for i := first; i < d.arrivalCount; i++ {
		v.ArrivalIntervalsUS = append(v.ArrivalIntervalsUS, d.arrivals[i%512])
	}
	v.Stages = make(map[string]paperLatency, len(d.view.Stages))
	for k, x := range d.view.Stages {
		v.Stages[k] = x
	}
	if !d.activeSince.IsZero() {
		v.ActiveMS = float64(time.Since(d.activeSince)) / float64(time.Millisecond)
	}
	now := time.Now().Unix()
	v.Rates = []paperRate{}
	for _, r := range d.rates {
		if r.Second > now-120 && r.Second <= now {
			v.Rates = append(v.Rates, r)
		}
	}
	return &v
}

// A full queue is not proof of missing data. A producer may outrun an
// unscheduled consumer for a few milliseconds. Yield with bounded backpressure,
// retaining every quote and its original reception clock. The existing one
// second processing-age guard remains authoritative; never enlarge the queue.
func paperEnqueueStream(out chan<- paperMessage, msg paperMessage, trace *paperDiagnostics) *paperDiagnosticView {
	select {
	case out <- msg:
		trace.receive(msg.At, len(out), true)
		return nil
	default:
	}
	start := time.Now()
	wait := time.Until(msg.At.Add(time.Second))
	accepted := false
	if wait > 0 {
		timer := time.NewTimer(wait)
		select {
		case out <- msg:
			accepted = true
		case <-timer.C:
		}
		timer.Stop()
	}
	if trace != nil {
		trace.measure("enqueue_backpressure", time.Since(start))
	}
	trace.receive(msg.At, cap(out), accepted)
	if accepted {
		return nil
	}
	if trace == nil {
		return &paperDiagnosticView{Overflows: 1, HighWater: cap(out)}
	}
	return trace.failureSnapshot()
}

func (d *paperDiagnostics) measure(name string, elapsed time.Duration) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.init()
	d.observe(name, elapsed)
}
func (d *paperDiagnostics) sampleCPU() {
	var u syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &u) != nil {
		return
	}
	seconds := float64(u.Utime.Sec+u.Stime.Sec) + float64(u.Utime.Usec+u.Stime.Usec)/1e6
	d.mu.Lock()
	defer d.mu.Unlock()
	d.init()
	d.rate(time.Now()).CPUSeconds = seconds
}

func (d *paperDiagnostics) failureSnapshot() *paperDiagnosticView {
	v := d.snapshot()
	if v == nil {
		return nil
	}
	// Persist only the five seconds around failure, not the entire live ring.
	cutoff := time.Now().Unix() - 5
	rates := []paperRate{}
	for _, r := range v.Rates {
		if r.Second >= cutoff {
			rates = append(rates, r)
		}
	}
	v.Rates = rates
	return v
}
