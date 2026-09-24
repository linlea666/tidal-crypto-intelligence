package datahub

import (
	"context"
	"encoding/json"
	"math"
	"time"
)

// Forward samples are first-write-only. Later backfill/revisions cannot make a
// formerly missing live decision window look as though it had been available.
type ShadowSample struct {
	At            time.Time  `json:"at"`
	Through       time.Time  `json:"dataThrough"`
	Eligible      bool       `json:"eligible"`
	BaselineValid bool       `json:"baselineValid"`
	BookPressure  *float64   `json:"bookPressure"`
	BookCoverage  []Coverage `json:"bookCoverage"`
}

func (h *Hub) recordShadow(ctx context.Context, a string, now, through time.Time, eligible, baseline bool) error {
	tick := now.Truncate(5 * time.Minute)
	var exists int
	if e := h.Store.research.QueryRowContext(ctx, "SELECT count(*) FROM shadow WHERE asset=? AND ts=?", a, tick.Unix()).Scan(&exists); e != nil || exists > 0 {
		return e
	}
	f := h.rawFrame(a, baseStep(a), now)
	s := ShadowSample{At: now, Through: through, Eligible: eligible, BaselineValid: baseline, BookCoverage: f.Coverage}
	complete := f.PriceValid && len(f.Coverage) == 5
	for _, c := range f.Coverage {
		complete = complete && c.Valid && c.Low <= f.Price*.99 && c.High >= f.Price*1.01
	}
	var buy, sell int64
	for _, z := range f.Zones {
		if z.Price < f.Price*.99 || z.Price+z.Step > f.Price*1.01 {
			continue
		}
		if z.Side == "bid" {
			buy += z.USD
		} else {
			sell += z.USD
		}
	}
	if complete && buy+sell > 0 {
		v := float64(buy-sell) / float64(buy+sell)
		s.BookPressure = &v
	}
	b, e := json.Marshal(s)
	if e != nil {
		return e
	}
	_, e = h.Store.research.ExecContext(ctx, "INSERT OR IGNORE INTO shadow VALUES(?,?,?)", a, tick.Unix(), b)
	return e
}

func (w *Warehouse) shadowSamples(ctx context.Context, a string, from, to time.Time) ([]ShadowSample, error) {
	rows, e := w.research.QueryContext(ctx, "SELECT payload FROM shadow WHERE asset=? AND ts>=? AND ts<? ORDER BY ts", a, from.Unix(), to.Unix())
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []ShadowSample{}
	for rows.Next() {
		var b []byte
		if e = rows.Scan(&b); e != nil {
			return nil, e
		}
		var s ShadowSample
		if e = json.Unmarshal(b, &s); e != nil {
			return nil, e
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

type ForwardReport struct {
	From          *time.Time `json:"from"`
	WindowFrom    time.Time  `json:"windowFrom"`
	To            time.Time  `json:"to"`
	Days          float64    `json:"days"`
	Samples       int        `json:"samples"`
	Expected      int        `json:"expected"`
	Eligible      int        `json:"eligible"`
	Coverage      float64    `json:"coverage"`
	Ready         bool       `json:"ready"`
	Signals       int        `json:"signals"`
	Confirmed     int        `json:"confirmed"`
	PriceEpisodes int        `json:"priceEpisodes"`
	Early         int        `json:"early"`
	Following     int        `json:"following"`
	Missed        int        `json:"missed"`
	LeadMinutes   []float64  `json:"leadMinutes"`
	Results       Experiment `json:"results"`
	Note          string     `json:"note"`
}

func (h *Hub) buildForwardReport(ctx context.Context, a string, now time.Time) error {
	from := now.Add(-14 * 24 * time.Hour).Truncate(5 * time.Minute)
	samples, e := h.Store.shadowSamples(ctx, a, from, now)
	if e != nil {
		return e
	}
	r := ForwardReport{To: now, LeadMinutes: []float64{}, Note: "至少14天且95%采样可用才具备观察条件；无足够信号仍不能证明效果。价格事件定义为两根完成5分钟K线突破此前冻结4小时区间，解除30分钟后重计。早于第一根突破K线开始才计提前；其后4小时内提醒计跟随。只在当时预警数据合格的窗口统计漏报。结果从发现后下一根完整K线起评估，最多保守延后5分钟。"}
	eligible := map[int64]bool{}
	var origin time.Time
	if !h.Store.LoadState("forward/origin/"+a, &origin) {
		var b []byte
		if h.Store.research.QueryRowContext(ctx, "SELECT payload FROM shadow WHERE asset=? ORDER BY ts LIMIT 1", a).Scan(&b) == nil {
			var first ShadowSample
			if json.Unmarshal(b, &first) == nil {
				origin = first.At
				if e := h.Store.SaveState("forward/origin/"+a, origin); e != nil {
					return e
				}
			}
		}
	}
	if !origin.IsZero() {
		r.From = &origin
		r.Days = now.Sub(origin).Hours() / 24
		if origin.After(from) {
			from = origin.Truncate(5 * time.Minute)
		}
		// Missing first/last samples still count against coverage. The current
		// unstarted tick is excluded when now falls exactly on a boundary.
		r.Expected = int(math.Ceil(float64(now.Sub(from)) / float64(5*time.Minute)))
	}
	r.WindowFrom = from
	for _, s := range samples {
		r.Samples++
		if s.Eligible && s.BaselineValid {
			r.Eligible++
			eligible[s.At.Truncate(5*time.Minute).Unix()] = true
		}
	}
	if r.Expected > 0 {
		r.Coverage = float64(r.Eligible) / float64(r.Expected)
	}
	r.Ready = r.Days >= 14 && r.Coverage >= .95
	_, candles, e := h.signalInput(ctx, a, from.Add(-16*time.Hour), now, now)
	if e != nil {
		return e
	}
	rows, e := h.Store.research.QueryContext(ctx, "SELECT payload FROM documents WHERE kind='signal' AND asset=? AND at>=? ORDER BY at LIMIT 2000", a, from.Unix())
	if e != nil {
		return e
	}
	signals := []Signal{}
	for rows.Next() {
		var b []byte
		if e = rows.Scan(&b); e != nil {
			rows.Close()
			return e
		}
		var s Signal
		if e = json.Unmarshal(b, &s); e != nil {
			rows.Close()
			return e
		}
		signals = append(signals, s)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	events := []StudyEvent{}
	for _, s := range signals {
		r.Signals++
		if s.ConfirmedAt != nil {
			r.Confirmed++
		}
		at := s.At.Truncate(5 * time.Minute).Add(5 * time.Minute)
		p := 0.0
		if s.DetectionPrice != nil {
			p = *s.DetectionPrice
		}
		events = append(events, StudyEvent{Outcome: outcomeAt(candles, at, s.Direction, p, s.ATR)})
	}
	r.Results = summarizeExperiment("实际发现后表现", events, func(StudyEvent) bool { return true })
	active := map[string]bool{}
	clear := map[string]*time.Time{}
	for t := from.Add(4 * time.Hour); !t.After(now.Add(-4 * time.Hour)); t = t.Add(5 * time.Minute) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !eligible[t.Unix()] {
			clear = map[string]*time.Time{}
			continue
		}
		hi, lo, _, ok := candleBounds(candles, t.Add(-10*time.Minute))
		if !ok {
			continue
		}
		c1, ok1 := candles[t.Add(-10*time.Minute).Unix()]
		c2, ok2 := candles[t.Add(-5*time.Minute).Unix()]
		if !ok1 || !ok2 {
			continue
		}
		for _, side := range []string{"buy", "sell"} {
			cross := c1.Close > hi && c2.Close > hi
			if side == "sell" {
				cross = c1.Close < lo && c2.Close < lo
			}
			if active[side] {
				if !cross {
					if clear[side] == nil {
						v := t
						clear[side] = &v
					}
					if t.Sub(*clear[side]) >= 30*time.Minute {
						active[side] = false
						clear[side] = nil
					}
				} else {
					clear[side] = nil
				}
				continue
			}
			if !cross {
				continue
			}
			active[side] = true
			r.PriceEpisodes++
			start := t.Add(-10 * time.Minute)
			var earliest *Signal
			for i := range signals {
				s := &signals[i]
				if s.Direction != side || s.At.Before(start.Add(-4*time.Hour)) || s.At.After(t.Add(4*time.Hour)) {
					continue
				}
				if earliest == nil || s.At.Before(earliest.At) {
					earliest = s
				}
			}
			if earliest == nil {
				r.Missed++
			} else if earliest.At.Before(start) {
				r.Early++
				r.LeadMinutes = append(r.LeadMinutes, math.Round(start.Sub(earliest.At).Minutes()*10)/10)
			} else {
				r.Following++
			}
		}
	}
	return h.Store.saveDocument("forward-report", a, a, now, r)
}

func (h *Hub) forwardReport(ctx context.Context, a string) any {
	var r ForwardReport
	if h.Store.document(ctx, "forward-report", a, &r) != nil {
		return map[string]any{"ready": false, "note": "等待首轮前向观察；未宣称已完成14天"}
	}
	return r
}
