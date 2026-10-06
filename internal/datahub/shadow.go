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
	MultifactorRules    string     `json:"multifactorRulesVersion,omitempty"`
	MultifactorEligible bool       `json:"multifactorEligible"`
	CandidateRules      string     `json:"candidateRulesVersion,omitempty"`
	CandidateEligible   bool       `json:"candidateEligible"`
	At                  time.Time  `json:"at"`
	Through             time.Time  `json:"dataThrough"`
	Eligible            bool       `json:"eligible"`
	BaselineValid       bool       `json:"baselineValid"`
	BookPressure        *float64   `json:"bookPressure"`
	BookCoverage        []Coverage `json:"bookCoverage"`
}

func (h *Hub) recordShadow(ctx context.Context, a string, now, through time.Time, eligible, baseline bool, candidate ...bool) error {
	tick := now.Truncate(5 * time.Minute)
	var exists int
	if e := h.Store.research.QueryRowContext(ctx, "SELECT count(*) FROM shadow WHERE asset=? AND ts=?", a, tick.Unix()).Scan(&exists); e != nil || exists > 0 {
		return e
	}
	f := h.rawFrame(a, baseStep(a), now)
	s := ShadowSample{At: now, Through: through, Eligible: eligible, BaselineValid: baseline, BookCoverage: f.Coverage}
	if len(candidate) > 0 {
		s.CandidateRules = CandidateRules
		s.CandidateEligible = candidate[0]
	}
	if len(candidate) > 1 {
		s.MultifactorRules = MultifactorRules
		s.MultifactorEligible = candidate[1]
	}
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
	if id, ok := ctx.Value(studySnapshotKey{}).(string); ok {
		out := []ShadowSample{}
		err := w.snapshotRows(ctx, id, "@shadow/"+a, from, to, func(f snapshotFact) error {
			var v ShadowSample
			if e := json.Unmarshal(f.Payload, &v); e != nil {
				return e
			}
			out = append(out, v)
			return nil
		})
		return out, err
	}

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
	Multifactor       *MultifactorForward `json:"multifactor,omitempty"`
	Evaluation        string              `json:"evaluationVersion,omitempty"`
	Comparisons       []RuleEvaluation    `json:"comparisons"`
	CandidateDays     float64             `json:"candidateDays"`
	CandidateCoverage float64             `json:"candidateCoverage"`
	CandidateReady    bool                `json:"candidateReady"`
	CandidateOrigin   *time.Time          `json:"candidateOrigin"`
	CandidateEpisodes int                 `json:"candidateEpisodes"`
	Unmatched         int                 `json:"unmatchedSignals"`
	Matches           []EventMatch        `json:"matches"`

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
	r := ForwardReport{Evaluation: EvaluationVersion, To: now, LeadMinutes: []float64{}, Note: "同方向行情合并4小时；提醒与行情按时间顺序一对一匹配，已计跟随不再重复计提前。候选规则独立累计至少14天、95%共同有效窗口、30个买方行情事件后仅进入人工审查，不自动启用邮件。未匹配提醒不等同亏损，未覆盖行情不计漏报。未来表现从实际发现后下一根完整K线起计算；历史修订不能证明当时可用。旧行情重建单独标记，候选门槛只计上线后现场识别的事件。"}
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
		if s.Rules != "" && s.Rules != SignalRules {
			continue
		}
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
	if e := h.priceEventLedger(ctx, a, now, candles, now.Truncate(5*time.Minute), false); e != nil {
		return e
	}
	allEpisodes, e := h.loadPriceEvents(ctx, a, from, now.Add(-4*time.Hour))
	if e != nil {
		return e
	}
	episodes := []PriceEpisode{}
	for _, ev := range allEpisodes {
		if !ev.Start.Before(from) && eligible[ev.Detected.Truncate(5*time.Minute).Unix()] {
			episodes = append(episodes, ev)
		}
	}
	legacy := []Signal{}
	for _, sig := range signals {
		if sig.Rules == "" || sig.Rules == SignalRules {
			legacy = append(legacy, sig)
		}
	}
	legacyResult := matchEpisodes(episodes, legacy, now)
	r.Signals = len(legacy)
	r.Confirmed = 0
	for _, sig := range legacy {
		if sig.ConfirmedAt != nil {
			r.Confirmed++
		}
	}
	r.PriceEpisodes = legacyResult.PriceEpisodes
	r.Early = legacyResult.Early
	r.Following = legacyResult.Following
	r.Missed = legacyResult.Missed
	r.Unmatched = legacyResult.Unmatched
	r.Matches = legacyResult.Matches
	for _, m := range r.Matches {
		if m.LeadMinutes != nil {
			r.LeadMinutes = append(r.LeadMinutes, *m.LeadMinutes)
		}
	}
	r.Ready = r.Days >= 14 && r.Coverage >= .95 && r.PriceEpisodes >= 30
	var candidateOrigin time.Time
	_ = h.Store.LoadState("forward/candidate-origin/"+a+"/"+CandidateRules, &candidateOrigin)
	common := map[int64]bool{}
	if !candidateOrigin.IsZero() {
		r.CandidateOrigin = &candidateOrigin
		r.CandidateDays = now.Sub(candidateOrigin).Hours() / 24
		start := maxTime(from, candidateOrigin.Truncate(5*time.Minute))
		expected := int(math.Ceil(float64(now.Sub(start)) / float64(5*time.Minute)))
		count := 0
		for _, v := range samples {
			if !v.At.Before(start) && v.CandidateRules == CandidateRules && v.CandidateEligible && v.Eligible && v.BaselineValid {
				count++
				common[v.At.Truncate(5*time.Minute).Unix()] = true
			}
		}
		if expected > 0 {
			r.CandidateCoverage = float64(count) / float64(expected)
		}
	}
	commonEpisodes := []PriceEpisode{}
	for _, ev := range episodes {
		if ev.Direction == "buy" && common[ev.Detected.Truncate(5*time.Minute).Unix()] && ev.ReconstructedAt == nil {
			commonEpisodes = append(commonEpisodes, ev)
		}
	}
	r.CandidateEpisodes = len(commonEpisodes)
	r.CandidateReady = r.CandidateDays >= 14 && r.CandidateCoverage >= .95 && r.CandidateEpisodes >= 30
	for _, rule := range []string{SignalRules, CandidateRules, "price-breakout"} {
		chosen := []Signal{}
		for _, sig := range signals {
			if sig.Rules != rule || sig.Direction != "buy" {
				continue
			}
			if sig.Rules == CandidateRules {
				if sig.Upgrade == nil {
					continue
				}
				sig.At = sig.Upgrade.At
				sig.DetectionPrice = sig.Upgrade.DetectionPrice
				sig.ATR = &sig.Upgrade.Features.PriorATR
				f := sig.Upgrade.Features
				sig.Features = &f
			}
			if common[sig.At.Truncate(5*time.Minute).Unix()] {
				chosen = append(chosen, sig)
			}
		}
		if rule == "price-breakout" {
			for _, ev := range commonEpisodes {
				p := candles[ev.DataThrough.Add(-5*time.Minute).Unix()].Close
				chosen = append(chosen, Signal{ID: ev.ID, Direction: "buy", At: ev.Detected, DetectionPrice: &p, ATR: hourlyATR(candles, ev.Detected)})
			}
		}
		result := matchEpisodes(commonEpisodes, chosen, now)
		result.Rules = rule
		evs := []StudyEvent{}
		for _, sig := range chosen {
			p := 0.0
			if sig.DetectionPrice != nil {
				p = *sig.DetectionPrice
			}
			evs = append(evs, StudyEvent{Outcome: outcomeAt(candles, sig.At.Truncate(5*time.Minute).Add(5*time.Minute), sig.Direction, p, sig.ATR)})
		}
		result.Outcomes = summarizeExperiment("共同有效窗口", evs, func(StudyEvent) bool { return true })
		enrichEvaluation(&result, evs)
		r.Comparisons = append(r.Comparisons, result)
	}
	// Archive the old definition once; never overwrite its original evidence.
	var old ForwardReport
	if h.Store.document(ctx, "forward-report", a, &old) == nil && old.Evaluation == "" {
		if e := h.Store.saveDocument("forward-report-archive", a+"/legacy", a, old.To, old); e != nil {
			return e
		}
	}

	r.Multifactor = h.multifactorForward(ctx, samples, signals, allEpisodes, candles, now)
	return h.Store.saveDocument("forward-report", a, a, now, r)
}

func (h *Hub) forwardReport(ctx context.Context, a string) any {
	var r ForwardReport
	if h.Store.document(ctx, "forward-report", a, &r) != nil {
		return map[string]any{"ready": false, "note": "等待首轮前向观察；未宣称已完成14天"}
	}
	return r
}
