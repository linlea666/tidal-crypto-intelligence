package datahub

import (
	"context"
	"fmt"
	"time"
)

type CandidateComparison struct {
	Auxiliary        []CandidateTrial `json:"auxiliary"`
	AuxiliaryWindows int              `json:"auxiliaryWindows"`
	EqualBudget      []CandidateTrial `json:"equalBudget"`
	DailyBudget      map[string]int   `json:"dailyBudget"`
	Evaluation       string           `json:"evaluationVersion"`
	Rules            string           `json:"rulesVersion"`
	CommonWindows    int              `json:"commonWindows"`
	Development      []CandidateTrial `json:"development"`
	Holdout          []CandidateTrial `json:"holdout"`
	Note             string           `json:"note"`
}
type CandidateTrial struct {
	RuleEvaluation
	DelayMinutes int `json:"delayMinutes"`
}

func selectedCase(t time.Time) bool {
	for _, date := range []string{"2026-08-19", "2026-09-03", "2026-09-18"} {
		d, _ := time.ParseInLocation("2006-01-02", date, time.FixedZone("CST", 8*3600))
		if !t.Before(d.Add(-48*time.Hour)) && t.Before(d.Add(72*time.Hour)) {
			return true
		}
	}
	return false
}
func completeCandles(c map[int64]Candle, from, to time.Time) bool {
	for t := from; t.Before(to); t = t.Add(5 * time.Minute) {
		if v, ok := c[t.Unix()]; !ok || v.Close <= 0 {
			return false
		}
	}
	return true
}
func candidateTrial(rule string, signals []Signal, episodes []PriceEpisode, c map[int64]Candle, delay int) CandidateTrial {
	shifted := append([]Signal(nil), signals...)
	events := []StudyEvent{}
	r := CandidateTrial{DelayMinutes: delay}
	for i := range shifted {
		s := &shifted[i]
		s.At = s.At.Add(time.Duration(delay) * time.Minute)
		price := c[s.At.Add(-5*time.Minute).Unix()].Close
		o := outcomeAt(c, s.At, s.Direction, price, s.ATR)
		events = append(events, StudyEvent{Outcome: o})
	}
	r.RuleEvaluation = matchEpisodes(episodes, shifted)
	r.Rules = rule
	r.Outcomes = summarizeExperiment("共同有效样本上的历史关联", events, func(StudyEvent) bool { return true })
	enrichEvaluation(&r.RuleEvaluation, events)
	return r
}

// This is association analysis. Discovery availability is unknown historically.
// Thresholds are fixed, delays share the same complete future-window universe,
// and selected cases never enter the independent holdout.
type CandidateContextSeries struct {
	Futures map[int64]FlowBar
	OI      map[int64]float64
}

func evaluateCandidates(ctx context.Context, bars map[int64]FlowBar, c map[int64]Candle, from, to time.Time, derivatives ...CandidateContextSeries) (*CandidateComparison, error) {
	r := &CandidateComparison{Auxiliary: []CandidateTrial{}, EqualBudget: []CandidateTrial{}, DailyBudget: map[string]int{}, Evaluation: EvaluationVersion, Rules: CandidateRules, Development: []CandidateTrial{}, Holdout: []CandidateTrial{}, Note: "固定3000万观察／5000、6000、8000万、1亿强候选；所有规则与0/5/10分钟延迟使用共同有效窗口。历史发布时间未知，只描述关联；指定案例排除独立留出，未匹配提醒不等同亏损。升档沿用最初观察事件的4小时合并窗口；没有自动选优或启用邮件。独立留出零延迟保留逐事件匹配，其他面板仅保留汇总。"}
	variants := []int64{5_000_000_000, 6_000_000_000, 8_000_000_000, 10_000_000_000}
	names := []string{SignalRules, "observe-3000万", "strong-5000万", "strong-6000万", "strong-8000万", "strong-1亿", "price-breakout"}
	series := map[string][]Signal{}
	groups := map[int64]Signal{}
	common := map[int64]bool{}
	auxiliary := map[int64]bool{}
	active := false
	var clear time.Time
	var rolling *rollingBaseline
	var lastCut time.Time
	var base SignalBaseline
	for t := from.Add(30*24*time.Hour + time.Hour); !t.After(to.Add(-4*time.Hour - 10*time.Minute)); t = t.Add(5 * time.Minute) {
		if e := ctx.Err(); e != nil {
			return nil, e
		}
		cut := t.Truncate(time.Hour).Add(-time.Hour)
		if !cut.Equal(lastCut) {
			if rolling == nil {
				rolling = newRollingBaseline(bars, cut.Add(-30*24*time.Hour), cut)
			} else {
				rolling.advance(cut)
			}
			base = rolling.result(t)
			lastCut = cut
		}
		f, ok := candidateFeatures(bars, c, t, base)
		if !ok || !completeCandles(c, t, t.Add(4*time.Hour+10*time.Minute)) {
			clear = time.Time{}
			continue
		}
		if len(derivatives) > 0 {
			if v, ok := sumBars(derivatives[0].Futures, t, 12); ok {
				n := v.Net()
				f.FuturesNet1H = &n
			}
			f.OIChange = scalarChange(derivatives[0].OI, t)
			if f.FuturesNet1H != nil && f.OIChange != nil {
				auxiliary[t.Unix()] = true
				r.AuxiliaryWindows++
			}
		}
		common[t.Unix()] = true
		r.CommonWindows++
		pattern, valid := signalCondition(bars, t, base, "buy")
		if valid {
			if active {
				if pattern == "" {
					if clear.IsZero() {
						clear = t
					}
					if t.Sub(clear) >= 30*time.Minute {
						active = false
						clear = time.Time{}
					}
				} else {
					clear = time.Time{}
				}
			} else if pattern != "" {
				active = true
				series[SignalRules] = append(series[SignalRules], Signal{ID: fmt.Sprintf("legacy-%d", t.Unix()), Direction: "buy", At: t, ATR: hourlyATR(c, t)})
			}
		}
		for _, threshold := range variants {
			level := candidateLevel(f, base, threshold)
			if level == "" {
				continue
			}
			g := groups[threshold]
			if g.At.IsZero() || !t.Before(g.Expires) {
				g = Signal{ID: fmt.Sprintf("candidate-%d-%d", threshold, t.Unix()), Direction: "buy", At: t, Expires: t.Add(4 * time.Hour), Features: f, ATR: &f.PriorATR}
				if threshold == variants[0] {
					series[names[1]] = append(series[names[1]], g)
				}
			}
			if level == "strong" && g.Upgrade == nil {
				g.Upgrade = &SignalUpgrade{At: t, DataThrough: t, Features: *f, Baseline: base}
				event := g
				event.At = t
				event.Features = f
				event.ATR = &f.PriorATR
				name := names[2]
				for i, v := range variants {
					if v == threshold {
						name = names[i+2]
					}
				}
				series[name] = append(series[name], event)
			}
			groups[threshold] = g
		}
	}
	episodes := priceEpisodes(c, from.Add(30*24*time.Hour-4*time.Hour), to.Add(-4*time.Hour-10*time.Minute))
	for _, ev := range episodes {
		if ev.Direction == "buy" && common[ev.Detected.Unix()] {
			series["price-breakout"] = append(series["price-breakout"], Signal{ID: ev.ID, Direction: "buy", At: ev.Detected, ATR: hourlyATR(c, ev.Detected)})
		}
	}
	for _, holdout := range []bool{false, true} {
		inPhase := func(t time.Time) bool {
			isHold := !t.Before(from.Add(60 * 24 * time.Hour))
			return isHold == holdout && (!holdout || !selectedCase(t))
		}
		eps := []PriceEpisode{}
		for _, ev := range episodes {
			if ev.Direction == "buy" && common[ev.Detected.Unix()] && inPhase(ev.Start) {
				eps = append(eps, ev)
			}
		}
		for _, name := range names {
			chosen := []Signal{}
			for _, sig := range series[name] {
				if inPhase(sig.At) {
					chosen = append(chosen, sig)
				}
			}
			for _, delay := range []int{0, 5, 10} {
				result := candidateTrial(name, chosen, eps, c, delay)
				// Full one-to-one audit details are retained for the independent
				// holdout at zero delay; other panels are aggregate-only.
				if !holdout || delay != 0 {
					result.Matches = nil
				}
				if holdout {
					r.Holdout = append(r.Holdout, result)
				} else {
					r.Development = append(r.Development, result)
				}
			}
		}
	}
	holdout := func(t time.Time) bool { return !t.Before(from.Add(60*24*time.Hour)) && !selectedCase(t) }
	auxEpisodes := []PriceEpisode{}
	for _, ev := range episodes {
		if ev.Direction == "buy" && holdout(ev.Start) && auxiliary[ev.Detected.Unix()] {
			auxEpisodes = append(auxEpisodes, ev)
		}
	}
	filters := []struct {
		name string
		pick func(*SignalFeatures) bool
	}{
		{"合约共同覆盖基准", func(*SignalFeatures) bool { return true }},
		{"合约主动净买入同向", func(f *SignalFeatures) bool { return *f.FuturesNet1H > 0 }},
		{"美元OI上升背景", func(f *SignalFeatures) bool { return *f.OIChange > 0 }},
		{"美元OI下降背景", func(f *SignalFeatures) bool { return *f.OIChange < 0 }},
	}
	for _, filter := range filters {
		chosen := []Signal{}
		for _, sig := range series["strong-5000万"] {
			if holdout(sig.At) && auxiliary[sig.At.Unix()] && filter.pick(sig.Features) {
				chosen = append(chosen, sig)
			}
		}
		result := candidateTrial(filter.name, chosen, auxEpisodes, c, 0)
		result.Matches = nil
		r.Auxiliary = append(r.Auxiliary, result)
	}
	// Ex-post daily equal-count comparison, without selecting on future returns.
	// Each rule receives the same minimum daily budget; keep earliest arrivals.
	// This is a review aid, explicitly not an independently validated live rule.
	equalRules := []string{SignalRules, "strong-5000万", "price-breakout"}
	counts := map[string]map[string]int{}
	for _, rule := range equalRules {
		counts[rule] = map[string]int{}
		for _, sig := range series[rule] {
			if holdout(sig.At) {
				counts[rule][sig.At.UTC().Format("2006-01-02")]++
			}
		}
	}
	for day, n := range counts[equalRules[0]] {
		for _, rule := range equalRules[1:] {
			n = min(n, counts[rule][day])
		}
		if n > 0 {
			r.DailyBudget[day] = n
		}
	}
	equalEpisodes := []PriceEpisode{}
	for _, ev := range episodes {
		if ev.Direction == "buy" && holdout(ev.Start) && common[ev.Detected.Unix()] && r.DailyBudget[ev.Start.UTC().Format("2006-01-02")] > 0 {
			equalEpisodes = append(equalEpisodes, ev)
		}
	}
	for _, rule := range equalRules {
		chosen := []Signal{}
		used := map[string]int{}
		for _, sig := range series[rule] {
			day := sig.At.UTC().Format("2006-01-02")
			if holdout(sig.At) && used[day] < r.DailyBudget[day] {
				chosen = append(chosen, sig)
				used[day]++
			}
		}
		r.EqualBudget = append(r.EqualBudget, candidateTrial(rule, chosen, equalEpisodes, c, 0))
	}
	r.Note += " 合约过滤仅比较期货1小时流量和美元OI均完整的共同窗口，OI升降不直接解释为开多。等提醒数量比较按UTC日取三规则最小数量、各保留最早到达的提醒；只比较共同有配额日期，属事后归一人工审查辅助，不是新线上策略。"
	return r, nil
}
