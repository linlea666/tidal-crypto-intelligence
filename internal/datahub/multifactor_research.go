package datahub

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"time"
)

type MultifactorForward struct {
	Rules       string             `json:"rulesVersion"`
	Origin      *time.Time         `json:"origin"`
	Days        float64            `json:"days"`
	Coverage    float64            `json:"coverage"`
	Episodes    int                `json:"episodes"`
	ReviewReady bool               `json:"reviewReady"`
	Comparisons []DirectionalTrial `json:"comparisons"`
	EqualBudget []DirectionalTrial `json:"equalBudget"`
	Note        string             `json:"note"`
}
type DirectionalTrial struct {
	CandidateTrial
	Direction string `json:"direction"`
}

// The outcome entry is always a subsequent complete candle OPEN. No return is
// credited from a closing price already observed before the notification.
func multifactorTrial(rule, side string, signals []Signal, episodes []PriceEpisode, c map[int64]Candle, delay int, through time.Time) DirectionalTrial {
	shifted := []Signal{}
	outcomes := []StudyEvent{}
	for _, s := range signals {
		s.At = s.At.Add(time.Duration(delay) * time.Minute)
		shifted = append(shifted, s)
		entry := s.At.Truncate(5 * time.Minute).Add(5 * time.Minute)
		p := c[entry.Unix()].Open
		o := outcomeAt(c, entry, s.Direction, p, s.ATR)
		if !completeCandles(c, entry, entry.Add(time.Hour)) {
			o.Return1H = nil
		}
		if !completeCandles(c, entry, entry.Add(4*time.Hour)) {
			o.Return4H = nil
			o.MFE4H = nil
			o.MAE4H = nil
		}
		outcomes = append(outcomes, StudyEvent{Outcome: o})
	}
	r := DirectionalTrial{Direction: side, CandidateTrial: CandidateTrial{DelayMinutes: delay, RuleEvaluation: matchEpisodes(episodes, shifted, through)}}
	r.Rules = rule
	r.Outcomes = summarizeExperiment("下一根完整五分钟开盘后的方向收益", outcomes, func(StudyEvent) bool { return true })
	enrichEvaluation(&r.RuleEvaluation, outcomes)
	return r
}
func (h *Hub) multifactorForward(ctx context.Context, samples []ShadowSample, signals []Signal, episodes []PriceEpisode, c map[int64]Candle, now time.Time) *MultifactorForward {
	var origin time.Time
	if !h.Store.LoadState("signals/multifactor-cutover", &origin) {
		return nil
	}
	r := &MultifactorForward{Rules: MultifactorRules, Origin: &origin, Days: now.Sub(origin).Hours() / 24, Comparisons: []DirectionalTrial{}, Note: "新规则邮件直接启用，效果独立验证中。至少14天、95%覆盖、30个独立双向行情事件后进入阶段审查，不自动改参。等提醒预算是按UTC日各方向最小数量的事后对照，并非线上策略。旧规则与新规则按同一有效窗口分方向比较；未匹配不是亏损，方向收益不含手续费，不是交易回测。"}
	from := maxTime(origin.Truncate(5*time.Minute), now.Add(-14*24*time.Hour).Truncate(5*time.Minute))
	valid := map[int64]bool{}
	for _, s := range samples {
		if !s.At.Before(from) && s.MultifactorRules == MultifactorRules && s.MultifactorEligible && s.Eligible && s.BaselineValid {
			valid[s.At.Truncate(5*time.Minute).Unix()] = true
		}
	}
	expected := int(math.Ceil(float64(now.Sub(from)) / float64(5*time.Minute)))
	if expected > 0 {
		r.Coverage = float64(len(valid)) / float64(expected)
	}
	for _, side := range []string{"buy", "sell"} {
		eps := []PriceEpisode{}
		for _, ev := range episodes {
			if ev.Direction == side && ev.ReconstructedAt == nil && !ev.Detected.Before(origin) && valid[ev.Detected.Truncate(5*time.Minute).Unix()] {
				eps = append(eps, ev)
			}
		}
		r.Episodes += len(eps)
		byRule := map[string][]Signal{}
		for _, rule := range []string{SignalRules, MultifactorRules, "price-breakout"} {
			chosen := []Signal{}
			for _, sig := range signals {
				if sig.Rules == rule && sig.Direction == side && !sig.At.Before(origin) && valid[sig.At.Truncate(5*time.Minute).Unix()] {
					chosen = append(chosen, sig)
				}
			}
			if rule == "price-breakout" {
				for _, ev := range eps {
					chosen = append(chosen, Signal{ID: ev.ID, Direction: side, At: ev.Detected, ATR: hourlyATR(c, ev.DataThrough)})
				}
			}
			byRule[rule] = chosen
			for _, delay := range []int{0, 5, 10} {
				r.Comparisons = append(r.Comparisons, multifactorTrial(rule, side, chosen, eps, c, delay, now))
			}
		}
		r.EqualBudget = append(r.EqualBudget, equalBudgetTrials([]string{SignalRules, "price-breakout", MultifactorRules}, side, byRule, eps, c, now)...)
	}
	r.ReviewReady = r.Days >= 14 && r.Coverage >= .95 && r.Episodes >= 30
	return r
}

type MultifactorStudy struct {
	Rules   string                  `json:"rulesVersion"`
	State   string                  `json:"state"`
	Through time.Time               `json:"calculatedThrough"`
	Groups  []MultifactorStudyGroup `json:"groups"`
	Note    string                  `json:"note"`
}
type MultifactorStudyGroup struct {
	Name        string             `json:"name"`
	Phase       string             `json:"phase"`
	Windows     int                `json:"commonWindows"`
	Trials      []DirectionalTrial `json:"trials"`
	EqualBudget []DirectionalTrial `json:"equalBudget"`
}
type multiResearchTick struct {
	At   time.Time `json:"at"`
	Mask int       `json:"mask"`
}
type multiResearchSignal struct {
	ID        string    `json:"i"`
	Rules     string    `json:"r"`
	At        time.Time `json:"t"`
	Direction string    `json:"d"`
	ATR       *float64  `json:"a,omitempty"`
	Following bool      `json:"f,omitempty"`
	Mask      int       `json:"m"`
}

func (s multiResearchSignal) asSignal() Signal {
	return Signal{ID: s.ID, Rules: s.Rules, At: s.At, Direction: s.Direction, ATR: s.ATR, Multifactor: &FlowSnapshot{Price: PriceContext{Following: map[string]bool{s.Direction: s.Following}}}}
}

type multiCheckpoint struct {
	Version string                `json:"version"`
	Cursor  time.Time             `json:"cursor"`
	Active  map[string]bool       `json:"active"`
	Clear   map[string]*time.Time `json:"clear"`
	Ticks   []multiResearchTick   `json:"ticks,omitempty"`
	Signals []multiResearchSignal `json:"signals,omitempty"`
}

// A complete 60-day decision trace plus all rule variants exceeds the 1 MiB
// document cap as plain JSON. Keep only required features and compress this
// internal checkpoint; decoding is capped independently of compressed size.
type multiCheckpointAlias multiCheckpoint
type multiCheckpointWire struct {
	multiCheckpointAlias
	Records []byte `json:"records"`
}
type multiCheckpointRecords struct {
	Ticks   []multiResearchTick   `json:"ticks"`
	Signals []multiResearchSignal `json:"signals"`
}

const maxMultifactorCheckpointBytes = 16 << 20

func (p multiCheckpoint) MarshalJSON() ([]byte, error) {
	raw, err := json.Marshal(multiCheckpointRecords{p.Ticks, p.Signals})
	if err != nil {
		return nil, err
	}
	if len(raw) > maxMultifactorCheckpointBytes {
		return nil, errors.New("多因素检查点超过解码预算")
	}
	var buf bytes.Buffer
	z, err := gzip.NewWriterLevel(&buf, gzip.BestSpeed)
	if err != nil {
		return nil, err
	}
	if _, err = z.Write(raw); err != nil {
		return nil, err
	}
	if err = z.Close(); err != nil {
		return nil, err
	}
	v := multiCheckpointAlias(p)
	v.Ticks = nil
	v.Signals = nil
	return json.Marshal(multiCheckpointWire{v, buf.Bytes()})
}
func (p *multiCheckpoint) UnmarshalJSON(b []byte) error {
	var v multiCheckpointWire
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	*p = multiCheckpoint(v.multiCheckpointAlias)
	if len(v.Records) == 0 {
		return nil
	}
	z, err := gzip.NewReader(bytes.NewReader(v.Records))
	if err != nil {
		return err
	}
	defer z.Close()
	raw, err := io.ReadAll(io.LimitReader(z, maxMultifactorCheckpointBytes+1))
	if err != nil {
		return err
	}
	if len(raw) > maxMultifactorCheckpointBytes {
		return errors.New("多因素检查点超过解码预算")
	}
	var records multiCheckpointRecords
	if err = json.Unmarshal(raw, &records); err != nil {
		return err
	}
	p.Ticks, p.Signals = records.Ticks, records.Signals
	return nil
}

func multifactorCase(t time.Time) bool {
	d := time.Date(2026, 9, 28, 0, 0, 0, 0, time.FixedZone("UTC+8", 8*3600))
	return selectedCase(t) || (!t.Before(d.Add(-48*time.Hour)) && t.Before(d.Add(72*time.Hour)))
}

// At most two historical days per worker step, with a durable cursor. Results
// are explicitly retrospective associations; historical funding is never filled
// from today's snapshot. Every factor comparison has its own common universe.
func (h *Hub) evaluateMultifactor(ctx context.Context, study Study, bars map[int64]FlowBar, c map[int64]Candle, now time.Time) (*MultifactorStudy, error) {
	r := &MultifactorStudy{Rules: MultifactorRules, State: "calculating", Groups: []MultifactorStudyGroup{}, Note: "历史关联研究，不证明数据当时可获得。旧规则、价格突破、新资金核心与逐项过滤使用各组共同有效窗口；买卖分开。30天基线、随后30天开发、最后30天留出，已知案例及其邻近窗口排除留出。0分钟延迟也从发现后下一根完整5分钟开盘计算，5/10分钟再延后。等提醒预算按各UTC日各方向的共同最小数量，保留最早提醒，属事后归一而非线上策略。Funding无可比历史时相应组留空。"}
	start := study.From.Add(30*24*time.Hour + time.Hour)
	stop := study.To.Add(-4*time.Hour - 15*time.Minute)
	version, e := h.studyInputVersion(ctx, study)
	if e != nil {
		return nil, e
	}
	p := multiCheckpoint{Version: version, Cursor: start, Active: map[string]bool{}, Clear: map[string]*time.Time{}, Ticks: []multiResearchTick{}, Signals: []multiResearchSignal{}}
	var saved multiCheckpoint
	savedErr := sql.ErrNoRows
	if study.ID != "" {
		savedErr = h.Store.document(ctx, "multifactor-study-progress", study.ID, &saved)
	}
	if savedErr != nil && savedErr != sql.ErrNoRows {
		return nil, savedErr
	}
	if savedErr == nil && saved.Version == version && !saved.Cursor.Before(start) {
		p = saved
	}
	until := stop
	if study.ID != "" {
		until = minTime(until, p.Cursor.Add(2*24*time.Hour))
	}
	series, e := h.contextSeries(ctx, study.Asset, study.From, study.To, now)
	if e != nil {
		return nil, e
	}
	sort.Slice(series.Funding, func(i, j int) bool { return series.Funding[i].FetchedAt.Before(series.Funding[j].FetchedAt) })
	var rolling *rollingBaseline
	var base SignalBaseline
	var cb contextBaseline
	var cutTime time.Time
	for at := p.Cursor; !at.After(until); at = at.Add(5 * time.Minute) {
		if e := ctx.Err(); e != nil {
			return nil, e
		}
		cut := at.Truncate(time.Hour).Add(-time.Hour)
		if !cut.Equal(cutTime) {
			if rolling == nil {
				rolling = newRollingBaseline(bars, cut.Add(-30*24*time.Hour), cut)
			} else {
				rolling.advance(cut)
			}
			base = rolling.result(at)
			cutTime = cut
			cb = contextBaseline{}
		}
		p.Cursor = at.Add(5 * time.Minute)
		_, flowOK := sumBars(bars, at, 36)
		if !base.Valid || !flowOK || !completeCandles(c, at.Add(-16*time.Hour), at.Add(4*time.Hour+15*time.Minute)) {
			for key := range p.Clear {
				p.Clear[key] = nil
			}
			continue
		}
		if cb.To.IsZero() {
			cb = makeContextBaseline(series, cut.Add(-30*24*time.Hour), cut, at)
		}
		input := series
		j := sort.Search(len(series.Funding), func(i int) bool { return series.Funding[i].FetchedAt.After(at) })
		input.Funding = series.Funding[max(0, j-2):j]
		snap := buildFlowSnapshot(bars, c, at, at, base, true)
		snap.Context = buildDerivativeContext(input, cb, at, at)
		mask := 1
		perp := snap.Context.Futures["60"]
		if perp.Net != nil && perp.BuyShare != nil && perp.VolumeRatio != nil {
			mask |= 2
		}
		if snap.Context.OI.Coin1H != nil && snap.Context.OI.AbsP75 != nil {
			mask |= 4
		}
		if known, _ := snap.Context.fundingWarning("buy"); known {
			mask |= 8
		}
		liq := snap.Context.Liquidations["60"]
		if liq.Long != nil && liq.Short != nil && liq.LongP95 != nil && liq.ShortP95 != nil {
			mask |= 16
		}
		p.Ticks = append(p.Ticks, multiResearchTick{at, mask})
		for _, side := range []string{"buy", "sell"} {
			s := snapshotForSide(snap, side)
			var assessment FlowAssessment
			for _, a := range s.Assessments {
				if a.Direction == side {
					assessment = a
				}
			}
			pattern, _ := signalCondition(bars, at, base, side)
			core := assessment.pattern() != ""
			perpSame := false
			for _, ev := range s.Evidence {
				if ev.Factor == "futures" && ev.State == "support" {
					perpSame = true
				}
			}
			_, crowded := s.Context.fundingWarning(side)
			liqSame := false
			if mask&16 != 0 {
				if side == "buy" {
					liqSame = *liq.Short > 0 && float64(*liq.Short) >= *liq.ShortP95
				} else {
					liqSame = *liq.Long > 0 && float64(*liq.Long) >= *liq.LongP95
				}
			}
			conditions := map[string]bool{SignalRules: pattern != "", MultifactorRules: core, "core+futures": core && perpSame, "core+oi": core && mask&4 != 0, "core+funding": core && mask&8 != 0 && !crowded, "core+liquidations": core && liqSame, "multifactor-supported": assessment.Supported}
			for _, threshold := range []int64{5e9, 6e9, 8e9, 10e9} {
				w := snap.Spot["60"]
				conditions[fmt.Sprintf("scale-%d", threshold/100000000)] = core && assessment.Large && w.Net != nil && *w.Net*sideSign(side) >= threshold
			}
			for rule, on := range conditions {
				key := rule + "/" + side
				required := map[string]int{"core+futures": 3, "core+oi": 5, "core+funding": 9, "core+liquidations": 17, "multifactor-supported": 15}[rule]
				if mask&required != required {
					p.Clear[key] = nil // unavailable evidence cannot re-arm a rule
					continue
				}
				if p.Active[key] {
					if !on {
						if p.Clear[key] == nil {
							t := at
							p.Clear[key] = &t
						}
						if at.Sub(*p.Clear[key]) >= 30*time.Minute {
							p.Active[key] = false
							p.Clear[key] = nil
						}
					} else {
						p.Clear[key] = nil
					}
					continue
				}
				if on {
					p.Active[key] = true
					p.Signals = append(p.Signals, multiResearchSignal{ID: fmt.Sprintf("%s-%s-%d", rule, side, at.Unix()), Rules: rule, At: at, Direction: side, ATR: s.Price.PriorATR, Following: s.Price.Following[side], Mask: mask})
				}
			}
		}
	}
	r.Through = minTime(p.Cursor, study.To)
	if study.ID != "" {
		if e = h.Store.saveDocument("multifactor-study-progress", study.ID, study.Asset, study.Created, p); e != nil {
			return nil, e
		}
	}
	if !p.Cursor.After(stop) {
		return r, nil
	}
	r.State = "association_calculated"
	return finishMultifactorStudy(r, p, c, study, now), nil
}
func finishMultifactorStudy(r *MultifactorStudy, p multiCheckpoint, c map[int64]Candle, study Study, now time.Time) *MultifactorStudy {
	episodes := priceEpisodes(c, study.From.Add(30*24*time.Hour-4*time.Hour), study.To.Add(-4*time.Hour-15*time.Minute))
	groups := []struct {
		name, variant string
		mask          int
	}{{"资金核心", MultifactorRules, 1}, {"加入合约同向", "core+futures", 3}, {"加入币计价OI背景", "core+oi", 5}, {"加入Funding风险过滤", "core+funding", 9}, {"加入清算集中", "core+liquidations", 17}, {"完整多因素支持", "multifactor-supported", 15}}
	for _, group := range groups {
		for _, holdout := range []bool{false, true} {
			phase := "development"
			if holdout {
				phase = "holdout"
			}
			inPhase := func(t time.Time) bool {
				return (!t.Before(study.From.Add(60*24*time.Hour))) == holdout && (!holdout || (!multifactorCase(t) && !multifactorCase(t.Add(4*time.Hour))))
			}
			valid := map[int64]bool{}
			for _, t := range p.Ticks {
				if t.Mask&group.mask == group.mask && inPhase(t.At) {
					valid[t.At.Unix()] = true
				}
			}
			g := MultifactorStudyGroup{Name: group.name, Phase: phase, Windows: len(valid), Trials: []DirectionalTrial{}, EqualBudget: []DirectionalTrial{}}
			if len(valid) == 0 {
				r.Groups = append(r.Groups, g)
				continue
			}
			for _, side := range []string{"buy", "sell"} {
				eps := []PriceEpisode{}
				for _, ev := range episodes {
					if ev.Direction == side && valid[ev.Detected.Unix()] && inPhase(ev.Start) {
						eps = append(eps, ev)
					}
				}
				names := []string{SignalRules, "price-breakout", MultifactorRules}
				if group.variant != MultifactorRules {
					names = append(names, group.variant)
				} else {
					names = append(names, "scale-50", "scale-60", "scale-80", "scale-100")
				}
				series := map[string][]Signal{}
				for _, s := range p.Signals {
					if s.Direction == side && valid[s.At.Unix()] {
						series[s.Rules] = append(series[s.Rules], s.asSignal())
					}
				}
				for _, ev := range eps {
					series["price-breakout"] = append(series["price-breakout"], Signal{ID: ev.ID, At: ev.Detected, Direction: side, ATR: hourlyATR(c, ev.DataThrough)})
				}
				for _, name := range names {
					for _, delay := range []int{0, 5, 10} {
						trial := multifactorTrial(name, side, series[name], eps, c, delay, now)
						if delay != 0 || !holdout {
							trial.Matches = nil
						}
						g.Trials = append(g.Trials, trial)
					}
				}
				// Equal budget excludes sensitivity variants so they cannot zero the main comparison.
				g.EqualBudget = append(g.EqualBudget, equalBudgetTrials([]string{SignalRules, "price-breakout", group.variant}, side, series, eps, c, now)...)
			}
			r.Groups = append(r.Groups, g)
		}
	}
	return r
}

// Retrospective normalization, not a tradable rule: same UTC-day/side budgets,
// chronological first alerts. Keep the unnormalized report alongside it.
func equalBudgetTrials(names []string, side string, series map[string][]Signal, eps []PriceEpisode, c map[int64]Candle, now time.Time) []DirectionalTrial {
	budgets := map[string]int{}
	for i, name := range names {
		counts := map[string]int{}
		for _, s := range series[name] {
			counts[s.At.UTC().Format("2006-01-02")]++
		}
		if i == 0 {
			budgets = counts
		} else {
			for day, n := range budgets {
				budgets[day] = min(n, counts[day])
			}
		}
	}
	beps := []PriceEpisode{}
	for _, ev := range eps {
		if budgets[ev.Detected.UTC().Format("2006-01-02")] > 0 {
			beps = append(beps, ev)
		}
	}
	out := []DirectionalTrial{}
	for _, name := range names {
		ordered := append([]Signal{}, series[name]...)
		sort.Slice(ordered, func(i, j int) bool { return ordered[i].At.Before(ordered[j].At) })
		used := map[string]int{}
		chosen := []Signal{}
		for _, s := range ordered {
			day := s.At.UTC().Format("2006-01-02")
			if used[day] < budgets[day] {
				chosen = append(chosen, s)
				used[day]++
			}
		}
		for _, delay := range []int{0, 5, 10} {
			trial := multifactorTrial(name, side, chosen, beps, c, delay, now)
			trial.Matches = nil
			out = append(out, trial)
		}
	}
	return out
}
