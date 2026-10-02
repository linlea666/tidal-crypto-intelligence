package datahub

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"
)

type ShortCoverage struct {
	Through  time.Time `json:"through"`
	Seen     time.Time `json:"seenAt"`
	Valid    bool      `json:"valid"`
	Pipeline string    `json:"pipelineVersion,omitempty"`
	Reasons  []string  `json:"reasons,omitempty"`
}
type ShortOutcome struct {
	Minutes  int      `json:"minutes"`
	State    string   `json:"state"`
	Coverage float64  `json:"coverage"`
	Return   *float64 `json:"returnPercent"`
	MFE      *float64 `json:"mfePercent"`
	MAE      *float64 `json:"maePercent"`
}
type ShortTrial struct {
	ID        string            `json:"id"`
	Rule      string            `json:"rule"`
	Direction string            `json:"direction"`
	At        time.Time         `json:"at"`
	Through   time.Time         `json:"dataThrough"`
	Snapshot  *ShortObservation `json:"snapshot,omitempty"`
	Following bool              `json:"following"`
	Start     time.Time         `json:"start"`
	Cursor    time.Time         `json:"cursor"`
	Reference *float64          `json:"referenceUsdt"`
	Valid     int               `json:"validBars"`
	MFE       *float64          `json:"mfePercent"`
	MAE       *float64          `json:"maePercent"`
	Outcomes  []ShortOutcome    `json:"outcomes"`
	Done      bool              `json:"done"`
}
type ShortEpisode struct {
	ID        string                 `json:"id"`
	Direction string                 `json:"direction"`
	At        time.Time              `json:"at"`
	Updated   time.Time              `json:"updatedAt"`
	Trials    map[string]*ShortTrial `json:"trials"`
	Done      bool                   `json:"done"`
}

func newShortTrial(id, rule, side string, at, through time.Time, snapshot *ShortObservation) *ShortTrial {
	start := at.Truncate(5 * time.Minute).Add(5 * time.Minute)
	t := &ShortTrial{ID: id, Rule: rule, Direction: side, At: at, Through: through, Snapshot: snapshot, Start: start, Cursor: start, Outcomes: []ShortOutcome{}}
	if snapshot != nil {
		t.Following = snapshot.Following[side]
	}
	for _, m := range []int{15, 30, 60, 240} {
		t.Outcomes = append(t.Outcomes, ShortOutcome{Minutes: m, State: "pending"})
	}
	return t
}

// Immutable first-visible coverage and first hint snapshots commit atomically.
// Corrections can update the current screen, but cannot create a second sample.
func (h *Hub) recordShortObservation(ctx context.Context, s ShortObservation, now time.Time) error {
	var origin time.Time
	if e := h.Store.shortLoad(ctx, "origin", ShortFlowRules, &origin); e != nil {
		return e
	}
	if s.Through.Before(origin) || s.Through.After(now) {
		return nil
	}
	if h.Store.Status().ResearchPaused || h.Store.Status().Paused {
		return errors.New("短周期研究因总容量暂停")
	}
	id := strconv.FormatInt(s.Through.Unix(), 10)
	var exists int
	if e := h.Store.shortDB().QueryRowContext(ctx, "SELECT count(*) FROM sf_records WHERE kind='coverage' AND id=?", id).Scan(&exists); e != nil {
		return e
	}
	if exists > 0 {
		return nil
	}
	eligible := !s.ResearchPaused && s.Fresh && s.Baseline.Valid && s.Windows["5"].Net != nil && !s.Windows["5"].From.Before(origin)
	// Common comparison coverage is stricter than enrollment. A missing hour
	// must not erase a valid five-minute hint; keep it for the excluded count.
	cov := ShortCoverage{Through: s.Through, Seen: now, Valid: eligible && s.Windows["10"].Net != nil && s.Windows["60"].Net != nil}
	cov.Pipeline = ShortPipeline
	if s.ResearchPaused {
		cov.Reasons = append(cov.Reasons, "research_paused")
	}
	if !s.Fresh {
		cov.Reasons = append(cov.Reasons, "stale_flow")
	}
	if !s.Baseline.Valid {
		cov.Reasons = append(cov.Reasons, "baseline_unavailable")
	}
	if s.Windows["5"].From.Before(origin) {
		cov.Reasons = append(cov.Reasons, "origin_boundary")
	}
	for _, m := range []string{"5", "10", "60"} {
		if s.Windows[m].Net == nil {
			cov.Reasons = append(cov.Reasons, "missing_"+m+"m")
		}
	}
	updates := []ShortEpisode{}
	if eligible {
		for _, side := range []string{"buy", "sell"} {
			active := []ShortHint{}
			for _, hint := range s.Hints {
				if hint.Active && hint.Direction == side && !s.Windows[strconv.Itoa(hint.Minutes)].From.Before(origin) {
					active = append(active, hint)
				}
			}
			if len(active) == 0 {
				continue
			}
			var event ShortEpisode
			var raw []byte
			e := h.Store.shortDB().QueryRowContext(ctx, "SELECT payload FROM sf_records WHERE kind='episode' AND json_extract(payload,'$.direction')=? ORDER BY at DESC LIMIT 1", side).Scan(&raw)
			if e != nil && e != sql.ErrNoRows {
				return e
			}
			if len(raw) > shortFlowEventLimit {
				return errors.New("短周期事件过大")
			}
			if e == nil {
				if e = json.Unmarshal(raw, &event); e != nil {
					return e
				}
			}
			if event.At.IsZero() || now.Sub(event.At) >= 4*time.Hour {
				event = ShortEpisode{ID: fmt.Sprintf("%s-%s-%d", ShortFlowRules, side, now.Unix()), Direction: side, At: now, Trials: map[string]*ShortTrial{}}
			}
			changed := false
			for _, hint := range active {
				key := fmt.Sprint(hint.Minutes)
				if event.Trials[key] != nil {
					continue
				}
				frozen := s
				frozen.ResearchPaused = false
				frozen.ResearchReason = ""
				event.Trials[key] = newShortTrial(event.ID+"/"+key, "short-"+key, side, now, s.Through, &frozen)
				changed = true
			}
			if changed {
				event.Done = false
				event.Updated = now
				updates = append(updates, event)
			}
		}
	}
	tx, e := h.Store.shortDB().BeginTx(ctx, nil)
	if e != nil {
		return shortWriteError(e)
	}
	defer tx.Rollback()
	b, _ := json.Marshal(cov)
	if _, e = tx.ExecContext(ctx, "INSERT OR IGNORE INTO sf_records VALUES('coverage',?,?,?)", id, s.Through.Unix(), b); e != nil {
		return shortWriteError(e)
	}
	for _, v := range updates {
		b, e = json.Marshal(v)
		if e != nil {
			return e
		}
		if len(b) > shortFlowEventLimit {
			return errors.New("短周期事件超过写入上限")
		}
		if _, e = tx.ExecContext(ctx, "INSERT INTO sf_records VALUES('episode',?,?,?) ON CONFLICT(kind,id) DO UPDATE SET payload=excluded.payload", v.ID, v.At.Unix(), b); e != nil {
			return shortWriteError(e)
		}
	}
	if e = tx.Commit(); e != nil {
		return shortWriteError(e)
	}
	return nil
}

// Bounded batch of pending episodes. Progress freezes each processed candle;
// late backfill cannot repair a previously recorded gap or realized displacement.
func (h *Hub) advanceShortTrial(ctx context.Context, t *ShortTrial, now time.Time) error {
	if t.Done {
		return nil
	}
	end := minTime(t.Start.Add(4*time.Hour), now.Truncate(5*time.Minute))
	c, e := liquidationCandleSeries(ctx, h.Store.shortDB(), "BTC", t.Cursor, end, now, true)
	if e != nil {
		return e
	}
	for at := t.Cursor; at.Before(end); at = at.Add(5 * time.Minute) {
		v, ok := c[at.Unix()]
		if !ok && now.Before(at.Add(20*time.Minute)) {
			break
		}
		if at.Equal(t.Start) && ok {
			t.Reference = flowPtr(v.Open)
		}
		if ok {
			t.Valid++
		}
		var ret *float64
		if ok && t.Reference != nil && *t.Reference > 0 {
			ref := *t.Reference
			sign := float64(sideSign(t.Direction))
			ret = flowPtr((v.Close/ref - 1) * 100 * sign)
			a, b := (v.High/ref-1)*100*sign, (v.Low/ref-1)*100*sign
			hi, lo := math.Max(0, math.Max(a, b)), math.Min(0, math.Min(a, b))
			if t.MFE == nil || hi > *t.MFE {
				t.MFE = &hi
			}
			if t.MAE == nil || lo < *t.MAE {
				t.MAE = &lo
			}
		}
		t.Cursor = at.Add(5 * time.Minute)
		for i := range t.Outcomes {
			o := &t.Outcomes[i]
			if !t.Cursor.Equal(t.Start.Add(time.Duration(o.Minutes) * time.Minute)) {
				continue
			}
			o.Coverage = float64(t.Valid) / float64(o.Minutes/5)
			o.State = "incomplete"
			if o.Coverage == 1 && t.Reference != nil && ret != nil {
				o.State = "complete"
				o.Return = ret
				if t.MFE != nil {
					o.MFE = flowPtr(*t.MFE)
				}
				if t.MAE != nil {
					o.MAE = flowPtr(*t.MAE)
				}
			}
		}
	}
	t.Done = !t.Cursor.Before(t.Start.Add(4 * time.Hour))
	return nil
}

func (h *Hub) shortControls(ctx context.Context, origin, now time.Time) error {
	// At most the last day is needed to discover fresh formal / price controls.
	from := maxTime(origin, now.Add(-24*time.Hour))
	rows, e := h.Store.shortDB().QueryContext(ctx, "SELECT payload FROM documents WHERE kind='signal' AND asset='BTC' AND at>=? AND json_extract(payload,'$.rulesVersion')=? ORDER BY at LIMIT 100", from.Unix(), MultifactorRules)
	if e != nil {
		return e
	}
	controls := []*ShortTrial{}
	for rows.Next() {
		var b []byte
		if e = rows.Scan(&b); e != nil {
			break
		}
		s := decodeSignal(b)
		if s.At.Before(origin) {
			continue
		}
		t := newShortTrial("formal/"+s.ID, "formal", s.Direction, s.At, s.DataThrough, nil)
		if s.Multifactor != nil {
			t.Following = s.Multifactor.Price.Following[s.Direction]
		}
		controls = append(controls, t)
	}
	if e == nil {
		e = rows.Err()
	}
	rows.Close()
	if e != nil {
		return e
	}
	events, e := loadPriceEvents(ctx, h.Store.shortDB(), "BTC", from, now)
	if e != nil {
		return e
	}
	for _, v := range events {
		if v.ReconstructedAt != nil || v.Start.Before(origin) {
			continue
		}
		t := newShortTrial("price/"+v.ID, "price-breakout", v.Direction, v.Detected, v.DataThrough, nil)
		t.Following = true
		controls = append(controls, t)
	}
	for _, t := range controls {
		v := ShortEpisode{ID: t.ID, Direction: t.Direction, At: t.At, Updated: t.At, Trials: map[string]*ShortTrial{t.Rule: t}}
		b, e := json.Marshal(v)
		if e != nil {
			return e
		}
		if _, e = h.Store.shortDB().ExecContext(ctx, "INSERT OR IGNORE INTO sf_records VALUES('control',?,?,?)", v.ID, v.At.Unix(), b); e != nil {
			return shortWriteError(e)
		}
	}
	return nil
}

type ShortStudyGroup struct {
	Rule            string                `json:"rule"`
	Direction       string                `json:"direction"`
	Signals         int                   `json:"signals"`
	PriceEvents     int                   `json:"priceEvents"`
	ExcludedEvents  int                   `json:"excludedEvents"`
	ExcludedSignals int                   `json:"excludedSignals"`
	Early           int                   `json:"early"`
	Following       int                   `json:"following"`
	Missed          int                   `json:"missed"`
	Unmatched       int                   `json:"unmatched"`
	Pending         int                   `json:"pending"`
	LeadMedian      *float64              `json:"leadMinutesMedian"`
	EarlyRate       *float64              `json:"earlyRate"`
	Interval        []float64             `json:"interval"`
	Ready           bool                  `json:"reviewReady"`
	Outcomes        []ShortOutcomeSummary `json:"outcomes"`
}
type ShortOutcomeSummary struct {
	Minutes    int      `json:"minutes"`
	Complete   int      `json:"complete"`
	Incomplete int      `json:"incomplete"`
	Pending    int      `json:"pending"`
	Return     *float64 `json:"medianReturnPercent"`
	MFE        *float64 `json:"medianMfePercent"`
	MAE        *float64 `json:"medianMaePercent"`
}
type ShortStudyReport struct {
	Rule        string            `json:"rulesVersion"`
	Origin      time.Time         `json:"origin"`
	At          time.Time         `json:"at"`
	From        time.Time         `json:"from"`
	Days        float64           `json:"days"`
	Coverage    float64           `json:"coverage"`
	Observed    int               `json:"observedWindows"`
	Expected    int               `json:"expectedWindows"`
	Groups      []ShortStudyGroup `json:"groups"`
	Gap         shortGap          `json:"gap"`
	Note        string            `json:"note"`
	GapReasons  map[string]int    `json:"gapReasons,omitempty"`
	Diagnostics *ShortRuntime     `json:"diagnostics,omitempty"`
}

func (h *Hub) shortStudyStep(ctx context.Context, now time.Time) error {
	if h.Store.Status().ResearchPaused || h.Store.Status().Paused {
		return errors.New("研究总容量保护")
	}
	if e := h.repairPriceProgress(ctx, now); e != nil {
		return e
	}
	var origin time.Time
	if e := h.Store.shortLoad(ctx, "origin", ShortFlowRules, &origin); e != nil {
		return e
	}
	if _, e := h.Store.shortDB().ExecContext(ctx, "DELETE FROM sf_records WHERE kind IN ('coverage','episode','control','publication') AND at<?", now.Add(-30*24*time.Hour).Unix()); e != nil {
		return shortWriteError(e)
	}
	if e := h.shortControls(ctx, origin, now); e != nil {
		return e
	}
	rows, e := h.Store.shortDB().QueryContext(ctx, "SELECT kind,payload FROM sf_records WHERE kind IN ('episode','control') AND json_extract(payload,'$.done')=0 ORDER BY json_extract(payload,'$.updatedAt') LIMIT 4")
	if e != nil {
		return e
	}
	type item struct {
		kind  string
		event ShortEpisode
	}
	pending := []item{}
	for rows.Next() {
		var k string
		var b []byte
		if e = rows.Scan(&k, &b); e != nil {
			break
		}
		if len(b) > shortFlowEventLimit {
			e = errors.New("研究工作集超限")
			break
		}
		var v ShortEpisode
		if e = json.Unmarshal(b, &v); e != nil {
			break
		}
		pending = append(pending, item{k, v})
	}
	if e == nil {
		e = rows.Err()
	}
	rows.Close()
	if e != nil {
		return e
	}
	for _, p := range pending {
		v := p.event
		v.Done = true
		for _, t := range v.Trials {
			if e = h.advanceShortTrial(ctx, t, now); e != nil {
				return e
			}
			v.Done = v.Done && t.Done
		}
		v.Updated = now
		if e = h.Store.shortPut(ctx, p.kind, v.ID, v.At, v); e != nil {
			return e
		}
	}
	var last ShortStudyReport
	if h.Store.shortState(ctx, "short-flow/report", &last) && now.Sub(last.At) < 5*time.Minute {
		return nil
	}
	return h.buildShortReport(ctx, origin, now)
}

func shortWilson(k, n int) []float64 {
	if n == 0 {
		return nil
	}
	p := float64(k) / float64(n)
	z := 1.96
	d := 1 + z*z/float64(n)
	m := (p + z*z/(2*float64(n))) / d
	h := z * math.Sqrt(p*(1-p)/float64(n)+z*z/(4*float64(n*n))) / d
	return []float64{math.Max(0, m-h), math.Min(1, m+h)}
}
func shortMedian(v []float64) *float64 {
	if len(v) == 0 {
		return nil
	}
	return flowPtr(percentile(v, .5))
}

func (h *Hub) buildShortReport(ctx context.Context, origin, now time.Time) error {
	defer shortMeasure(ctx, "report", time.Now())
	from := maxTime(origin, now.Add(-30*24*time.Hour))
	r := ShortStudyReport{Rule: ShortFlowRules, Origin: origin, At: now, From: from, Days: now.Sub(origin).Hours() / 24, Groups: []ShortStudyGroup{}, Note: "独立前向观察；5/10分钟不发送邮件。位移从发现后的下一根完整5分钟开盘计算，不含交易成本，不是交易收益。共同事件要求前后4小时≥95%有效观察；行情成熟8小时后比较，未匹配观察最长等待12小时；缺口事件排除，未匹配不等于亏损。每方向≥14天、95%覆盖、30个独立事件后才展示比例；不自动改参。明细保留30天，已知上线前案例不计入。"}
	coverage := map[int64]bool{}
	r.GapReasons = map[string]int{}
	rows, e := h.Store.shortDB().QueryContext(ctx, "SELECT payload FROM sf_records WHERE kind='coverage' AND at>=? ORDER BY at", from.Unix())
	if e != nil {
		return e
	}
	for rows.Next() {
		var b []byte
		var v ShortCoverage
		if e = rows.Scan(&b); e != nil {
			break
		}
		if e = json.Unmarshal(b, &v); e != nil {
			break
		}
		coverage[v.Through.Unix()] = v.Valid
		if v.Valid {
			r.Observed++
		} else if len(v.Reasons) == 0 {
			r.GapReasons["legacy_unknown"]++
		} else {
			for _, reason := range v.Reasons {
				r.GapReasons[reason]++
			}
		}
		if len(coverage) > 8641 {
			e = errors.New("观察覆盖工作集超限")
			break
		}
	}
	if e == nil {
		e = rows.Err()
	}
	rows.Close()
	if e != nil {
		return e
	}
	start := from.Truncate(5 * time.Minute).Add(5 * time.Minute)
	r.Expected = max(0, int(now.Truncate(5*time.Minute).Sub(start)/(5*time.Minute))+1)
	for at := start; !at.After(now.Truncate(5 * time.Minute)); at = at.Add(5 * time.Minute) {
		if _, ok := coverage[at.Unix()]; !ok {
			r.GapReasons["unrecorded_unknown"]++
		}
	}
	if r.Expected > 0 {
		r.Coverage = math.Min(1, float64(r.Observed)/float64(r.Expected))
	}
	priceEvents, e := loadPriceEvents(ctx, h.Store.shortDB(), "BTC", from, now)
	if e != nil {
		return e
	}
	eligible := []PriceEpisode{}
	excluded := map[string]int{}
	common := func(at time.Time) bool {
		if at.Add(-4*time.Hour).Before(from) || at.Add(4*time.Hour).After(now) {
			return false
		}
		n := 0
		end := at.Truncate(5 * time.Minute).Add(4 * time.Hour)
		for t := end.Add(-8*time.Hour + 5*time.Minute); !t.After(end); t = t.Add(5 * time.Minute) {
			if coverage[t.Unix()] {
				n++
			}
		}
		return n >= 92
	}
	for _, p := range priceEvents {
		// Wait until every potential following trial's common coverage window
		// has matured, so it cannot first look like a miss merely due to timing.
		if p.ReconstructedAt != nil || p.Start.Before(origin) || p.Start.Add(8*time.Hour).After(now) {
			continue
		}
		if common(p.Start) {
			eligible = append(eligible, p)
		} else {
			excluded[p.Direction]++
		}
	}
	// Stream one event at a time; retain compact trials only, never all snapshots.
	trials := []ShortTrial{}
	rows, e = h.Store.shortDB().QueryContext(ctx, "SELECT payload FROM sf_records WHERE kind IN ('episode','control') AND at>=? ORDER BY at", from.Unix())
	if e != nil {
		return e
	}
	for rows.Next() {
		var b []byte
		var event ShortEpisode
		if e = rows.Scan(&b); e != nil {
			break
		}
		if len(b) > shortFlowEventLimit {
			e = errors.New("研究行超限")
			break
		}
		if e = json.Unmarshal(b, &event); e != nil {
			break
		}
		for _, t := range event.Trials {
			t.Snapshot = nil
			trials = append(trials, *t)
		}
		if len(trials) > 2000 {
			e = errors.New("研究报告工作集超限")
			break
		}
	}
	if e == nil {
		e = rows.Err()
	}
	rows.Close()
	if e != nil {
		return e
	}
	sort.Slice(trials, func(i, j int) bool { return trials[i].At.Before(trials[j].At) })
	for _, side := range []string{"buy", "sell"} {
		for _, rule := range []string{"short-5", "short-10", "formal", "price-breakout"} {
			g := ShortStudyGroup{Rule: rule, Direction: side, ExcludedEvents: excluded[side], Outcomes: []ShortOutcomeSummary{}}
			ss := []Signal{}
			pp := []PriceEpisode{}
			tt := []ShortTrial{}
			for _, p := range eligible {
				if p.Direction == side {
					pp = append(pp, p)
				}
			}
			for _, t := range trials {
				if t.Rule != rule || t.Direction != side {
					continue
				}
				g.Signals++
				if t.At.Add(4 * time.Hour).After(now) {
					g.Pending++
					continue
				}
				if !common(t.At) {
					g.ExcludedSignals++
					continue
				}
				tt = append(tt, t)
				sig := Signal{ID: t.ID, At: t.At, Direction: side}
				if t.Following {
					sig.Multifactor = &FlowSnapshot{Price: PriceContext{Following: map[string]bool{side: true}}}
				}
				ss = append(ss, sig)
			}
			eval := matchEpisodes(pp, ss, now.Add(-4*time.Hour))
			g.PriceEvents, g.Early, g.Following, g.Missed, g.Unmatched = eval.PriceEpisodes, eval.Early, eval.Following, eval.Missed, eval.Unmatched
			g.Pending += eval.Pending
			lead := []float64{}
			for _, m := range eval.Matches {
				if m.LeadMinutes != nil {
					lead = append(lead, *m.LeadMinutes)
				}
			}
			g.LeadMedian = shortMedian(lead)
			g.Ready = r.Days >= 14 && r.Coverage >= .95 && g.PriceEvents >= 30
			if g.Ready {
				g.EarlyRate = flowPtr(float64(g.Early) / float64(g.PriceEvents))
				g.Interval = shortWilson(g.Early, g.PriceEvents)
			}
			for _, minutes := range []int{15, 30, 60, 240} {
				o := ShortOutcomeSummary{Minutes: minutes}
				ret, mfe, mae := []float64{}, []float64{}, []float64{}
				for _, t := range tt {
					for _, v := range t.Outcomes {
						if v.Minutes != minutes {
							continue
						}
						switch v.State {
						case "complete":
							o.Complete++
							if v.Return != nil {
								ret = append(ret, *v.Return)
							}
							if v.MFE != nil {
								mfe = append(mfe, *v.MFE)
							}
							if v.MAE != nil {
								mae = append(mae, *v.MAE)
							}
						case "incomplete":
							o.Incomplete++
						default:
							o.Pending++
						}
					}
				}
				o.Return, o.MFE, o.MAE = shortMedian(ret), shortMedian(mfe), shortMedian(mae)
				g.Outcomes = append(g.Outcomes, o)
			}
			r.Groups = append(r.Groups, g)
		}
	}
	h.Store.shortState(ctx, "short-flow/gap", &r.Gap)
	return h.Store.shortSaveState(ctx, "short-flow/report", r)
}

func (h *Hub) shortStudyView(a string) any {
	if a != "BTC" {
		return nil
	}
	var r ShortStudyReport
	if !h.Store.LoadState("short-flow/report", &r) {
		return nil
	}
	h.Store.LoadState("short-flow/gap", &r.Gap)
	r.Diagnostics = h.Store.shortRuntimeView()
	return r
}
