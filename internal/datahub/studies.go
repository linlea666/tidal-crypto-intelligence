package datahub

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"sort"
	"strconv"
	"time"
)

type StudyRequest struct {
	ParentStudyID string     `json:"parentStudyId,omitempty"`
	Asset         string     `json:"asset"`
	From          *time.Time `json:"from"`
	To            *time.Time `json:"to"`
}
type Study struct {
	ParentStudyID       string           `json:"parentStudyId,omitempty"`
	InputSnapshotID     string           `json:"inputSnapshotId,omitempty"`
	InputFrozenAt       *time.Time       `json:"inputFrozenAt,omitempty"`
	InputIntegrity      string           `json:"inputIntegrity,omitempty"`
	TerminalReason      string           `json:"terminalReason,omitempty"`
	FrozenCoverage      []map[string]any `json:"frozenCoverage,omitempty"`
	ValidationID        string           `json:"validationId,omitempty"`
	UnavailableRequests []DataRequest    `json:"unavailableRequests,omitempty"`
	Pipeline            string           `json:"pipeline,omitempty"`
	QueueCursor         int              `json:"queueCursor"`
	InputVersion        string           `json:"inputVersion,omitempty"`
	ID                  string           `json:"id"`
	Asset               string           `json:"asset"`
	From                time.Time        `json:"from"`
	To                  time.Time        `json:"to"`
	Created             time.Time        `json:"createdAt"`
	Updated             time.Time        `json:"updatedAt"`
	State               string           `json:"state"`
	Rules               string           `json:"rulesVersion"`
	Mode                string           `json:"mode"`
	Error               string           `json:"error,omitempty"`
	Jobs                []string         `json:"jobs"`
	CandleCursor        time.Time        `json:"candleCursor"`
	LastRun             *time.Time       `json:"lastRun"`
	Result              *StudyResult     `json:"result"`
}
type studyCheckpoint struct {
	Version string                `json:"version"`
	Cursor  time.Time             `json:"cursor"`
	Active  map[string]bool       `json:"active"`
	Clear   map[string]*time.Time `json:"clear"`
	Events  []StudyEvent          `json:"events"`
}
type Outcome struct {
	MFE4H     *float64 `json:"mfe4h"`
	MAE4H     *float64 `json:"mae4h"`
	Return1H  *float64 `json:"return1h"`
	Return4H  *float64 `json:"return4h"`
	Return24H *float64 `json:"return24h"`
	Barrier   string   `json:"barrier"`
}
type StudyEvent struct {
	At           time.Time  `json:"at"`
	Direction    string     `json:"direction"`
	Phase        string     `json:"phase"`
	Pattern      string     `json:"pattern"`
	Outcome      Outcome    `json:"outcome"`
	Wallet       *float64   `json:"walletDelta"`
	OI           *float64   `json:"oiChange"`
	Premium      *float64   `json:"premiumUsd"`
	BookPressure *float64   `json:"bookPressure"`
	Timing       string     `json:"timing"`
	ConfirmedAt  *time.Time `json:"confirmedAt"`
}
type Experiment struct {
	Name       string     `json:"name"`
	Samples    int        `json:"samples"`
	Favorable  int        `json:"favorable"`
	Adverse    int        `json:"adverse"`
	Ambiguous  int        `json:"ambiguous"`
	Incomplete int        `json:"incomplete"`
	TimedOut   int        `json:"timedOut"`
	Coverage   float64    `json:"coverage"`
	Rate       *float64   `json:"rate"`
	Interval   [2]float64 `json:"interval"`
}
type StudyResult struct {
	MultifactorComparison *MultifactorStudy    `json:"multifactorComparison,omitempty"`
	CandidateComparison   *CandidateComparison `json:"candidateComparison"`
	Evaluation            string               `json:"evaluationVersion,omitempty"`
	Coverage              []map[string]any     `json:"coverage"`
	CaseState             string               `json:"caseState"`
	StrategyState         string               `json:"strategyState"`
	FlowCoverage          float64              `json:"flowCoverage"`
	CandleCoverage        float64              `json:"candleCoverage"`
	AvailableAtKnown      bool                 `json:"availableAtKnown"`
	BaselineDays          int                  `json:"baselineDays"`
	DevelopmentDays       int                  `json:"developmentDays"`
	HoldoutDays           int                  `json:"holdoutDays"`
	CoreCalculated        bool                 `json:"coreCalculated"`
	Events                []StudyEvent         `json:"events"`
	Experiments           []Experiment         `json:"experiments"`
	Delays                []Experiment         `json:"delays"`
	Cases                 []map[string]any     `json:"cases"`
	Missing               []string             `json:"missing"`
	Notes                 []string             `json:"notes"`
}

func (h *Hub) CreateStudy(req StudyRequest) (Study, error) {
	h.studyMu.Lock()
	defer h.studyMu.Unlock()
	now := time.Now().UTC()
	if !researchAsset(req.Asset) {
		return Study{}, errors.New("预警与新建研究仅支持BTC；ETH日常行情保留")
	}
	if req.ParentStudyID != "" {
		var parent Study
		if err := h.Store.document(context.Background(), "study", req.ParentStudyID, &parent); err != nil {
			return Study{}, err
		}
		if parent.Asset != req.Asset {
			return Study{}, errors.New("研究币种与原版本不符")
		}
		if req.From == nil {
			v := maxTime(parent.From, now.Add(-90*24*time.Hour).Truncate(time.Hour).Add(time.Hour))
			req.From = &v
		}
		if req.To == nil {
			v := parent.To
			req.To = &v
		}
	}
	to := now.Truncate(time.Hour)
	if req.To != nil {
		to = req.To.UTC().Truncate(time.Hour)
	}
	from := to.Add(-90 * 24 * time.Hour)
	if req.From != nil {
		from = req.From.UTC().Truncate(time.Hour)
	}
	if !from.Before(to) || to.After(now) || from.Before(now.Add(-90*24*time.Hour-time.Hour)) || to.Sub(from) > 90*24*time.Hour {
		return Study{}, errors.New("研究范围须在保留的90天内")
	}
	if h.Store.Status().Paused || h.Store.Status().ResearchPaused {
		return Study{}, errors.New("容量保护：暂停研究补采")
	}
	revision := SignalRules + "/" + studyPipeline + "/" + req.ParentStudyID
	if req.ParentStudyID != "" {
		revision += "/" + now.Truncate(time.Minute).Format(time.RFC3339)
	}
	hash := sha256.Sum256([]byte(fmt.Sprintf("%s/%s/%s/%s", req.Asset, from.Format(time.RFC3339), to.Format(time.RFC3339), revision)))
	id := fmt.Sprintf("study-%x", hash[:10])
	var existing Study
	if e := h.Store.document(context.Background(), "study", id, &existing); e == nil {
		return existing, nil
	} else if e != sql.ErrNoRows {
		return Study{}, e
	}
	all, e := h.Store.documents(context.Background(), "study", "", 100)
	if e != nil {
		return Study{}, e
	}
	if req.From == nil && req.To == nil {
		for _, b := range all {
			var old Study
			if json.Unmarshal(b, &old) == nil && old.Asset == req.Asset && old.Pipeline == studyPipeline && now.Sub(old.Created) < 24*time.Hour {
				return old, nil
			}
		}
	}
	active := 0
	for _, b := range all {
		var s Study
		_ = json.Unmarshal(b, &s)
		if researchAsset(s.Asset) && (s.State == "queued" || s.State == "collecting" || s.State == "partial_queue" || s.State == "calculating" || s.State == "freezing") {
			active++
		}
	}
	if active >= 4 {
		return Study{}, errors.New("最多同时进行4个研究任务")
	}
	s := Study{ParentStudyID: req.ParentStudyID, Pipeline: studyPipeline, ID: id, Asset: req.Asset, From: from, To: to, Created: now, Updated: now, State: "queued", Rules: SignalRules, Mode: "association_only", Jobs: []string{}, CandleCursor: from}
	h.queueStudy(&s, now)
	if e := h.Store.saveDocument("study", id, req.Asset, now, s); e != nil {
		return s, e
	}
	return s, nil
}
func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
func (h *Hub) StudiesView(ctx context.Context, a, id string) (any, error) {
	if id != "" {
		var s Study
		e := h.Store.document(ctx, "study", id, &s)
		return s, e
	}
	rows, e := h.Store.documents(ctx, "study", a, 20)
	return map[string]any{"enabled": researchAsset(a), "enabledAssets": ResearchAssets(), "items": rows, "rulesVersion": SignalRules, "weightPolicy": "所有辅助指标为0；验证报告经确认后才可变更规则", "requiredShadowDays": 14, "forward": h.forwardReport(ctx, a), "shortTerm": h.shortStudyView(a)}, e
}
func (h *Hub) studyCandles(ctx context.Context, s *Study, now time.Time) error {
	if !s.CandleCursor.Before(s.To) {
		return nil
	}
	// This price-only background request is outside CoinGlass's quota, bounded to
	// one batch per worker cycle. It shares normalized facts with live candles.
	q := url.Values{"symbol": {s.Asset + "USDT"}, "interval": {"5m"}, "limit": {"1000"}, "startTime": {strconv.FormatInt(s.CandleCursor.UnixMilli(), 10)}, "endTime": {strconv.FormatInt(s.To.UnixMilli()-1, 10)}}
	v, e := directGet(ctx, "https://data-api.binance.vision/api/v3/klines?"+q.Encode())
	if e != nil {
		return e
	}
	d, _ := h.Dataset(ID("candles", s.Asset, "Binance", "spot"))
	last := s.CandleCursor
	for _, raw := range array(v) {
		r := array(raw)
		if len(r) < 6 {
			return errors.New("K线历史契约不匹配")
		}
		at := timestamp(r[0])
		if at == nil || at.Before(s.CandleCursor) || !at.Before(s.To) {
			continue
		}
		if at.Add(5 * time.Minute).After(now) {
			continue
		}
		vals := []float64{}
		for _, x := range r[1:6] {
			n, e := validNumber(x, false)
			if e != nil {
				return e
			}
			vals = append(vals, num(n))
		}
		c := Candle{vals[0], vals[1], vals[2], vals[3], vals[4]}
		if c.High < c.Low || c.Open <= 0 || c.Close <= 0 {
			return errors.New("K线价格无效")
		}
		if _, e = h.Store.Ingest(d, Observation{Dataset: d.ID, Source: d.Source, ObservedAt: at, FetchedAt: now, Resolution: 300, Quality: "valid", TimeBasis: "source", Payload: Payload{Candle: &c}}); e != nil {
			return e
		}
		last = maxTime(last, at.Add(5*time.Minute))
	}
	if !last.After(s.CandleCursor) {
		return errors.New("K线历史未推进")
	}
	s.CandleCursor = last
	return nil
}
func outcomeAt(c map[int64]Candle, at time.Time, side string, price float64, atr *float64) Outcome {
	o := Outcome{Barrier: "incomplete"}
	sign := 1.0
	if side == "sell" {
		sign = -1
	}
	if price <= 0 {
		return o
	}
	ret := func(hours int) *float64 {
		v, ok := c[at.Add(time.Duration(hours)*time.Hour-5*time.Minute).Unix()]
		if !ok {
			return nil
		}
		r := sign * (v.Close/price - 1) * 100
		return &r
	}
	hi, lo, full := price, price, true
	for t := at; t.Before(at.Add(4 * time.Hour)); t = t.Add(5 * time.Minute) {
		v, ok := c[t.Unix()]
		if !ok {
			full = false
			break
		}
		hi = max(hi, v.High)
		lo = min(lo, v.Low)
	}
	if full {
		mf, ma := (hi/price-1)*100, (lo/price-1)*100
		if sign < 0 {
			mf, ma = -ma, -mf
		}
		o.MFE4H = &mf
		o.MAE4H = &ma
	}
	o.Return1H = ret(1)
	o.Return4H = ret(4)
	o.Return24H = ret(24)
	if atr == nil || *atr <= 0 {
		return o
	}
	favorable := price + sign*2*(*atr)
	adverse := price - sign*(*atr)
	for t := at; t.Before(at.Add(24 * time.Hour)); t = t.Add(5 * time.Minute) {
		v, ok := c[t.Unix()]
		if !ok {
			return o
		}
		win, loss := v.High >= favorable, v.Low <= adverse
		if sign < 0 {
			win, loss = v.Low <= favorable, v.High >= adverse
		}
		if win && loss {
			o.Barrier = "ambiguous"
			return o
		}
		if win {
			o.Barrier = "favorable"
			return o
		}
		if loss {
			o.Barrier = "adverse"
			return o
		}
	}
	o.Barrier = "timeout"
	return o
}
func summarizeExperiment(name string, events []StudyEvent, pick func(StudyEvent) bool) Experiment {
	x := Experiment{Name: name}
	for _, e := range events {
		if !pick(e) {
			continue
		}
		x.Samples++
		switch e.Outcome.Barrier {
		case "favorable":
			x.Favorable++
		case "adverse":
			x.Adverse++
		case "ambiguous":
			x.Ambiguous++
		case "timeout":
			x.TimedOut++
		default:
			x.Incomplete++
		}
	}
	if len(events) > 0 {
		x.Coverage = float64(x.Samples) / float64(len(events))
	}
	n := x.Favorable + x.Adverse
	if n > 0 {
		p := float64(x.Favorable) / float64(n)
		x.Rate = &p
		z := 1.96
		den := 1 + z*z/float64(n)
		center := (p + z*z/(2*float64(n))) / den
		half := z * math.Sqrt(p*(1-p)/float64(n)+z*z/(4*float64(n*n))) / den
		x.Interval = [2]float64{center - half, center + half}
	}
	return x
}
func (h *Hub) evaluateStudy(ctx context.Context, s Study, now time.Time) (*StudyResult, error) {
	if s.InputSnapshotID != "" {
		ctx = context.WithValue(ctx, studySnapshotKey{}, s.InputSnapshotID)
	}
	version, e := h.studyInputVersion(ctx, s)
	if e != nil {
		return nil, e
	}
	bars, candles, e := h.signalInput(ctx, s.Asset, s.From, s.To, now)
	if e != nil {
		return nil, e
	}
	r := &StudyResult{Evaluation: EvaluationVersion, Events: []StudyEvent{}, Experiments: []Experiment{}, Delays: []Experiment{}, Cases: []map[string]any{}, Missing: []string{}, Notes: []string{"历史发布时间不可证明：本结果只描述关联，不能解释为当时可提前预警", "指定案例不计入独立留出成绩；5/10分钟延迟测试只衡量执行延迟敏感性", "辅助过滤器是固定对照实验，不进入线上权重；CVD未重复计分"}}
	expected := s.To.Sub(s.From).Minutes() / 5
	if expected > 0 {
		r.FlowCoverage = float64(len(bars)) / expected
		r.CandleCoverage = float64(len(candles)) / expected
	}
	if s.To.Sub(s.From) < 90*24*time.Hour {
		r.Missing = append(r.Missing, "保留范围不足90天，无法完成30/30/30检验")
	}
	if r.FlowCoverage < .95 {
		r.Missing = append(r.Missing, "五分钟现货成交历史不足95%")
	}
	if r.CandleCoverage < .95 {
		r.Missing = append(r.Missing, "五分钟价格历史不足95%")
	}
	r.Cases = h.studyCases(ctx, s, bars, candles, now)
	r.Coverage = h.studyCoverage(s)
	r.CaseState = "hourly_available"
	for _, c := range r.Cases {
		if c["state"] != "hourly_available" {
			r.CaseState = "partial"
		}
	}
	r.StrategyState = "incomplete"
	r.MultifactorComparison, e = h.evaluateMultifactor(ctx, s, bars, candles, now)
	if e != nil {
		return nil, e
	}
	// The new comparison takes smaller steps than the legacy control. Once
	// that control has completed on this exact fact version, preserve it while
	// advancing the new checkpoint instead of restarting the old calculation.
	if s.Result != nil && s.Result.CoreCalculated && s.InputVersion == version {
		previous := *s.Result
		previous.MultifactorComparison = r.MultifactorComparison
		previous.Coverage = r.Coverage
		return &previous, nil
	}
	if len(r.Missing) > 0 {
		return r, nil
	}
	r.BaselineDays = 30
	r.DevelopmentDays = 30
	r.HoldoutDays = 30
	balances := []Observation{}
	if e := h.Store.FactsAsOf(ctx, ID("balance-history", s.Asset, "", "chain"), s.From, s.To, now, func(o Observation) error { balances = append(balances, o); return nil }); e != nil {
		return nil, e
	}
	oi, e := h.closedScalars(ctx, ID("oi-history", s.Asset, "", "futures"), s.From, s.To, now)
	if e != nil {
		return nil, e
	}
	premium, e := h.closedScalars(ctx, ID("premium", s.Asset, "Coinbase", "spot"), s.From, s.To, now)
	if e != nil {
		return nil, e
	}
	baselineTo := s.From.Add(30 * 24 * time.Hour)
	baseline := newRollingBaseline(bars, s.From, baselineTo).result(now)
	if !baseline.Valid {
		r.Missing = append(r.Missing, "30天基线有效日期/样本门槛不满足")
		return r, nil
	}

	active := map[string]bool{}
	clear := map[string]*time.Time{}
	lastBaseline := time.Time{}
	var rolling *rollingBaseline
	shadows, e := h.Store.shadowSamples(ctx, s.Asset, s.From, s.To)
	if e != nil {
		return nil, e
	}
	book := map[int64]*float64{}
	for _, v := range shadows {
		book[v.At.Truncate(5*time.Minute).Unix()] = v.BookPressure
	}
	cursor := baselineTo
	var checkpoint studyCheckpoint
	checkpointErr := sql.ErrNoRows
	if s.ID != "" {
		checkpointErr = h.Store.document(ctx, "study-progress", s.ID, &checkpoint)
	}
	if checkpointErr != nil && checkpointErr != sql.ErrNoRows {
		return nil, checkpointErr
	}
	if checkpointErr == nil && checkpoint.Version == version && !checkpoint.Cursor.Before(baselineTo) && checkpoint.Cursor.Before(s.To) {
		cursor = checkpoint.Cursor
		active = checkpoint.Active
		clear = checkpoint.Clear
		r.Events = checkpoint.Events
	}
	until := s.To
	if s.ID != "" {
		until = minTime(until, cursor.Add(3*24*time.Hour))
	}
	for t := cursor; t.Before(until); t = t.Add(5 * time.Minute) {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		cut := t.Truncate(time.Hour).Add(-time.Hour)
		if !cut.Equal(lastBaseline) {
			if rolling == nil {
				rolling = newRollingBaseline(bars, cut.Add(-30*24*time.Hour), cut)
			} else {
				rolling.advance(cut)
			}
			baseline = rolling.result(now)
			lastBaseline = cut
		}
		for _, side := range []string{"buy", "sell"} {
			pattern, complete := signalCondition(bars, t, baseline, side)
			if !complete {
				clear[side] = nil
				continue
			}
			if active[side] {
				if pattern == "" {
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
			if pattern == "" {
				continue
			}
			hi, lo, price, ok := candleBounds(candles, t)
			if !ok {
				continue
			}
			active[side] = true
			phase := "development"
			if !t.Before(s.From.Add(60 * 24 * time.Hour)) {
				phase = "holdout"
			}
			ev := StudyEvent{At: t, Direction: side, Pattern: pattern, Phase: phase, Timing: "unknown_historical_availability", Outcome: outcomeAt(candles, t, side, price, hourlyATR(candles, t))}
			ev.BookPressure = book[t.Add(-5*time.Minute).Unix()]
			sig := Signal{DataThrough: t, FrozenHigh: hi, FrozenLow: lo, Direction: side, Expires: t.Add(4 * time.Hour)}
			for next := t.Add(10 * time.Minute); !next.After(sig.Expires); next = next.Add(5 * time.Minute) {
				if confirms(sig, bars, candles, next) {
					v := next
					ev.ConfirmedAt = &v
					break
				}
			}
			var last, previous *Observation
			for i := range balances {
				if balances[i].Time().After(t) {
					break
				}
				previous = last
				last = &balances[i]
			}
			if last != nil && previous != nil && t.Sub(last.Time()) <= 48*time.Hour {
				change := comparableBalances(*previous, *last)
				if change.Delta != nil {
					v := num(*change.Delta)
					ev.Wallet = &v
				}
			}
			ev.OI = scalarChange(oi, t)
			if v, ok := premium[t.Unix()]; ok {
				ev.Premium = &v
			}
			r.Events = append(r.Events, ev)
		}
	}
	if until.Before(s.To) {
		r.StrategyState = "calculating"
		e = h.Store.saveDocument("study-progress", s.ID, s.Asset, s.Created, studyCheckpoint{version, until, active, clear, r.Events})
		r.Events = nil // Partial events are not complete strategy statistics.
		return r, e
	}
	futures := newFlowAccumulator(300)
	if e = h.Store.FactsAsOf(ctx, ID("flow", s.Asset, "", "futures"), s.From, s.To, now, func(o Observation) error { futures.add(o); return nil }); e != nil {
		return nil, e
	}
	r.CandidateComparison, e = evaluateCandidates(ctx, bars, candles, s.From, s.To, CandidateContextSeries{futures.finish(), oi})
	if e != nil {
		return nil, e
	}
	r.CoreCalculated = true
	r.StrategyState = "calculated"
	if s.ID != "" {
		_, _ = h.Store.research.Exec("DELETE FROM documents WHERE kind='study-progress' AND id=?", s.ID)
	}
	selected := func(t time.Time) bool {
		for _, date := range []string{"2026-08-19", "2026-09-03", "2026-09-18"} {
			d, _ := time.ParseInLocation("2006-01-02", date, time.FixedZone("CST", 8*3600))
			if !t.Before(d.Add(-48*time.Hour)) && t.Before(d.Add(72*time.Hour)) {
				return true
			}
		}
		return false
	}
	holdout := []StudyEvent{}
	for _, ev := range r.Events {
		if ev.Phase == "holdout" && !selected(ev.At) {
			holdout = append(holdout, ev)
		}
	}
	filters := []struct {
		name string
		fn   func(StudyEvent) bool
	}{{"现货成交＋价格基准", func(StudyEvent) bool { return true }}, {"加钱包净变化同向过滤（实验）", func(e StudyEvent) bool {
		return e.Wallet != nil && (e.Direction == "buy" && *e.Wallet < 0 || e.Direction == "sell" && *e.Wallet > 0)
	}}, {"加OI增长过滤（实验）", func(e StudyEvent) bool { return e.OI != nil && *e.OI > 0 }}, {"加Coinbase溢价同向过滤（实验）", func(e StudyEvent) bool {
		return e.Premium != nil && (e.Direction == "buy" && *e.Premium > 0 || e.Direction == "sell" && *e.Premium < 0)
	}}, {"加五家已覆盖±1%盘口压力过滤（实验）", func(e StudyEvent) bool {
		return e.BookPressure != nil && (e.Direction == "buy" && *e.BookPressure > 0 || e.Direction == "sell" && *e.BookPressure < 0)
	}}}
	common := []StudyEvent{}
	for _, ev := range holdout {
		if ev.Wallet != nil && ev.OI != nil && ev.Premium != nil && ev.BookPressure != nil {
			common = append(common, ev)
		}
	}
	r.Notes = append(r.Notes, fmt.Sprintf("旧规则辅助消融统一使用全部辅助字段有效的共同样本：%d/%d；不足不评分", len(common), len(holdout)))
	for _, filter := range filters {
		r.Experiments = append(r.Experiments, summarizeExperiment(filter.name, common, filter.fn))
	}
	r.Missing = append(r.Missing, "历史盘口同口径证据尚不足：该消融项不评分", "历史发布时点未知，提前量/真实误报漏报须经至少14天前向观察验证")
	for _, delay := range []int{5, 10} {
		delayed := []StudyEvent{}
		for _, ev := range holdout {
			t := ev.At.Add(time.Duration(delay) * time.Minute)
			c, ok := candles[t.Add(-5*time.Minute).Unix()]
			if !ok {
				continue
			}
			x := ev
			x.Outcome = outcomeAt(candles, t, x.Direction, c.Close, hourlyATR(candles, ev.At))
			delayed = append(delayed, x)
		}
		r.Delays = append(r.Delays, summarizeExperiment(fmt.Sprintf("延迟%d分钟", delay), delayed, func(StudyEvent) bool { return true }))
	}
	for _, c := range r.Cases {
		d, _ := time.ParseInLocation("2006-01-02", c["date"].(string), time.FixedZone("CST", 8*3600))
		n := 0
		for _, ev := range r.Events {
			if !ev.At.Before(d.Add(-48*time.Hour)) && ev.At.Before(d.Add(72*time.Hour)) {
				n++
			}
		}
		c["observations"] = n
	}
	if len(r.Events) > 300 {
		r.Events = r.Events[len(r.Events)-300:]
		r.Notes = append(r.Notes, "详情只保留最近300个事件，汇总使用所有计算事件")
	}
	return r, nil
}
func (h *Hub) processStudies(ctx context.Context, now time.Time) error {
	list, e := h.Store.documents(ctx, "study", "", 100)
	if e != nil {
		return e
	}
	// Old mutable studies are archived once; their results and checkpoints remain.
	for _, raw := range list {
		var s Study
		if e = json.Unmarshal(raw, &s); e != nil {
			return e
		}
		if !researchAsset(s.Asset) {
			continue
		}
		if s.Pipeline != studyPipeline {
			if s.State == "complete" || s.State == "incomplete" || s.TerminalReason != "" {
				continue
			}
			if e = h.Store.saveDocument("study-legacy", s.ID, s.Asset, now, s); e != nil {
				return e
			}
			s.State = "incomplete"
			s.TerminalReason = "旧版可变输入研究已停止；原检查点与结果保留"
			s.Updated = now
			if e = h.Store.saveDocument("study", s.ID, s.Asset, s.Created, s); e != nil {
				return e
			}
			_, e = h.CreateStudy(StudyRequest{Asset: s.Asset, ParentStudyID: s.ID})
			return e
		}
	}
	// The oldest eligible task goes first, preventing a new task starving a lease.
	sort.SliceStable(list, func(i, j int) bool {
		var a, b Study
		_ = json.Unmarshal(list[i], &a)
		_ = json.Unmarshal(list[j], &b)
		return a.Created.Before(b.Created)
	})
	for _, raw := range list {
		var s Study
		if e = json.Unmarshal(raw, &s); e != nil {
			return e
		}
		if !researchAsset(s.Asset) || s.Pipeline != studyPipeline || s.State == "complete" || s.State == "incomplete" {
			continue
		}
		if s.LastRun != nil && now.Sub(*s.LastRun) < time.Minute {
			continue
		}
		s.Updated = now
		s.LastRun = &now
		save := func() error { return h.Store.saveDocument("study", s.ID, s.Asset, s.Created, s) }
		if s.InputSnapshotID == "" {
			deadline := !now.Before(s.Created.Add(24 * time.Hour))
			if !deadline {
				h.queueStudy(&s, now)
			}
			if !deadline && !h.offline && s.CandleCursor.Before(s.To) {
				cd, _ := h.Dataset(ID("candles", s.Asset, "Binance", "spot"))
				cd.Resolution = 300
				next, err := h.Store.nativeCursor(ctx, cd, s.CandleCursor, s.To, now)
				if err != nil {
					s.Error = err.Error()
					return save()
				}
				s.CandleCursor = next
				if err = h.studyCandles(ctx, &s, now); err != nil {
					s.Error = err.Error()
				}
				s.State = "collecting"
				return save()
			}
			done := s.QueueCursor >= len(studyRequests(s, s.Created))
			h.Scheduler.mu.Lock()
			for _, id := range s.Jobs {
				if j, ok := h.Scheduler.historyJobLocked(id); ok && !j.Completed && !j.Disabled {
					done = false
				}
			}
			h.Scheduler.mu.Unlock()
			if !done && !deadline {
				s.State = "collecting"
				return save()
			}
			m, err := h.Store.beginStudySnapshot(ctx, s, now)
			if err != nil {
				s.Error = err.Error()
				return save()
			}
			s.InputSnapshotID = m.ID
			s.InputFrozenAt = &m.AsOf
			s.FrozenCoverage = h.studyCoverage(s)
			s.State = "freezing"
			s.Error = ""
			return save()
		}
		m, err := h.Store.studySnapshot(ctx, s.InputSnapshotID)
		if err != nil {
			return err
		}
		started := time.Now()
		for m.State == "building" && time.Since(started) < time.Second {
			m, err = h.Store.advanceStudySnapshot(ctx, m, now)
			if err != nil {
				s.Error = err.Error()
				return save()
			}
		}
		if m.State == "failed" {
			s.State = "incomplete"
			s.InputIntegrity = "unavailable"
			s.TerminalReason = m.Reason
			return save()
		}
		if m.State != "ready" {
			s.State = "freezing"
			return save()
		}
		s.InputVersion, err = h.studyInputVersion(ctx, s)
		if err != nil {
			s.Error = err.Error()
			return save()
		}
		s.Result, err = h.evaluateStudy(ctx, s, m.AsOf)
		if err != nil {
			s.Error = err.Error()
			return save()
		}
		s.Error = ""
		s.State = "calculating"
		s.InputIntegrity = "frozen"
		r := s.Result
		if r != nil && r.StrategyState != "calculating" && (r.MultifactorComparison == nil || r.MultifactorComparison.State != "calculating") {
			s.State = "incomplete"
			s.TerminalReason = "冻结输入未达到完整研究门槛"
			if r.CoreCalculated && r.FlowCoverage >= .95 && r.CandleCoverage >= .95 && r.BaselineDays == 30 && r.DevelopmentDays == 30 && r.HoldoutDays == 30 {
				s.State = "complete"
				s.TerminalReason = ""
				s.InputIntegrity = "complete"
				s.ValidationID, err = h.freezeStudyValidation(ctx, s, now)
				if err != nil {
					return err
				}
			} else {
				s.InputIntegrity = "partial"
			}
		}
		return save()
	}
	return nil
}
func (h *Hub) forwardReportDue(now time.Time) bool {
	var last time.Time
	var evaluation string
	return !h.Store.LoadState("signals/forwardEvaluation", &evaluation) || evaluation != EvaluationVersion || !h.Store.LoadState("signals/forwardAt", &last) || now.Sub(last) >= time.Hour
}

func (h *Hub) researchWorker(ctx context.Context) {
	tick := time.NewTicker(time.Minute)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			now := time.Now().UTC()
			var seeded string
			if !h.offline && (!h.Store.LoadState("studies/pipeline", &seeded) || seeded != studyPipeline) {
				if _, e := h.CreateStudy(StudyRequest{Asset: "BTC"}); e == nil {
					_ = h.Store.SaveState("studies/pipeline", studyPipeline)
				}
			}
			step, cancel := context.WithTimeout(ctx, 40*time.Second)
			err := h.processSignals(step, now)
			if err != nil {
				_ = h.Store.SaveState("signals/error", map[string]any{"at": now, "error": err.Error()})
			}
			if !h.Store.Status().ResearchPaused && !h.Store.Status().Paused {
				if h.forwardReportDue(now) {
					for _, a := range ResearchAssets() {
						if e := h.buildForwardReport(step, a, now); e != nil {
							err = e
							break
						}
					}
					if err == nil {
						_ = h.Store.SaveState("signals/forwardAt", now)
						_ = h.Store.SaveState("signals/forwardEvaluation", EvaluationVersion)
					}
				}
				if e := h.processStudies(step, now); e != nil {
					_ = h.Store.SaveState("studies/error", map[string]any{"at": now, "error": e.Error()})
				}
			}
			cancel()
		}
	}
}
