package datahub

import (
	"context"
	"crypto/sha256"
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
	Asset string     `json:"asset"`
	From  *time.Time `json:"from"`
	To    *time.Time `json:"to"`
}
type Study struct {
	Pipeline     string       `json:"pipeline,omitempty"`
	QueueCursor  int          `json:"queueCursor"`
	InputVersion string       `json:"inputVersion,omitempty"`
	ID           string       `json:"id"`
	Asset        string       `json:"asset"`
	From         time.Time    `json:"from"`
	To           time.Time    `json:"to"`
	Created      time.Time    `json:"createdAt"`
	Updated      time.Time    `json:"updatedAt"`
	State        string       `json:"state"`
	Rules        string       `json:"rulesVersion"`
	Mode         string       `json:"mode"`
	Error        string       `json:"error,omitempty"`
	Jobs         []string     `json:"jobs"`
	CandleCursor time.Time    `json:"candleCursor"`
	LastRun      *time.Time   `json:"lastRun"`
	Result       *StudyResult `json:"result"`
}
type studyCheckpoint struct {
	Version string                `json:"version"`
	Cursor  time.Time             `json:"cursor"`
	Active  map[string]bool       `json:"active"`
	Clear   map[string]*time.Time `json:"clear"`
	Events  []StudyEvent          `json:"events"`
}
type Outcome struct {
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
	Coverage         []map[string]any `json:"coverage"`
	CaseState        string           `json:"caseState"`
	StrategyState    string           `json:"strategyState"`
	FlowCoverage     float64          `json:"flowCoverage"`
	CandleCoverage   float64          `json:"candleCoverage"`
	AvailableAtKnown bool             `json:"availableAtKnown"`
	BaselineDays     int              `json:"baselineDays"`
	DevelopmentDays  int              `json:"developmentDays"`
	HoldoutDays      int              `json:"holdoutDays"`
	CoreCalculated   bool             `json:"coreCalculated"`
	Events           []StudyEvent     `json:"events"`
	Experiments      []Experiment     `json:"experiments"`
	Delays           []Experiment     `json:"delays"`
	Cases            []map[string]any `json:"cases"`
	Missing          []string         `json:"missing"`
	Notes            []string         `json:"notes"`
}

func (h *Hub) CreateStudy(req StudyRequest) (Study, error) {
	h.studyMu.Lock()
	defer h.studyMu.Unlock()
	now := time.Now().UTC()
	if !researchAsset(req.Asset) {
		return Study{}, errors.New("预警与新建研究仅支持BTC；ETH日常行情保留")
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
	hash := sha256.Sum256([]byte(fmt.Sprintf("%s/%s/%s/%s", req.Asset, from.Format(time.RFC3339), to.Format(time.RFC3339), SignalRules+"/"+studyPipeline)))
	id := fmt.Sprintf("study-%x", hash[:10])
	var existing Study
	if e := h.Store.document(context.Background(), "study", id, &existing); e == nil {
		return existing, nil
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
		if researchAsset(s.Asset) && (s.State == "queued" || s.State == "collecting" || s.State == "partial_queue" || s.State == "calculating") {
			active++
		}
	}
	if active >= 4 {
		return Study{}, errors.New("最多同时进行4个研究任务")
	}
	s := Study{Pipeline: studyPipeline, ID: id, Asset: req.Asset, From: from, To: to, Created: now, Updated: now, State: "queued", Rules: SignalRules, Mode: "association_only", Jobs: []string{}, CandleCursor: from}
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
	return map[string]any{"enabled": researchAsset(a), "enabledAssets": ResearchAssets(), "items": rows, "rulesVersion": SignalRules, "weightPolicy": "所有辅助指标为0；验证报告经确认后才可变更规则", "requiredShadowDays": 14, "forward": h.forwardReport(ctx, a)}, e
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
	bars, candles, e := h.signalInput(ctx, s.Asset, s.From, s.To, now)
	if e != nil {
		return nil, e
	}
	r := &StudyResult{Events: []StudyEvent{}, Experiments: []Experiment{}, Delays: []Experiment{}, Cases: []map[string]any{}, Missing: []string{}, Notes: []string{"历史发布时间不可证明：本结果只描述关联，不能解释为当时可提前预警", "指定案例不计入独立留出成绩；5/10分钟延迟测试只衡量执行延迟敏感性", "辅助过滤器是固定对照实验，不进入线上权重；CVD未重复计分"}}
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
	if len(r.Missing) > 0 {
		return r, nil
	}
	r.BaselineDays = 30
	r.DevelopmentDays = 30
	r.HoldoutDays = 30
	balances := []Observation{}
	_ = h.Store.FactsAsOf(ctx, ID("balance-history", s.Asset, "", "chain"), s.From, s.To, now, func(o Observation) error { balances = append(balances, o); return nil })
	oi := map[int64]float64{}
	premium := map[int64]float64{}
	_ = h.Store.FactsAsOf(ctx, ID("oi-history", s.Asset, "", "futures"), s.From, s.To, now, func(o Observation) error {
		if len(o.Payload.OI) > 0 {
			oi[recordTime(o).Unix()] = num(o.Payload.OI[0].USD)
		}
		return nil
	})
	if s.Asset == "BTC" {
		_ = h.Store.FactsAsOf(ctx, ID("premium", s.Asset, "Coinbase", "spot"), s.From, s.To, now, func(o Observation) error {
			if o.Payload.Premium != nil {
				premium[recordTime(o).Unix()] = num(o.Payload.Premium.USD)
			}
			return nil
		})
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
	version := h.studyInputVersion(ctx, s)
	var checkpoint studyCheckpoint
	if s.ID != "" && h.Store.document(ctx, "study-progress", s.ID, &checkpoint) == nil && checkpoint.Version == version && !checkpoint.Cursor.Before(baselineTo) && checkpoint.Cursor.Before(s.To) {
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
			x, xok := oi[t.Add(-5*time.Minute).Unix()]
			y, yok := oi[t.Add(-65*time.Minute).Unix()]
			if xok && yok && y > 0 {
				v := (x/y - 1) * 100
				ev.OI = &v
			}
			if v, ok := premium[t.Add(-5*time.Minute).Unix()]; ok {
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
	for _, filter := range filters {
		r.Experiments = append(r.Experiments, summarizeExperiment(filter.name, holdout, filter.fn))
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
	list, e := h.Store.documents(ctx, "study", "", 20)
	if e != nil {
		return e
	}
	sort.Slice(list, func(i, j int) bool { return string(list[i]) < string(list[j]) })
	for _, raw := range list {
		var s Study
		if json.Unmarshal(raw, &s) != nil {
			continue
		}
		if !researchAsset(s.Asset) || ((s.State == "complete" || s.State == "incomplete") && s.Pipeline != studyPipeline) {
			continue
		}
		if s.LastRun != nil && now.Sub(*s.LastRun) < time.Minute && s.CandleCursor.After(s.To.Add(-time.Second)) {
			continue
		}
		if s.Pipeline == studyPipeline {
			h.queueStudy(&s, now)
		}
		s.State = "collecting"
		s.Updated = now
		if !h.offline && s.CandleCursor.Before(s.To) {
			cd, _ := h.Dataset(ID("candles", s.Asset, "Binance", "spot"))
			cd.Resolution = 300
			if next, e := h.Store.nativeCursor(ctx, cd, s.CandleCursor, s.To, now); e == nil {
				s.CandleCursor = next
			}
			if e := h.studyCandles(ctx, &s, now); e != nil {
				s.Error = e.Error()
			}
			return h.Store.saveDocument("study", s.ID, s.Asset, s.Created, s)
		}
		jobsDone, jobFailed := true, false
		h.Scheduler.mu.Lock()
		for _, id := range s.Jobs {
			j, ok := h.Scheduler.jobs[id]
			if !ok {
				jobFailed = true
			}
			if ok && !j.Completed && !j.Disabled {
				jobsDone = false
			}
			if ok && j.Disabled {
				jobFailed = true
			}
		}
		if len(s.Jobs) < 3 {
			jobFailed = true
		}
		h.Scheduler.mu.Unlock()
		version := h.studyInputVersion(ctx, s)
		if s.InputVersion == version && s.Result != nil && s.Result.StrategyState != "calculating" {
			s.Result.Coverage = h.studyCoverage(s)
			if !jobsDone {
				s.State = "collecting"
			} else if s.Result.CoreCalculated && !jobFailed {
				s.State = "complete"
			} else {
				s.State = "incomplete"
			}
			if e = h.Store.saveDocument("study", s.ID, s.Asset, s.Created, s); e != nil {
				return e
			}
			continue
		}
		s.Result, e = h.evaluateStudy(ctx, s, now)
		if e == nil {
			s.InputVersion = version
		}
		if e != nil {
			s.Error = e.Error()
		} else {
			s.Error = ""
		}
		s.LastRun = &now
		if s.Result != nil && s.Result.StrategyState == "calculating" {
			s.State = "calculating"
		}
		if jobsDone && s.State != "calculating" {
			s.State = "incomplete"
			if e == nil && !jobFailed && s.Result != nil && s.Result.CoreCalculated && s.Result.FlowCoverage >= .95 && s.Result.CandleCoverage >= .95 && s.Result.BaselineDays == 30 && s.Result.DevelopmentDays == 30 && s.Result.HoldoutDays == 30 {
				s.State = "complete"
			}
		}
		return h.Store.saveDocument("study", s.ID, s.Asset, s.Created, s)
	}
	return nil
}
func (h *Hub) researchWorker(ctx context.Context) {
	tick := time.NewTicker(time.Minute)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-tick.C:
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
				var last time.Time
				if !h.Store.LoadState("signals/forwardAt", &last) || now.Sub(last) >= time.Hour {
					for _, a := range ResearchAssets() {
						if e := h.buildForwardReport(step, a, now); e != nil {
							err = e
							break
						}
					}
					if err == nil {
						_ = h.Store.SaveState("signals/forwardAt", now)
					}
				}
				if e := h.processStudies(step, now); e != nil {
					_ = h.Store.SaveState("studies/error", map[string]any{"at": now, "error": e.Error()})
				}
			}
			cancel()
			mailCtx, done := context.WithTimeout(ctx, 30*time.Second)
			if e := h.processNotices(mailCtx, now); e != nil {
				_ = h.Store.SaveState("mail/error", map[string]any{"at": now, "error": e.Error()})
			}
			done()
		}
	}
}
