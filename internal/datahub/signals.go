package datahub

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"
)

const SignalRules = "flow-experiment-2.2.0"

type FlowBar struct {
	At        time.Time
	Buy, Sell int64
}

func (b FlowBar) Net() int64 { return b.Buy - b.Sell }
func (b FlowBar) Share() float64 {
	if b.Buy+b.Sell <= 0 {
		return 0
	}
	return float64(b.Buy) / float64(b.Buy+b.Sell)
}

type SignalBaseline struct {
	At       time.Time `json:"at"`
	From     time.Time `json:"from"`
	To       time.Time `json:"to"`
	Coverage float64   `json:"coverage"`
	Dates    int       `json:"validDates"`
	Valid    bool      `json:"valid"`
	P95      float64   `json:"p95Cents"`
	P05      float64   `json:"p05Cents"`
	P90      float64   `json:"p90HourCents"`
	P10      float64   `json:"p10HourCents"`
	Median15 float64   `json:"median15VolumeCents"`
	Median60 float64   `json:"median60VolumeCents"`
}
type Signal struct {
	ID             string         `json:"id"`
	Asset          string         `json:"asset"`
	Direction      string         `json:"direction"`
	Pattern        string         `json:"pattern"`
	State          string         `json:"state"`
	Rules          string         `json:"rulesVersion"`
	At             time.Time      `json:"at"`
	DataThrough    time.Time      `json:"dataThrough"`
	Updated        time.Time      `json:"updatedAt"`
	ConfirmedAt    *time.Time     `json:"confirmedAt"`
	Expires        time.Time      `json:"expiresAt"`
	FrozenHigh     float64        `json:"frozenHigh"`
	FrozenLow      float64        `json:"frozenLow"`
	ReferencePrice float64        `json:"referencePrice"`
	DetectionPrice *float64       `json:"detectionPriceUsdt"`
	ATR            *float64       `json:"atr1h"`
	Net15          int64          `json:"net15Cents"`
	BuyShare       float64        `json:"buyShare"`
	Baseline       SignalBaseline `json:"baseline"`
	Evidence       []string       `json:"evidence"`
	Conflicts      []string       `json:"conflicts"`
	Missing        []string       `json:"missing"`
}
type signalState struct {
	Last   time.Time             `json:"last"`
	Active map[string]string     `json:"active"`
	Clear  map[string]*time.Time `json:"clear"`
}

func percentile(a []float64, p float64) float64 {
	if len(a) == 0 {
		return 0
	}
	sort.Float64s(a)
	f := float64(len(a)-1) * p
	i := int(f)
	j := min(i+1, len(a)-1)
	return a[i] + (a[j]-a[i])*(f-float64(i))
}
func flowBars(rows []Observation, res int) map[int64]FlowBar {
	out := map[int64]FlowBar{}
	counts := map[int64]int{}
	bad := map[int64]bool{}
	direct := map[int64]FlowBar{}
	for _, o := range rows {
		if o.Payload.Flow == nil {
			continue
		}
		at := recordTime(o)
		native := max(60, o.Resolution)
		if native > res || res%native != 0 || at.Unix()%int64(native) != 0 {
			continue
		}
		if native == res {
			if o.Quality == "valid" {
				direct[at.Unix()] = FlowBar{At: at, Buy: money(o.Payload.Flow.Buy), Sell: money(o.Payload.Flow.Sell)}
			}
			continue
		}
		key := at.Truncate(time.Duration(res) * time.Second).Unix()
		b := out[key]
		b.At = time.Unix(key, 0).UTC()
		if o.Quality != "valid" {
			bad[key] = true
		}
		b.Buy += money(o.Payload.Flow.Buy)
		b.Sell += money(o.Payload.Flow.Sell)
		counts[key] += native
		out[key] = b
	}
	for k := range out {
		if counts[k] != res || bad[k] {
			delete(out, k)
		}
	}
	for k, b := range direct {
		if _, ok := out[k]; !ok {
			out[k] = b
		}
	}
	return out
}
func sumBars(bars map[int64]FlowBar, end time.Time, n int) (FlowBar, bool) {
	b := FlowBar{At: end.Add(-time.Duration(n) * 5 * time.Minute)}
	for i := n; i > 0; i-- {
		p, ok := bars[end.Add(-time.Duration(i)*5*time.Minute).Unix()]
		if !ok {
			return b, false
		}
		b.Buy += p.Buy
		b.Sell += p.Sell
	}
	return b, true
}
func buildSignalBaseline(rows []Observation, from, to, now time.Time) SignalBaseline {
	return baselineFromBars(flowBars(rows, 300), from, to, now)
}
func baselineFromBars(bars map[int64]FlowBar, from, to, now time.Time) SignalBaseline {
	b := SignalBaseline{At: now, From: from, To: to}
	dates := map[string]int{}
	net15, vol15, net60, vol60 := []float64{}, []float64{}, []float64{}, []float64{}
	valid := 0
	for t := from; t.Before(to); t = t.Add(5 * time.Minute) {
		if _, ok := bars[t.Unix()]; ok {
			valid++
			dates[t.Format("2006-01-02")]++
		}
		end := t.Add(5 * time.Minute)
		if end.Sub(from) >= 15*time.Minute {
			if v, ok := sumBars(bars, end, 3); ok {
				net15 = append(net15, float64(v.Net()))
				vol15 = append(vol15, float64(v.Buy+v.Sell))
			}
		}
		if end.Sub(from) >= time.Hour {
			if v, ok := sumBars(bars, end, 12); ok {
				net60 = append(net60, float64(v.Net()))
				vol60 = append(vol60, float64(v.Buy+v.Sell))
			}
		}
	}
	for _, n := range dates {
		if n >= 274 {
			b.Dates++
		}
	}
	expected := int(to.Sub(from) / (5 * time.Minute))
	if expected > 0 {
		b.Coverage = float64(valid) / float64(expected)
	}
	b.Valid = b.Dates >= 21 && b.Coverage >= .95 && len(net15) > 0 && len(net60) > 0
	b.P95 = percentile(net15, .95)
	b.P05 = percentile(net15, .05)
	b.P90 = percentile(net60, .9)
	b.P10 = percentile(net60, .1)
	b.Median15 = percentile(vol15, .5)
	b.Median60 = percentile(vol60, .5)
	return b
}
func signalCondition(bars map[int64]FlowBar, end time.Time, baseline SignalBaseline, side string) (string, bool) {
	if !baseline.Valid {
		return "", false
	}
	v15, a := sumBars(bars, end, 3)
	v60, b := sumBars(bars, end, 12)
	v180, c := sumBars(bars, end, 36)
	if !a || !b || !c {
		return "", false
	}
	sign := int64(1)
	if side == "sell" {
		sign = -1
	}
	share := func(v FlowBar) float64 {
		if sign > 0 {
			return v.Share()
		}
		return 1 - v.Share()
	}
	threshold := func(net int64, buy, sell float64) bool {
		if sign > 0 {
			return float64(net) >= buy
		}
		return float64(net) <= sell
	}
	consecutive := true
	for i := 1; i <= 3; i++ {
		v, ok := bars[end.Add(-time.Duration(i)*5*time.Minute).Unix()]
		if !ok || v.Net()*sign <= 0 {
			consecutive = false
		}
	}
	if consecutive && threshold(v15.Net(), baseline.P95, baseline.P05) && share(v15) >= .55 && v60.Net()*sign > 0 && float64(v15.Buy+v15.Sell) >= baseline.Median15 {
		return "burst", true
	}
	hours := true
	for i := 0; i < 3; i++ {
		v, ok := sumBars(bars, end.Add(-time.Duration(i)*time.Hour), 12)
		if !ok || v.Net()*sign <= 0 {
			hours = false
		}
	}
	if hours && threshold(v60.Net(), baseline.P90, baseline.P10) && share(v180) >= .55 && float64(v60.Buy+v60.Sell) >= baseline.Median60 {
		return "sustained", true
	}
	return "", true
}
func reverseBars(bars map[int64]FlowBar, end time.Time, side string) bool {
	sign := int64(1)
	if side == "sell" {
		sign = -1
	}
	for i := 1; i <= 2; i++ {
		v, ok := bars[end.Add(-time.Duration(i)*5*time.Minute).Unix()]
		if !ok || v.Net()*sign >= 0 {
			return false
		}
	}
	return true
}
func confirms(s Signal, bars map[int64]FlowBar, candles map[int64]Candle, end time.Time) bool {
	if end.After(s.Expires) || end.Before(s.DataThrough.Add(10*time.Minute)) {
		return false
	}
	v, ok := sumBars(bars, end, 3)
	if !ok {
		return false
	}
	if s.Direction == "buy" && v.Net() <= 0 || s.Direction == "sell" && v.Net() >= 0 {
		return false
	}
	for i := 1; i <= 2; i++ {
		t := end.Add(-time.Duration(i) * 5 * time.Minute)
		if t.Before(s.DataThrough) {
			return false
		}
		c, ok := candles[t.Unix()]
		if !ok {
			return false
		}
		if s.Direction == "buy" && c.Close <= s.FrozenHigh || s.Direction == "sell" && c.Close >= s.FrozenLow {
			return false
		}
	}
	return true
}
func (h *Hub) signalInput(ctx context.Context, a string, from, to, asOf time.Time) ([]Observation, map[int64]Candle, error) {
	rows := []Observation{}
	candles := map[int64]Candle{}
	fd := ID("flow", a, "", "spot")
	e := h.Store.FactsAsOf(ctx, fd, from, to, asOf, func(o Observation) error { rows = append(rows, o); return nil })
	if e != nil {
		return nil, nil, e
	}
	e = h.Store.FactsAsOf(ctx, ID("candles", a, "Binance", "spot"), from, to, asOf, func(o Observation) error {
		if o.Payload.Candle != nil && o.Quality == "valid" && o.Resolution == 300 {
			candles[recordTime(o).Unix()] = *o.Payload.Candle
		}
		return nil
	})
	return rows, candles, e
}
func candleBounds(c map[int64]Candle, to time.Time) (float64, float64, float64, bool) {
	high, low, last := 0.0, math.Inf(1), 0.0
	for i := 48; i > 0; i-- {
		v, ok := c[to.Add(-time.Duration(i)*5*time.Minute).Unix()]
		if !ok || v.High <= 0 || v.Low <= 0 {
			return 0, 0, 0, false
		}
		high = max(high, v.High)
		low = min(low, v.Low)
		last = v.Close
	}
	return high, low, last, true
}
func hourlyATR(c map[int64]Candle, to time.Time) *float64 {
	// Fourteen completed hourly true ranges, requiring the prior close.
	to = to.Truncate(time.Hour)
	sum := 0.0
	prev, ok := c[to.Add(-14*time.Hour-5*time.Minute).Unix()]
	if !ok {
		return nil
	}
	last := prev.Close
	for i := 14; i > 0; i-- {
		end := to.Add(-time.Duration(i-1) * time.Hour)
		hi, lo, cl := 0.0, math.Inf(1), 0.0
		for j := 12; j > 0; j-- {
			v, ok := c[end.Add(-time.Duration(j)*5*time.Minute).Unix()]
			if !ok {
				return nil
			}
			hi = max(hi, v.High)
			lo = min(lo, v.Low)
			cl = v.Close
		}
		sum += max(hi-lo, max(math.Abs(hi-last), math.Abs(lo-last)))
		last = cl
	}
	v := sum / 14
	return &v
}
func (h *Hub) processSignals(ctx context.Context, now time.Time) error {
	if h.Store.Status().ResearchPaused || h.Store.Status().Paused {
		return errors.New("容量保护：预警记录暂停，停止发出新告警")
	}
	for _, a := range Assets() {
		var state signalState
		if e := h.Store.document(ctx, "signal-engine", a, &state); e != nil && e != sql.ErrNoRows {
			return e
		}
		updates := []Signal{}
		notices := map[string]string{}
		if state.Active == nil {
			state.Active = map[string]string{}
		}
		if state.Clear == nil {
			state.Clear = map[string]*time.Time{}
		}
		fd, _ := h.Dataset(ID("flow", a, "", "spot"))
		cd, _ := h.Dataset(ID("candles", a, "Binance", "spot"))
		f, fo := h.Store.Latest(fd.ID)
		c, co := h.Store.Latest(cd.ID)
		end := now.Truncate(5 * time.Minute)
		if fo {
			end = minTime(end, f.Time().Add(time.Minute).Truncate(5*time.Minute))
		}
		if co {
			end = minTime(end, c.Time().Add(5*time.Minute))
		}
		var baseline SignalBaseline
		_ = h.Store.LoadState("signals/baseline/"+a, &baseline)
		if baseline.At.IsZero() || now.Sub(baseline.At) >= time.Hour {
			to := end.Truncate(time.Hour).Add(-time.Hour)
			from := to.Add(-30 * 24 * time.Hour)
			rows := []Observation{}
			if e := h.Store.FactsAsOf(ctx, fd.ID, from, to, now, func(o Observation) error { rows = append(rows, o); return nil }); e != nil {
				return e
			}
			baseline = buildSignalBaseline(rows, from, to, now)
			if e := h.Store.SaveState("signals/baseline/"+a, baseline); e != nil {
				return e
			}
		}
		fresh := fo && co && f.Fresh(fd, now) && c.Fresh(cd, now) && now.Sub(end) <= 12*time.Minute
		reason := "观察中 / 基线不足：需21个有效日期且30天样本达到95%"
		if baseline.Valid {
			reason = "观察中"
		}
		if !fresh {
			reason = "数据不足 / 当前成交或价格过期"
		}
		rows, candles, e := h.signalInput(ctx, a, end.Add(-16*time.Hour), end, now)
		if e != nil {
			return e
		}
		bars := flowBars(rows, 300)
		_, windowComplete := sumBars(bars, end, 36)
		_, _, _, priceComplete := candleBounds(candles, end)
		if !windowComplete || !priceComplete {
			reason = "数据不足 / 成交或价格窗口有缺口"
		}
		if e := h.Store.SaveState("signals/quality/"+a, map[string]any{"at": now, "fresh": fresh, "windowComplete": windowComplete && priceComplete, "reason": reason, "baseline": baseline}); e != nil {
			return e
		}
		if e := h.recordShadow(ctx, a, now, end, fresh && windowComplete && priceComplete, baseline.Valid); e != nil {
			return e
		}
		// Startup establishes a cursor; historical backfill never emits old mail.
		initialized := state.Last.IsZero() || state.Last.Before(h.boot.Truncate(5*time.Minute))
		newBar := end.After(state.Last)
		if initialized {
			state.Last = end
			newBar = false
		}
		for _, side := range []string{"buy", "sell"} {
			pattern, complete := signalCondition(bars, end, baseline, side)
			valid := fresh && complete
			if !valid {
				state.Clear[side] = nil
			}
			id := state.Active[side]
			if id != "" {
				var sig Signal
				if e := h.Store.document(ctx, "signal", id, &sig); e != nil {
					return e
				}
				old := sig.State
				if sig.ConfirmedAt == nil && !now.Before(sig.Expires) {
					sig.State = "expired"
				} else if valid && newBar && sig.ConfirmedAt == nil {
					if confirms(sig, bars, candles, end) {
						sig.State = "confirmed"
						sig.ConfirmedAt = &now
					} else if reverseBars(bars, end, side) {
						sig.State = "weakened"
					}
				}
				if old != sig.State {
					sig.Updated = now
					if sig.State == "confirmed" {
						sig.Conflicts = []string{"价格确认只是后续证据，不代表行情必然持续"}
						sig.Evidence = append(sig.Evidence, "两根完成5分钟K线突破冻结区间，成交方向仍一致")
					}
					updates = append(updates, sig)
					if sig.State == "confirmed" && !initialized {
						notices[sig.ID] = "confirmed"
					}
				}
				if valid && pattern == "" {
					if state.Clear[side] == nil {
						v := now
						state.Clear[side] = &v
					}
					if now.Sub(*state.Clear[side]) >= 30*time.Minute {
						delete(state.Active, side)
						delete(state.Clear, side)
					}
				} else if valid {
					state.Clear[side] = nil
				}
				continue
			}
			if !valid || !newBar || pattern == "" {
				continue
			}
			high, low, reference, ok := candleBounds(candles, end)
			if !ok {
				continue
			}
			v, _ := sumBars(bars, end, 3)
			sig := Signal{ID: fmt.Sprintf("%s-%s-%d", a, side, end.Unix()), Asset: a, Direction: side, Pattern: pattern, State: "anomaly", Rules: SignalRules, At: now, DataThrough: end, Updated: now, Expires: now.Add(4 * time.Hour), FrozenHigh: high, FrozenLow: low, ReferencePrice: reference, ATR: hourlyATR(candles, end), Net15: v.Net(), BuyShare: v.Share() * 100, Baseline: baseline, Evidence: []string{"同口径成交分位、连续性与成交量满足实验规则", "CVD与净买卖属于同一类证据"}, Conflicts: []string{"价格尚未突破触发前冻结的4小时区间"}, Missing: []string{"辅助指标尚未通过权重准入；不提升告警等级"}}
			updates = append(updates, sig)
			pd, _ := h.Dataset(ID("price", a, "Binance", "spot"))
			if p, ok := h.Store.Latest(pd.ID); ok && p.Fresh(pd, now) && p.Payload.Price != nil {
				v := num(p.Payload.Price.Value)
				updates[len(updates)-1].DetectionPrice = &v
			}
			state.Active[side] = sig.ID
			notices[sig.ID] = "anomaly"
		}
		if newBar {
			state.Last = end
		}
		if e := h.commitSignals(ctx, a, state, updates, notices, now); e != nil {
			return e
		}
	}
	return nil
}
func (h *Hub) commitSignals(ctx context.Context, a string, state signalState, updates []Signal, notices map[string]string, now time.Time) error {
	tx, e := h.Store.research.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	put := func(kind, id string, at time.Time, v any) error {
		b, e := json.Marshal(v)
		if e != nil {
			return e
		}
		_, e = tx.ExecContext(ctx, "INSERT INTO documents VALUES(?,?,?,?,?) ON CONFLICT(kind,id) DO UPDATE SET at=excluded.at,payload=excluded.payload", kind, id, a, at.Unix(), b)
		return e
	}
	for _, s := range updates {
		if e = put("signal", s.ID, s.At, s); e != nil {
			return e
		}
		if kind := notices[s.ID]; kind != "" {
			b, e := json.Marshal(map[string]any{"asset": a, "direction": s.Direction, "kind": kind, "at": s.At, "dataThrough": s.DataThrough, "id": s.ID})
			if e != nil {
				return e
			}
			status := "pending"
			if h.mail == nil {
				status = "unconfigured"
			}
			if _, e = tx.ExecContext(ctx, "INSERT OR IGNORE INTO notices(id,signal_id,kind,created,status,payload) VALUES(?,?,?,?,?,?)", s.ID+"/"+kind, s.ID, kind, now.Unix(), status, b); e != nil {
				return e
			}
		}
	}
	if e = put("signal-engine", a, now, state); e != nil {
		return e
	}
	return tx.Commit()
}
func (h *Hub) SignalsView(ctx context.Context, a, id string) (any, error) {
	if id != "" {
		var s Signal
		e := h.Store.document(ctx, "signal", id, &s)
		return s, e
	}
	rows, e := h.Store.documents(ctx, "signal", a, 100)
	if e != nil {
		return nil, e
	}
	var quality map[string]any
	_ = h.Store.LoadState("signals/quality/"+a, &quality)
	observers := []map[string]any{}
	for _, id := range []string{ID("oi-history", a, "", "futures"), ID("premium", a, "Coinbase", "spot")} {
		if d, ok := h.Dataset(id); ok {
			o, exists := h.Store.Latest(id)
			observers = append(observers, map[string]any{"kind": d.Kind, "meta": metadata(d, o, exists), "data": o.Payload, "weight": 0})
		}
	}
	return map[string]any{"items": rows, "quality": quality, "observers": observers, "rulesVersion": SignalRules, "auxiliaryWeight": 0, "mail": h.mailStatus(), "note": "实验性资金异动：不识别交易者身份，不承诺提前量或胜率。数据缺失时停发。"}, nil
}
func nextETF(now time.Time) time.Time {
	loc := time.FixedZone("CST", 8*3600)
	t := now.In(loc)
	next := time.Date(t.Year(), t.Month(), t.Day(), 8, 30, 0, 0, loc)
	if !next.After(now) {
		next = next.AddDate(0, 0, 1)
	}
	return next.UTC()
}

// Unmarshal helper keeps study and live signal rules on identical inputs.
func decodeSignal(b json.RawMessage) Signal { var s Signal; _ = json.Unmarshal(b, &s); return s }
