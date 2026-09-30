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

func (w *Warehouse) initShortFlow() error {
	_, err := w.research.Exec(`
CREATE TABLE IF NOT EXISTS sf_records(kind TEXT,id TEXT,at INTEGER,payload BLOB,PRIMARY KEY(kind,id)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS sf_time ON sf_records(kind,at);
CREATE TABLE IF NOT EXISTS sf_budget(id INTEGER PRIMARY KEY,used INTEGER NOT NULL);
INSERT OR IGNORE INTO sf_budget VALUES(1,0);
CREATE TRIGGER IF NOT EXISTS sf_insert BEFORE INSERT ON sf_records WHEN NOT EXISTS(SELECT 1 FROM sf_records WHERE kind=NEW.kind AND id=NEW.id) AND (SELECT used FROM sf_budget WHERE id=1)+length(NEW.payload)+256>33554432 BEGIN SELECT RAISE(ABORT,'short flow sub-budget full'); END;
CREATE TRIGGER IF NOT EXISTS sf_update BEFORE UPDATE OF payload ON sf_records WHEN (SELECT used FROM sf_budget WHERE id=1)+length(NEW.payload)-length(OLD.payload)>33554432 BEGIN SELECT RAISE(ABORT,'short flow sub-budget full'); END;
CREATE TRIGGER IF NOT EXISTS sf_added AFTER INSERT ON sf_records BEGIN UPDATE sf_budget SET used=used+length(NEW.payload)+256 WHERE id=1; END;
CREATE TRIGGER IF NOT EXISTS sf_changed AFTER UPDATE OF payload ON sf_records BEGIN UPDATE sf_budget SET used=used+length(NEW.payload)-length(OLD.payload) WHERE id=1; END;
CREATE TRIGGER IF NOT EXISTS sf_removed AFTER DELETE ON sf_records BEGIN UPDATE sf_budget SET used=used-length(OLD.payload)-256 WHERE id=1; END;`)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	b, _ := json.Marshal(now)
	_, err = w.research.Exec("INSERT OR IGNORE INTO sf_records VALUES('origin',?,?,?)", ShortFlowRules, now.Unix(), b)
	return err
}

func (w *Warehouse) shortLoad(ctx context.Context, kind, id string, v any) error {
	var b []byte
	if e := w.research.QueryRowContext(ctx, "SELECT payload FROM sf_records WHERE kind=? AND id=?", kind, id).Scan(&b); e != nil {
		return e
	}
	if len(b) > shortFlowRowLimit || ((kind == "episode" || kind == "control") && len(b) > shortFlowEventLimit) {
		return errors.New("短周期研究行超过读取上限")
	}
	return json.Unmarshal(b, v)
}

func (w *Warehouse) shortSaveState(ctx context.Context, key string, v any) error {
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	if len(b) > shortFlowRowLimit {
		return errors.New("短周期状态工作集超限")
	}
	_, e = w.db.ExecContext(ctx, "INSERT INTO state(key,payload) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET payload=excluded.payload", key, b)
	return e
}
func (w *Warehouse) shortState(ctx context.Context, key string, v any) bool {
	var b []byte
	return w.db.QueryRowContext(ctx, "SELECT payload FROM state WHERE key=?", key).Scan(&b) == nil && len(b) <= shortFlowRowLimit && json.Unmarshal(b, v) == nil
}
func (w *Warehouse) shortPut(ctx context.Context, kind, id string, at time.Time, v any) error {
	if w.Status().Paused || w.Status().ResearchPaused {
		return errors.New("研究总容量保护")
	}
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	if len(b) > shortFlowRowLimit || ((kind == "episode" || kind == "control") && len(b) > shortFlowEventLimit) {
		return errors.New("短周期研究行超过写入上限")
	}
	_, e = w.research.ExecContext(ctx, "INSERT INTO sf_records VALUES(?,?,?,?) ON CONFLICT(kind,id) DO UPDATE SET at=excluded.at,payload=excluded.payload", kind, id, at.Unix(), b)
	return e
}

type shortGap struct {
	At     time.Time `json:"at"`
	Reason string    `json:"reason"`
	Paused bool      `json:"paused"`
	Count  int       `json:"count"`
}

func (w *Warehouse) shortGap(now time.Time, e error) {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	var g shortGap
	w.shortState(ctx, "short-flow/gap", &g)
	g.At, g.Reason, g.Paused = now, e.Error(), true
	g.Count++
	_ = w.shortSaveState(ctx, "short-flow/gap", g)
}
func (w *Warehouse) shortResume() {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	var g shortGap
	if w.shortState(ctx, "short-flow/gap", &g) && g.Paused {
		g.Paused = false
		_ = w.shortSaveState(ctx, "short-flow/gap", g)
	}
}

type shortBaselineWork struct {
	From    time.Time `json:"from"`
	To      time.Time `json:"to"`
	Cursor  time.Time `json:"cursor"`
	AsOf    time.Time `json:"asOf"`
	Version string    `json:"version"`
	Bars    []FlowBar `json:"bars"`
}

// One or two days per checkpoint keeps cold-start / historical corrections bounded.
// An ordinary hourly advance reuses the unchanged prior range and reads one hour.
func (h *Hub) shortBaselineStep(ctx context.Context, now time.Time) error {
	var current ShortObservation
	if !h.Store.shortState(ctx, "short-flow/current", &current) {
		return ctx.Err()
	}
	if current.Through.IsZero() {
		return nil
	}
	to := current.Through.Truncate(time.Hour).Add(-time.Hour)
	from := to.Add(-30 * 24 * time.Hour)
	id := ID("flow", "BTC", "", "spot")
	w := shortBaselineWork{Bars: make([]FlowBar, 0, 8640)}
	err := h.Store.shortLoad(ctx, "work", ShortFlowRules, &w)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	if w.Cursor.Equal(w.To) && !w.To.IsZero() {
		version := h.Store.datasetRangeVersion(ctx, id, w.From, w.To)
		if w.To.Equal(to) && w.Version == version {
			return nil
		}
		if w.Version == version && to.After(w.To) && to.Sub(w.To) <= 24*time.Hour {
			keep := make([]FlowBar, 0, 8640)
			for _, b := range w.Bars {
				if !b.At.Before(from) {
					keep = append(keep, b)
				}
			}
			w.From, w.To, w.AsOf, w.Bars = from, to, now, keep
			w.Version = h.Store.datasetRangeVersion(ctx, id, from, to)
		} else {
			w = shortBaselineWork{}
		}
	}
	if w.To.IsZero() {
		w = shortBaselineWork{From: from, To: to, Cursor: from, AsOf: now, Version: h.Store.datasetRangeVersion(ctx, id, from, to), Bars: []FlowBar{}}
	}
	end := minTime(w.Cursor.Add(48*time.Hour), w.To)
	acc := newFlowAccumulator(300)
	if err = h.Store.FactsAsOf(ctx, id, w.Cursor, end, w.AsOf, func(o Observation) error {
		if shortClosedFact(o) {
			acc.add(o)
		}
		return nil
	}); err != nil {
		return err
	}
	for _, b := range acc.finish() {
		w.Bars = append(w.Bars, b)
	}
	sort.Slice(w.Bars, func(i, j int) bool { return w.Bars[i].At.Before(w.Bars[j].At) })
	if len(w.Bars) > 8640 {
		return errors.New("短周期基线工作集超限")
	}
	w.Cursor = end
	if w.Cursor.Equal(w.To) {
		bars := map[int64]FlowBar{}
		for _, b := range w.Bars {
			bars[b.At.Unix()] = b
		}
		b := shortBaseline(bars, w.From, w.To, w.AsOf)
		if err = h.Store.shortPut(ctx, "baseline", ShortFlowRules, now, b); err != nil {
			return err
		}
	}
	return h.Store.shortPut(ctx, "work", ShortFlowRules, now, w)
}

// Legacy forming revisions remain in the ledger for audit, not new evidence.
func shortClosedFact(o Observation) bool {
	return o.Quality == "valid" && !recordTime(o).Add(time.Duration(max(60, o.Resolution))*time.Second).After(o.FetchedAt)
}

func (h *Hub) shortInput(ctx context.Context, now time.Time) (map[int64]FlowBar, map[int64]time.Time, time.Time, error) {
	id := ID("flow", "BTC", "", "spot")
	latest, ok := h.Store.Latest(id)
	end := now.Truncate(5 * time.Minute)
	if ok {
		end = minTime(end, latest.Time().Add(time.Duration(max(60, latest.Resolution))*time.Second).Truncate(5*time.Minute))
	}
	acc := newFlowAccumulator(300)
	available := map[int64]time.Time{}
	e := h.Store.FactsAsOf(ctx, id, end.Add(-4*time.Hour), end, now, func(o Observation) error {
		if !shortClosedFact(o) {
			return nil
		}
		acc.add(o)
		at := recordTime(o).Truncate(5 * time.Minute).Unix()
		seen := o.FetchedAt
		if o.FirstFetchedAt != nil {
			seen = *o.FirstFetchedAt
		}
		available[at] = maxTime(available[at], seen)
		return nil
	})
	bars := acc.finish()
	// Do not pretend a missing latest bucket is complete. An older complete end
	// is displayed with its real age and cannot silently re-arm an experiment.
	for end.After(now.Add(-4 * time.Hour)) {
		if _, exists := bars[end.Add(-5*time.Minute).Unix()]; exists {
			break
		}
		end = end.Add(-5 * time.Minute)
	}
	return bars, available, end, e
}

func (h *Hub) shortObservationStep(ctx context.Context, now time.Time) error {
	bars, available, end, e := h.shortInput(ctx, now)
	if e != nil {
		return e
	}
	var baseline ShortBaseline
	baselineErr := h.Store.shortLoad(ctx, "baseline", ShortFlowRules, &baseline)
	candles, ce := h.liquidationCandleSeries(ctx, "BTC", end.Add(-16*time.Hour), end, now, false)
	if ce != nil {
		candles = map[int64]Candle{}
	}
	s := buildShortObservation(bars, candles, end, now, baseline)
	if t := available[end.Add(-5*time.Minute).Unix()]; !t.IsZero() {
		s.Available = &t
		s.Delay = flowPtr(math.Max(0, now.Sub(t).Seconds()))
	}
	var formal FlowSnapshot
	if h.Store.shortState(ctx, "signals/current/BTC", &formal) && !formal.At.After(now) && now.Sub(formal.DataThrough) <= 12*time.Minute {
		s.Background = &formal.Context
		s.BackgroundAt = &formal.At
		s.BackgroundThrough = &formal.DataThrough
	}
	h.shortZones(ctx, &s, now)
	var gap shortGap
	h.Store.shortState(ctx, "short-flow/gap", &gap)
	s.ResearchPaused = gap.Paused || h.Store.Status().ResearchPaused || h.Store.Status().Paused
	if s.ResearchPaused {
		s.ResearchReason = gap.Reason
	}
	// Current observations remain available when research storage is full.
	if e = h.Store.shortSaveState(ctx, "short-flow/current", s); e != nil {
		return e
	}
	if baselineErr != nil && baselineErr != sql.ErrNoRows {
		return baselineErr
	}
	return h.recordShortObservation(ctx, s, now)
}

func (h *Hub) shortZones(ctx context.Context, s *ShortObservation, now time.Time) {
	var model LiquidationMapSnapshot
	if h.Store.liquidationLoad(ctx, "state", ID("map", "BTC", "", "futures"), &model) != nil || !model.Complete || model.Available.After(now) || now.Sub(model.Fetched) > 35*time.Minute || model.Quote != "USDT" {
		return
	}
	pd, _ := h.Dataset(ID("price", "BTC", "Binance", "spot"))
	p, ok := h.Store.Latest(pd.ID)
	if !ok || !p.Fresh(pd, now) || p.Payload.Price == nil || p.Payload.Price.Quote != model.Quote {
		return
	}
	price := num(p.Payload.Price.Value)
	if price <= 0 {
		return
	}
	for _, side := range []string{"short", "long"} {
		var best *LiquidationZone
		for _, z := range model.Zones {
			if z.Side != side || z.Missing > 0 || z.Touch != nil || z.Cross != nil || z.Uncertain != "" || (side == "short" && z.Low <= price) || (side == "long" && z.High >= price) {
				continue
			}
			if best == nil || zoneDistance(z, price) < zoneDistance(*best, price) {
				v := z
				best = &v
			}
		}
		if best != nil {
			z := best
			s.Zones = append(s.Zones, ShortZone{ID: z.ID, Side: side, Low: z.Low, High: z.High, Quote: z.Quote, Strength: z.Strength, Relative: z.Relative, Distance: zoneDistance(*z, price) / price * 100, Revision: model.Revision, Available: model.Available, Fetched: model.Fetched, Reference: price})
		}
	}
	if len(s.Zones) > 0 {
		s.ZoneNote = "当时有效的最近未触及区域；模型强度·非美元，不作为目标价或概率"
	}
}

func (h *Hub) shortObservationView(now time.Time) any {
	var s ShortObservation
	if !h.Store.LoadState("short-flow/current", &s) {
		return nil
	}
	s.age(now)
	var g shortGap
	h.Store.LoadState("short-flow/gap", &g)
	s.ResearchPaused = g.Paused || h.Store.Status().ResearchPaused || h.Store.Status().Paused
	if s.ResearchPaused {
		s.ResearchReason = g.Reason
	}
	return s
}

func (h *Hub) shortFlowWorker(ctx context.Context) {
	// Independent of slow price collection, historical studies and mail delivery.
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	phase, successes := 0, 0
	for {
		now := time.Now().UTC()
		// Reserve 200ms of the two-second slice for persisting gap/recovery state.
		step, cancel := context.WithTimeout(ctx, 1800*time.Millisecond)
		var e error
		switch phase % 3 {
		case 0:
			e = h.shortObservationStep(step, now)
		case 1:
			e = h.shortBaselineStep(step, now)
		case 2:
			e = h.shortStudyStep(step, now)
		}
		cancel()
		if e != nil {
			successes = 0
			h.Store.shortGap(now, fmt.Errorf("短周期阶段%d: %w", phase%3, e))
		} else {
			successes++
			if successes >= 3 {
				h.Store.shortResume()
			}
		}
		phase++
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}
