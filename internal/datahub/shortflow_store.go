package datahub

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"path/filepath"
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
	if err != nil {
		return err
	}
	// Keep the bounded reader available even if a full research budget prevents
	// creation of new diagnostic metadata; live amounts still need isolation.
	_, pipelineErr := w.research.Exec("INSERT OR IGNORE INTO sf_records VALUES('pipeline-origin',?,?,?)", ShortPipeline, now.Unix(), b)
	var runtime ShortRuntime
	if w.LoadState("short-flow/runtime-v2", &runtime) && runtime.Version == ShortPipeline {
		w.shortRuntime = &runtime
	}
	// Long formal-history readers occupy the original single-connection pool.
	// A dedicated, bounded WAL connection isolates short work without changing
	// existing collectors/readers, the database file, or its page ceiling.
	u := url.URL{Scheme: "file", Path: filepath.Join(w.root, "research.sqlite")}
	u.RawQuery = "mode=rw&_pragma=busy_timeout(1500)&_pragma=cache_size(-256)&_pragma=max_page_count(126976)&_pragma=synchronous(NORMAL)"
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err = db.Ping(); err != nil {
		db.Close()
		return err
	}
	w.shortResearch = db
	return pipelineErr
}

func (w *Warehouse) shortLoad(ctx context.Context, kind, id string, v any) error {
	var b []byte
	if e := w.shortDB().QueryRowContext(ctx, "SELECT payload FROM sf_records WHERE kind=? AND id=?", kind, id).Scan(&b); e != nil {
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
	return shortWriteError(e)
}
func (w *Warehouse) shortState(ctx context.Context, key string, v any) bool {
	return w.shortStateResult(ctx, key, v) == nil
}
func (w *Warehouse) shortStateResult(ctx context.Context, key string, v any) error {
	var b []byte
	if e := w.db.QueryRowContext(ctx, "SELECT payload FROM state WHERE key=?", key).Scan(&b); e != nil {
		return e
	}
	if len(b) > shortFlowRowLimit {
		return errors.New("短周期状态读取超过上限")
	}
	return json.Unmarshal(b, v)
}
func (w *Warehouse) shortPut(ctx context.Context, kind, id string, at time.Time, v any) error {
	defer shortMeasure(ctx, "checkpoint_write", time.Now())
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
	_, e = w.shortDB().ExecContext(ctx, "INSERT INTO sf_records VALUES(?,?,?,?) ON CONFLICT(kind,id) DO UPDATE SET at=excluded.at,payload=excluded.payload", kind, id, at.Unix(), b)
	return shortWriteError(e)
}

type shortGap struct {
	At     time.Time `json:"at"`
	Reason string    `json:"reason"`
	Paused bool      `json:"paused"`
	Count  int       `json:"count"`
}

func (w *Warehouse) shortGap(now time.Time, e error, pause ...bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	var g shortGap
	var raw []byte
	err := w.db.QueryRowContext(ctx, "SELECT payload FROM state WHERE key='short-flow/gap'").Scan(&raw)
	if err != nil && err != sql.ErrNoRows {
		return
	}
	if err == nil {
		if json.Unmarshal(raw, &g) != nil {
			return
		}
	}
	wasPaused := g.Paused
	g.At, g.Reason, g.Paused = now, e.Error(), true
	if len(pause) > 0 {
		g.Paused = wasPaused || pause[0]
	}
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
	From    time.Time        `json:"from"`
	To      time.Time        `json:"to"`
	Cursor  time.Time        `json:"cursor"`
	AsOf    time.Time        `json:"asOf"`
	Version string           `json:"version"`
	Bars    []shortStoredBar `json:"bars"`
}

// Compact integer checkpoint: UNIX seconds, buy cents, sell cents. This avoids
// repeated timestamp strings and field names for 8640 bars without precision loss.
type shortStoredBar [3]int64

func storeShortBar(b FlowBar) shortStoredBar { return shortStoredBar{b.At.Unix(), b.Buy, b.Sell} }
func (b shortStoredBar) flow() FlowBar {
	return FlowBar{At: time.Unix(b[0], 0).UTC(), Buy: b[1], Sell: b[2]}
}

// Prepare the baseline from the clock, independently of the shared hub state
// connection. The old entry point remains for bounded tests and callers.
func (h *Hub) shortBaselineStep(ctx context.Context, now time.Time) error {
	return h.shortBaselineAt(ctx, now.Add(time.Minute).Truncate(time.Hour), now)
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
	unknown := map[int64]bool{}
	e := factsAsOf(ctx, h.Store.shortDB(), id, end.Add(-4*time.Hour), end, now, func(o Observation) error {
		if !shortClosedFact(o) {
			return nil
		}
		acc.add(o)
		at := recordTime(o).Truncate(5 * time.Minute).Unix()
		if o.FirstFetchedAt == nil {
			unknown[at] = true
		} else {
			available[at] = maxTime(available[at], *o.FirstFetchedAt)
		}
		return nil
	})
	bars := acc.finish()
	for at := range unknown {
		delete(available, at)
	}
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
	inputStart := time.Now()
	bars, available, end, e := h.shortInput(ctx, now)
	shortMeasure(ctx, "flow_read", inputStart)
	if e != nil {
		return e
	}
	var baseline ShortBaseline
	optional, cancelOptional := context.WithTimeout(ctx, 300*time.Millisecond)
	baselineErr := h.Store.shortLoad(optional, "baseline-v2", end.Truncate(time.Hour).Add(-time.Hour).Format(time.RFC3339), &baseline)
	if baselineErr == sql.ErrNoRows {
		baselineErr = h.Store.shortLoad(optional, "baseline", ShortFlowRules, &baseline)
	}
	cancelOptional()
	priceStart := time.Now()
	optional, cancelOptional = context.WithTimeout(ctx, 200*time.Millisecond)
	candles, ce := liquidationCandleSeries(optional, h.Store.shortDB(), "BTC", end.Add(-16*time.Hour), end, now, false)
	cancelOptional()
	shortMeasure(ctx, "price_read", priceStart)
	if ce != nil {
		candles = map[int64]Candle{}
	}
	s := buildShortObservation(bars, candles, end, now, baseline)
	s.Pipeline = ShortPipeline
	for at, v := range available {
		if at >= end.Add(-4*time.Hour).Unix() && at < end.Unix() && (s.InputAvailable == nil || v.After(*s.InputAvailable)) {
			s.InputAvailable = flowPtr(v)
		}
	}
	for at := range bars {
		if at >= end.Add(-4*time.Hour).Unix() && at < end.Unix() && available[at].IsZero() {
			s.InputAvailable = nil
			break
		}
	}
	if t := available[end.Add(-5*time.Minute).Unix()]; !t.IsZero() {
		s.Available = &t
		s.Delay = flowPtr(math.Max(0, now.Sub(t).Seconds()))
	}
	var formal FlowSnapshot
	optional, cancelOptional = context.WithTimeout(ctx, 100*time.Millisecond)
	var gap shortGap
	gapErr := h.Store.shortStateResult(optional, "short-flow/gap", &gap)
	if h.Store.shortState(optional, "signals/current/BTC", &formal) && !formal.At.After(now) && now.Sub(formal.DataThrough) <= 12*time.Minute {
		s.Background = &formal.Context
		s.BackgroundAt = &formal.At
		s.BackgroundThrough = &formal.DataThrough
	}
	h.shortZones(optional, &s, now)
	cancelOptional()
	s.ResearchPaused = gap.Paused || h.Store.Status().ResearchPaused || h.Store.Status().Paused
	if s.ResearchPaused {
		s.ResearchReason = gap.Reason
	}
	if h.Store.Status().ResearchPaused || h.Store.Status().Paused {
		s.ResearchReason = "研究总容量保护"
	}
	if gapErr != nil && gapErr != sql.ErrNoRows {
		s.ResearchPaused = true
		s.ResearchReason = "研究暂停状态暂不可核验"
	}
	s.At = now.Add(time.Since(inputStart))
	s.Refreshed = flowPtr(s.At)
	publication, pubErr := h.shortPublication(ctx, &s, s.At)
	// Current observations remain available when research storage is full.
	persistStart := time.Now()
	if e = h.Store.shortSaveState(ctx, "short-flow/current", s); e != nil {
		return e
	}
	shortMeasure(ctx, "current_write", persistStart)
	if pubErr != nil {
		return pubErr
	}
	if publication != nil {
		if e := h.Store.shortPut(ctx, "publication", s.InputVersion, s.Through, publication); e != nil {
			return e
		}
	}
	if baselineErr != nil && baselineErr != sql.ErrNoRows {
		return baselineErr
	}
	if gapErr != nil && gapErr != sql.ErrNoRows {
		return gapErr
	}
	return h.recordShortObservation(ctx, s, now)
}

func (h *Hub) shortZones(ctx context.Context, s *ShortObservation, now time.Time) {
	var model LiquidationMapSnapshot
	if liquidationLoad(ctx, h.Store.shortDB(), "state", ID("map", "BTC", "", "futures"), &model) != nil || !model.Complete || model.Available.After(now) || now.Sub(model.Fetched) > 35*time.Minute || model.Quote != "USDT" {
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
	s.Diagnostics = h.Store.shortRuntimeView()
	var g shortGap
	h.Store.LoadState("short-flow/gap", &g)
	s.ResearchPaused = g.Paused || h.Store.Status().ResearchPaused || h.Store.Status().Paused
	if s.ResearchPaused {
		s.ResearchReason = g.Reason
	}
	if h.Store.Status().ResearchPaused || h.Store.Status().Paused {
		s.ResearchReason = "研究总容量保护"
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
		// Reserve 350ms of the two-second slice for diagnostics and recovery state.
		step, cancel := context.WithTimeout(ctx, 1650*time.Millisecond)
		trace := &shortTrace{Operations: map[string]time.Duration{}}
		step = context.WithValue(step, shortTraceKey{}, trace)
		started := time.Now()
		hubWait := h.Store.db.Stats().WaitDuration
		researchWait := h.Store.shortDB().Stats().WaitDuration
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
		if ctx.Err() != nil {
			return
		}
		trace.Operations["hub_pool_wait"] = h.Store.db.Stats().WaitDuration - hubWait
		trace.Operations["research_pool_wait"] = h.Store.shortDB().Stats().WaitDuration - researchWait
		h.Store.recordShortRuntime([]string{"observation", "baseline", "study"}[phase%3], now, time.Since(started), trace, e)
		if e != nil {
			successes = 0
			class := shortErrorClass(e)
			h.Store.shortGap(now, fmt.Errorf("短周期阶段%d: %w", phase%3, e), class == "write" || class == "capacity")
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

// Fall back only if optional initialization failed; bounded contexts still
// preserve the original service and surface the research gap.
func (w *Warehouse) shortDB() *sql.DB {
	if w.shortResearch != nil {
		return w.shortResearch
	}
	return w.research
}
func (w *Warehouse) shortRangeVersion(ctx context.Context, id string, from, to time.Time) (string, error) {
	defer shortMeasure(ctx, "input_version", time.Now())
	var n, last int64
	e := w.shortDB().QueryRowContext(ctx, "SELECT count(*),coalesce(max(available),0) FROM facts WHERE dataset=? AND ts>=? AND ts<?", id, from.Unix(), to.Unix()).Scan(&n, &last)
	return fmt.Sprintf("%d/%d", n, last), e
}
