package datahub

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

// Discovery, confirmed tracking, and result measurement have separate clocks.
type CostCase struct {
	AttentionClearByKind map[string]int `json:"attentionClearByKind"`
	FirstTrackingGap     string         `json:"firstTrackingGap"`
	InvalidatedAt        *time.Time     `json:"invalidatedAt"`
	ID                   string         `json:"id"`
	Rules                string         `json:"rulesVersion"`
	Group                string         `json:"group"`
	EpisodeID            string         `json:"episodeId"`
	Cycle                string         `json:"cycle"`
	Zone                 CostZone       `json:"zone"`
	Direction            string         `json:"direction"`
	Buffer               string         `json:"bufferPercent"`
	Boundary             string         `json:"boundary"`
	FrozenAt             time.Time      `json:"frozenAt"`
	From                 string         `json:"from"`
	Through              string         `json:"through"`
	LastDate             string         `json:"lastDate"`
	ConfirmedDate        string         `json:"confirmedDate"`
	TrackThrough         string         `json:"trackThrough"`
	State                string         `json:"state"`
	Quality              string         `json:"quality"`
	Count                int            `json:"count"`
	Revision             string         `json:"revision"`
	Method               string         `json:"method"`
	PriceSource          string         `json:"priceSource"`
	FirstPrice           string         `json:"firstPrice"`
	FirstDetectedAt      *time.Time     `json:"firstDetectedAt"`
	LastPrice            string         `json:"lastPrice"`
	InvalidatedDate      string         `json:"invalidatedDate"`
	AttentionState       string         `json:"attentionState"`
	AttentionClear       int            `json:"attentionClear"`
	AttentionAt          *time.Time     `json:"attentionAt"`
	FirstAttentionAt     *time.Time     `json:"firstAttentionAt"`
	Shadow               bool           `json:"shadow"`
	Legacy               bool           `json:"legacy"`
}

func costCaseActive(c CostCase) bool {
	return c.State == "watching" || c.State == "pending" || c.State == "confirmed"
}
func costBeyond(c CostCase, price string) bool {
	if c.Direction == "down" {
		return dec(price).LessThan(dec(c.Boundary))
	}
	return dec(price).GreaterThan(dec(c.Boundary))
}
func costDayAdd(day string, n int) string { d, _ := costDay(day); return costDate(d.AddDate(0, 0, n)) }
func costCaseEvent(c CostCase, kind string, p CostPrice, now time.Time) CostEvent {
	end := costClose(p.Date)
	frozen := c.FrozenAt
	var firstSeen *time.Time
	if !p.FirstSeen.IsZero() {
		firstSeen = &p.FirstSeen
	}
	return CostEvent{ID: costHash([]string{c.ID, kind, p.Date}), Kind: kind, Date: p.Date, DetectedAt: now, Rules: c.Rules, Revision: c.Revision, Price: p.Value, Cycle: c.Cycle, Zone: &c.Zone, CaseID: c.ID, EpisodeID: c.EpisodeID, Direction: c.Direction, Group: c.Group, OccurredAt: &end, FirstSeen: firstSeen, ValidatedAt: p.ValidatedAt, PriceRevision: p.Revision, FirstConditionPrice: c.FirstPrice, PriceSource: c.PriceSource, Shadow: c.Shadow, FrozenAt: &frozen, Note: costEventName(kind) + "；边界冻结，当前价格、辅助证据与历史事实分别展示"}
}
func costNewCases(s *costState, zones []CostZone, group, method, revision string, now time.Time) {
	cycle := costHash([]string{OnchainRules, group, now.Format(time.RFC3339Nano), revision})
	from := costDate(now)
	through := costDayAdd(from, 6)
	if group == "onchain" {
		s.Cycle = cycle
		s.FrozenAt = now
		s.From = from
		s.Through = through
		s.Revision = revision
	}
	if group == "price_only" {
		s.BaselineThrough = through
	}
	for _, z := range zones {
		dirs := []string{"up"}
		if z.Side == "below" {
			dirs = []string{"down"}
		}
		if z.Side == "inside" {
			dirs = []string{"up", "down"}
		}
		buffers := []string{"0"}
		if group == "onchain" {
			buffers = append(buffers, "0.5")
		}
		for _, direction := range dirs {
			for _, buffer := range buffers {
				boundary := dec(z.High).Mul(dec("1").Add(dec(buffer).Div(dec("100"))))
				if direction == "down" {
					boundary = dec(z.Low).Mul(dec("1").Sub(dec(buffer).Div(dec("100"))))
				}
				duplicate := false
				for _, c := range s.Cases {
					if costCaseActive(c) && !c.Legacy && c.Group == group && c.Method == method && c.Direction == direction && c.Buffer == buffer && c.Zone.Low == z.Low && c.Zone.High == z.High && c.PriceSource == onchainSource {
						duplicate = true
						break
					}
				}
				if duplicate {
					continue
				}
				id := costHash([]string{cycle, z.Low, z.High, direction, buffer})
				s.Cases = append(s.Cases, CostCase{ID: id, Rules: OnchainRules, Group: group, EpisodeID: id, Cycle: cycle, Zone: z, Direction: direction, Buffer: buffer, Boundary: boundary.String(), FrozenAt: now, From: from, Through: through, State: "watching", Quality: "waiting", Revision: revision, Method: method, PriceSource: onchainSource, Shadow: buffer != "0" || group != "onchain"})
			}
		}
	}
}

// Only valid, actually completed prices advance a case. Missing dates break the
// count, but never turn a former confirmation into a claim that it remains valid.
func costAdvanceCase(c *CostCase, p *CostPrice, expected string, now time.Time) []CostEvent {
	if !costCaseActive(*c) {
		return nil
	}
	c.Quality = "unknown"
	lastPendingMayConfirm := c.State == "pending" && c.Count == 1 && c.LastDate == c.Through && expected == costDayAdd(c.Through, 1)
	if (c.State == "confirmed" && c.TrackThrough != "" && expected > c.TrackThrough) || (c.State != "confirmed" && expected > c.Through && !lastPendingMayConfirm) {
		kind := "discovery_expired"
		if c.State == "confirmed" {
			kind = "tracking_expired"
		} else if c.State == "pending" {
			kind = "pending_expired"
		}
		c.State = kind
		observed := CostPrice{Date: expected}
		if p != nil {
			observed = *p
		}
		return []CostEvent{costCaseEvent(*c, kind, observed, now)}
	}
	if p == nil || p.Date != expected || !costClose(p.Date).After(c.FrozenAt) || c.LastDate >= p.Date {
		if p != nil && c.LastDate == p.Date {
			c.Quality = "available"
		}
		if p == nil && c.State == "pending" && !now.Before(costClose(expected).Add(6*time.Hour)) {
			c.Count = 0
		}
		return nil
	}
	c.Quality = "available"
	consecutive := costDayAdd(c.LastDate, 1) == p.Date
	if c.State == "confirmed" && !consecutive && c.LastDate != "" && c.FirstTrackingGap == "" {
		c.FirstTrackingGap = costDayAdd(c.LastDate, 1)
	}
	if !consecutive {
		c.Count = 0
	}
	c.LastDate = p.Date
	c.LastPrice = p.Value
	event := func(kind string) []CostEvent { return []CostEvent{costCaseEvent(*c, kind, *p, now)} }
	if c.State == "confirmed" {
		if c.TrackThrough != "" && p.Date > c.TrackThrough {
			c.State = "tracking_expired"
			return event(c.State)
		}
		if !costBeyond(*c, p.Value) {
			c.State = "invalidated"
			c.InvalidatedDate = p.Date
			c.InvalidatedAt = &now
			return event(c.State)
		}
		return nil
	}
	if p.Date < c.From {
		return nil
	}
	if p.Date > c.Through && !(c.State == "pending" && c.Count == 1 && consecutive && p.Date == costDayAdd(c.Through, 1)) {
		kind := "discovery_expired"
		if c.State == "pending" {
			kind = "pending_expired"
		}
		c.State = kind
		return event(kind)
	}
	if costBeyond(*c, p.Value) {
		c.Count++
		if c.Count >= 2 && consecutive {
			c.State = "confirmed"
			c.ConfirmedDate = p.Date
			c.TrackThrough = costDayAdd(p.Date, 60)
			return event("confirmed")
		}
		c.State = "pending"
		if c.FirstDetectedAt == nil {
			t := now
			c.FirstDetectedAt = &t
			c.FirstPrice = p.Value
		}
		return event("pending")
	}
	wasPending := c.State == "pending"
	c.Count = 0
	c.State = "watching"
	if p.Date > c.Through {
		c.State = "pending_expired"
		return event(c.State)
	}
	if wasPending {
		return event("unconfirmed")
	}
	return nil
}
func costPriceChannel(day string, prices map[string]string) []CostZone {
	lo, hi := "", ""
	for i := 0; i < 20; i++ {
		v, ok := prices[costDayAdd(day, -i)]
		if !ok || !dec(v).IsPositive() {
			return nil
		}
		if lo == "" || dec(v).LessThan(dec(lo)) {
			lo = v
		}
		if hi == "" || dec(v).GreaterThan(dec(hi)) {
			hi = v
		}
	}
	if lo == hi {
		return nil
	}
	return []CostZone{{Side: "inside", Low: lo, High: hi, Supply: "0"}}
}
func costAdvanceV2(s *costState, f *CostFrame, m *CostMetrics, p *CostPrice, prices map[string]string, now time.Time, structure bool) []CostEvent {
	events := []CostEvent{}
	day := costDate(now.AddDate(0, 0, -1))
	initial := s.Rules != OnchainRules
	if initial {
		s.Rules = OnchainRules
		s.EnabledAt = now
		s.Meet = 0
		s.Clear = 0
		s.Compression = false
		s.StructureLastDate = ""
		s.LastDate = ""
		s.LowLastDate = ""
		s.LowMeet = 0
		s.LowClear = 0
		s.LowActive = false
		s.Through = ""
		s.BaselineThrough = ""
	}
	for i := range s.Cases {
		events = append(events, costAdvanceCase(&s.Cases[i], p, day, now)...)
	}
	structureInitial := s.Through == ""
	if structure && f != nil && m != nil && (s.Through == "" || day > s.Through) {
		costNewCases(s, m.Zones, "onchain", f.Method, f.Revision, now)
		if structureInitial {
			t := costClose(f.Date)
			events = append(events, CostEvent{ID: costHash([]string{OnchainRules, "initialized", s.Cycle}), Kind: "initialized", Date: f.Date, DetectedAt: now, Rules: OnchainRules, Revision: f.Revision, Price: f.Price, Cycle: s.Cycle, OccurredAt: &t, Note: "新版从实际启用时冻结；下一次未来日收盘起观察，旧事件和邮件开关保持"})
		}
	}
	if p != nil && (s.BaselineThrough == "" || day > s.BaselineThrough) {
		if z := costPriceChannel(day, prices); len(z) > 0 {
			costNewCases(s, z, "price_only", "20-completed-daily-close-channel", p.Revision, now)
		}
	}
	if structure && f != nil && m != nil && f.Date == day && s.StructureLastDate != day {
		consecutive := costDayAdd(s.StructureLastDate, 1) == day
		if !consecutive {
			s.Meet = 0
			s.Clear = 0
		}
		b := costCompressed(*m)
		if b == nil {
			s.Meet = 0
			s.Clear = 0
		} else if *b {
			s.Clear = 0
			s.Meet++
			if structureInitial {
				s.Compression = true
				s.EpisodeID = costHash([]string{OnchainRules, "initial-structure", now.Format(time.RFC3339Nano)})
			} else if s.Meet >= 2 && consecutive && !s.Compression {
				s.Compression = true
				s.EpisodeID = costHash([]string{OnchainRules, "structure", day, now.Format(time.RFC3339Nano)})
				t := costClose(day)
				events = append(events, CostEvent{ID: s.EpisodeID, EpisodeID: s.EpisodeID, Kind: "concentrated", Group: "structure", Date: day, DetectedAt: now, Rules: OnchainRules, Revision: f.Revision, Price: f.Price, Cycle: s.Cycle, Metrics: m, OccurredAt: &t, FirstSeen: &f.FirstSeen, ValidatedAt: f.ValidatedAt, PriceSource: onchainSource, Note: "连续两个UTC日确定高集中且低波动；方向未确认"})
			}
		} else {
			s.Meet = 0
			s.Clear++
			if s.Clear >= 2 {
				s.Compression = false
			}
		}
		s.StructureLastDate = day
	} else if !structure && s.StructureLastDate != day && !now.Before(costClose(day).Add(6*time.Hour)) {
		s.Meet = 0
		s.Clear = 0
	}
	if p != nil {
		s.LastDate = day
	}
	s.Initial = initial
	s.Watches = nil
	for _, c := range s.Cases {
		if costCaseActive(c) && !c.Shadow {
			s.Watches = append(s.Watches, costWatch{Zone: c.Zone, State: c.State, Count: c.Count})
		}
	}
	return events
}
func (h *Hub) costMigrateCases(ctx context.Context, state *costState) error {
	if state.Rules == OnchainRules {
		return nil
	}
	for _, w := range state.Watches {
		if w.State != "confirmed" {
			continue
		}
		rows, e := h.Store.onchain.db.QueryContext(ctx, "SELECT payload FROM events WHERE kind='confirmed' ORDER BY detected DESC LIMIT 200")
		if e != nil {
			return e
		}
		var found *CostEvent
		for rows.Next() {
			var raw []byte
			var ev CostEvent
			if e = rows.Scan(&raw); e != nil {
				break
			}
			if e = json.Unmarshal(raw, &ev); e != nil {
				break
			}
			if ev.Cycle == state.Cycle && ev.Zone != nil && ev.Zone.Side == w.Zone.Side {
				found = &ev
				break
			}
		}
		re := rows.Err()
		rows.Close()
		if e != nil {
			return e
		}
		if re != nil {
			return re
		}
		if found == nil {
			continue
		}
		dir, boundary := "up", w.Zone.High
		if w.Zone.Side == "below" {
			dir, boundary = "down", w.Zone.Low
		}
		state.Cases = append(state.Cases, CostCase{ID: "legacy-" + found.ID, Rules: found.Rules, Group: "onchain", EpisodeID: found.ID, Cycle: found.Cycle, Zone: w.Zone, Direction: dir, Buffer: "0", Boundary: boundary, FrozenAt: state.FrozenAt, From: state.From, Through: state.Through, LastDate: state.LastDate, ConfirmedDate: found.Date, TrackThrough: costDayAdd(found.Date, 60), State: "confirmed", Revision: found.Revision, Method: onchainMethod, PriceSource: onchainSource, FirstPrice: found.Price, FirstDetectedAt: &found.DetectedAt, Legacy: true})
	}
	return nil
}
func (h *Hub) evaluateCostDay(ctx context.Context, now time.Time, _ bool) error {
	s := h.Store.onchain
	if e := s.writable(); e != nil {
		return e
	}
	if h.onchainEventsDisabled || h.onchainDisabled {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// Serialize case publication with send-time validation; lock order matches settings.
	h.noticeMu.Lock()
	defer h.noticeMu.Unlock()
	var state costState
	if e := costLoad(ctx, s.db, "observation", &state); e != nil {
		return e
	}
	before, _ := json.Marshal(state)
	if e := h.costMigrateCases(ctx, &state); e != nil {
		return e
	}
	f, e := s.frame(ctx, "", now)
	if e != nil {
		return e
	}
	day := costDate(now.AddDate(0, 0, -1))
	p, e := s.price(ctx, day, now)
	if e != nil {
		return e
	}
	feed, e := h.costFeed(ctx)
	if e != nil {
		return e
	}
	status, _ := costFeedStatus(feed, now)
	frameRevision, priceRevision := "", ""
	if f != nil {
		frameRevision = f.Revision
	}
	if p != nil {
		priceRevision = p.Revision
	}
	inputKey := costHash([]string{day, frameRevision, priceRevision, status, func() string {
		if now.Before(costClose(day).Add(6 * time.Hour)) {
			return "publication_grace"
		}
		return "after_grace"
	}()})
	if state.Rules == OnchainRules && state.InputKey == inputKey {
		return nil
	}
	state.InputKey = inputKey
	prices, e := s.prices(ctx, now)
	if e != nil {
		return e
	}
	var m *CostMetrics
	structure := status == "fresh" && f != nil && f.Date == day && feed.CostError == ""
	if structure {
		v, err := s.metrics(ctx, *f, now)
		if err != nil {
			return err
		}
		m = &v
	}
	events := costAdvanceV2(&state, f, m, p, prices, now, structure)
	// Daily controls share the same actually available decision clock.
	trials, daily := costDailyResearch(&state, f, m, p, prices, day, now, structure)
	var settings CostSettings
	if e = costLoad(ctx, s.db, "settings", &settings); e != nil {
		return e
	}
	needsEvidence := false
	for _, event := range events {
		if !event.Shadow && event.Kind != "initialized" {
			needsEvidence = true
		}
	}
	evidence := []CostEvidence{}
	if needsEvidence {
		evidence = h.costEvidence(ctx, day, now)
	}
	for i := range events {
		if !events[i].Shadow {
			events[i].Evidence = evidence
		}
		if events[i].Metrics == nil {
			if m != nil {
				events[i].Metrics = m
			} else {
				events[i].Metrics = &CostMetrics{Volatility: costVolatility(day, prices)}
			}
		}
		h.decorateCostEvent(&events[i], now)
	}
	after, _ := json.Marshal(state)
	if string(before) == string(after) && len(events) == 0 && len(trials) == 0 {
		return h.costSaveDaily(ctx, daily)
	}
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	if e = costSave(ctx, tx, "observation", state); e != nil {
		return e
	}
	for _, c := range state.Cases {
		raw, _ := json.Marshal(c)
		if _, e = tx.ExecContext(ctx, "INSERT INTO cases VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET updated=excluded.updated,payload=excluded.payload", c.ID, now.UnixNano(), raw); e != nil {
			return e
		}
	}
	for _, event := range events {
		send := settings.EmailEnabled && !event.Shadow && (event.Kind == "confirmed" || event.Kind == "invalidated" || event.Kind == "concentrated")
		if e = costInsertEvent(ctx, tx, event, send); e != nil {
			return e
		}
		if event.Kind == "confirmed" || event.Kind == "concentrated" {
			trials = append(trials, costTrialFromEvent(event, state))
		}
	}
	for _, t := range trials {
		raw, _ := json.Marshal(t)
		if _, e = tx.ExecContext(ctx, "INSERT OR IGNORE INTO trials VALUES(?,?,?,?)", t.ID, t.DetectedAt.UnixNano(), t.Group, raw); e != nil {
			return e
		}
	}
	if e = costInsertDaily(ctx, tx, daily); e != nil {
		return e
	}
	// Archived cases remain in the cases/event tables; bound active state size.
	active := state.Cases[:0]
	for _, c := range state.Cases {
		if costCaseActive(c) {
			active = append(active, c)
		}
	}
	state.Cases = active
	if len(state.Cases) > 128 {
		return errors.New("链上活跃情景超过保护上限")
	}
	if e = costSave(ctx, tx, "observation", state); e != nil {
		return e
	}
	if e = s.commit(ctx, tx); e != nil {
		return e
	}
	s.epoch.Add(1)
	return nil
}
func (h *Hub) decorateCostEvent(event *CostEvent, now time.Time) {
	p, at, ok := h.CurrentPrice("BTC", now)
	if !ok {
		return
	}
	v := fmtCostFloat(p)
	event.MarketPrice = &v
	event.MarketPriceAt = at
	if event.DiscoveredMarketPrice == nil && event.DetectedAt.Equal(now) {
		event.DiscoveredMarketPrice = &v
	}
	if event.Zone != nil {
		boundary := event.Zone.High
		if event.Direction == "down" || event.Zone.Side == "below" {
			boundary = event.Zone.Low
		}
		if dec(boundary).IsPositive() {
			d := dec(v).Div(dec(boundary)).Sub(dec("1")).Mul(dec("100")).String()
			event.BoundaryDistance = &d
		}
	}
	if event.DiscoveredMarketPrice != nil && dec(*event.DiscoveredMarketPrice).IsPositive() {
		d := dec(v).Div(dec(*event.DiscoveredMarketPrice)).Sub(dec("1")).Mul(dec("100")).String()
		event.DecisionDistance = &d
	}
	reference := event.FirstConditionPrice
	if reference == "" {
		reference = event.Price
	}
	if dec(reference).IsPositive() {
		d := dec(v).Div(dec(reference)).Sub(dec("1")).Mul(dec("100")).String()
		event.DiscoveryDistance = &d
	}
}
func (s *costStore) caseByID(ctx context.Context, id string) (*CostCase, error) {
	var b []byte
	e := s.db.QueryRowContext(ctx, "SELECT payload FROM cases WHERE id=?", id).Scan(&b)
	if e == sql.ErrNoRows {
		return nil, nil
	}
	if e != nil {
		return nil, e
	}
	var c CostCase
	e = json.Unmarshal(b, &c)
	return &c, e
}
