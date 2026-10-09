package datahub

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"
)

type bookFlowRuntime struct {
	At         *time.Time           `json:"lastAttemptAt"`
	Success    *time.Time           `json:"lastSuccessAt"`
	FailureAt  *time.Time           `json:"lastFailureAt"`
	Failure    string               `json:"lastFailure,omitempty"`
	Failures   int                  `json:"failures"`
	Registered bool                 `json:"registered"`
	Skipped    map[string]time.Time `json:"-"`
}
type bookFlowDay struct {
	Samples    int       `json:"observedMinutes"`
	Minute     time.Time `json:"through"`
	Timely     int       `json:"timelyMinutes"`
	BookTimely int       `json:"bookTimelyMinutes"`
}
type bookFlowState struct {
	Coverage        map[int64]uint8        `json:"pendingCoverage"`
	CoverageThrough time.Time              `json:"coverageThrough"`
	Parameters      json.RawMessage        `json:"parameters"`
	Origin          time.Time              `json:"origin"`
	Last            time.Time              `json:"lastProcessedAt"`
	History         map[string][]bookPoint `json:"history"`
	Current         map[string]bookPoint   `json:"current"`
	Blocked         []bookEvent            `json:"blocked"`
	FootThrough     map[string]time.Time   `json:"footThrough"`
	Days            map[string]bookFlowDay `json:"days"`
}

func (h *Hub) bookRate(now time.Time) *string {
	v, _, ok := h.Rate("USDT", now)
	if ok && dec(v).IsPositive() {
		return &v
	}
	return nil
}
func compactBookPoint(p bookPoint) bookPoint {
	p.Zones = append([]bookZone(nil), p.Zones...)
	for i := range p.Zones {
		p.Zones[i].Levels = nil
		p.Zones[i].Baseline = nil
	}
	return p
}
func (h *Hub) bookFlowStep(ctx context.Context, now time.Time) error {
	if h.Store.bookFlowError != "" {
		return errors.New(h.Store.bookFlowError)
	}
	if h.bookFlowMode != "run" {
		return nil
	}
	s := bookFlowState{}
	err := h.Store.bookFlowLoad(ctx, "state", BookFlowRules, &s)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	if s.Origin.IsZero() {
		s.Parameters = bookFlowParameters()
		s.Origin = now
		s.History = map[string][]bookPoint{}
		s.Current = map[string]bookPoint{}
		s.FootThrough = map[string]time.Time{}
		s.Days = map[string]bookFlowDay{}
	}
	if !bytes.Equal(s.Parameters, bookFlowParameters()) {
		return errors.New("挂单承接实验参数不一致，须创建新规则版本与起点")
	}
	if s.Coverage == nil {
		s.Coverage = map[int64]uint8{}
	}
	for i := range s.Blocked {
		var clock time.Time
		e := h.Store.bookFlowLoad(ctx, "clock", s.Blocked[i].ID+"/enhanced", &clock)
		if e != nil && e != sql.ErrNoRows {
			return e
		}
		if e == nil {
			s.Blocked[i].At = clock
			s.Blocked[i].Expires = clock.Add(30 * time.Minute)
		}
	}
	h.mu.RLock()
	skip := map[string]time.Time{}
	for k, v := range h.bookFlowRuntime.Skipped {
		skip[k] = v
	}
	h.mu.RUnlock()
	updates := map[string]bookEvent{}
	changed := s.Last.IsZero() || !s.Last.Truncate(time.Minute).Equal(now.Truncate(time.Minute))
	rate := h.bookRate(now)
	for _, venue := range []string{"Binance", "OKX"} {
		bookBit, footBit := uint8(1), uint8(4)
		if venue == "OKX" {
			bookBit, footBit = 2, 8
		}
		d, _ := h.Dataset(ID("book", "BTC", venue, "spot"))
		o, exists := h.Store.Latest(d.ID)
		fresh := exists && o.Quality == "valid" && !o.Time().After(now) && now.Sub(o.Time()) <= 3*time.Minute && !o.FetchedAt.After(now) && now.Sub(o.FetchedAt) <= 3*time.Minute
		p := buildBookPoint(o, rate, now)
		fresh = fresh && p.Reference != nil && rate != nil
		previous := s.Current[venue]
		newPoint := fresh && p.At.After(previous.At) && p.At.After(skip[venue]) && !p.At.Before(h.boot) && !p.At.Before(s.Origin)
		gap := !s.Last.IsZero() && (now.Sub(s.Last) > 3*time.Minute || h.boot.After(s.Last)) || !fresh
		if newPoint && !previous.At.IsZero() && p.At.Sub(previous.At) > 90*time.Second {
			gap = true
		}
		for i := range s.Blocked {
			e := &s.Blocked[i]
			if e.Venue != venue || e.Rearmed {
				continue
			}
			if gap {
				e.ClearSince = nil
				e.BadPrice = 0
				if !e.Ended {
					appendBookUpdate(e, "data_gap", now, p.At, nil)
					e.CompletePath = false
					e.Ended = true
					updates[e.ID] = *e
					changed = true
				}
			}
			if !now.Before(e.Expires) && !e.Ended {
				appendBookUpdate(e, "expired", now, p.At, nil)
				e.Ended = true
				updates[e.ID] = *e
				changed = true
			}
		}
		if newPoint {
			minute := p.At.Truncate(time.Minute)
			if minute.After(s.CoverageThrough) && now.Sub(minute) <= 3*time.Minute {
				if s.Coverage[minute.Unix()]&bookBit == 0 {
					changed = true
				}
				s.Coverage[minute.Unix()] |= bookBit
			}
			changed = true
			for i := range p.Zones {
				p.Zones[i] = evaluateBookZone(p.Zones[i], p, s.History[venue], s.Origin)
			}
			for i := range s.Blocked {
				e := &s.Blocked[i]
				if e.Venue != venue || e.Rearmed {
					continue
				}
				z := zoneIn(p, e.Zone)
				// A valid reference outside the frozen zone's search range is
				// known ineligible, without treating absent depth as zero.
				outside := p.Reference != nil && (dec(e.Zone.High).LessThan(dec(*p.Reference).Mul(dec("0.99"))) || dec(e.Zone.Low).GreaterThan(dec(*p.Reference).Mul(dec("1.01"))))
				if !outside && (z == nil || z.Quantity == nil || z.Multiple == nil) {
					e.ClearSince = nil
				} else if outside || !z.Enhanced {
					if e.ClearSince == nil {
						e.ClearSince = flowPtr(p.At)
					}
					if p.At.Sub(*e.ClearSince) >= 10*time.Minute {
						e.Rearmed = true
					}
				} else {
					e.ClearSince = nil
				}
				if e.Ended {
					continue
				}
				if z == nil || z.Quantity == nil {
					appendBookUpdate(e, "evidence_unknown", now, p.At, nil)
					e.CompletePath = false
				} else {
					if dec(*z.Quantity).LessThanOrEqual(dec(*e.Zone.Quantity).Mul(dec("0.5"))) {
						appendBookUpdate(e, "faded", now, p.At, nil)
						e.Ended = true
					}
					if z.Enhanced && p.At.Sub(e.First.At) >= time.Minute && p.Fetched.After(e.LastFetched) {
						appendBookUpdate(e, "persistent", now, p.At, nil)
					}
				}
				adverse := p.Reference != nil && ((e.Direction == "buy" && dec(*p.Reference).LessThan(dec(e.Zone.Low))) || (e.Direction == "sell" && dec(*p.Reference).GreaterThan(dec(e.Zone.High))))
				if adverse && p.Fetched.After(e.LastFetched) {
					e.BadPrice++
				} else {
					e.BadPrice = 0
				}
				if e.BadPrice >= 2 {
					appendBookUpdate(e, "broken", now, p.At, nil)
					e.Ended = true
				}
				e.LastSeen, e.LastFetched = p.At, p.Fetched
				updates[e.ID] = *e
			}
			candidates := append([]bookZone(nil), p.Zones...)
			sort.Slice(candidates, func(i, j int) bool {
				a, b := candidates[i], candidates[j]
				if a.IncreaseUSD != nil && b.IncreaseUSD != nil && *a.IncreaseUSD != *b.IncreaseUSD {
					return dec(*a.IncreaseUSD).GreaterThan(dec(*b.IncreaseUSD))
				}
				if a.Low == b.Low {
					return a.Side < b.Side
				}
				return dec(a.Low).LessThan(dec(b.Low))
			})
			for _, z := range candidates {
				if !z.Enhanced {
					continue
				}
				blocked := false
				for _, e := range s.Blocked {
					if !e.Rearmed && e.Venue == venue && overlaps(e.Zone, z) {
						blocked = true
						break
					}
				}
				if blocked {
					continue
				}
				computed := now
				if !h.offline {
					computed = time.Now().UTC()
				}
				e := newBookEvent(venue, p, z, computed)
				s.Blocked = append(s.Blocked, e)
				updates[e.ID] = e
			}
			// Persist only compact baseline measurements, not full books.
			s.Current[venue] = compactBookPoint(p)
			history := []bookPoint{}
			for _, old := range s.History[venue] {
				if !old.At.Before(now.Add(-40 * time.Minute)) {
					history = append(history, old)
				}
			}
			s.History[venue] = append(history, compactBookPoint(p))
		}
		md, _ := h.Dataset(minuteFootID(venue))
		latest, ok := h.Store.Latest(md.ID)
		var contract minuteContract
		ce := h.Store.bookFlowLoad(ctx, "contract", md.ID, &contract)
		if ce != nil && ce != sql.ErrNoRows {
			return ce
		}
		footOK := ok && contract.VerifiedAt != nil && latest.Quality == "valid" && !latest.Time().Add(time.Minute).After(now) && now.Sub(latest.Time().Add(time.Minute)) <= 3*time.Minute && now.Sub(latest.FetchedAt) <= 3*time.Minute
		if !footOK {
			for i := range s.Blocked {
				e := &s.Blocked[i]
				if e.Venue == venue && !e.Ended && e.CompletePath && now.Sub(e.At) > 3*time.Minute {
					e.CompletePath = false
					appendBookUpdate(e, "foot_gap", now, latest.Time(), nil)
					updates[e.ID] = *e
					changed = true
				}
			}
		}
		if footOK {
			to := latest.Time().Add(time.Minute)
			rows := []Observation{}
			if err = h.Store.Visit(ctx, md, 60, to.Add(-5*time.Minute), to, func(o Observation) error { rows = append(rows, o); return nil }); err != nil {
				return err
			}
			for _, o := range rows {
				at := o.Time()
				end := at.Add(time.Minute)
				if at.After(s.CoverageThrough) && !at.Before(s.Origin) && o.Resolution == 60 && o.Quality == "valid" && !end.After(now) && now.Sub(end) <= 3*time.Minute && !o.FetchedAt.After(now) && o.FetchedAt.Sub(end) <= 3*time.Minute {
					if s.Coverage[at.Unix()]&footBit == 0 {
						changed = true
					}
					s.Coverage[at.Unix()] |= footBit
				}
			}
			for i := range s.Blocked {
				e := &s.Blocked[i]
				if e.Venue != venue || e.Ended || !to.After(s.FootThrough[e.ID]) {
					continue
				}
				if e.CompletePath && bookFootPathGap(*e, rows, to, now) {
					e.CompletePath = false
					appendBookUpdate(e, "foot_gap", now, to, nil)
					updates[e.ID] = *e
					changed = true
				}
				v := evaluateBookFoot(*e, rows, s.History[venue], rate, now)
				if v == nil {
					continue
				}
				if v.Absorption && !hasBookUpdate(*e, "absorption") {
					if err = h.freezeBookDepth(ctx, d, *e, v); err != nil {
						return err
					}
				}
				// Give the causal post-window snapshot its bounded 90 seconds.
				if v.After == nil && now.Before(to.Add(90*time.Second)) {
					continue
				}
				s.FootThrough[e.ID] = to
				changed = true
				if v.Absorption {
					appendBookUpdate(e, "absorption", now, to, v)
				}
				if v.Following {
					appendBookUpdate(e, "following", now, to, v)
				}
				updates[e.ID] = *e
			}
		}
	}
	// Finalize each source minute only after its three-minute acquisition
	// allowance. Duplicates cannot turn one received minute into three covered
	// minutes; after finalization a backfill cannot repair the recorded gap.
	start := s.CoverageThrough.Add(time.Minute)
	if s.CoverageThrough.IsZero() {
		start = s.Origin.Truncate(time.Minute).Add(time.Minute)
	}
	for at := start; !at.After(now.Truncate(time.Minute).Add(-4 * time.Minute)); at = at.Add(time.Minute) {
		day := at.UTC().Format("2006-01-02")
		cov := s.Days[day]
		bits := s.Coverage[at.Unix()]
		cov.Minute = at
		cov.Samples++
		if bits&3 == 3 {
			cov.BookTimely++
		}
		if bits == 15 {
			cov.Timely++
		}
		s.Days[day] = cov
		s.CoverageThrough = at
		delete(s.Coverage, at.Unix())
		changed = true
	}
	kept := []bookEvent{}
	for _, e := range s.Blocked {
		if !e.Rearmed {
			if e.Ended {
				// Full evidence remains in immutable rows and event projection;
				// cooldown only needs region, clocks and eligibility continuity.
				e.Zone.Levels, e.Zone.Baseline, e.Updates = nil, nil, nil
			}
			kept = append(kept, e)
		} else {
			delete(s.FootThrough, e.ID)
		}
	}
	s.Blocked = kept
	if len(s.Blocked) > 128 {
		return errors.New("挂单承接活动区域超过有界上限")
	}
	s.Last = now
	if !changed {
		return h.publishBookFlow(ctx, now)
	}
	err = h.commitBookFlow(ctx, s, updates)
	if err != nil {
		h.mu.Lock()
		if h.bookFlowRuntime.Skipped == nil {
			h.bookFlowRuntime.Skipped = map[string]time.Time{}
		}
		for venue, p := range s.Current {
			h.bookFlowRuntime.Skipped[venue] = p.At
		}
		h.mu.Unlock()
	}
	if err == nil {
		err = h.publishBookFlow(ctx, now)
	}
	return err
}

// Preserve the actual regional price/quantity rows used on both sides of the
// trade window. A later native-history revision cannot stand in for that input.
func (h *Hub) freezeBookDepth(ctx context.Context, d Dataset, e bookEvent, v *bookEvidence) error {
	for i, point := range []*bookBaselinePoint{v.Before, v.After} {
		levels := []Level{}
		if point != nil {
			err := h.Store.Visit(ctx, d, 60, point.At, point.At.Add(time.Second), func(o Observation) error {
				if o.Revision != point.Revision || o.Payload.Book == nil {
					return nil
				}
				raw := o.Payload.Book.Bids
				if e.Direction == "sell" {
					raw = o.Payload.Book.Asks
				}
				z := rawBookZone(raw, e.Direction, dec(e.Zone.Low))
				if z.Quantity != nil && *z.Quantity == point.Quantity {
					levels = z.Levels
				}
				return nil
			})
			if err != nil {
				return err
			}
		}
		if len(levels) == 0 {
			v.Absorption = false
			v.Retention = nil
		}
		if i == 0 {
			v.BeforeLevels = levels
		} else {
			v.AfterLevels = levels
		}
	}
	return nil
}

func (h *Hub) commitBookFlow(ctx context.Context, s bookFlowState, events map[string]bookEvent) error {
	if h.Store.Status().ResearchPaused || h.Store.Status().Paused {
		return errors.New("研究容量保护，观察未登记")
	}
	conn, err := h.Store.shortDB().Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err = conn.ExecContext(ctx, "PRAGMA busy_timeout=50"); err != nil {
		return err
	}
	defer func() {
		reset, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		_, _ = conn.ExecContext(reset, "PRAGMA busy_timeout=1500")
	}()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	put := func(kind, id, parent string, at time.Time, v any, immutable bool) error {
		b, e := json.Marshal(v)
		if e != nil {
			return e
		}
		if len(b) > 1<<20 {
			return errors.New("挂单承接状态超过1MiB工作集")
		}
		q := "INSERT INTO book_flow VALUES(?,?,?,?,?) ON CONFLICT(kind,id) DO UPDATE SET payload=excluded.payload"
		if immutable {
			q = "INSERT OR IGNORE INTO book_flow VALUES(?,?,?,?,?)"
		}
		_, e = tx.ExecContext(ctx, q, kind, id, parent, at.Unix(), b)
		return e
	}
	if err = put("origin", BookFlowRules, "", s.Origin, s.Origin, true); err != nil {
		return err
	}
	ids := []string{}
	for id := range events {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		e := events[id]
		if err = put("first", id, "", e.At, e, true); err != nil {
			return err
		}
		if err = put("event", id, "", e.At, e, false); err != nil {
			return err
		}
		states := []bookUpdate{{Kind: "enhanced", At: e.At, SourceAt: e.First.At}}
		states = append(states, e.Updates...)
		for _, u := range states {
			key := e.ID + "/" + u.Kind
			if err = put("update", key, e.ID, u.At, u, true); err != nil {
				return err
			}
		}
	}
	if err = put("state", BookFlowRules, "", s.Last, s, false); err != nil {
		return err
	}
	return tx.Commit()
}

// First-readable is observed only after the evidence transaction committed.
// Clocks and outcome enrollment share one transaction; retry never backdates a
// missing clock to its input or computation time. The API hides unclocked rows.
func (h *Hub) publishBookFlow(ctx context.Context, now time.Time) error {
	rows, err := h.Store.shortDB().QueryContext(ctx, `SELECT u.id,u.parent,u.payload,e.payload FROM book_flow u JOIN book_flow e ON e.kind='event' AND e.id=u.parent WHERE u.kind='update' AND NOT EXISTS(SELECT 1 FROM book_flow c WHERE c.kind='clock' AND c.id=u.id) ORDER BY u.at,u.id LIMIT 64`)
	if err != nil {
		return err
	}
	type pending struct {
		key, parent string
		update      bookUpdate
		event       bookEvent
	}
	items := []pending{}
	for rows.Next() {
		var p pending
		var a, b []byte
		if err = rows.Scan(&p.key, &p.parent, &a, &b); err != nil {
			break
		}
		if err = json.Unmarshal(a, &p.update); err != nil {
			break
		}
		if err = json.Unmarshal(b, &p.event); err != nil {
			break
		}
		items = append(items, p)
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return err
	}
	if len(items) == 0 {
		return nil
	}
	readable := now
	if !h.offline {
		readable = time.Now().UTC()
	}
	conn, err := h.Store.shortDB().Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err = conn.ExecContext(ctx, "PRAGMA busy_timeout=50"); err != nil {
		return err
	}
	defer func() {
		reset, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		_, _ = conn.ExecContext(reset, "PRAGMA busy_timeout=1500")
	}()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, p := range items {
		b, _ := json.Marshal(readable)
		res, e := tx.ExecContext(ctx, "INSERT OR IGNORE INTO book_flow VALUES('clock',?,?,?,?)", p.key, p.parent, readable.Unix(), b)
		if e != nil {
			return e
		}
		n, e := res.RowsAffected()
		if e != nil {
			return e
		}
		if n == 0 {
			continue
		}
		if p.update.Kind != "enhanced" && p.update.Kind != "absorption" && p.update.Kind != "following" {
			continue
		}
		t := newShortTrial(p.key, BookFlowRules+"/"+p.update.Kind, p.event.Direction, readable, p.update.SourceAt, nil)
		t.Outcomes = append([]ShortOutcome{{Minutes: 5, State: "pending"}}, t.Outcomes...)
		b, e = json.Marshal(t)
		if e != nil {
			return e
		}
		if _, err = tx.ExecContext(ctx, "INSERT OR IGNORE INTO book_flow VALUES('trial',?,?,?,?)", p.key, p.parent, readable.Unix(), b); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (h *Hub) advanceBookTrials(ctx context.Context, now time.Time) error {
	trials, err := paperRows[ShortTrial](ctx, h.Store.shortDB(), "SELECT payload FROM book_flow WHERE kind='trial' AND json_extract(payload,'$.done')=0 ORDER BY at,id LIMIT 16")
	if err != nil {
		return err
	}
	for _, t := range trials {
		old := t.Cursor
		if err = h.advanceShortTrial(ctx, &t, now); err != nil {
			return err
		}
		if t.Cursor.Equal(old) {
			continue
		}
		if err = h.Store.bookFlowSave(ctx, "trial", t.ID, t.At, t, false); err != nil {
			return err
		}
	}
	return nil
}

func (h *Hub) bookFlowWorker(ctx context.Context) {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			now := time.Now().UTC()
			step, cancel := context.WithTimeout(ctx, 2*time.Second)
			err := h.bookFlowStep(step, now)
			cancel()
			h.mu.Lock()
			h.bookFlowRuntime.At = &now
			h.bookFlowRuntime.Registered = err == nil
			if err != nil {
				h.bookFlowRuntime.FailureAt = &now
				h.bookFlowRuntime.Failure = err.Error()
				h.bookFlowRuntime.Failures++
			} else {
				h.bookFlowRuntime.Success = &now
			}
			h.mu.Unlock()
			if err != nil {
				h.saveBookFlowRuntime(ctx)
			}
			if err == nil && now.Second() < 10 && h.bookFlowMode == "run" {
				step, cancel = context.WithTimeout(ctx, time.Second)
				err = h.advanceBookTrials(step, now)
				cancel()
				if err != nil {
					h.mu.Lock()
					h.bookFlowRuntime.FailureAt = &now
					h.bookFlowRuntime.Failure = fmt.Sprintf("观察结果: %v", err)
					h.bookFlowRuntime.Failures++
					h.mu.Unlock()
					h.saveBookFlowRuntime(ctx)
				}
			}
		}
	}
}

func bookFlowParameters() json.RawMessage {
	b, _ := json.Marshal(map[string]any{"rules": BookFlowRules, "collection": BookFlowCollection, "venues": []string{"Binance", "OKX"}, "grid": "100", "width": "200", "range": "0.01", "baselineMinutes": 30, "minimumCoverage": "0.95", "multiple": "2", "increaseUsd": "1000000", "footMinutes": 5, "footMinimumUsd": "1000000", "share": "0.6", "retention": "0.7", "fade": "0.5", "expiryMinutes": 30, "rearmMinutes": 10, "sourceMaxAgeSeconds": 180, "bookFootBoundarySeconds": 90})
	return b
}

func (h *Hub) saveBookFlowRuntime(ctx context.Context) {
	h.mu.RLock()
	r := h.bookFlowRuntime
	h.mu.RUnlock()
	b, _ := json.Marshal(r)
	save, stop := context.WithTimeout(ctx, 150*time.Millisecond)
	defer stop()
	_, _ = boundedExec(save, h.Store.db, 5000, "INSERT INTO state VALUES('book-flow/runtime',?) ON CONFLICT(key) DO UPDATE SET payload=excluded.payload", b)
}
