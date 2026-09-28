package datahub

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/mail"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
)

type VIXSettings struct {
	EmailEnabled bool `json:"emailEnabled"`
}
type vixState struct {
	EmailEnabled     bool       `json:"emailEnabled"`
	EverEnabled      bool       `json:"everEnabled"`
	FirstObservation bool       `json:"firstObservation"`
	LastPoint        *VIXPoint  `json:"lastPoint"`
	CycleID          string     `json:"cycleId"`
	CycleStartedAt   *time.Time `json:"cycleStartedAt"`
	LowSince         *time.Time `json:"lowSince"`
	Watch            bool       `json:"watchRecorded"`
	Priority         bool       `json:"priorityRecorded"`
}
type VIXEvent struct {
	ID          string    `json:"id"`
	CycleID     string    `json:"cycleId"`
	Level       string    `json:"level"`
	Value       string    `json:"value"`
	ObservedAt  time.Time `json:"observedAt"`
	DetectedAt  time.Time `json:"detectedAt"`
	TradingDate string    `json:"tradingDate"`
	Initial     bool      `json:"initial"`
	Status      string    `json:"status,omitempty"`
}

func (w *Warehouse) initVIX() error {
	_, err := w.research.Exec(`
CREATE TABLE IF NOT EXISTS vix_feed(source TEXT PRIMARY KEY,payload BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS vix_state(id INTEGER PRIMARY KEY CHECK(id=1),payload BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS vix_minute(ts INTEGER PRIMARY KEY,trade_date TEXT NOT NULL,value TEXT NOT NULL,fetched INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS vix_minute_date ON vix_minute(trade_date,ts);
CREATE TABLE IF NOT EXISTS vix_daily(day TEXT PRIMARY KEY,open TEXT NOT NULL,high TEXT NOT NULL,low TEXT NOT NULL,close TEXT NOT NULL,fetched INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS vix_events(id TEXT PRIMARY KEY,created INTEGER NOT NULL,payload BLOB NOT NULL);
CREATE INDEX IF NOT EXISTS vix_events_time ON vix_events(created,id);
CREATE TABLE IF NOT EXISTS mail_batches(id TEXT PRIMARY KEY,attempted INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS mail_batch_items(notice_id TEXT PRIMARY KEY,batch_id TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS mail_batches_time ON mail_batches(attempted);
UPDATE notices SET status='unknown_after_restart' WHERE kind LIKE 'vix:%' AND status='sending';
UPDATE notices SET status='suppressed_restart' WHERE kind LIKE 'vix:%' AND status='pending';`)
	return err
}

type vixQuerier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func vixLoad(ctx context.Context, q vixQuerier, statement string, value any, args ...any) error {
	var b []byte
	err := q.QueryRowContext(ctx, statement, args...).Scan(&b)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}
	return json.Unmarshal(b, value)
}
func vixSaveState(ctx context.Context, tx *sql.Tx, state vixState) error {
	b, err := json.Marshal(state)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO vix_state VALUES(1,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload", b)
	return err
}
func vixSaveFeed(ctx context.Context, tx *sql.Tx, feed VIXFeed) error {
	b, err := json.Marshal(feed)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO vix_feed VALUES(?,?) ON CONFLICT(source) DO UPDATE SET payload=excluded.payload", feed.Source, b)
	return err
}
func (h *Hub) vixFeed(ctx context.Context, source string) (VIXFeed, error) {
	f := VIXFeed{Source: source}
	err := vixLoad(ctx, h.Store.research, "SELECT payload FROM vix_feed WHERE source=?", &f, source)
	return f, err
}
func (h *Hub) vixFailure(ctx context.Context, source, message string, now time.Time) error {
	h.vixMu.Lock()
	defer h.vixMu.Unlock()
	tx, err := h.Store.research.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	f := VIXFeed{Source: source}
	if err = vixLoad(ctx, tx, "SELECT payload FROM vix_feed WHERE source=?", &f, source); err != nil {
		return err
	}
	f.Error, f.AttemptedAt = message, &now
	if err = vixSaveFeed(ctx, tx, f); err != nil {
		return err
	}
	if source == "sina" {
		var s vixState
		if err = vixLoad(ctx, tx, "SELECT payload FROM vix_state WHERE id=1", &s); err != nil {
			return err
		}
		s.LowSince = nil
		if err = vixSaveState(ctx, tx, s); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// The only input eligible for alert evaluation is the newest point from a
// successful fetch, never a replay of the other intraday or daily records.
func (h *Hub) ingestVIX(ctx context.Context, points []VIXPoint, now time.Time) error {
	if len(points) == 0 {
		return errors.New("VIX分时为空")
	}
	if h.Store.Status().Paused || h.Store.Status().ResearchPaused {
		return errors.New("存储容量保护，VIX采集暂停")
	}
	h.vixMu.Lock()
	defer h.vixMu.Unlock()
	tx, err := h.Store.research.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	f := VIXFeed{Source: "sina"}
	if err = vixLoad(ctx, tx, "SELECT payload FROM vix_feed WHERE source='sina'", &f); err != nil {
		return err
	}
	p := points[len(points)-1]
	if f.Latest != nil && p.At.Before(f.Latest.At) {
		return errors.New("新浪VIX来源时间倒退，保留最后有效观察")
	}
	stmt, err := tx.PrepareContext(ctx, "INSERT INTO vix_minute VALUES(?,?,?,?) ON CONFLICT(ts) DO UPDATE SET value=excluded.value,fetched=excluded.fetched WHERE value<>excluded.value")
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, point := range points {
		if point.At.Before(now.Add(-30 * 24 * time.Hour)) {
			continue
		}
		if _, err = stmt.ExecContext(ctx, point.At.Unix(), point.TradingDate, point.Value, now.Unix()); err != nil {
			return err
		}
	}
	f.Latest, f.FetchedAt, f.AttemptedAt, f.Error = &p, &now, &now, ""
	if err = vixSaveFeed(ctx, tx, f); err != nil {
		return err
	}
	var s vixState
	if err = vixLoad(ctx, tx, "SELECT payload FROM vix_state WHERE id=1", &s); err != nil {
		return err
	}
	if s.LowSince != nil {
		// Backfilled/revised points never generate alerts. However, a known
		// intervening high disproves an otherwise continuous recovery window.
		for _, point := range points {
			if !point.At.Before(*s.LowSince) && !point.At.After(p.At) && dec(point.Value).GreaterThanOrEqual(dec("30")) {
				s.LowSince = nil
				break
			}
		}
	}
	if quality, _ := vixQuality(f, now); quality == "delayed" {
		err = h.evaluateVIX(ctx, tx, &s, p, now)
	} else {
		s.LowSince = nil
	}
	if err != nil {
		return err
	}
	if err = vixSaveState(ctx, tx, s); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	h.wakeVIXMail()
	return nil
}

func (h *Hub) evaluateVIX(ctx context.Context, tx *sql.Tx, s *vixState, p VIXPoint, now time.Time) error {
	if s.LastPoint != nil {
		if !p.At.After(s.LastPoint.At) {
			if p.At.Equal(s.LastPoint.At) && p.Value != s.LastPoint.Value {
				// A correction cannot issue a historical alert, but does invalidate
				// any recovery interval that relied on the replaced observation.
				s.LowSince = nil
				s.LastPoint = &p
			}
			return nil
		}
		if p.At.Sub(s.LastPoint.At) > 5*time.Minute {
			s.LowSince = nil
		}
	}
	s.LastPoint = &p
	initial := s.FirstObservation
	s.FirstObservation = false
	level := vixLevel(p.Value)
	if level == "low" || level == "elevated" {
		if s.CycleID != "" {
			if s.LowSince == nil {
				t := p.At
				s.LowSince = &t
			}
			if p.At.Sub(*s.LowSince) >= 30*time.Minute {
				s.CycleID, s.CycleStartedAt, s.LowSince, s.Watch, s.Priority = "", nil, nil, false, false
			}
		}
		return nil
	}
	s.LowSince = nil
	if level == "priority" && s.Priority || level == "watch" && s.Watch {
		return nil
	}
	if s.CycleID == "" {
		s.CycleID = "VIX-" + uuid.NewString()
		t := p.At
		s.CycleStartedAt = &t
	}
	s.Watch = true
	if level == "priority" {
		s.Priority = true
		// An unsent ordinary notification is superseded by the higher band.
		if _, err := tx.ExecContext(ctx, "UPDATE notices SET status='superseded' WHERE id=? AND status='pending'", s.CycleID+"/watch"); err != nil {
			return err
		}
	}
	e := VIXEvent{ID: s.CycleID + "/" + level, CycleID: s.CycleID, Level: level, Value: p.Value, ObservedAt: p.At, DetectedAt: now, TradingDate: p.TradingDate, Initial: initial}
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO vix_events VALUES(?,?,?)", e.ID, now.UnixNano(), b); err != nil {
		return err
	}
	notice := noticePayload{Topic: "vix", At: now, DataThrough: p.At, Expires: p.At.Add(vixSourceTTL), ID: e.ID, VIX: &e}
	b, err = json.Marshal(notice)
	if err != nil {
		return err
	}
	status := "pending"
	if !s.EmailEnabled {
		status = "disabled"
	} else if h.offline {
		status = "offline"
	} else if h.mail == nil {
		status = "unconfigured"
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO notices(id,signal_id,kind,created,status,payload) VALUES(?,?,?,?,?,?)", e.ID, e.ID, "vix:"+level, now.Unix(), status, b)
	return err
}

func (h *Hub) ingestVIXDaily(ctx context.Context, rows []VIXDaily, now time.Time) error {
	if len(rows) == 0 {
		return errors.New("Cboe日线为空")
	}
	if h.Store.Status().Paused || h.Store.Status().ResearchPaused {
		return errors.New("存储容量保护，Cboe日线暂停")
	}
	tx, err := h.Store.research.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	f := VIXFeed{Source: "cboe"}
	if err = vixLoad(ctx, tx, "SELECT payload FROM vix_feed WHERE source='cboe'", &f); err != nil {
		return err
	}
	if rows[len(rows)-1].Date < f.LastDate {
		return errors.New("Cboe日线日期倒退")
	}
	stmt, err := tx.PrepareContext(ctx, "INSERT INTO vix_daily VALUES(?,?,?,?,?,?) ON CONFLICT(day) DO UPDATE SET open=excluded.open,high=excluded.high,low=excluded.low,close=excluded.close,fetched=excluded.fetched")
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, r := range rows {
		if _, err = stmt.ExecContext(ctx, r.Date, r.Open, r.High, r.Low, r.Close, now.Unix()); err != nil {
			return err
		}
	}
	f.LastDate, f.FetchedAt, f.AttemptedAt, f.Error = rows[len(rows)-1].Date, &now, &now, ""
	if err = vixSaveFeed(ctx, tx, f); err != nil {
		return err
	}
	return tx.Commit()
}

func (h *Hub) SetVIXSettings(ctx context.Context, settings VIXSettings, now time.Time) (any, error) {
	h.vixMu.Lock()
	err := func() error {
		tx, err := h.Store.research.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		var s vixState
		if err = vixLoad(ctx, tx, "SELECT payload FROM vix_state WHERE id=1", &s); err != nil {
			return err
		}
		first := settings.EmailEnabled && !s.EverEnabled
		s.EmailEnabled = settings.EmailEnabled
		if first {
			s.EverEnabled, s.FirstObservation = true, false
			// First opt-in is a new explicit observation boundary, not a replay.
			s.CycleID, s.CycleStartedAt, s.LowSince, s.LastPoint, s.Watch, s.Priority = "", nil, nil, nil, false, false
			f := VIXFeed{Source: "sina"}
			if err = vixLoad(ctx, tx, "SELECT payload FROM vix_feed WHERE source='sina'", &f); err != nil {
				return err
			}
			if status, _ := vixQuality(f, now); status == "delayed" {
				// This label describes a condition known at the moment of opt-in,
				// never a new observation collected after a closed/stale period.
				s.FirstObservation = true
				if err = h.evaluateVIX(ctx, tx, &s, *f.Latest, now); err != nil {
					return err
				}
			}
		}
		if !s.EmailEnabled {
			if _, err = tx.ExecContext(ctx, "UPDATE notices SET status='disabled' WHERE kind LIKE 'vix:%' AND status='pending'"); err != nil {
				return err
			}
		}
		if err = vixSaveState(ctx, tx, s); err != nil {
			return err
		}
		return tx.Commit()
	}()
	h.vixMu.Unlock()
	if err != nil {
		return nil, err
	}
	h.wakeVIXMail()
	return h.vixSettings(ctx)
}
func (h *Hub) vixSettings(ctx context.Context) (any, error) {
	var s vixState
	if err := vixLoad(ctx, h.Store.research, "SELECT payload FROM vix_state WHERE id=1", &s); err != nil {
		return nil, err
	}
	masked := "未配置"
	if h.mail != nil {
		if a, e := mail.ParseAddress(h.mail.To); e == nil {
			if i := strings.LastIndex(a.Address, "@"); i > 0 {
				masked = string([]rune(a.Address[:i])[:1]) + "***" + a.Address[i:]
			}
		}
	}
	return map[string]any{"emailEnabled": s.EmailEnabled, "configured": h.mail != nil, "offline": h.offline, "recipient": masked, "limitPerHour": 6, "cycle": s, "thresholds": []int{20, 30, 40}}, nil
}
func (h *Hub) vixCurrent(ctx context.Context, now time.Time) (any, error) {
	f, err := h.vixFeed(ctx, "sina")
	if err != nil {
		return nil, err
	}
	settings, err := h.vixSettings(ctx)
	if err != nil {
		return nil, err
	}
	status, reason := vixQuality(f, now)
	var mailError map[string]any
	h.Store.LoadState("vix/mail-error", &mailError)
	var level any
	var next any
	var delay any
	if f.Latest != nil {
		level = vixLevel(f.Latest.Value)
		if dec(f.Latest.Value).LessThan(dec("30")) {
			next = dec("30").Sub(dec(f.Latest.Value)).String()
		} else if !dec(f.Latest.Value).GreaterThan(dec("40")) {
			next = dec("40").Sub(dec(f.Latest.Value)).String()
		}
		delay = max(int64(0), int64(now.Sub(f.Latest.At).Seconds()))
	}
	return map[string]any{"symbol": "VIX", "latest": f.Latest, "source": "sina", "sourceUrl": vixMinuteURL, "fetchedAt": f.FetchedAt, "attemptedAt": f.AttemptedAt, "status": status, "reason": reason, "level": level, "nextThresholdDistance": next, "sourceAgeSeconds": delay, "settings": settings, "mailError": mailError, "now": now}, nil
}
func (h *Hub) vixRead(ctx context.Context, path string, q url.Values) (json.RawMessage, error) {
	now := time.Now().UTC()
	var out any
	var err error
	switch path {
	case "vix":
		out, err = h.vixCurrent(ctx, now)
	case "vix/settings":
		out, err = h.vixSettings(ctx)
	case "vix/alerts":
		limit := parseInt(q, "limit", 50, 1, 100)
		before := now.Add(time.Second).UnixNano()
		if q.Get("before") != "" {
			t, e := time.Parse(time.RFC3339Nano, q.Get("before"))
			if e != nil {
				return nil, errors.New("无效提醒游标")
			}
			before = t.UnixNano()
		}
		var rows *sql.Rows
		rows, err = h.Store.research.QueryContext(ctx, "SELECT e.payload,COALESCE(n.status,'unknown') FROM vix_events e LEFT JOIN notices n ON e.id=n.id WHERE e.created<? ORDER BY e.created DESC,e.id DESC LIMIT ?", before, limit+1)
		if err != nil {
			return nil, err
		}
		items := []VIXEvent{}
		for rows.Next() {
			var b []byte
			var status string
			if err = rows.Scan(&b, &status); err != nil {
				break
			}
			var item VIXEvent
			if err = json.Unmarshal(b, &item); err != nil {
				break
			}
			item.Status = status
			items = append(items, item)
		}
		if err == nil {
			err = rows.Err()
		}
		rows.Close()
		more := len(items) > limit
		if more {
			items = items[:limit]
		}
		var cursor any
		if more {
			cursor = items[len(items)-1].DetectedAt.Format(time.RFC3339Nano)
		}
		out = map[string]any{"items": items, "hasMore": more, "nextBefore": cursor}
	case "vix/history":
		rangeName := q.Get("range")
		if rangeName == "" {
			rangeName = "day"
		}
		if rangeName == "day" {
			f, e := h.vixFeed(ctx, "sina")
			if e != nil {
				return nil, e
			}
			points := []VIXPoint{}
			if f.Latest != nil {
				rows, e := h.Store.research.QueryContext(ctx, "SELECT ts,value,trade_date FROM vix_minute WHERE trade_date=? ORDER BY ts LIMIT 1500", f.Latest.TradingDate)
				if e != nil {
					return nil, e
				}
				for rows.Next() {
					var p VIXPoint
					var ts int64
					if err = rows.Scan(&ts, &p.Value, &p.TradingDate); err != nil {
						break
					}
					p.At = time.Unix(ts, 0).UTC()
					points = append(points, p)
				}
				if err == nil {
					err = rows.Err()
				}
				rows.Close()
			}
			out = map[string]any{"source": "sina", "range": rangeName, "points": points, "feed": f}
		} else {
			months := map[string]int{"1m": 1, "3m": 3, "1y": 12}[rangeName]
			if months == 0 {
				return nil, errors.New("历史范围仅支持day、1m、3m、1y")
			}
			f, e := h.vixFeed(ctx, "cboe")
			if e != nil {
				return nil, e
			}
			rows, e := h.Store.research.QueryContext(ctx, "SELECT day,open,high,low,close FROM vix_daily WHERE day>=? ORDER BY day LIMIT 370", now.In(vixNewYork).AddDate(0, -months, 0).Format("2006-01-02"))
			if e != nil {
				return nil, e
			}
			points := []VIXDaily{}
			for rows.Next() {
				var p VIXDaily
				if err = rows.Scan(&p.Date, &p.Open, &p.High, &p.Low, &p.Close); err != nil {
					break
				}
				points = append(points, p)
			}
			if err == nil {
				err = rows.Err()
			}
			rows.Close()
			out = map[string]any{"source": "cboe", "range": rangeName, "daily": points, "feed": f}
		}
	default:
		return nil, errors.New("未知VIX接口")
	}
	if err != nil {
		return nil, err
	}
	return json.Marshal(out)
}
func (h *Hub) vixHealth() any {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	f, err := h.vixFeed(ctx, "sina")
	if err != nil {
		return map[string]any{"status": "error", "reason": "VIX状态读取失败"}
	}
	status, reason := vixQuality(f, time.Now().UTC())
	daily, _ := h.vixFeed(ctx, "cboe")
	return map[string]any{"status": status, "reason": reason, "feed": f, "daily": daily}
}
func (w *Warehouse) maintainVIX(ctx context.Context, now time.Time) error {
	for _, s := range []struct {
		q   string
		arg any
	}{
		{"DELETE FROM vix_minute WHERE ts<?", now.Add(-30 * 24 * time.Hour).Unix()},
		{"DELETE FROM vix_daily WHERE day<?", now.In(vixNewYork).AddDate(-1, 0, -1).Format("2006-01-02")},
		{"DELETE FROM vix_events WHERE created<?", now.Add(-90 * 24 * time.Hour).UnixNano()},
		{"DELETE FROM notices WHERE kind LIKE 'vix:%' AND created<?", now.Add(-90 * 24 * time.Hour).Unix()},
		{"DELETE FROM mail_batch_items WHERE batch_id IN (SELECT id FROM mail_batches WHERE attempted<?)", now.Add(-90 * 24 * time.Hour).UnixNano()},
		{"DELETE FROM mail_results WHERE batch_id IN (SELECT id FROM mail_batches WHERE attempted<?)", now.Add(-90 * 24 * time.Hour).UnixNano()},
		{"DELETE FROM mail_batches WHERE attempted<?", now.Add(-90 * 24 * time.Hour).UnixNano()},
	} {
		if _, err := w.research.ExecContext(ctx, s.q, s.arg); err != nil {
			return fmt.Errorf("VIX维护: %w", err)
		}
	}
	return nil
}
