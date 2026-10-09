package datahub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/shopspring/decimal"
)

const LayeredRules = "flow-layered-shadow-v1"
const layeredBudget = 8 << 20 // Included in the existing 512 MiB research ceiling.

type AlertMeaning struct {
	PublishedLevel string   `json:"publishedLevel"`
	LaterLevel     string   `json:"laterLevel"`
	Labels         []string `json:"labels"`
	Note           string   `json:"note"`
}

// One interpretation shared by the page, audit and notification. Never use the
// current upgrade or later confirmation to describe what the first alert knew.
func alertMeaning(s Signal, confirmation bool) AlertMeaning {
	m := AlertMeaning{Labels: []string{}, Note: "主动成交变化属于风险观察；不等于做多或做空指令。"}
	snap := s.Multifactor
	if snap == nil {
		m.Labels = append(m.Labels, "首次发布背景未记录")
		return m
	}
	m.PublishedLevel = flowLevel(snap, s.Direction)
	if s.MultifactorUpgrade != nil {
		m.LaterLevel = flowLevel(s.MultifactorUpgrade, s.Direction)
	}
	if confirmation && s.ConfirmationSnapshot != nil {
		snap = s.ConfirmationSnapshot
	}
	name := "买盘"
	if s.Direction == "sell" {
		name = "卖压"
	}
	if w := snap.Spot["240"]; w.Net == nil {
		m.Labels = append(m.Labels, "四小时资金背景未知")
	} else if *w.Net*sideSign(s.Direction) < 0 && !confirmation {
		m.Labels = append(m.Labels, "逆势"+name+"异动，反转未确认")
	}
	if d := snap.Price.DisplacementATR; d == nil {
		m.Labels = append(m.Labels, "价格位移未知")
	} else if *d*float64(sideSign(s.Direction)) > 1.5 {
		m.Labels = append(m.Labels, "行情已明显移动，存在追价风险")
	}
	if confirmation {
		m.Labels = append(m.Labels, "已发生突破；确认时间独立于首次异动")
	} else {
		m.Labels = append(m.Labels, "首次异动时尚未确认反转")
	}
	return m
}

type LayeredInput struct {
	Through             time.Time  `json:"dataThrough"`
	SnapshotAt          time.Time  `json:"snapshotAt"`
	Fresh               bool       `json:"fresh"`
	Net1H               *string    `json:"net1hUsd"`
	Net4H               *string    `json:"net4hUsd"`
	Coverage1H          float64    `json:"coverage1h"`
	Coverage4H          float64    `json:"coverage4h"`
	Price               *string    `json:"priceUsdt"`
	ATR                 *string    `json:"priorAtrUsdt"`
	Displacement        *string    `json:"directionalDisplacementAtr"`
	ConfirmationAt      *time.Time `json:"confirmationAt"`
	ConfirmationThrough *time.Time `json:"confirmationDataThrough"`
	ConfirmationLine    string     `json:"confirmationLineUsdt"`
}

type LayeredDecision struct {
	Collection string       `json:"collectionVersion,omitempty"`
	ID         string       `json:"id"`
	Rules      string       `json:"rulesVersion"`
	ParentID   string       `json:"parentId"`
	Direction  string       `json:"direction"`
	At         time.Time    `json:"at"`
	Accepted   bool         `json:"accepted"`
	Reasons    []string     `json:"reasons"`
	Input      LayeredInput `json:"input"`
}

func finiteDecimal(v *float64) *string {
	if v == nil || math.IsNaN(*v) || math.IsInf(*v, 0) {
		return nil
	}
	return flowPtr(decimal.NewFromFloat(*v).String())
}
func netDecimal(v *int64) *string {
	if v == nil {
		return nil
	}
	return flowPtr(decimal.NewFromInt(*v).Div(decimal.NewFromInt(100)).String())
}
func evaluateLayered(s Signal, current FlowSnapshot, origin, now time.Time) LayeredDecision {
	sign := float64(sideSign(s.Direction))
	w1, w4 := current.Spot["60"], current.Spot["240"]
	d := LayeredDecision{Rules: LayeredRules, Collection: s.Collection, ParentID: s.ID, Direction: s.Direction, At: now, Reasons: []string{}}
	in := LayeredInput{Through: current.DataThrough, SnapshotAt: current.At, Fresh: current.Fresh, Net1H: netDecimal(w1.Net), Net4H: netDecimal(w4.Net), Coverage1H: w1.Coverage, Coverage4H: w4.Coverage, Price: finiteDecimal(current.Price.Close), ATR: finiteDecimal(current.Price.PriorATR), ConfirmationAt: s.ConfirmedAt, ConfirmationThrough: s.ConfirmedThrough}
	line := s.FrozenHigh
	if s.Direction == "sell" {
		line = s.FrozenLow
	}
	in.ConfirmationLine = decimal.NewFromFloat(line).String()
	if v := current.Price.DisplacementATR; v != nil {
		in.Displacement = finiteDecimal(flowPtr(*v * sign))
	}
	confirmationKey := int64(0)
	if s.ConfirmedThrough != nil {
		confirmationKey = s.ConfirmedThrough.Unix()
	}
	d.ID = fmt.Sprintf("%s/%s/%d/%d", LayeredRules, s.ID, current.DataThrough.Unix(), confirmationKey)
	if s.Rules != MultifactorRules || s.Multifactor == nil || s.Asset != "BTC" || (s.Direction != "buy" && s.Direction != "sell") || s.At.Before(origin) || s.At.After(now) {
		d.Reasons = append(d.Reasons, "not_new_formal_publication")
	}
	if !now.Before(s.Expires) {
		d.Reasons = append(d.Reasons, "parent_expired")
	}
	if s.ConfirmedAt == nil || s.ConfirmedThrough == nil || s.ConfirmationSnapshot == nil || s.ConfirmedAt.After(now) || s.ConfirmedThrough.After(current.DataThrough) || !s.ConfirmedThrough.Equal(s.ConfirmationSnapshot.DataThrough) || s.ConfirmedThrough.Before(s.DataThrough.Add(10*time.Minute)) {
		d.Reasons = append(d.Reasons, "own_breakout_unconfirmed")
	}
	if !current.Fresh || current.DataThrough.IsZero() || current.DataThrough.After(now) || now.Sub(current.DataThrough) > 12*time.Minute || current.At.After(now) || now.Sub(current.At) > 12*time.Minute {
		d.Reasons = append(d.Reasons, "stale_input")
	}
	if w1.Net == nil || w4.Net == nil || w1.Coverage != 1 || w4.Coverage != 1 || !w1.To.Equal(current.DataThrough) || !w4.To.Equal(current.DataThrough) {
		d.Reasons = append(d.Reasons, "flow_window_missing")
	} else if *w1.Net*sideSign(s.Direction) <= 0 || *w4.Net*sideSign(s.Direction) <= 0 {
		d.Reasons = append(d.Reasons, "flow_direction_conflict")
	}
	if in.Price == nil || current.Price.Close == nil || *current.Price.Close <= 0 || in.ATR == nil || current.Price.PriorATR == nil || *current.Price.PriorATR <= 0 || in.Displacement == nil {
		d.Reasons = append(d.Reasons, "price_atr_missing")
	} else if *current.Price.DisplacementATR*sign > 1.5 {
		d.Reasons = append(d.Reasons, "extended_move")
	}
	d.Input = in
	d.Accepted = len(d.Reasons) == 0
	return d
}

func (w *Warehouse) initAlertAudit() error {
	_, err := w.research.Exec(`CREATE TABLE IF NOT EXISTS alert_audit(kind TEXT,id TEXT,parent TEXT,at INTEGER,payload BLOB,PRIMARY KEY(kind,id)) WITHOUT ROWID;
 CREATE INDEX IF NOT EXISTS alert_audit_parent ON alert_audit(parent,kind,at);
 CREATE INDEX IF NOT EXISTS alert_audit_time ON alert_audit(kind,at);
 CREATE TABLE IF NOT EXISTS alert_audit_budget(id INTEGER PRIMARY KEY,used INTEGER NOT NULL);
 INSERT OR IGNORE INTO alert_audit_budget VALUES(1,0);
 CREATE TRIGGER IF NOT EXISTS alert_audit_insert BEFORE INSERT ON alert_audit WHEN NOT EXISTS(SELECT 1 FROM alert_audit WHERE kind=NEW.kind AND id=NEW.id) AND (SELECT used FROM alert_audit_budget WHERE id=1)+length(NEW.payload)+256>8388608 BEGIN SELECT RAISE(ABORT,'alert audit sub-budget full'); END;
 CREATE TRIGGER IF NOT EXISTS alert_audit_update BEFORE UPDATE OF payload ON alert_audit WHEN (SELECT used FROM alert_audit_budget WHERE id=1)+length(NEW.payload)-length(OLD.payload)>8388608 BEGIN SELECT RAISE(ABORT,'alert audit sub-budget full'); END;
 CREATE TRIGGER IF NOT EXISTS alert_audit_added AFTER INSERT ON alert_audit BEGIN UPDATE alert_audit_budget SET used=used+length(NEW.payload)+256 WHERE id=1; END;
 CREATE TRIGGER IF NOT EXISTS alert_audit_changed AFTER UPDATE OF payload ON alert_audit BEGIN UPDATE alert_audit_budget SET used=used+length(NEW.payload)-length(OLD.payload) WHERE id=1; END;
 CREATE TRIGGER IF NOT EXISTS alert_audit_removed AFTER DELETE ON alert_audit BEGIN UPDATE alert_audit_budget SET used=used-length(OLD.payload)-256 WHERE id=1; END;`)
	return err
}
func (h *Hub) layeredStep(ctx context.Context, now time.Time) error {
	if h.Store.alertAuditError != "" {
		return errors.New("候选存储初始化失败：" + h.Store.alertAuditError)
	}
	if h.Store.Status().ResearchPaused || h.Store.Status().Paused {
		return errors.New("研究容量暂停，影子候选未登记")
	}
	db := h.Store.shortDB()
	raw, _ := json.Marshal(now)
	if _, e := db.ExecContext(ctx, "INSERT OR IGNORE INTO alert_audit VALUES('origin',?,'',?,?)", LayeredRules, now.Unix(), raw); e != nil {
		return e
	}
	var origin time.Time
	if e := db.QueryRowContext(ctx, "SELECT payload FROM alert_audit WHERE kind='origin' AND id=?", LayeredRules).Scan(&raw); e != nil {
		return e
	}
	if e := json.Unmarshal(raw, &origin); e != nil {
		return e
	}
	var current FlowSnapshot
	if e := h.Store.shortStateResult(ctx, "signals/current/BTC", &current); e != nil {
		return e
	}
	// Only immutable real first publications are parents. The lifecycle joins
	// supply later confirmation, never a replacement initial snapshot.
	rows, e := db.QueryContext(ctx, `SELECT p.payload,d.payload FROM signal_publications p JOIN documents d ON d.kind='signal' AND p.id=json_extract(d.payload,'$.rulesVersion')||'/'||d.id WHERE p.at>=? AND p.at<=? ORDER BY p.seq LIMIT 64`, maxTime(origin, now.Add(-4*time.Hour)).UnixMilli(), now.UnixMilli())
	if e != nil {
		return e
	}
	parents := []Signal{}
	for rows.Next() {
		var first, later []byte
		if e = rows.Scan(&first, &later); e != nil {
			break
		}
		s := decodeSignal(first)
		life := decodeSignal(later)
		s.ConfirmedAt = life.ConfirmedAt
		s.ConfirmedThrough = life.ConfirmedThrough
		s.ConfirmationSnapshot = life.ConfirmationSnapshot
		parents = append(parents, s)
	}
	if e == nil {
		e = rows.Err()
	}
	rows.Close()
	if e != nil {
		return e
	}
	for _, s := range parents {
		var exists int
		if e = db.QueryRowContext(ctx, "SELECT count(*) FROM alert_audit WHERE kind='candidate' AND id=?", LayeredRules+"/"+s.ID).Scan(&exists); e != nil {
			return e
		}
		if exists > 0 {
			continue
		}
		decision := evaluateLayered(s, current, origin, now)
		decision.Collection = h.collectionVersion()
		if e = h.saveLayeredDecision(ctx, decision); e != nil {
			return e
		}
	}
	// Progress is independently bounded; failures do not retry or manufacture an
	// initial candidate. The shared closed-candle statistics retain null gaps.
	trials, e := paperRows[ShortTrial](ctx, db, `SELECT payload FROM alert_audit WHERE kind='outcome' AND json_extract(payload,'$.done')=0 ORDER BY at LIMIT 4`)
	if e != nil {
		return e
	}
	for _, t := range trials {
		if e = h.advanceShortTrial(ctx, &t, now); e != nil {
			return e
		}
		b, _ := json.Marshal(t)
		if _, e = db.ExecContext(ctx, "UPDATE alert_audit SET payload=? WHERE kind='outcome' AND id=?", b, t.ID); e != nil {
			return e
		}
	}
	return nil
}
func (h *Hub) saveLayeredDecision(ctx context.Context, d LayeredDecision) error {
	db := h.Store.shortDB()
	tx, e := db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	b, e := json.Marshal(d)
	if e != nil {
		return e
	}
	r, e := tx.ExecContext(ctx, "INSERT OR IGNORE INTO alert_audit VALUES('decision',?,?,?,?)", d.ID, d.ParentID, d.At.Unix(), b)
	if e != nil {
		return e
	}
	n, e := r.RowsAffected()
	if e != nil {
		return e
	}
	if n > 0 && d.Accepted {
		id := LayeredRules + "/" + d.ParentID
		r, e = tx.ExecContext(ctx, "INSERT OR IGNORE INTO alert_audit VALUES('candidate',?,?,?,?)", id, d.ParentID, d.At.Unix(), b)
		if e != nil {
			return e
		}
		n, e = r.RowsAffected()
		if e != nil {
			return e
		}
		if n > 0 {
			t := newShortTrial(id, LayeredRules, d.Direction, d.At, d.Input.Through, nil)
			b, _ = json.Marshal(t)
			if _, e = tx.ExecContext(ctx, "INSERT INTO alert_audit VALUES('outcome',?,?,?,?)", id, d.ParentID, d.At.Unix(), b); e != nil {
				return e
			}
		}
	}
	return tx.Commit()
}
func (h *Hub) layeredWorker(ctx context.Context) {
	tick := time.NewTicker(20 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			now := time.Now().UTC()
			step, cancel := context.WithTimeout(ctx, 2*time.Second)
			err := h.layeredStep(step, now)
			cancel()
			h.mu.Lock()
			h.layeredLastAttempt = &now
			if err != nil {
				h.layeredLastFailure = &now
				h.layeredFailure = err.Error()
			} else {
				h.layeredLastSuccess = &now
			}
			h.mu.Unlock()
		}
	}
}
