package datahub

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"time"
)

const OrderHistoryLimit = 512 << 20

type TrackedOrder struct {
	FirstSeen time.Time  `json:"firstSeenAt"`
	Key       string     `json:"key"`
	Dataset   string     `json:"dataset"`
	Asset     string     `json:"asset"`
	Venue     string     `json:"venue"`
	Quote     string     `json:"quote"`
	Order     LargeOrder `json:"order"`
	Seen      time.Time  `json:"seenAt"`
	FactAt    time.Time  `json:"factAt"`
	Revision  string     `json:"revision"`
}
type OrderEvent struct {
	Key                   string      `json:"key"`
	Asset                 string      `json:"asset"`
	Venue                 string      `json:"venue"`
	Quote                 string      `json:"quote"`
	OrderID               string      `json:"orderId"`
	Side                  string      `json:"side"`
	Price                 string      `json:"price"`
	Kind                  string      `json:"kind"`
	At                    time.Time   `json:"at"`
	From                  *time.Time  `json:"from"`
	QuantityDelta         *string     `json:"quantityDelta"`
	ExecutedQuantityDelta *string     `json:"executedQuantityDelta"`
	ExecutedDelta         *string     `json:"executedDeltaUsd"`
	Before                *LargeOrder `json:"before,omitempty"`
	After                 *LargeOrder `json:"after,omitempty"`
	Note                  string      `json:"note"`
}

func orderKey(d Dataset, r LargeOrder) string {
	return strings.Join([]string{d.Source, d.Market, d.Venue, d.Symbol, r.ID}, ":")
}
func orderFactTime(r LargeOrder, fetched time.Time) time.Time {
	at := fetched
	if r.Changed != nil {
		at = *r.Changed
	}
	if r.End != nil && r.End.After(at) {
		at = *r.End
	}
	return at
}
func (w *Warehouse) initOrders() error {
	_, err := w.db.Exec(`CREATE TABLE IF NOT EXISTS tracked_orders(k TEXT PRIMARY KEY,asset TEXT,venue TEXT,seen INTEGER,payload BLOB);
 CREATE INDEX IF NOT EXISTS tracked_asset ON tracked_orders(asset,seen);
 CREATE TABLE IF NOT EXISTS order_events(k TEXT PRIMARY KEY,order_key TEXT,asset TEXT,ts INTEGER,kind TEXT,payload BLOB);
 CREATE INDEX IF NOT EXISTS order_event_window ON order_events(asset,ts);
 CREATE INDEX IF NOT EXISTS order_event_identity ON order_events(order_key);
 CREATE TABLE IF NOT EXISTS order_hours(asset TEXT,ts INTEGER,payload BLOB,PRIMARY KEY(asset,ts));
 CREATE TABLE IF NOT EXISTS order_gaps(dataset TEXT,ts INTEGER,PRIMARY KEY(dataset,ts));
 CREATE TABLE IF NOT EXISTS order_storage(id INTEGER PRIMARY KEY,bytes INTEGER NOT NULL);
 INSERT OR IGNORE INTO order_storage SELECT 1,coalesce((SELECT sum(length(payload)+length(k)+length(order_key)+128) FROM order_events),0)+coalesce((SELECT sum(length(payload)+length(k)+128) FROM tracked_orders),0)+coalesce((SELECT sum(length(payload)+64) FROM order_hours),0);`)
	if err != nil {
		return err
	}
	for _, table := range []string{"tracked_orders", "order_events", "order_hours"} {
		size := "length(NEW.payload)+64"
		old := "length(OLD.payload)+64"
		if table != "order_hours" {
			size = "length(NEW.payload)+length(NEW.k)+128"
			old = "length(OLD.payload)+length(OLD.k)+128"
		}
		if table == "order_events" {
			size += "+length(NEW.order_key)"
			old += "+length(OLD.order_key)"
		}
		for _, q := range []string{
			"CREATE TRIGGER IF NOT EXISTS " + table + "_size_i AFTER INSERT ON " + table + " BEGIN UPDATE order_storage SET bytes=bytes+" + size + " WHERE id=1; END;",
			"CREATE TRIGGER IF NOT EXISTS " + table + "_size_u AFTER UPDATE ON " + table + " BEGIN UPDATE order_storage SET bytes=bytes+(" + size + ")-(" + old + ") WHERE id=1; END;",
			"CREATE TRIGGER IF NOT EXISTS " + table + "_size_d AFTER DELETE ON " + table + " BEGIN UPDATE order_storage SET bytes=bytes-(" + old + ") WHERE id=1; END;",
		} {
			if _, err = w.db.Exec(q); err != nil {
				return err
			}
		}
	}
	return nil
}

// Called under the warehouse writer lock. Current and ended lists share one
// identity. Cumulative execution is never attributed to the first observation.
func (w *Warehouse) ingestOrders(d Dataset, o Observation) error {
	tx, err := w.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	w.mu.RLock()
	paused := w.status.Paused || w.status.OrderPaused
	w.mu.RUnlock()
	var bytes int64
	if err = tx.QueryRow("SELECT bytes FROM order_storage WHERE id=1").Scan(&bytes); err != nil {
		return err
	}
	if paused || bytes >= OrderHistoryLimit {
		if _, err = tx.Exec("INSERT OR IGNORE INTO order_gaps VALUES(?,?)", d.ID, o.FetchedAt.Truncate(time.Hour).Unix()); err != nil {
			return err
		}
	}
	for _, r := range o.Payload.Large {
		key := orderKey(d, r)
		fact := orderFactTime(r, o.FetchedAt)
		factFields := r
		factFields.Changed = nil
		rev := digest(Observation{Payload: Payload{Large: []LargeOrder{factFields}}})
		next := TrackedOrder{o.FetchedAt, key, ID("large", d.Asset, d.Venue, d.Market), d.Asset, d.Venue, d.Quote, r, o.FetchedAt, fact, rev}
		var old TrackedOrder
		var raw []byte
		err = tx.QueryRow("SELECT payload FROM tracked_orders WHERE k=?", key).Scan(&raw)
		exists := err == nil
		if err != nil && err != sql.ErrNoRows {
			return err
		}
		if (!exists && paused) || (!exists && bytes >= OrderHistoryLimit) {
			continue
		}
		if exists {
			if err = json.Unmarshal(raw, &old); err != nil {
				return err
			}
		}
		if exists {
			next.FirstSeen = old.FirstSeen
			if next.FirstSeen.IsZero() {
				next.FirstSeen = old.Seen
			}
		}
		if exists && (fact.Before(old.FactAt) || o.FetchedAt.Before(old.Seen)) {
			continue
		}
		// A finished record cannot be resurrected by an older active-list response.
		if exists && old.Order.RawState > 1 && r.RawState == 1 && !fact.After(old.FactAt) {
			continue
		}
		event := OrderEvent{Key: key + ":" + rev, Asset: d.Asset, Venue: d.Venue, Quote: d.Quote, OrderID: r.ID, Side: r.Side, Price: r.Price, Kind: "discovered", At: o.FetchedAt, Note: "首次发现；累计历史成交不计入本期增量"}
		changed := !exists || old.Revision != rev
		event.After = &r
		if exists && changed {
			event.Before = &old.Order
			event.From = &old.Seen
			event.Kind = "change"
			dq := dec(r.Quantity).Sub(dec(old.Order.Quantity)).String()
			event.QuantityDelta = &dq
			de := dec(r.ExecutedUSD).Sub(dec(old.Order.ExecutedUSD))
			execKnown := r.ExecutedQuantity != nil && old.Order.ExecutedQuantity != nil
			eq := dec("0")
			if execKnown {
				eq = dec(*r.ExecutedQuantity).Sub(dec(*old.Order.ExecutedQuantity))
			}
			correction := (execKnown && eq.IsNegative()) || de.IsNegative() || (fact.Equal(old.FactAt) && r.RawState == old.Order.RawState)
			if correction {
				event.Kind = "correction"
				event.QuantityDelta = nil
				event.Note = "上游修正；本订单旧增量已撤回，重新建立基线"
				if _, err = tx.Exec("DELETE FROM order_events WHERE order_key=? AND kind IN ('change','ended','revoked')", key); err != nil {
					return err
				}
			} else {
				if execKnown {
					q := eq.String()
					event.ExecutedQuantityDelta = &q
				}
				if execKnown && eq.IsPositive() {
					delta := de.String()
					event.ExecutedDelta = &delta
				}
				event.Note = "两次采样之间发现的累计成交变化；不是逐笔成交，也不是主动买卖方向"
			}
		}
		if r.RawState == 2 && event.Kind != "correction" {
			event.Kind = "ended"
			event.Note = "上游记录已结束；余量不代表当前仍在挂单"
		}
		if r.RawState == 3 && event.Kind != "correction" {
			event.Kind = "revoked"
			event.Note = "上游标记撤销；不推断挂单者身份或意图"
		}
		if !exists && r.RawState > 1 {
			event.From = nil
			event.ExecutedDelta = nil
		}
		b, e := json.Marshal(next)
		if e != nil {
			return e
		}
		if _, err = tx.Exec("INSERT INTO tracked_orders VALUES(?,?,?,?,?) ON CONFLICT(k) DO UPDATE SET seen=excluded.seen,payload=excluded.payload", key, d.Asset, d.Venue, o.FetchedAt.Unix(), b); err != nil {
			return err
		}
		if changed && !paused && bytes < OrderHistoryLimit {
			b, e = json.Marshal(event)
			if e != nil {
				return e
			}
			if _, err = tx.Exec("INSERT INTO order_events VALUES(?,?,?,?,?,?) ON CONFLICT(k) DO UPDATE SET payload=excluded.payload,kind=excluded.kind,ts=excluded.ts", event.Key, key, d.Asset, event.At.Unix(), event.Kind, b); err != nil {
				return err
			}
		}
		if err = tx.QueryRow("SELECT bytes FROM order_storage WHERE id=1").Scan(&bytes); err != nil {
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	w.mu.Lock()
	w.status.OrderBytes = bytes
	w.status.OrderPaused = bytes >= OrderHistoryLimit
	w.mu.Unlock()
	return nil
}
func (w *Warehouse) OrderEvents(ctx context.Context, asset string, from, to time.Time, limit, offset int) ([]OrderEvent, bool, error) {
	rows, err := w.db.QueryContext(ctx, "SELECT payload FROM order_events WHERE asset=? AND ts>=? AND ts<? ORDER BY ts DESC,k LIMIT ? OFFSET ?", asset, from.Unix(), to.Unix(), limit+1, offset)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	out := []OrderEvent{}
	for rows.Next() {
		var b []byte
		var e OrderEvent
		if err = rows.Scan(&b); err != nil {
			return nil, false, err
		}
		if err = json.Unmarshal(b, &e); err != nil {
			return nil, false, err
		}
		out = append(out, e)
	}
	more := len(out) > limit
	if more {
		out = out[:limit]
	}
	return out, more, rows.Err()
}
func (w *Warehouse) EndedOrders(ctx context.Context, asset string, limit, offset int) ([]TrackedOrder, bool, error) {
	// The JSON flag is inspected in SQL, so pagination covers only ended records.
	rows, err := w.db.QueryContext(ctx, `SELECT payload FROM tracked_orders WHERE asset=? AND json_extract(payload,'$.order.rawState')>1 ORDER BY seen DESC,k LIMIT ? OFFSET ?`, asset, limit+1, offset)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	out := []TrackedOrder{}
	for rows.Next() {
		var b []byte
		var r TrackedOrder
		if err = rows.Scan(&b); err != nil {
			return nil, false, err
		}
		if err = json.Unmarshal(b, &r); err != nil {
			return nil, false, err
		}
		out = append(out, r)
	}
	more := len(out) > limit
	if more {
		out = out[:limit]
	}
	return out, more, rows.Err()
}
func (w *Warehouse) maintainOrders(ctx context.Context, now time.Time, days int) error {
	if _, err := w.db.ExecContext(ctx, "DELETE FROM liquidity_events WHERE ts<?", now.Add(-time.Duration(days)*24*time.Hour).Unix()); err != nil {
		return err
	}
	w.write.Lock()
	defer w.write.Unlock()
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Summaries are replaceable counts, not sums of cumulative fills. Recompute
	// before deleting detail, and retain their explicit sampled-event semantics.
	_, err = tx.ExecContext(ctx, `INSERT INTO order_hours SELECT asset,(ts/3600)*3600,json_object('observedEvents',count(*),'kind','sampled_order_events') FROM order_events WHERE ts<? GROUP BY asset,(ts/3600) ON CONFLICT(asset,ts) DO UPDATE SET payload=excluded.payload`, now.Add(-30*24*time.Hour).Truncate(time.Hour).Unix())
	if err != nil {
		return err
	}
	for _, q := range []struct {
		sql string
		at  time.Time
	}{
		{"DELETE FROM order_events WHERE ts<?", now.Add(-30 * 24 * time.Hour).Truncate(time.Hour)},
		{"DELETE FROM tracked_orders WHERE seen<?", now.Add(-30 * 24 * time.Hour).Truncate(time.Hour)},
		{"DELETE FROM order_gaps WHERE ts<?", now.Add(-time.Duration(days) * 24 * time.Hour)},
		{"DELETE FROM order_hours WHERE ts<?", now.Add(-time.Duration(days) * 24 * time.Hour)},
	} {
		if _, err = tx.ExecContext(ctx, q.sql, q.at.Unix()); err != nil {
			return err
		}
	}
	var bytes int64
	if err = tx.QueryRowContext(ctx, `SELECT coalesce((SELECT sum(length(payload)+length(k)+length(order_key)+128) FROM order_events),0)+coalesce((SELECT sum(length(payload)+length(k)+128) FROM tracked_orders),0)+coalesce((SELECT sum(length(payload)+64) FROM order_hours),0)+coalesce((SELECT sum(length(payload)+length(k)+128) FROM liquidity_events),0)`).Scan(&bytes); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	w.mu.Lock()
	w.status.OrderBytes = bytes
	w.status.OrderPaused = bytes >= OrderHistoryLimit
	w.mu.Unlock()
	return nil
}

func (w *Warehouse) OrderGap(ctx context.Context, asset string, from, to time.Time) (bool, error) {
	var n int
	err := w.db.QueryRowContext(ctx, "SELECT count(*) FROM order_gaps WHERE dataset LIKE ? AND ts>=? AND ts<?", "%."+strings.ToLower(asset)+".%", from.Truncate(time.Hour).Unix(), to.Unix()).Scan(&n)
	return n > 0, err
}
