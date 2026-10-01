package datahub

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"
)

const orderZoneLimit = 64 << 20

// These are actual successful list observations, not inferred lifetimes. Keeping
// unchanged observations is necessary to distinguish stability from a gap.
func (w *Warehouse) initOrderZoneSamples() error {
	_, err := w.db.Exec(`CREATE TABLE IF NOT EXISTS order_zone_samples(dataset TEXT,asset TEXT,ts INTEGER,payload BLOB,PRIMARY KEY(dataset,ts));
CREATE INDEX IF NOT EXISTS order_zone_sample_time ON order_zone_samples(asset,ts);
CREATE INDEX IF NOT EXISTS order_zone_sample_age ON order_zone_samples(ts,dataset);
CREATE TABLE IF NOT EXISTS order_zone_storage(id INTEGER PRIMARY KEY,bytes INTEGER NOT NULL);
INSERT OR IGNORE INTO order_zone_storage SELECT 1,coalesce(sum(length(payload)+length(dataset)+128),0) FROM order_zone_samples;
CREATE TRIGGER IF NOT EXISTS order_zone_sample_i AFTER INSERT ON order_zone_samples BEGIN
 UPDATE order_zone_storage SET bytes=bytes+length(NEW.payload)+length(NEW.dataset)+128 WHERE id=1;
 UPDATE order_storage SET bytes=bytes+length(NEW.payload)+length(NEW.dataset)+128 WHERE id=1; END;
CREATE TRIGGER IF NOT EXISTS order_zone_sample_d AFTER DELETE ON order_zone_samples BEGIN
 UPDATE order_zone_storage SET bytes=bytes-length(OLD.payload)-length(OLD.dataset)-128 WHERE id=1;
 UPDATE order_storage SET bytes=bytes-length(OLD.payload)-length(OLD.dataset)-128 WHERE id=1; END;`)
	return err
}

// Called under the existing writer lock. No network request or extra collector.
func (w *Warehouse) recordOrderZoneSample(d Dataset, o Observation) error {
	if d.Kind != "large" || o.Quality != "valid" {
		return nil
	}
	w.mu.RLock()
	paused := w.status.Paused
	w.mu.RUnlock()
	if paused {
		return w.SaveState("orderZoneGapAt", o.FetchedAt)
	}
	s := Observation{Dataset: d.ID, Source: d.Source, FetchedAt: o.FetchedAt, ObservedAt: o.ObservedAt, TimeBasis: o.TimeBasis, Quality: o.Quality, Resolution: 300, Payload: Payload{Large: []LargeOrder{}}}
	seen := map[string]LargeOrder{}
	for _, r := range o.Payload.Large {
		if r.RawState != 1 {
			continue
		}
		if old, ok := seen[r.ID]; ok {
			if old.Price != r.Price || old.Quantity != r.Quantity || old.Side != r.Side {
				return nil
			}
			continue
		}
		seen[r.ID] = r
		var raw []byte
		var canonical TrackedOrder
		err := w.db.QueryRow("SELECT payload FROM tracked_orders WHERE k=?", orderKey(d, r)).Scan(&raw)
		if err != nil && err != sql.ErrNoRows {
			return err
		}
		if err == nil {
			if err = json.Unmarshal(raw, &canonical); err != nil {
				return err
			}
			if canonical.Order.RawState > 1 || canonical.Order.Price != r.Price || canonical.Order.Quantity != r.Quantity {
				return nil
			}
		}
		s.Payload.Large = append(s.Payload.Large, LargeOrder{ID: r.ID, Side: r.Side, Price: r.Price, Quantity: r.Quantity, RawState: r.RawState})
	}
	if d.Quote == "USD" {
		s.Payload.Rates = []Rate{{Quote: "USD", USD: "1"}}
	} else if fx, ok := w.Latest("fx.usd.kraken"); ok {
		fd, _ := FindDataset("fx.usd.kraken")
		if fx.Fresh(fd, o.FetchedAt) && !fx.Time().After(o.FetchedAt) {
			for _, r := range fx.Payload.Rates {
				if r.Quote == d.Quote && dec(r.USD).IsPositive() {
					s.Payload.Rates = append(s.Payload.Rates, r)
				}
			}
			s.Dependencies = map[string]string{"fxAt": fx.Time().Format(time.RFC3339Nano)}
		}
	}
	b, err := pack(s)
	if err != nil {
		return err
	}
	if len(b) > 1<<20 {
		return w.SaveState("orderZoneGapAt", o.FetchedAt)
	}
	tx, err := w.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var exists int
	if err = tx.QueryRow("SELECT count(*) FROM order_zone_samples WHERE dataset=? AND ts=?", d.ID, o.FetchedAt.UnixMilli()).Scan(&exists); err != nil {
		return err
	}
	if exists > 0 {
		return nil
	}
	if _, err = tx.Exec("DELETE FROM order_zone_samples WHERE ts<?", o.FetchedAt.Add(-7*24*time.Hour).UnixMilli()); err != nil {
		return err
	}
	var beforeTrim int64
	if err = tx.QueryRow("SELECT bytes FROM order_zone_storage WHERE id=1").Scan(&beforeTrim); err != nil {
		return err
	}
	fit, err := trimOrderZoneSamples(tx, int64(len(b)+len(d.ID)+128), orderZoneLimit, OrderHistoryLimit)
	if err != nil {
		return err
	}
	var afterTrim int64
	if err = tx.QueryRow("SELECT bytes FROM order_zone_storage WHERE id=1").Scan(&afterTrim); err != nil {
		return err
	}
	if afterTrim < beforeTrim {
		if _, err = tx.Exec("INSERT INTO state VALUES('orderZoneGapAt',?) ON CONFLICT(key) DO UPDATE SET payload=excluded.payload", `"`+o.FetchedAt.Format(time.RFC3339Nano)+`"`); err != nil {
			return err
		}
	}
	if fit {
		_, err = tx.Exec("INSERT OR IGNORE INTO order_zone_samples VALUES(?,?,?,?)", d.ID, d.Asset, o.FetchedAt.UnixMilli(), b)
	} else {
		_, err = tx.Exec("INSERT INTO state VALUES('orderZoneGapAt',?) ON CONFLICT(key) DO UPDATE SET payload=excluded.payload", `"`+o.FetchedAt.Format(time.RFC3339Nano)+`"`)
	}
	if err != nil {
		return err
	}
	var total int64
	if err = tx.QueryRow("SELECT bytes FROM order_storage WHERE id=1").Scan(&total); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	w.mu.Lock()
	w.status.OrderBytes = total
	w.status.OrderPaused = total >= OrderHistoryLimit
	w.mu.Unlock()
	return nil
}

func trimOrderZoneSamples(tx *sql.Tx, incoming, ownLimit, sharedLimit int64) (bool, error) {
	// Bounded work per ingest, only evict this feature's oldest observations.
	for n := 0; n < 32; n++ {
		var own, all int64
		if err := tx.QueryRow("SELECT bytes FROM order_zone_storage WHERE id=1").Scan(&own); err != nil {
			return false, err
		}
		if err := tx.QueryRow("SELECT bytes FROM order_storage WHERE id=1").Scan(&all); err != nil {
			return false, err
		}
		if own+incoming <= ownLimit && all+incoming <= sharedLimit {
			return true, nil
		}
		if own == 0 {
			return false, nil
		}
		if _, err := tx.Exec("DELETE FROM order_zone_samples WHERE (dataset,ts) IN (SELECT dataset,ts FROM order_zone_samples ORDER BY ts,dataset LIMIT 8)"); err != nil {
			return false, err
		}
	}
	return false, nil
}

func (w *Warehouse) visitOrderZoneSamples(ctx context.Context, asset string, from, to time.Time, fn func(Observation) error) error {
	rows, err := w.db.QueryContext(ctx, "SELECT payload FROM order_zone_samples WHERE asset=? AND ts>=? AND ts<? ORDER BY ts,dataset", asset, from.UnixMilli(), to.UnixMilli())
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		if err = ctx.Err(); err != nil {
			return err
		}
		var b []byte
		if err = rows.Scan(&b); err != nil {
			return err
		}
		o, e := unpack(b)
		if e != nil {
			return e
		}
		if err = fn(o); err != nil {
			return err
		}
	}
	return rows.Err()
}
