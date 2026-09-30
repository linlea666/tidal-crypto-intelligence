package datahub

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

func (w *Warehouse) initLiquidationZones() error {
	_, e := w.research.Exec(`
 CREATE TABLE IF NOT EXISTS lz_records(kind TEXT,id TEXT,asset TEXT,at INTEGER,payload BLOB,done INTEGER DEFAULT 0,PRIMARY KEY(kind,id)) WITHOUT ROWID;
 CREATE INDEX IF NOT EXISTS lz_time ON lz_records(kind,asset,at);
 CREATE TABLE IF NOT EXISTS lz_budget(id INTEGER PRIMARY KEY,used INTEGER NOT NULL);
 INSERT OR IGNORE INTO lz_budget VALUES(1,0);
 CREATE TRIGGER IF NOT EXISTS lz_insert BEFORE INSERT ON lz_records WHEN NOT EXISTS(SELECT 1 FROM lz_records WHERE kind=NEW.kind AND id=NEW.id) AND (SELECT used FROM lz_budget WHERE id=1)+length(NEW.payload)+256>67108864 BEGIN SELECT RAISE(ABORT,'liquidation sub-budget full'); END;
 CREATE TRIGGER IF NOT EXISTS lz_update BEFORE UPDATE OF payload ON lz_records WHEN (SELECT used FROM lz_budget WHERE id=1)+length(NEW.payload)-length(OLD.payload)>67108864 BEGIN SELECT RAISE(ABORT,'liquidation sub-budget full'); END;
 CREATE TRIGGER IF NOT EXISTS lz_added AFTER INSERT ON lz_records BEGIN UPDATE lz_budget SET used=used+length(NEW.payload)+256 WHERE id=1; END;
 CREATE TRIGGER IF NOT EXISTS lz_changed AFTER UPDATE OF payload ON lz_records BEGIN UPDATE lz_budget SET used=used+length(NEW.payload)-length(OLD.payload) WHERE id=1; END;
 CREATE TRIGGER IF NOT EXISTS lz_removed AFTER DELETE ON lz_records BEGIN UPDATE lz_budget SET used=used-length(OLD.payload)-256 WHERE id=1; END;`)
	if e != nil {
		return e
	}
	b, _ := liquidationJSON(time.Now().UTC())
	_, e = w.research.Exec("INSERT OR IGNORE INTO lz_records(kind,id,asset,at,payload) VALUES('origin',?,'BTC',?,?)", LiquidationRules, time.Now().Unix(), b)
	return e
}

type liquidationGap struct {
	At     time.Time `json:"at"`
	Reason string    `json:"reason"`
	Paused bool      `json:"paused"`
}

func (w *Warehouse) liquidationGap(at time.Time, e error) {
	_ = w.SaveState("liquidation/gap", liquidationGap{at, e.Error(), true})
}
func (w *Warehouse) liquidationLoad(ctx context.Context, kind, id string, v any) error {
	return liquidationLoad(ctx, w.research, kind, id, v)
}

func liquidationLoad(ctx context.Context, db *sql.DB, kind, id string, v any) error {
	var b []byte
	e := db.QueryRowContext(ctx, "SELECT payload FROM lz_records WHERE kind=? AND id=?", kind, id).Scan(&b)
	if e != nil {
		return e
	}
	if len(b) > liquidationRowLimit {
		return errors.New("清算研究数据超过读取上限")
	}
	return liquidationDecode(b, v)
}
func (w *Warehouse) liquidationPut(ctx context.Context, kind, id, asset string, at time.Time, v any) error {
	if w.Status().ResearchPaused || w.Status().Paused {
		return errors.New("研究容量保护")
	}
	b, e := liquidationJSON(v)
	if e != nil {
		return e
	}
	_, e = w.research.ExecContext(ctx, `INSERT INTO lz_records(kind,id,asset,at,payload) VALUES(?,?,?,?,?) ON CONFLICT(kind,id) DO UPDATE SET at=excluded.at,payload=excluded.payload`, kind, id, asset, at.UnixNano(), b)
	return e
}

// Runs at ingestion, not in a GET. Errors never block the original map snapshot.
func (w *Warehouse) recordLiquidationMap(d Dataset, o Observation) {
	if d.Kind != "map" || o.Payload.Model == nil || o.Payload.Model.Contract != LiquidationContract {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	now := time.Now().UTC()
	var origin time.Time
	if e := w.liquidationLoad(ctx, "origin", LiquidationRules, &origin); e != nil {
		w.liquidationGap(now, e)
		return
	}
	// A pre-upgrade/backfilled snapshot cannot become a forward observation.
	if o.FetchedAt.Before(origin) {
		return
	}
	s, e := makeLiquidationMap(d, o, now)
	if !o.Fresh(d, now) {
		s.Complete = false
		s.Reason = "过期模型仅用于历史浏览"
	}
	if e != nil {
		w.liquidationGap(now, e)
		return
	}
	if w.Status().ResearchPaused || w.Status().Paused {
		w.liquidationGap(now, errors.New("研究容量保护"))
		return
	}
	pd, _ := FindDataset(ID("price", d.Asset, "Binance", "spot"))
	if po, ok := w.Latest(pd.ID); ok && po.Payload.Price != nil && po.Payload.Price.Quote == s.Quote && po.Fresh(pd, now) {
		v := num(po.Payload.Price.Value)
		s.MarketPrice = &v
		at := po.Time()
		s.MarketAt = &at
	}
	b, e := liquidationJSON(s)
	if e == nil {
		_, e = w.research.ExecContext(ctx, "INSERT OR IGNORE INTO lz_records(kind,id,asset,at,payload) VALUES('map',?,?,?,?)", d.ID+"/"+o.FetchedAt.Format(time.RFC3339Nano)+"/"+o.Revision, d.Asset, now.UnixNano(), b)
	}
	if e != nil {
		w.liquidationGap(now, e)
	}
}

type liquidationRecord struct {
	ID      string
	At      int64
	Payload []byte
}

func (w *Warehouse) liquidationRows(ctx context.Context, kind, asset string, from, to int64, limit int) ([]liquidationRecord, error) {
	rows, e := w.research.QueryContext(ctx, "SELECT id,at,payload FROM lz_records WHERE kind=? AND asset=? AND at>=? AND at<? ORDER BY at,id LIMIT ?", kind, asset, from, to, min(500, limit))
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []liquidationRecord{}
	used := 0
	for rows.Next() {
		var r liquidationRecord
		if e = rows.Scan(&r.ID, &r.At, &r.Payload); e != nil {
			return nil, e
		}
		used += len(r.Payload)
		if used > liquidationWorkLimit/2 {
			return nil, errors.New("清算研究读取达到内存保护上限")
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
func (w *Warehouse) pruneLiquidations(ctx context.Context, now time.Time) error {
	_, e := w.research.ExecContext(ctx, "DELETE FROM lz_records WHERE (kind!='origin' AND at<?) OR (kind='map' AND done=1 AND at<?)", now.Add(-30*24*time.Hour).UnixNano(), now.Add(-24*time.Hour).UnixNano())
	return e
}

func (h *Hub) liquidationWorker(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		h.processLiquidations(ctx, time.Now().UTC())
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// Each tick is one bounded transaction/checkpoint. A backlog resumes next tick.
func (h *Hub) processLiquidations(parent context.Context, now time.Time) {
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	if e := h.liquidationBatch(ctx, now); e != nil {
		h.Store.liquidationGap(now, e)
	}
}
func (h *Hub) liquidationBatch(ctx context.Context, now time.Time) error {
	if h.Store.Status().ResearchPaused || h.Store.Status().Paused {
		return errors.New("清算研究容量保护")
	}
	var id string
	var b []byte
	e := h.Store.research.QueryRowContext(ctx, "SELECT id,payload FROM lz_records WHERE kind='map' AND done=0 ORDER BY at,id LIMIT 1").Scan(&id, &b)
	if e != nil && e != sql.ErrNoRows {
		return e
	}
	if e == nil {
		var s LiquidationMapSnapshot
		if e = liquidationDecode(b, &s); e != nil {
			return e
		}
		if s.Complete {
			if e = h.processLiquidationMap(ctx, s); e != nil {
				return e
			}
		}
		if _, e = h.Store.research.ExecContext(ctx, "UPDATE lz_records SET done=1 WHERE kind='map' AND id=?", id); e != nil {
			return e
		}
	}
	// Update lifecycle even when the next map has not arrived. State checkpoints
	// preserve the first evaluated candle revision; corrections never rewrite it.
	if e = h.advanceLiquidationStates(ctx, now); e != nil {
		return e
	}
	if e = h.selectCurrentLiquidationStudy(ctx, now); e != nil {
		return e
	}
	if e = h.advanceLiquidationStudy(ctx, now); e != nil {
		return e
	}
	var gap liquidationGap
	h.Store.LoadState("liquidation/gap", &gap)
	gap.Paused = false
	_ = h.Store.SaveState("liquidation/gap", gap)
	return nil
}
func (h *Hub) processLiquidationMap(ctx context.Context, s LiquidationMapSnapshot) error {
	var prior LiquidationMapSnapshot
	e := h.Store.liquidationLoad(ctx, "state", s.Dataset, &prior)
	if e != nil && e != sql.ErrNoRows {
		return e
	}
	if !s.Available.After(prior.Available) {
		if s.Available.Equal(prior.Available) && s.Asset == "BTC" && s.Period == "24h" {
			return h.selectLiquidationStudy(ctx, prior)
		}
		return nil
	}
	old := map[string]LiquidationZone{}
	for _, z := range prior.Zones {
		old[z.Key] = z
	}
	zones := make([]LiquidationZone, 0, len(s.Zones))
	seen := map[string]bool{}
	for _, z := range s.Zones {
		seen[z.Key] = true
		var prev *LiquidationZone
		if p, ok := old[z.Key]; ok {
			prev = &p
		}
		zones = append(zones, evolveLiquidationZone(prev, z, s))
	}
	if prior.Comparable == s.Comparable {
		for key, z := range old {
			if !seen[key] && z.Missing < 2 {
				z.Missing++
				if z.Missing >= 2 {
					z.State = "disappeared"
				}
				zones = append(zones, z)
			}
		}
	}
	s.Zones = zones
	// Freeze the source map and transition log together with the restart cursor.
	tx, e := h.Store.research.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	// One compressed distribution per snapshot avoids a row and repeated source
	// identities for every bucket. Live state keeps contributions for inspection.
	history := compactLiquidationSnapshot(s)
	hb, e := liquidationJSON(history)
	if e != nil {
		return e
	}
	if _, e = tx.ExecContext(ctx, "INSERT OR IGNORE INTO lz_records(kind,id,asset,at,payload) VALUES('history',?,?,?,?)", s.Dataset+"/"+s.Available.Format(time.RFC3339Nano), s.Asset, s.Available.UnixNano(), hb); e != nil {
		return e
	}

	payload, e := liquidationJSON(s)
	if e != nil {
		return e
	}
	_, e = tx.ExecContext(ctx, `INSERT INTO lz_records(kind,id,asset,at,payload) VALUES('state',?,?,?,?) ON CONFLICT(kind,id) DO UPDATE SET at=excluded.at,payload=excluded.payload`, s.Dataset, s.Asset, s.Available.UnixNano(), payload)
	if e != nil {
		return e
	}
	if e = tx.Commit(); e != nil {
		return e
	}
	if s.Period == "24h" && s.Asset == "BTC" {
		return h.selectLiquidationStudy(ctx, s)
	}
	return nil
}
func (h *Hub) liquidationCandles(ctx context.Context, a string, from, to, asOf time.Time) (map[int64]Candle, error) {
	return h.liquidationCandleSeries(ctx, a, from, to, asOf, true)
}
func (h *Hub) liquidationCandleSeries(ctx context.Context, a string, from, to, asOf time.Time, timely bool) (map[int64]Candle, error) {
	return liquidationCandleSeries(ctx, h.Store.research, a, from, to, asOf, timely)
}

func liquidationCandleSeries(ctx context.Context, db *sql.DB, a string, from, to, asOf time.Time, timely bool) (map[int64]Candle, error) {
	out := map[int64]Candle{}
	if to.Sub(from) > 31*24*time.Hour {
		return nil, errors.New("清算K线窗口过大")
	}
	e := factsAsOf(ctx, db, ID("candles", a, "Binance", "spot"), from, to, asOf, func(o Observation) error {
		at := recordTime(o)
		available := o.FetchedAt
		if o.FirstFetchedAt != nil {
			available = *o.FirstFetchedAt
		}
		if o.Quality == "valid" && o.Payload.Candle != nil && validLiquidationCandle(*o.Payload.Candle) && o.Resolution == 300 && !at.Add(5*time.Minute).After(to) && (!timely || !available.After(at.Add(20*time.Minute))) {
			out[at.Unix()] = *o.Payload.Candle
		}
		if len(out) > 9000 {
			return errors.New("清算K线工作集上限")
		}
		return nil
	})
	return out, e
}
func (h *Hub) advanceLiquidationStates(ctx context.Context, now time.Time) error {
	for _, a := range Assets() {
		for _, period := range []string{"24h", "7d", "30d"} {
			id := ID("map", a, "", "futures")
			if period != "24h" {
				id += "@" + period
			}
			var s LiquidationMapSnapshot
			e := h.Store.liquidationLoad(ctx, "state", id, &s)
			if e == sql.ErrNoRows {
				continue
			}
			if e != nil {
				return e
			}
			// Price proxies are Binance USDT; an unverified cross-quote conversion must
			// not change native identity or silently produce touches.
			if s.Quote != "USDT" {
				continue
			}
			from := now.Add(-35 * time.Minute)
			for _, z := range s.Zones {
				if z.Missing >= 2 || z.Side == "neutral" {
					continue
				}
				start := z.LastBar.Add(5 * time.Minute)
				if z.LastBar.IsZero() {
					start = z.First
				}
				if start.Before(from) {
					from = start
				}
			}
			if from.Before(now.Add(-30 * 24 * time.Hour)) {
				from = now.Add(-30 * 24 * time.Hour)
			}
			to := minTime(now.Truncate(5*time.Minute), from.Add(6*time.Hour))
			candles, e := h.liquidationCandles(ctx, a, from.Truncate(5*time.Minute), to, now)
			if e != nil {
				return e
			}
			changed := false
			transitions := []LiquidationZone{}
			ref := h.liquidationReference(ctx, a, s.Quote, now)
			for i := range s.Zones {
				if len(transitions) >= 64 {
					break
				}
				z := &s.Zones[i]
				if z.Missing >= 2 || z.Side == "neutral" {
					continue
				}
				oldBar := z.LastBar
				for t := from.Truncate(5 * time.Minute); !t.Add(5 * time.Minute).After(to); t = t.Add(5 * time.Minute) {
					if t.Before(z.First) || !t.After(z.LastBar) {
						continue
					}
					before, uncertain := z.State, z.Uncertain
					if c, ok := candles[t.Unix()]; ok {
						applyLiquidationCandle(z, t, c)
					} else if !now.Before(t.Add(20 * time.Minute)) {
						z.Uncertain = "缺少完整5分钟K线"
						z.LastBar = t
						z.FarCloses, z.NearCloses = 0, 0
					} else {
						break
					}
					if z.State != before || z.Uncertain != uncertain {
						v := *z
						v.Contributions = nil
						transitions = append(transitions, v)
						if len(transitions) >= 64 {
							break
						}
					}
				}
				if !z.LastBar.Equal(oldBar) {
					changed = true
				}
				if len(transitions) < 64 && z.Touch == nil && z.Cross == nil && z.Uncertain == "" && now.Sub(s.Fetched) <= 35*time.Minute {
					before := z.State
					z.State = "new"
					if z.Samples >= 3 && z.Last.Sub(z.Continuous) >= 30*time.Minute {
						z.State = "persistent"
					}
					if ref.Native != nil && ref.ATR != nil && zoneDistance(*z, *ref.Native) <= .25**ref.ATR {
						z.State = "approaching"
					}
					if z.State != before {
						changed = true
						v := *z
						v.Contributions = nil
						transitions = append(transitions, v)
					}
				}
			}
			if changed {
				tx, e := h.Store.research.BeginTx(ctx, nil)
				if e != nil {
					return e
				}
				for _, z := range transitions {
					var raw []byte
					raw, e = liquidationJSON(z)
					if e != nil {
						break
					}
					_, e = tx.ExecContext(ctx, "INSERT OR IGNORE INTO lz_records(kind,id,asset,at,payload) VALUES('transition',?,?,?,?)", fmt.Sprintf("%s/%d/%d/%s", z.ID, now.UnixNano(), z.LastBar.Unix(), z.State), a, now.UnixNano(), raw)
					if e != nil {
						break
					}
				}
				if e == nil {
					raw, err := liquidationJSON(s)
					e = err
					if e == nil {
						_, e = tx.ExecContext(ctx, "UPDATE lz_records SET payload=? WHERE kind='state' AND id=?", raw, id)
					}
				}
				if e != nil {
					tx.Rollback()
					return e
				}
				if e = tx.Commit(); e != nil {
					return e
				}
			}
		}
	}
	return nil
}
