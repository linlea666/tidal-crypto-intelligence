package datahub

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"
)

// The event ledger freezes clustering anchors. Rebuilding a moving 14-day
// window would otherwise move the four-hour grouping boundary on every report.
func (h *Hub) priceEventLedger(ctx context.Context, a string, now time.Time, candles map[int64]Candle, end time.Time, live bool) error {
	var seeded string
	if !h.Store.LoadState("forward/event-ledger/"+a, &seeded) || seeded != EvaluationVersion {
		_, history, e := h.signalInput(ctx, a, now.Add(-14*24*time.Hour-4*time.Hour), end, now)
		if e != nil {
			return e
		}
		// Reconstruction is explicitly marked and cannot count toward candidate readiness.
		for _, ev := range priceEpisodes(history, now.Add(-14*24*time.Hour).Truncate(5*time.Minute), end) {
			ev.ReconstructedAt = &now
			if e = h.insertPriceEvent(ctx, a, ev); e != nil {
				return e
			}
		}
		if e = h.Store.SaveState("forward/event-ledger/"+a, EvaluationVersion); e != nil {
			return e
		}
	}
	if !live {
		return nil
	}
	rows, e := h.Store.documents(ctx, "price-event", a, 100)
	if e != nil {
		return e
	}
	last := map[string]time.Time{}
	for _, raw := range rows {
		var ev PriceEpisode
		if json.Unmarshal(raw, &ev) == nil {
			last[ev.Direction] = maxTime(last[ev.Direction], ev.Start)
		}
	}
	start := end.Add(-10 * time.Minute)
	for _, side := range []string{"buy", "sell"} {
		if start.Sub(last[side]) < 4*time.Hour || !priceCross(candles, end, side) {
			continue
		}
		ev := PriceEpisode{ID: priceEventID(side, start), Direction: side, Start: start, Detected: now, DataThrough: end}
		if e = h.insertPriceEvent(ctx, a, ev); e != nil {
			return e
		}
	}
	return nil
}
func (h *Hub) insertPriceEvent(ctx context.Context, a string, ev PriceEpisode) error {
	b, e := json.Marshal(ev)
	if e != nil {
		return e
	}
	_, e = h.Store.research.ExecContext(ctx, "INSERT OR IGNORE INTO documents(kind,id,asset,at,payload) VALUES('price-event',?,?,?,?)", ev.ID, a, ev.Start.Unix(), b)
	return e
}
func (h *Hub) loadPriceEvents(ctx context.Context, a string, from, to time.Time) ([]PriceEpisode, error) {
	return loadPriceEvents(ctx, h.Store.research, a, from, to)
}

func loadPriceEvents(ctx context.Context, db *sql.DB, a string, from, to time.Time) ([]PriceEpisode, error) {
	rows, e := db.QueryContext(ctx, "SELECT payload FROM documents WHERE kind='price-event' AND asset=? AND at>=? AND at<? ORDER BY at", a, from.Unix(), to.Unix())
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []PriceEpisode{}
	for rows.Next() {
		var b []byte
		if e = rows.Scan(&b); e != nil {
			return nil, e
		}
		var ev PriceEpisode
		if e = json.Unmarshal(b, &ev); e != nil {
			return nil, e
		}
		out = append(out, ev)
	}
	return out, rows.Err()
}
