package datahub

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

const ShortPipeline = "short-pipeline-2"

type shortBaselineV2 struct {
	shortBaselineWork
	Phase    string        `json:"phase"`
	Horizon  int           `json:"horizon"`
	Baseline ShortBaseline `json:"result"`
}

// Complete five-minute buckets are the checkpoint boundary. An incomplete last
// page bucket is re-read (at most six normal facts), never saved as a full bar.
func (h *Hub) shortFlowPage(ctx context.Context, w shortBaselineV2) ([]shortStoredBar, time.Time, error) {
	ctx, cancel := context.WithTimeout(ctx, 400*time.Millisecond)
	defer cancel()
	startedPage := time.Now()
	defer shortMeasure(ctx, "baseline_page", startedPage)
	queryStarted := time.Now()
	rows, e := h.Store.shortDB().QueryContext(ctx, `SELECT f.payload FROM facts f WHERE f.dataset=? AND f.ts>=? AND f.ts<? AND f.available<=? AND f.available=(SELECT max(g.available) FROM facts g WHERE g.dataset=f.dataset AND g.ts=f.ts AND g.res=f.res AND g.available<=?) ORDER BY f.ts,f.res LIMIT 257`, ID("flow", "BTC", "", "spot"), w.Cursor.Unix(), w.To.Unix(), w.AsOf.UnixNano(), w.AsOf.UnixNano())
	shortMeasure(ctx, "sql_read", queryStarted)
	if e != nil {
		return nil, w.Cursor, e
	}
	defer rows.Close()
	out := []shortStoredBar{}
	next := w.Cursor
	var bucket time.Time
	acc := newFlowAccumulator(300)
	count, size := 0, 0
	started := time.Now()
	flush := func() {
		defer shortMeasure(ctx, "aggregation", time.Now())
		if bucket.IsZero() {
			return
		}
		if b, ok := acc.finish()[bucket.Unix()]; ok {
			out = append(out, storeShortBar(b))
		}
		next = bucket.Add(5 * time.Minute)
		acc = newFlowAccumulator(300)
	}
	for {
		readStarted := time.Now()
		if !rows.Next() {
			shortMeasure(ctx, "sql_read", readStarted)
			break
		}
		var raw []byte
		if e = rows.Scan(&raw); e != nil {
			return nil, w.Cursor, e
		}
		shortMeasure(ctx, "sql_read", readStarted)
		count++
		size += len(raw)
		if count > 256 || size > 256<<10 {
			if !next.After(w.Cursor) {
				return nil, w.Cursor, errors.New("单个成交桶超过分页上限")
			}
			return out, next, nil
		}
		decodeStarted := time.Now()
		o, e := unpack(raw)
		shortMeasure(ctx, "decode", decodeStarted)
		if e != nil {
			return nil, w.Cursor, e
		}
		at := recordTime(o).Truncate(5 * time.Minute)
		if !at.Equal(bucket) {
			flush()
			if time.Since(started) > 400*time.Millisecond && next.After(w.Cursor) {
				return out, next, nil
			}
			bucket = at
		}
		if shortClosedFact(o) {
			aggStarted := time.Now()
			acc.add(o)
			shortMeasure(ctx, "aggregation", aggStarted)
		}
	}
	if e = rows.Err(); e != nil {
		return nil, w.Cursor, e
	}
	flush()
	return out, w.To, nil
}

func (h *Hub) shortBaselineAt(ctx context.Context, through, now time.Time) error {
	to := through.Truncate(time.Hour).Add(-time.Hour)
	from := to.Add(-30 * 24 * time.Hour)
	w := shortBaselineV2{shortBaselineWork: shortBaselineWork{Bars: make([]shortStoredBar, 0, 8640)}}
	loadStart := time.Now()
	e := h.Store.shortLoad(ctx, "work-v2", ShortFlowRules, &w)
	shortMeasure(ctx, "checkpoint_read", loadStart)
	newCheckpoint := e == sql.ErrNoRows
	if e != nil && e != sql.ErrNoRows {
		return e
	}
	if e == sql.ErrNoRows {
		// Reuse the existing durable bars once, without modifying old checkpoints.
		var legacy shortBaselineWork
		if e = h.Store.shortLoad(ctx, "work", ShortFlowRules, &legacy); e != nil && e != sql.ErrNoRows {
			return e
		}
		if e == nil {
			w.shortBaselineWork = legacy
			w.Phase = "read"
			if legacy.Cursor.Equal(legacy.To) {
				w.Phase = "ready"
			}
		}
	}
	id := ID("flow", "BTC", "", "spot")
	if w.Phase == "ready" {
		version, e := h.Store.shortRangeVersion(ctx, id, w.From, w.To)
		if e != nil {
			return e
		}
		if w.To.Equal(to) && version == w.Version {
			// Migration can reuse an already published legacy result.
			if w.Baseline.Windows == nil {
				var b ShortBaseline
				if e = h.Store.shortLoad(ctx, "baseline", ShortFlowRules, &b); e != nil {
					return e
				}
				w.Baseline = b
			}
			if !now.Before(w.To.Add(time.Hour)) {
				var current ShortBaseline
				if e := h.Store.shortLoad(ctx, "baseline", ShortFlowRules, &current); e != nil && e != sql.ErrNoRows {
					return e
				}
				if !current.To.Equal(w.To) {
					return h.Store.commitShortBaseline(ctx, w, now)
				}
			}
			if !newCheckpoint {
				return nil
			}
			return h.Store.shortPut(ctx, "work-v2", ShortFlowRules, now, w)
		}
		if version == w.Version && to.After(w.To) && to.Sub(w.To) <= 24*time.Hour {
			keep := make([]shortStoredBar, 0, 8640)
			for _, b := range w.Bars {
				if b[0] >= from.Unix() {
					keep = append(keep, b)
				}
			}
			w.From, w.To, w.AsOf, w.Bars = from, to, now, keep
			w.Phase = "read"
			w.Horizon = 0
			w.Baseline = ShortBaseline{}
			w.Version, e = h.Store.shortRangeVersion(ctx, id, from, to)
			if e != nil {
				return e
			}
		} else {
			w = shortBaselineV2{}
		}
	}
	if w.To.IsZero() {
		v, e := h.Store.shortRangeVersion(ctx, id, from, to)
		if e != nil {
			return e
		}
		w = shortBaselineV2{shortBaselineWork: shortBaselineWork{From: from, To: to, Cursor: from, AsOf: now, Version: v, Bars: make([]shortStoredBar, 0, 8640)}, Phase: "read"}
	}
	// Reserve time to persist completed pages. A saved yield is not a timeout.
	until := time.Now().Add(1200 * time.Millisecond)
	if d, ok := ctx.Deadline(); ok && d.Add(-300*time.Millisecond).Before(until) {
		until = d.Add(-300 * time.Millisecond)
	}
	var bars map[int64]FlowBar
	for time.Now().Before(until) {
		if e = ctx.Err(); e != nil {
			return e
		}
		if w.Cursor.Before(w.To) {
			page, next, err := h.shortFlowPage(ctx, w)
			if err != nil {
				// Preserve already completed pages, but still report the real error.
				if saveErr := h.Store.shortPut(ctx, "work-v2", ShortFlowRules, now, w); saveErr != nil {
					return errors.Join(err, saveErr)
				}
				return err
			}
			w.Bars = append(w.Bars, page...)
			w.Cursor = next
			if len(w.Bars) > 8640 {
				return errors.New("短周期基线工作集超限")
			}
			if w.Cursor.Before(w.To) {
				continue
			}
			w.Phase = "quantiles"
		}
		if bars == nil {
			bars = make(map[int64]FlowBar, len(w.Bars))
			for _, b := range w.Bars {
				bars[b[0]] = b.flow()
			}
		}
		if w.Baseline.Windows == nil {
			w.Baseline = shortBaselineCoverage(bars, w.From, w.To, w.AsOf)
		}
		if w.Horizon < 5 {
			m := []int{5, 10, 15, 60, 240}[w.Horizon]
			computeStart := time.Now()
			w.Baseline.Windows[fmt.Sprint(m)] = shortThreshold(bars, w.From, w.To, m)
			shortMeasure(ctx, "quantiles", computeStart)
			w.Horizon++
		}
		if w.Horizon == 5 {
			w.Phase = "ready"
			return h.Store.commitShortBaseline(ctx, w, now)
		}
	}
	shortYield(ctx)
	return h.Store.shortPut(ctx, "work-v2", ShortFlowRules, now, w)
}

func (w *Warehouse) commitShortBaseline(ctx context.Context, work shortBaselineV2, now time.Time) (err error) {
	start := time.Now()
	defer shortMeasure(ctx, "baseline_commit", start)
	defer func() { err = shortWriteError(err) }()
	if w.Status().Paused || w.Status().ResearchPaused {
		return errors.New("研究总容量保护")
	}
	a, e := json.Marshal(work)
	if e != nil {
		return e
	}
	b, e := json.Marshal(work.Baseline)
	if e != nil {
		return e
	}
	if len(a) > shortFlowRowLimit || len(b) > shortFlowRowLimit {
		return errors.New("基线检查点工作集超限")
	}
	tx, e := w.shortDB().BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	rows := []struct {
		kind, id string
		raw      []byte
	}{{"work-v2", ShortFlowRules, a}, {"baseline-v2", work.To.Format(time.RFC3339), b}}
	if !now.Before(work.To.Add(time.Hour)) {
		rows = append(rows, struct {
			kind, id string
			raw      []byte
		}{"baseline", ShortFlowRules, b})
	}
	for _, row := range rows {
		if _, e = tx.ExecContext(ctx, "INSERT INTO sf_records VALUES(?,?,?,?) ON CONFLICT(kind,id) DO UPDATE SET at=excluded.at,payload=excluded.payload", row.kind, row.id, now.Unix(), row.raw); e != nil {
			return e
		}
	}
	if _, e = tx.ExecContext(ctx, "DELETE FROM sf_records WHERE kind='baseline-v2' AND at<?", now.Add(-3*time.Hour).Unix()); e != nil {
		return e
	}
	return tx.Commit()
}
