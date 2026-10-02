package datahub

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const ProgressVersion = "progress-2"
const progressGrace = 20 * time.Minute

type ProgressFact struct {
	Dataset   string    `json:"dataset"`
	At        time.Time `json:"at"`
	Revision  string    `json:"revision"`
	Available time.Time `json:"availableAt"`
}

type ProgressRepair struct {
	Version  string        `json:"evaluationVersion"`
	At       time.Time     `json:"repairedAt"`
	AsOf     time.Time     `json:"evidenceAsOf"`
	Original PriceProgress `json:"original"`
	Result   PriceProgress `json:"result"`
	Reason   string        `json:"reason"`
}

func progressTerminal(p *PriceProgress) bool {
	return p != nil && (strings.HasPrefix(p.Status, "completed_") || p.Status == "ended_with_gap")
}

// Read only the frozen tail, not today's current window. The fact ledger bounds
// revisions by when they were first actually available, including after restart.
func (h *Hub) finalPriceProgress(ctx context.Context, db *sql.DB, s Signal, now time.Time) (PriceProgress, error) {
	end := s.ConfirmedThrough.Add(4 * time.Hour)
	asOf := minTime(now, end.Add(progressGrace))
	acc := newFlowAccumulator(300)
	candles := map[int64]Candle{}
	evidence := []ProgressFact{}
	for _, id := range []string{ID("flow", s.Asset, "", "spot"), ID("candles", s.Asset, "Binance", "spot")} {
		e := factsAsOf(ctx, db, id, end.Add(-15*time.Minute), end, asOf, func(o Observation) error {
			at := recordTime(o)
			if o.FirstFetchedAt == nil || o.FirstFetchedAt.After(asOf) || !shortClosedFact(o) {
				return nil
			}
			if o.Payload.Candle != nil {
				if o.Resolution != 300 || at.Unix()%300 != 0 || at.Before(end.Add(-10*time.Minute)) || !validLiquidationCandle(*o.Payload.Candle) {
					return nil
				}
				candles[at.Unix()] = *o.Payload.Candle
			} else if o.Payload.Flow != nil {
				acc.add(o)
			} else {
				return nil
			}
			evidence = append(evidence, ProgressFact{id, at, o.Revision, *o.FirstFetchedAt})
			return nil
		})
		if e != nil {
			return PriceProgress{}, e
		}
	}
	p := priceProgress(s, acc.finish(), candles, end, now, true)
	p.Evidence = evidence
	return p, nil
}

// Append an audit result; do not rewrite the signal, its original progress,
// confirmation, notices, coverage or research origin. INSERT OR IGNORE is also
// the durable per-version resume cursor.
func (h *Hub) repairPriceProgress(ctx context.Context, now time.Time) error {
	if h.Store.Status().ResearchPaused || h.Store.Status().Paused {
		return nil
	}
	db := h.Store.shortDB()
	rows, e := db.QueryContext(ctx, `SELECT s.payload FROM documents s WHERE s.kind='signal' AND s.asset='BTC' AND json_extract(s.payload,'$.rulesVersion')=? AND json_extract(s.payload,'$.priceProgress.status')='ended_with_gap' AND json_extract(s.payload,'$.priceProgress.evaluationVersion') IS NULL AND json_extract(s.payload,'$.confirmedAt') IS NOT NULL AND unixepoch(json_extract(s.payload,'$.priceProgress.at'))>=unixepoch(json_extract(s.payload,'$.confirmedDataThrough'))+14400 AND unixepoch(json_extract(s.payload,'$.priceProgress.at'))<unixepoch(json_extract(s.payload,'$.confirmedDataThrough'))+15600 AND unixepoch(json_extract(s.payload,'$.priceProgress.dataThrough'))<unixepoch(json_extract(s.payload,'$.confirmedDataThrough'))+14400 AND NOT EXISTS(SELECT 1 FROM documents r WHERE r.kind='signal-progress-repair' AND r.id=s.id||'/progress-2') ORDER BY s.at LIMIT 4`, MultifactorRules)
	if e != nil {
		return e
	}
	signals := []Signal{}
	for rows.Next() {
		var b []byte
		if e = rows.Scan(&b); e != nil {
			break
		}
		var s Signal
		if e = json.Unmarshal(b, &s); e != nil {
			break
		}
		signals = append(signals, s)
	}
	if e == nil {
		e = rows.Err()
	}
	rows.Close()
	if e != nil {
		return e
	}
	for _, s := range signals {
		end := s.ConfirmedThrough.Add(4 * time.Hour)
		if s.Progress == nil || s.ConfirmedAt == nil || now.Before(end.Add(progressGrace)) {
			continue
		}
		// The SQL scan selects only premature old gaps; recheck exact Go times.
		r := ProgressRepair{Version: ProgressVersion, At: now, AsOf: end.Add(progressGrace), Original: *s.Progress}
		if !s.Progress.At.Before(end) && s.Progress.At.Before(end.Add(progressGrace)) && s.Progress.DataThrough.Before(end) {
			r.Result, e = h.finalPriceProgress(ctx, db, s, now)
			if e != nil {
				return e
			}
			r.Reason = "原逻辑在末段数据到达前结束；按原终点和20分钟可见期限追加审计，不追认提醒或补发邮件"
		} else {
			continue
		}
		b, e := json.Marshal(r)
		if e != nil {
			return e
		}
		if len(b) > 1<<20 {
			return fmt.Errorf("复盘修复记录超过上限")
		}
		if _, e = db.ExecContext(ctx, "INSERT OR IGNORE INTO documents VALUES('signal-progress-repair',?,'BTC',?,?)", s.ID+"/"+ProgressVersion, s.At.Unix(), b); e != nil {
			return shortWriteError(e)
		}
	}
	return nil
}

func (h *Hub) progressRepairs(ctx context.Context, asset string) (map[string]ProgressRepair, error) {
	rows, e := h.Store.research.QueryContext(ctx, "SELECT id,payload FROM documents WHERE kind='signal-progress-repair' AND asset=? ORDER BY at DESC LIMIT 100", asset)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := map[string]ProgressRepair{}
	for rows.Next() {
		var id string
		var b []byte
		if e = rows.Scan(&id, &b); e != nil {
			return nil, e
		}
		var v ProgressRepair
		if e = json.Unmarshal(b, &v); e != nil {
			return nil, e
		}
		out[strings.TrimSuffix(id, "/"+ProgressVersion)] = v
	}
	return out, rows.Err()
}
