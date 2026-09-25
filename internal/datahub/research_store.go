package datahub

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const ResearchBudget = 512 << 20

func (w *Warehouse) initResearch() error {
	db, e := database(filepath.Join(w.root, "research.sqlite"))
	if e != nil {
		return e
	}
	_, e = db.Exec(`PRAGMA auto_vacuum=INCREMENTAL;
 PRAGMA max_page_count=126976;
 CREATE TABLE IF NOT EXISTS facts(dataset TEXT, ts INTEGER,res INTEGER, revision TEXT, available INTEGER, payload BLOB, PRIMARY KEY(dataset,ts,res,revision)) WITHOUT ROWID;
 CREATE INDEX IF NOT EXISTS facts_available ON facts(dataset,ts,res,available);
 CREATE TABLE IF NOT EXISTS documents(kind TEXT,id TEXT,asset TEXT,at INTEGER,payload BLOB,PRIMARY KEY(kind,id)) WITHOUT ROWID;
 CREATE INDEX IF NOT EXISTS documents_time ON documents(kind,asset,at);
 CREATE TABLE IF NOT EXISTS gaps(start INTEGER PRIMARY KEY,end INTEGER,reason TEXT);
 CREATE TABLE IF NOT EXISTS shadow(asset TEXT,ts INTEGER,payload BLOB,PRIMARY KEY(asset,ts)) WITHOUT ROWID;
 CREATE TABLE IF NOT EXISTS notices(id TEXT PRIMARY KEY,signal_id TEXT,kind TEXT,created INTEGER,status TEXT,attempted INTEGER DEFAULT 0,payload BLOB);`)
	if e != nil {
		db.Close()
		return e
	}
	w.research = db
	return nil
}
func researchKind(d Dataset) bool {
	switch d.Kind {
	case "flow", "candles", "oi-history", "premium", "balance-list", "balance-history", "etf":
		return true
	}
	return false
}

// A revision's first retrieval is its conservative availability bound. Fetching
// it again cannot backdate that bound. Unknown historic publication stays unknown.
func (w *Warehouse) recordFact(d Dataset, o *Observation) error {
	if !researchKind(d) {
		return nil
	}
	w.mu.RLock()
	paused := w.status.ResearchPaused || w.status.Paused
	days := w.retention
	w.mu.RUnlock()
	if o.Time().Before(o.FetchedAt.Add(-time.Duration(days) * 24 * time.Hour)) {
		return nil
	}
	if paused {
		w.researchGap(o.FetchedAt)
		return nil
	}
	first := o.FetchedAt
	var available int64
	e := w.research.QueryRow("SELECT available FROM facts WHERE dataset=? AND ts=? AND res=? AND revision=?", d.ID, recordTime(*o).Unix(), o.Resolution, o.Revision).Scan(&available)
	if e == nil {
		first = time.Unix(0, available).UTC()
		o.FirstFetchedAt = &first
		return nil
	}
	if e != sql.ErrNoRows {
		return e
	}
	o.FirstFetchedAt = &first
	b, e := pack(*o)
	if e != nil {
		return e
	}
	w.mu.Lock()
	if w.status.ResearchBytes+int64(len(b)+160) >= ResearchBudget-(8<<20) {
		w.status.ResearchPaused = true
		w.mu.Unlock()
		w.researchGap(o.FetchedAt)
		return nil
	}
	w.status.ResearchBytes += int64(len(b) + 160)
	w.mu.Unlock()
	_, e = w.research.Exec("INSERT OR IGNORE INTO facts VALUES(?,?,?,?,?,?)", d.ID, recordTime(*o).Unix(), o.Resolution, o.Revision, first.UnixNano(), b)
	// SQLite may hit its page ceiling before the byte estimate. This must stop
	// research history, not the independent current market snapshot write.
	if e != nil && strings.Contains(e.Error(), "SQLITE_FULL") {
		w.mu.Lock()
		w.status.ResearchPaused = true
		w.mu.Unlock()
		w.researchGap(o.FetchedAt)
		return nil
	}
	return e
}

func (w *Warehouse) researchGap(at time.Time) {
	// Persist a small marker outside the capped database as well: the capped
	// database itself might have no pages available for its gaps table.
	_ = w.SaveState("research/gap", map[string]any{"at": at, "reason": "研究容量保护，细数据未保存"})
	_, _ = w.research.Exec("INSERT OR IGNORE INTO gaps VALUES(?,?,?)", at.Truncate(time.Hour).Unix(), at.Truncate(time.Hour).Add(time.Hour).Unix(), "容量保护，研究事实缺口")
}

// FactsAsOf reads the newest version actually available by asOf. Passing a
// present-day asOf for old market times is association analysis, never a replay.
func (w *Warehouse) FactsAsOf(ctx context.Context, id string, from, to, asOf time.Time, fn func(Observation) error) error {
	rows, e := w.research.QueryContext(ctx, `SELECT f.payload FROM facts f WHERE f.dataset=? AND f.ts>=? AND f.ts<? AND f.available<=? AND f.available=(SELECT max(g.available) FROM facts g WHERE g.dataset=f.dataset AND g.ts=f.ts AND g.res=f.res AND g.available<=?) ORDER BY f.ts,f.res`, id, from.Unix(), to.Unix(), asOf.UnixNano(), asOf.UnixNano())
	if e != nil {
		return e
	}
	defer rows.Close()
	for rows.Next() {
		var b []byte
		if e = rows.Scan(&b); e != nil {
			return e
		}
		o, e := unpack(b)
		if e != nil {
			return e
		}
		if e = fn(o); e != nil {
			return e
		}
	}
	return rows.Err()
}
func (w *Warehouse) saveDocument(kind, id, asset string, at time.Time, v any) error {
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	if len(b) > 1<<20 {
		return errors.New("研究结果超过单项容量")
	}
	if w.Status().ResearchPaused {
		return errors.New("研究存储容量保护已启用")
	}
	_, e = w.research.Exec("INSERT INTO documents VALUES(?,?,?,?,?) ON CONFLICT(kind,id) DO UPDATE SET at=excluded.at,payload=excluded.payload", kind, id, asset, at.Unix(), b)
	return e
}
func (w *Warehouse) document(ctx context.Context, kind, id string, v any) error {
	var b []byte
	if e := w.research.QueryRowContext(ctx, "SELECT payload FROM documents WHERE kind=? AND id=?", kind, id).Scan(&b); e != nil {
		return e
	}
	return json.Unmarshal(b, v)
}
func (w *Warehouse) documents(ctx context.Context, kind, asset string, limit int) ([]json.RawMessage, error) {
	rows, e := w.research.QueryContext(ctx, "SELECT payload FROM documents WHERE kind=? AND (?='' OR asset=?) ORDER BY at DESC LIMIT ?", kind, asset, asset, min(100, limit))
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []json.RawMessage{}
	for rows.Next() {
		var b []byte
		if e = rows.Scan(&b); e != nil {
			return nil, e
		}
		out = append(out, json.RawMessage(b))
	}
	return out, rows.Err()
}
func (w *Warehouse) maintainResearch(ctx context.Context, now time.Time, days int) error {
	// Native research facts are retained up to the selected policy (90 days are
	// necessary for the explicit 30/30/30 study); budget protection takes priority.
	cutoff := now.Add(-time.Duration(days) * 24 * time.Hour).Unix()
	for _, q := range []string{"DELETE FROM facts WHERE ts<?", "DELETE FROM documents WHERE at<?", "DELETE FROM notices WHERE created<?", "DELETE FROM gaps WHERE end<?", "DELETE FROM shadow WHERE ts<?"} {
		if _, e := w.research.ExecContext(ctx, q, cutoff); e != nil {
			return e
		}
	}
	if _, e := w.research.ExecContext(ctx, "PRAGMA incremental_vacuum(4096)"); e != nil {
		return e
	}
	if _, e := w.research.ExecContext(ctx, "PRAGMA wal_checkpoint(PASSIVE)"); e != nil {
		return e
	}
	var pages, free, size int64
	if e := w.research.QueryRowContext(ctx, "PRAGMA page_count").Scan(&pages); e != nil {
		return e
	}
	_ = w.research.QueryRowContext(ctx, "PRAGMA freelist_count").Scan(&free)
	_ = w.research.QueryRowContext(ctx, "PRAGMA page_size").Scan(&size)
	used := pages * size
	if st, e := os.Stat(filepath.Join(w.root, "research.sqlite-wal")); e == nil {
		used += st.Size()
	}
	// Incremental vacuum only operates if configured before schema creation; a
	// full VACUUM under low space would need another database-sized allocation.
	w.mu.Lock()
	w.status.ResearchBytes = used
	w.status.ResearchPaused = (pages-free)*size >= ResearchBudget-(8<<20) || used >= ResearchBudget
	w.mu.Unlock()
	return nil
}

// Includes corrections and newly acquired facts, without reacting to price ticks.
func (w *Warehouse) factVersion(ctx context.Context, a string) string {
	var n, last int64
	_ = w.research.QueryRowContext(ctx, "SELECT count(*),coalesce(max(available),0) FROM facts WHERE dataset IN (?,?,?)", ID("flow", a, "", "spot"), ID("candles", a, "Binance", "spot"), ID("oi-history", a, "", "futures")).Scan(&n, &last)
	return fmt.Sprintf("%d/%d", n, last)
}

func (w *Warehouse) researchVersion(ctx context.Context, a string, from, to time.Time) string {
	var n, last int64
	_ = w.research.QueryRowContext(ctx, "SELECT count(*),coalesce(max(available),0) FROM facts WHERE ts>=? AND ts<? AND dataset LIKE ?", from.Unix(), to.Unix(), "%."+strings.ToLower(a)+".%").Scan(&n, &last)
	return fmt.Sprintf("%d/%d", n, last)
}

func (w *Warehouse) nativeCursor(ctx context.Context, d Dataset, from, to, now time.Time) (time.Time, error) {
	cursor := from
	e := w.FactsAsOf(ctx, d.ID, from, to, now, func(o Observation) error {
		if o.Resolution == d.Resolution && o.Quality == "valid" && recordTime(o).Equal(cursor) {
			cursor = cursor.Add(time.Duration(d.Resolution) * time.Second)
		}
		return nil
	})
	return cursor, e
}
func (w *Warehouse) completeNativeWindow(ctx context.Context, d Dataset, from, to, now time.Time) bool {
	if !researchKind(d) {
		return false
	}
	cursor, e := w.nativeCursor(ctx, d, from, to, now)
	return e == nil && !cursor.Before(to)
}
