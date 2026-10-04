package datahub

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type costStore struct {
	db        *sql.DB
	mu        sync.Mutex
	path      string
	initError string
	epoch     atomic.Uint64
}

func (w *Warehouse) initOnchain() {
	s := &costStore{path: filepath.Join(w.root, "onchain.sqlite")}
	w.onchain = s
	db, e := database(s.path)
	if e != nil {
		s.initError = e.Error()
		return
	}
	s.db = db
	_, e = db.Exec(`PRAGMA cache_size=-512; PRAGMA busy_timeout=1000; PRAGMA max_page_count=32768;
 CREATE TABLE IF NOT EXISTS frames(day TEXT,revision TEXT,first_seen INTEGER,origin TEXT,method TEXT,payload BLOB,summary BLOB,PRIMARY KEY(day,revision)) WITHOUT ROWID;
 CREATE INDEX IF NOT EXISTS frame_available ON frames(day,first_seen);
 CREATE TABLE IF NOT EXISTS prices(day TEXT,revision TEXT,first_seen INTEGER,value TEXT,PRIMARY KEY(day,revision)) WITHOUT ROWID;
 CREATE TABLE IF NOT EXISTS state(key TEXT PRIMARY KEY,payload BLOB) WITHOUT ROWID;
 CREATE TABLE IF NOT EXISTS events(id TEXT PRIMARY KEY,detected INTEGER,kind TEXT,payload BLOB) WITHOUT ROWID;
 CREATE TABLE IF NOT EXISTS outbox(id TEXT PRIMARY KEY,created INTEGER,payload BLOB,status TEXT) WITHOUT ROWID;
 CREATE TABLE IF NOT EXISTS results(event_id TEXT,horizon INTEGER,payload BLOB,PRIMARY KEY(event_id,horizon)) WITHOUT ROWID;
 CREATE TABLE IF NOT EXISTS evidence(day TEXT,as_of INTEGER,payload BLOB,PRIMARY KEY(day,as_of)) WITHOUT ROWID;`)
	if e != nil {
		s.initError = e.Error()
		db.Close()
		s.db = nil
	}
}
func (s *costStore) available() error {
	if s == nil || s.db == nil {
		return errors.New("链上存储不可用")
	}
	return nil
}
func (s *costStore) bytes() int64 {
	var n int64
	if s == nil {
		return n
	}
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if st, e := os.Stat(s.path + suffix); e == nil {
			n += st.Size()
		}
	}
	return n
}
func (s *costStore) writable() error {
	if e := s.available(); e != nil {
		return e
	}
	if s.bytes() >= onchainBudget*95/100 {
		return errors.New("链上存储达到95%容量保护线")
	}
	return nil
}

// Check both the future checkpointed database and the WAL before publishing.
// Reserve dirty-cache / B-tree pages too: a successful commit must not silently
// consume the remaining audit budget. Failure rolls back the whole bundle.
func (s *costStore) commit(ctx context.Context, tx *sql.Tx) error {
	var pages, pageSize int64
	if e := tx.QueryRowContext(ctx, "PRAGMA page_count").Scan(&pages); e != nil {
		return e
	}
	if e := tx.QueryRowContext(ctx, "PRAGMA page_size").Scan(&pageSize); e != nil {
		return e
	}
	projected := pages*pageSize + 1<<20
	for _, suffix := range []string{"-wal", "-shm"} {
		if st, e := os.Stat(s.path + suffix); e == nil {
			projected += st.Size()
		}
	}
	if projected >= onchainBudget*95/100 {
		return errors.New("链上事务将触及95%容量保护线，已保留原数据")
	}
	return tx.Commit()
}

type costQuerier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func costLoad(ctx context.Context, q costQuerier, key string, dst any) error {
	var b []byte
	e := q.QueryRowContext(ctx, "SELECT payload FROM state WHERE key=?", key).Scan(&b)
	if e == sql.ErrNoRows {
		return nil
	}
	if e != nil {
		return e
	}
	return json.Unmarshal(b, dst)
}
func costSave(ctx context.Context, tx *sql.Tx, key string, value any) error {
	b, e := json.Marshal(value)
	if e != nil {
		return e
	}
	_, e = tx.ExecContext(ctx, "INSERT INTO state VALUES(?,?) ON CONFLICT(key) DO UPDATE SET payload=excluded.payload", key, b)
	return e
}
func (s *costStore) save(ctx context.Context, key string, value any) error {
	if e := s.available(); e != nil {
		return e
	}
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	if e = costSave(ctx, tx, key, value); e != nil {
		return e
	}
	return tx.Commit()
}
func costPack(v any) ([]byte, error) {
	var b bytes.Buffer
	z := gzip.NewWriter(&b)
	if e := json.NewEncoder(z).Encode(v); e != nil {
		return nil, e
	}
	if e := z.Close(); e != nil {
		return nil, e
	}
	return b.Bytes(), nil
}
func costUnpack(b []byte, v any) error {
	z, e := gzip.NewReader(bytes.NewReader(b))
	if e != nil {
		return e
	}
	defer z.Close()
	r := &io.LimitedReader{R: z, N: 4 << 20}
	e = json.NewDecoder(r).Decode(v)
	if r.N <= 0 {
		return errors.New("链上本地记录超限")
	}
	return e
}

// Content identity excludes retrieval/build times: unchanged fetches never move availability.
func costFrameRevision(f CostFrame) string {
	return costHash(struct {
		Date, Price, Method string
		STH, LTH            CostCohort
	}{f.Date, f.Price, f.Method, f.STH, f.LTH})
}

type costSummary struct {
	Date          string                `json:"date"`
	Revision      string                `json:"revision"`
	FirstSeen     time.Time             `json:"firstSeen"`
	Origin        string                `json:"origin"`
	Method        string                `json:"method"`
	Concentration map[string]CostBounds `json:"concentration"`
	Price         string                `json:"price"`
}

func (s *costStore) ingest(ctx context.Context, b costBundle, now time.Time, initial bool) error {
	if e := s.writable(); e != nil {
		return e
	}
	sort.Slice(b.Frames, func(i, j int) bool { return b.Frames[i].Date < b.Frames[j].Date })
	// Validate before any write; publish all same-day components transactionally.
	for _, f := range b.Frames {
		if e := validateCostFrame(f); e != nil {
			return e
		}
	}
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	estimated := s.bytes() + 1<<20
	for _, p := range b.Prices {
		if _, e := costDay(p.Date); e != nil {
			return e
		}
		if _, e := costNumber(p.Value, true); e != nil {
			return e
		}
		r := costHash([]string{p.Date, p.Value})
		res, err := tx.ExecContext(ctx, "INSERT OR IGNORE INTO prices VALUES(?,?,?,?)", p.Date, r, now.UnixNano(), p.Value)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		estimated += n * 16384 // Includes tree/index/WAL growth, not only payload bytes.
		if estimated >= onchainBudget*95/100 {
			return errors.New("链上事务将触及95%容量保护线，已保留原数据")
		}
	}
	for _, f := range b.Frames {
		f.Revision = costFrameRevision(f)
		var exists int
		if e = tx.QueryRowContext(ctx, "SELECT count(*) FROM frames WHERE day=? AND revision=?", f.Date, f.Revision).Scan(&exists); e != nil {
			return e
		}
		if exists > 0 {
			continue
		}
		f.FirstSeen = now
		f.BuiltAt = b.BuiltAt
		f.Origin = "imported"
		if !initial && f.Date == costDate(now.AddDate(0, 0, -1)) {
			f.Origin = "forward"
		}
		raw, e := costPack(f)
		if e != nil {
			return e
		}
		cs := costSummary{f.Date, f.Revision, now, f.Origin, f.Method, map[string]CostBounds{}, f.Price}
		for _, width := range []string{"2.5", "5", "10"} {
			cs.Concentration[width] = costConcentration(f, width)
		}
		summary, e := json.Marshal(cs)
		if e != nil {
			return e
		}
		estimated += int64((len(raw)+len(summary)+4095)/4096+6) * 8192
		if estimated >= onchainBudget*95/100 {
			return errors.New("链上事务将触及95%容量保护线，已保留原数据")
		}
		if _, e = tx.ExecContext(ctx, "INSERT INTO frames VALUES(?,?,?,?,?,?,?)", f.Date, f.Revision, now.UnixNano(), f.Origin, f.Method, raw, summary); e != nil {
			return e
		}
		// Revisions are separate audit events; never rewrite already-frozen judgments.
		var count int
		if e = tx.QueryRowContext(ctx, "SELECT count(*) FROM frames WHERE day=?", f.Date).Scan(&count); e != nil {
			return e
		}
		if count > 1 {
			event := CostEvent{ID: "revision-" + f.Date + "-" + f.Revision, Kind: "revision", Date: f.Date, DetectedAt: now, Rules: OnchainRules, Revision: f.Revision, Note: "来源追加修订；原观察及通知保持不变"}
			if e = costInsertEvent(ctx, tx, event, false); e != nil {
				return e
			}
		}
	}
	if e = s.commit(ctx, tx); e != nil {
		return e
	}
	_, e = s.db.ExecContext(ctx, "PRAGMA wal_checkpoint(PASSIVE)")
	return e
}
func (s *costStore) frame(ctx context.Context, date string, asOf time.Time) (*CostFrame, error) {
	if e := s.available(); e != nil {
		return nil, e
	}
	where := "first_seen<=?"
	args := []any{asOf.UnixNano()}
	if date != "" {
		where += " AND day=?"
		args = append(args, date)
	}
	var b []byte
	e := s.db.QueryRowContext(ctx, "SELECT payload FROM frames WHERE "+where+" ORDER BY day DESC,first_seen DESC,revision DESC LIMIT 1", args...).Scan(&b)
	if e == sql.ErrNoRows {
		return nil, nil
	}
	if e != nil {
		return nil, e
	}
	var f CostFrame
	if e = costUnpack(b, &f); e != nil {
		return nil, e
	}
	return &f, nil
}
func (s *costStore) prices(ctx context.Context, asOf time.Time) (map[string]string, error) {
	out := map[string]string{}
	if e := s.available(); e != nil {
		return out, e
	}
	rows, e := s.db.QueryContext(ctx, `SELECT day,value FROM (SELECT day,value,row_number() OVER(PARTITION BY day ORDER BY first_seen DESC,revision DESC) n FROM prices WHERE first_seen<=?) WHERE n=1 ORDER BY day LIMIT 20000`, asOf.UnixNano())
	if e != nil {
		return out, e
	}
	defer rows.Close()
	for rows.Next() {
		var d, v string
		if e = rows.Scan(&d, &v); e != nil {
			return out, e
		}
		out[d] = v
	}
	return out, rows.Err()
}
func (s *costStore) summaries(ctx context.Context, asOf time.Time) ([]costSummary, error) {
	out := []costSummary{}
	if e := s.available(); e != nil {
		return out, e
	}
	rows, e := s.db.QueryContext(ctx, `SELECT summary FROM (SELECT summary,row_number() OVER(PARTITION BY day ORDER BY first_seen DESC,revision DESC) n FROM frames WHERE first_seen<=?) WHERE n=1 ORDER BY json_extract(summary,'$.date') LIMIT 5000`, asOf.UnixNano())
	if e != nil {
		return out, e
	}
	defer rows.Close()
	for rows.Next() {
		var b []byte
		var v costSummary
		if e = rows.Scan(&b); e != nil {
			return out, e
		}
		if e = json.Unmarshal(b, &v); e != nil {
			return out, e
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
func (s *costStore) metrics(ctx context.Context, f CostFrame, asOf time.Time) (CostMetrics, error) {
	prices, e := s.prices(ctx, asOf)
	if e != nil {
		return CostMetrics{}, e
	}
	summaries, e := s.summaries(ctx, asOf)
	if e != nil {
		return CostMetrics{}, e
	}
	return costMetricsFromSummaries(f, summaries, prices), nil
}
func costMetricsFromSummaries(f CostFrame, summaries []costSummary, prices map[string]string) CostMetrics {
	m := CostMetrics{Concentration: map[string]CostBounds{}, Denominator: dec(f.STH.Total).Add(dec(f.LTH.Total)).String(), Volatility: costVolatility(f.Date, prices), Zones: costZones(f)}
	for _, w := range []string{"2.5", "5", "10"} {
		m.Concentration[w] = costConcentration(f, w)
	}
	costRanks(&m, f.Date, f.Method, summaries, prices)
	return m
}
func costRanks(m *CostMetrics, date, method string, summaries []costSummary, prices map[string]string) {
	byDay := map[string]costSummary{}
	for _, s := range summaries {
		byDay[s.Date] = s
	}
	day, _ := costDay(date)
	sun := day.AddDate(0, 0, -int(day.Weekday()))
	if sun.Equal(day) {
		sun = sun.AddDate(0, 0, -7)
	}
	cl, ch, vr := 0., 0., 0.
	c := m.Concentration["5"]
	for i := 0; i < 52; i++ {
		d := costDate(sun.AddDate(0, 0, -7*i))
		b, ok := byDay[d]
		if ok && b.Method == method {
			bc := b.Concentration["5"]
			m.BaselineSamples++
			if dec(bc.Upper).LessThanOrEqual(dec(c.Lower)) {
				cl++
			}
			if dec(bc.Lower).LessThanOrEqual(dec(c.Upper)) {
				ch++
			}
		}
		if v := costVolatility(d, prices); v != nil && m.Volatility != nil {
			m.VolatilitySamples++
			if *v <= *m.Volatility {
				vr++
			}
		}
	}
	if m.BaselineSamples >= 42 {
		m.ConcentrationRank = &CostBounds{fmtCostFloat(cl * 100 / float64(m.BaselineSamples)), fmtCostFloat(ch * 100 / float64(m.BaselineSamples))}
	}
	if m.VolatilitySamples >= 42 {
		v := vr * 100 / float64(m.VolatilitySamples)
		m.VolatilityRank = &v
	}
}
