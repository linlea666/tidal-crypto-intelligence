package datahub

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type paperStore struct {
	db          *sql.DB
	readDB      *sql.DB
	path        string
	mu          sync.RWMutex
	state       paperState
	mode        string
	quote       *paperQuote
	instrument  paperInstrument
	atr         *paperIntent
	markAt      time.Time
	sourceAt    time.Time
	lastQuoteID int64
	err         string
	feed        paperFeed
}

func paperExists(root string) bool {
	_, err := os.Stat(filepath.Join(root, "paper.sqlite"))
	return err == nil
}

type paperBatch struct {
	Trades      []*paperTrade
	Fills       []paperFill
	Intakes     []paperIntake
	Funding     []paperFundingEntry
	Settlements []paperFunding
	Equity      []paperEquity
	Events      []paperEvent
}

func paperGeneration() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}
func openPaper(root, mode string) (*paperStore, error) {
	if mode != "off" && mode != "collect" && mode != "run" {
		return nil, errors.New("TIDAL_PAPER_MODE must be off, collect or run")
	}
	p := &paperStore{path: filepath.Join(root, "paper.sqlite"), mode: mode}
	db, err := database(p.path)
	if err != nil {
		return nil, err
	}
	p.db = db
	_, err = db.Exec(`PRAGMA synchronous=FULL; PRAGMA busy_timeout=100; PRAGMA max_page_count=30720; PRAGMA journal_size_limit=4194304;
CREATE TABLE IF NOT EXISTS paper_state(id INTEGER PRIMARY KEY CHECK(id=1),payload BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS paper_trades(id TEXT PRIMARY KEY,group_id TEXT NOT NULL,entered INTEGER NOT NULL,exited INTEGER,payload BLOB NOT NULL);
CREATE INDEX IF NOT EXISTS paper_trade_time ON paper_trades(entered,id);
CREATE INDEX IF NOT EXISTS paper_trade_exit ON paper_trades(exited,entered);
CREATE TABLE IF NOT EXISTS paper_fills(id TEXT PRIMARY KEY,trade_id TEXT NOT NULL,at INTEGER NOT NULL,payload BLOB NOT NULL);
CREATE INDEX IF NOT EXISTS paper_fill_trade ON paper_fills(trade_id,at);
CREATE TABLE IF NOT EXISTS paper_intakes(id TEXT PRIMARY KEY,at INTEGER NOT NULL,payload BLOB NOT NULL);
CREATE INDEX IF NOT EXISTS paper_intake_time ON paper_intakes(at);
CREATE TABLE IF NOT EXISTS paper_settlements(at INTEGER PRIMARY KEY,payload BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS paper_funding(trade_id TEXT,at INTEGER,group_id TEXT NOT NULL,payload BLOB NOT NULL,PRIMARY KEY(trade_id,at)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS paper_funding_time ON paper_funding(group_id,at);
CREATE TABLE IF NOT EXISTS paper_events(at INTEGER,kind TEXT,payload BLOB NOT NULL,PRIMARY KEY(at,kind)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS paper_equity(group_id TEXT,at INTEGER,payload BLOB NOT NULL,PRIMARY KEY(group_id,at)) WITHOUT ROWID;`)
	if err != nil {
		db.Close()
		return nil, err
	}
	var raw []byte
	err = db.QueryRow("SELECT payload FROM paper_state WHERE id=1").Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		p.state = paperState{Parameters: paperParameters(), Version: PaperRules, Generation: paperGeneration(), Gap: true, Pause: "warming_up", Accounts: []paperAccount{{Group: "opposite"}, {Group: "risk"}}}
		err = p.commit(context.Background(), p.state, paperBatch{})
	} else if err == nil {
		err = json.Unmarshal(raw, &p.state)
	}
	if err == nil && (p.state.Version != PaperRules || len(p.state.Accounts) != 2 || p.state.Accounts[0].Group != "opposite" || p.state.Accounts[1].Group != "risk") {
		err = errors.New("paper experiment version/config mismatch; preserve existing ledger")
	}
	if err == nil && !bytes.Equal(p.state.Parameters, paperParameters()) {
		err = errors.New("paper frozen parameters differ from this binary; refusing to reinterpret ledger")
	}
	if err == nil {
		err = p.verifyRecovery()
	}
	if err != nil {
		db.Close()
		return nil, err
	}
	p.readDB, err = sql.Open("sqlite", "file:"+p.path+"?mode=ro&_pragma=busy_timeout(100)")
	if err != nil {
		db.Close()
		return nil, err
	}
	p.readDB.SetMaxOpenConns(1)
	p.lastQuoteID = p.state.LastQuoteID
	return p, nil
}

// commit publishes the in-memory projection only after the entire ledger and
// cursor transaction commits. A failed transaction cannot spend cash in memory.
func (p *paperStore) commit(ctx context.Context, s paperState, b paperBatch) error {
	if p.size() >= PaperBudget-(1<<20) {
		_, err := p.db.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)")
		if err != nil {
			return err
		}
		if p.size() >= PaperBudget-(1<<20) {
			return errors.New("paper database capacity reached; ledger preserved")
		}
	}
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	put := func(query string, value any, args ...any) error {
		raw, err := json.Marshal(value)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, query, append(args, raw)...)
		return err
	}
	for _, t := range b.Trades {
		var exited any
		if t.Exited != nil {
			exited = t.Exited.UnixMilli()
		}
		if err = put("INSERT INTO paper_trades VALUES(?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET exited=excluded.exited,payload=excluded.payload", t, t.ID, t.Group, t.Entered.UnixMilli(), exited); err != nil {
			return err
		}
	}
	for _, f := range b.Fills {
		if err = put("INSERT INTO paper_fills VALUES(?,?,?,?)", f, f.ID, f.TradeID, f.At.UnixMilli()); err != nil {
			return err
		}
	}
	for _, i := range b.Intakes {
		if err = put("INSERT INTO paper_intakes VALUES(?,?,?)", i, i.ID, i.At.UnixMilli()); err != nil {
			return err
		}
	}
	for _, f := range b.Settlements {
		if err = put("INSERT INTO paper_settlements VALUES(?,?)", f, f.At.UnixMilli()); err != nil {
			return err
		}
	}
	for _, f := range b.Funding {
		if err = put("INSERT INTO paper_funding VALUES(?,?,?,?)", f, f.TradeID, f.Funding.At.UnixMilli(), f.Group); err != nil {
			return err
		}
	}
	for _, e := range b.Equity {
		if err = put("INSERT INTO paper_equity VALUES(?,?,?) ON CONFLICT(group_id,at) DO NOTHING", e, e.Group, e.At.UnixMilli()); err != nil {
			return err
		}
	}
	for _, e := range b.Events {
		if err = put("INSERT OR IGNORE INTO paper_events VALUES(?,?,?)", e, e.At.UnixNano(), e.Kind); err != nil {
			return err
		}
	}
	if err = put("INSERT INTO paper_state VALUES(1,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload", s); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	p.mu.Lock()
	p.state = s
	p.mu.Unlock()
	return nil
}
func (p *paperStore) snapshot() paperState {
	p.mu.RLock()
	defer p.mu.RUnlock()
	s := p.state
	s.Accounts = append([]paperAccount(nil), s.Accounts...)
	s.ExpectedFunding = append([]int64(nil), s.ExpectedFunding...)
	s.SettledFunding = append([]int64(nil), s.SettledFunding...)
	for n := range s.Accounts {
		a := &s.Accounts[n]
		if a.Position != nil {
			v := *a.Position
			v.Quality = append([]string{}, v.Quality...)
			a.Position = &v
		}
		if a.Pending != nil {
			v := *a.Pending
			a.Pending = &v
		}
	}
	return s
}
func (p *paperStore) size() int64 {
	var size int64
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if st, err := os.Stat(p.path + suffix); err == nil {
			size += st.Size()
		}
	}
	return size
}
func (p *paperStore) failure(err error, at time.Time) {
	// Keep a volatile failure even when SQLITE_FULL prevents recording it.
	p.mu.Lock()
	p.err = err.Error()
	p.mu.Unlock()
	s := p.snapshot()
	s.LastFailure, s.LastFailureAt = err.Error(), &at
	s.Pause, s.Gap, s.GoodSince = "storage_error", true, time.Time{}
	s.EquityGap = true
	b := paperBatch{Events: []paperEvent{{At: at, Kind: "failure", Reason: err.Error()}}}
	for i := range s.Accounts {
		a := &s.Accounts[i]
		a.Pending = nil
		if a.Position != nil {
			a.Position.flag("data_gap")
			paperTrigger(a.Position, "data_gap", at)
			b.Trades = append(b.Trades, a.Position)
		}
	}
	p.mu.Lock()
	p.state = s
	p.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = p.commit(ctx, s, b)
}
func (p *paperStore) recordFailure(ctx context.Context, err error, at time.Time, operation string) error {
	s := p.snapshot()
	message := operation + ": " + err.Error()
	s.LastFailure, s.LastFailureAt = message, &at
	p.mu.Lock()
	p.err = message
	p.mu.Unlock()
	return p.commit(ctx, s, paperBatch{Events: []paperEvent{{At: at, Kind: "fetch_failure", Reason: message}}})
}

type paperQuerier interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func paperRows[T any](ctx context.Context, db paperQuerier, query string, args ...any) ([]T, error) {
	r, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	out := []T{}
	for r.Next() {
		var raw []byte
		var v T
		if err = r.Scan(&raw); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(raw, &v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, r.Err()
}

func (w *Warehouse) initPaperPublications() error {
	_, err := w.research.Exec(`CREATE TABLE IF NOT EXISTS paper_source(id INTEGER PRIMARY KEY CHECK(id=1),generation TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS signal_publications(seq INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT UNIQUE NOT NULL,at INTEGER NOT NULL,payload BLOB NOT NULL);
CREATE INDEX IF NOT EXISTS signal_publication_time ON signal_publications(at);`)
	if err == nil {
		_, err = w.research.Exec("INSERT OR IGNORE INTO paper_source VALUES(1,?)", paperGeneration())
	}
	return err
}

type paperPublication struct {
	Seq    int64
	At     time.Time
	Signal Signal
}

func (w *Warehouse) paperPublications(ctx context.Context, after int64) ([]paperPublication, string, int64, error) {
	var source string
	var end int64
	if err := w.research.QueryRowContext(ctx, "SELECT generation FROM paper_source WHERE id=1").Scan(&source); err != nil {
		return nil, "", 0, err
	}
	if err := w.research.QueryRowContext(ctx, "SELECT coalesce((SELECT seq FROM sqlite_sequence WHERE name='signal_publications'),0)").Scan(&end); err != nil {
		return nil, "", 0, err
	}
	r, err := w.research.QueryContext(ctx, "SELECT seq,at,payload FROM signal_publications WHERE seq>? ORDER BY seq LIMIT 32", after)
	if err != nil {
		return nil, "", 0, err
	}
	defer r.Close()
	out := []paperPublication{}
	for r.Next() {
		var v paperPublication
		var at int64
		var raw []byte
		if err = r.Scan(&v.Seq, &at, &raw); err != nil {
			return nil, "", 0, err
		}
		if err = json.Unmarshal(raw, &v.Signal); err != nil {
			return nil, "", 0, err
		}
		v.At = time.UnixMilli(at).UTC()
		out = append(out, v)
	}
	return out, source, end, r.Err()
}
func paperIntakeRecord(s paperState, a paperAccount, intent paperIntent, at time.Time, state, reason string) paperIntake {
	return paperIntake{ID: fmt.Sprintf("%s/%s/%s/%s", s.Generation, a.Group, intent.Signal.ID, state), Group: a.Group, SignalID: intent.Signal.ID, At: at, State: state, Reason: reason, Signal: intent.Signal}
}
