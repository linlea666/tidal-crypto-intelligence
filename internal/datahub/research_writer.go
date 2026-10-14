package datahub

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	sqlite3 "modernc.org/sqlite/lib"
)

// Both research pools share this gate. Waiting for a writer is cancellable and
// happens before obtaining a connection, so readers retain their own pool.
type researchWriter struct {
	gate            chan struct{}
	db              *sql.DB
	mu              sync.Mutex
	stats           researchWriteStats
	waiting         int
	connectionTrace *researchConnectionTrace
}
type researchWriteStats struct {
	Writes                  int64                  `json:"writes"`
	Failures                int64                  `json:"failures"`
	WaitMS                  float64                `json:"waitMs"`
	TransactionMS           float64                `json:"transactionMs"`
	CommitMS                float64                `json:"commitMs"`
	LastOperation           string                 `json:"lastOperation"`
	LastFailure             string                 `json:"lastFailure,omitempty"`
	LastFailureOperation    string                 `json:"lastFailureOperation,omitempty"`
	LastFailureAt           *time.Time             `json:"lastFailureAt"`
	LastFailurePhase        string                 `json:"lastFailurePhase,omitempty"`
	LastFailureGateWaitMS   float64                `json:"lastFailureGateWaitMs"`
	LastFailureConnWaitMS   float64                `json:"lastFailureConnectionWaitMs"`
	MaxWaitMS               float64                `json:"maxWaitMs"`
	MaxTransactionMS        float64                `json:"maxTransactionMs"`
	MaxTransactionOperation string                 `json:"maxTransactionOperation,omitempty"`
	MaxTransactionAt        *time.Time             `json:"maxTransactionAt"`
	MaxCommitMS             float64                `json:"maxCommitMs"`
	RecentFailures          []researchWriteFailure `json:"recentFailures,omitempty"`
}

type researchWriteFailure struct {
	At                  time.Time                  `json:"at"`
	Operation           string                     `json:"operation"`
	Phase               string                     `json:"phase"`
	Error               string                     `json:"error"`
	EntryBudgetMS       *float64                   `json:"entryBudgetMs"`
	GateWaitMS          float64                    `json:"gateWaitMs"`
	ConnectionWaitMS    float64                    `json:"connectionWaitMs"`
	ConnectionAtEntry   researchConnectionSnapshot `json:"connectionAtEntry"`
	ConnectionAtFailure researchConnectionSnapshot `json:"connectionAtFailure"`
}

const researchFailureHistoryLimit = 16

type researchWriteDetail struct {
	phase      string
	connWaitMS float64
}
type researchDB struct {
	*sql.DB
	writer *researchWriter
}
type sqlReader interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}
type sqlExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}
type maintenanceWriteKey struct{}
type researchOperationKey struct{}

var errMaintenanceDeferred = errors.New("research maintenance deferred to live writer")

func newResearchDB(db *sql.DB, writer *researchWriter) *researchDB {
	if writer == nil {
		writer = &researchWriter{gate: make(chan struct{}, 1)}
	}
	writer.db = db
	return &researchDB{DB: db, writer: writer}
}
func (w *researchWriter) snapshot() researchWriteStats {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := w.stats
	out.RecentFailures = append([]researchWriteFailure(nil), w.stats.RecentFailures...)
	return out
}
func (w *researchWriter) acquire(ctx context.Context, op string, detail ...*researchWriteDetail) (func(error, time.Duration), error) {
	start := time.Now()
	var entryBudget *float64
	if deadline, ok := ctx.Deadline(); ok {
		ms := float64(deadline.Sub(start).Microseconds()) / 1000
		entryBudget = &ms
	}
	connectionAtEntry := w.connectionTrace.snapshot()
	var trace *researchWriteDetail
	if len(detail) > 0 {
		trace = detail[0]
		trace.phase = "writer_queue"
	}
	var err error
	if ctx.Value(maintenanceWriteKey{}) != nil {
		w.mu.Lock()
		waiting := w.waiting
		w.mu.Unlock()
		if waiting > 0 {
			return nil, errMaintenanceDeferred
		}
		select {
		case w.gate <- struct{}{}:
		default:
			return nil, errMaintenanceDeferred
		}
	} else if ctx.Err() != nil {
		// A spent parent budget is not evidence that another writer held the
		// gate. Preserve failure counting, but don't randomly select a free gate
		// when both the gate and Done are ready.
		err = ctx.Err()
		if trace != nil {
			trace.phase = "caller_budget"
		}
	} else {
		w.mu.Lock()
		w.waiting++
		w.mu.Unlock()
		select {
		case w.gate <- struct{}{}:
		case <-ctx.Done():
			err = ctx.Err()
		}
		w.mu.Lock()
		w.waiting--
		w.mu.Unlock()
	}
	acquired := time.Now()
	w.mu.Lock()
	w.stats.WaitMS = float64(acquired.Sub(start).Microseconds()) / 1000
	w.stats.MaxWaitMS = max(w.stats.MaxWaitMS, w.stats.WaitMS)
	w.stats.LastOperation = op
	w.mu.Unlock()
	finish := func(e error, commit time.Duration) {
		w.mu.Lock()
		w.stats.Writes++
		w.stats.TransactionMS = float64(time.Since(acquired).Microseconds()) / 1000
		w.stats.CommitMS = float64(commit.Microseconds()) / 1000
		if w.stats.TransactionMS > w.stats.MaxTransactionMS {
			w.stats.MaxTransactionMS = w.stats.TransactionMS
			w.stats.MaxTransactionOperation = op
			at := time.Now().UTC()
			w.stats.MaxTransactionAt = &at
		}
		w.stats.MaxCommitMS = max(w.stats.MaxCommitMS, w.stats.CommitMS)
		if e != nil {
			at := time.Now().UTC()
			w.stats.Failures++
			w.stats.LastFailure = e.Error()
			w.stats.LastFailureOperation = op
			w.stats.LastFailureAt = &at
			w.stats.LastOperation = op
			w.stats.LastFailureGateWaitMS = float64(acquired.Sub(start).Microseconds()) / 1000
			w.stats.LastFailurePhase = ""
			w.stats.LastFailureConnWaitMS = 0
			if trace != nil {
				w.stats.LastFailurePhase = trace.phase
				w.stats.LastFailureConnWaitMS = trace.connWaitMS
			}
			failure := researchWriteFailure{At: at, Operation: op, Phase: w.stats.LastFailurePhase,
				Error: e.Error(), EntryBudgetMS: entryBudget, GateWaitMS: w.stats.LastFailureGateWaitMS,
				ConnectionWaitMS: w.stats.LastFailureConnWaitMS, ConnectionAtEntry: connectionAtEntry,
				ConnectionAtFailure: w.connectionTrace.snapshot()}
			if len(w.stats.RecentFailures) == researchFailureHistoryLimit {
				copy(w.stats.RecentFailures, w.stats.RecentFailures[1:])
				w.stats.RecentFailures[len(w.stats.RecentFailures)-1] = failure
			} else {
				w.stats.RecentFailures = append(w.stats.RecentFailures, failure)
			}
		}
		w.mu.Unlock()
	}
	if err != nil {
		finish(err, 0)
		return nil, err
	}
	return func(e error, commit time.Duration) { finish(e, commit); <-w.gate }, nil
}
func sqlOperation(q string) string {
	f := strings.Fields(q)
	if len(f) > 5 {
		f = f[:5]
	}
	return strings.Join(f, " ")
}
func retryResearchExec(ctx context.Context, db sqlExecutor, query string, args ...any) (sql.Result, error) {
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		// Recalculate the tail budget after every retry; a fixed 50ms SQLite
		// busy handler must not run past an almost exhausted stage deadline.
		if deadline, ok := ctx.Deadline(); ok {
			remaining := int(time.Until(deadline).Milliseconds()) - 5
			if remaining <= 0 {
				return nil, context.DeadlineExceeded
			}
			if remaining < 50 {
				if _, err := db.ExecContext(ctx, fmt.Sprintf("PRAGMA busy_timeout=%d", remaining)); err != nil {
					return nil, err
				}
			}
		}
		r, err := db.ExecContext(ctx, query, args...)
		// Never retry a snapshot conflict or an ambiguous commit. SQLite BUSY
		// statements have made no change; all retries share the caller's budget.
		if err == nil || !retryableSQLiteBusy(err) || attempt >= 3 {
			return r, err
		}
		t := time.NewTimer(time.Duration(attempt+1) * 10 * time.Millisecond)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil, ctx.Err()
		case <-t.C:
		}
	}
}

func retryableSQLiteBusy(err error) bool {
	// modernc includes the extended number (e.g. 261) without necessarily
	// including SQLITE_BUSY in Error(). Never classify errors by that text.
	var coded interface{ Code() int }
	if !errors.As(err, &coded) {
		return false
	}
	switch coded.Code() {
	case sqlite3.SQLITE_BUSY, sqlite3.SQLITE_BUSY_RECOVERY, sqlite3.SQLITE_BUSY_TIMEOUT:
		return true
	default:
		return false // Includes SQLITE_BUSY_SNAPSHOT and uncertain I/O failures.
	}
}
func (db *researchDB) Exec(query string, args ...any) (sql.Result, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return db.ExecContext(ctx, query, args...)
}
func (db *researchDB) ExecContext(ctx context.Context, query string, args ...any) (result sql.Result, err error) {
	op := sqlOperation(query)
	if name, ok := ctx.Value(researchOperationKey{}).(string); ok {
		op = name + ": " + op
	}
	detail := &researchWriteDetail{}
	finish, err := db.writer.acquire(ctx, op, detail)
	if err != nil {
		return nil, err
	}
	defer func() { finish(err, 0) }()
	detail.phase = "connection"
	started := time.Now()
	conn, err := db.writer.db.Conn(ctx)
	detail.connWaitMS = float64(time.Since(started).Microseconds()) / 1000
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return boundedConnExec(ctx, conn, 50, &detail.phase, query, args...)
}

type researchTx struct {
	*sql.Tx
	conn    *sql.Conn
	finish  func(error, time.Duration)
	once    sync.Once
	failure error
	detail  *researchWriteDetail
}

func (db *researchDB) Begin() (*researchTx, error) { return db.BeginTx(context.Background(), nil) }
func (db *researchDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*researchTx, error) {
	return db.BeginNamedTx(ctx, opts, "transaction")
}

// name is a fixed call-site label, never a dataset payload or query argument.
func (db *researchDB) BeginNamedTx(ctx context.Context, opts *sql.TxOptions, name string) (*researchTx, error) {
	detail := &researchWriteDetail{}
	finish, err := db.writer.acquire(ctx, name, detail)
	if err != nil {
		return nil, err
	}
	detail.phase = "connection"
	started := time.Now()
	conn, err := db.writer.db.Conn(ctx)
	detail.connWaitMS = float64(time.Since(started).Microseconds()) / 1000
	if err != nil {
		finish(err, 0)
		return nil, err
	}
	detail.phase = "sqlite_policy"
	if _, err = conn.ExecContext(ctx, "PRAGMA busy_timeout=50"); err != nil {
		conn.Close()
		finish(err, 0)
		return nil, err
	}
	detail.phase = "transaction_begin"
	tx, err := conn.BeginTx(ctx, opts)
	if err != nil {
		conn.Close()
		finish(err, 0)
		return nil, err
	}
	return &researchTx{Tx: tx, conn: conn, finish: finish, detail: detail}, nil
}
func (tx *researchTx) ExecContext(ctx context.Context, q string, args ...any) (sql.Result, error) {
	if tx.failure == nil {
		tx.detail.phase = "statement"
	}
	r, err := retryResearchExec(ctx, tx.Tx, q, args...)
	if err != nil {
		err = fmt.Errorf("%s: %w", sqlOperation(q), err)
		tx.failure = err
	}
	return r, err
}
func (tx *researchTx) Commit() error {
	start := time.Now()
	if tx.failure == nil {
		tx.detail.phase = "commit"
	}
	err := tx.Tx.Commit()
	tx.once.Do(func() { tx.conn.Close(); tx.finish(errors.Join(tx.failure, err), time.Since(start)) })
	return err
}
func (tx *researchTx) Rollback() error {
	err := tx.Tx.Rollback()
	tx.once.Do(func() { tx.conn.Close(); tx.finish(tx.failure, 0) })
	return err
}
