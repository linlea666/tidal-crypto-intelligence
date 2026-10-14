package datahub

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestResearchFailureCapturesOpenCursorAndSpentCallerBudget(t *testing.T) {
	w := testStore(t)
	ctx := context.Background()
	const query = "SELECT 'private literal' AS marker UNION ALL SELECT ?"
	rows, err := w.shortDB().QueryContext(ctx, query, "private argument")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal(rows.Err())
	}
	// The SQL call returned, but application row processing still owns the
	// one short connection. This is not a writer holding the serialization gate.
	short, cancel := context.WithTimeout(ctx, 40*time.Millisecond)
	_, err = w.shortDB().ExecContext(short, "INSERT INTO sf_records VALUES('test','trace',0,'{}')")
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	stats := w.research.writer.snapshot()
	failure := stats.RecentFailures[len(stats.RecentFailures)-1]
	if failure.Phase != "connection" || failure.EntryBudgetMS == nil || *failure.EntryBudgetMS <= 0 || failure.ConnectionWaitMS < 20 {
		t.Fatal(failure)
	}
	for _, snapshot := range []researchConnectionSnapshot{failure.ConnectionAtEntry, failure.ConnectionAtFailure} {
		if len(snapshot.Active) != 1 || snapshot.Active[0].Kind != "rows" || snapshot.Active[0].Statement != researchStatementID(query) {
			t.Fatal("cursor owner missing", snapshot)
		}
	}
	raw, _ := json.Marshal(stats)
	if strings.Contains(string(raw), "private") || strings.Contains(string(raw), query) {
		t.Fatal("query literal/argument leaked")
	}
	rows.Close()
	if active := w.research.writer.connectionTrace.snapshot(); len(active.Active) != 0 {
		t.Fatal("cursor lease survived Close", active)
	}
	// The write has not started: an expired parent must not be called a queue
	// or SQLite stall, even when a free gate would also be selectable.
	expired, done := context.WithDeadline(ctx, time.Now().Add(-time.Second))
	defer done()
	_, err = w.shortDB().ExecContext(expired, "INSERT INTO sf_records VALUES('test','must-not-exist',0,'{}')")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	stats = w.research.writer.snapshot()
	failure = stats.RecentFailures[len(stats.RecentFailures)-1]
	if failure.Phase != "caller_budget" || failure.EntryBudgetMS == nil || *failure.EntryBudgetMS >= 0 || failure.ConnectionWaitMS != 0 {
		t.Fatal(failure)
	}
	if _, err = w.shortDB().ExecContext(ctx, "INSERT INTO sf_records VALUES('test','recovered',0,'{}')"); err != nil {
		t.Fatal(err)
	}
	var count int
	if err = w.shortDB().QueryRow("SELECT count(*) FROM sf_records WHERE kind='test'").Scan(&count); err != nil || count != 1 {
		t.Fatal("expired/failed write was persisted", count, err)
	}
	if got := w.research.writer.snapshot(); got.Failures != stats.Failures || len(got.RecentFailures) != 2 {
		t.Fatal("successful write erased failures", got)
	}
}

func TestResearchTracePreparedStatementsMetadataAndCancellation(t *testing.T) {
	w := testStore(t)
	db := w.shortDB().DB
	ctx := context.Background()
	trace := w.research.writer.connectionTrace
	if _, err := db.Exec("CREATE TABLE trace_test(n INTEGER, s TEXT, b BLOB)"); err != nil {
		t.Fatal(err)
	}
	stmt, err := db.PrepareContext(ctx, "INSERT INTO trace_test VALUES(@n,@s,@b)")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = stmt.ExecContext(ctx, sql.Named("n", 7), sql.Named("s", "private"), sql.Named("b", []byte{1, 2})); err != nil {
		t.Fatal(err)
	}
	stmt.Close()
	query, err := db.Prepare("SELECT n,s,b FROM trace_test WHERE n=?")
	if err != nil {
		t.Fatal(err)
	}
	defer query.Close()
	read, cancel := context.WithCancel(ctx)
	rows, err := query.QueryContext(read, 7)
	if err != nil {
		t.Fatal(err)
	}
	types, err := rows.ColumnTypes()
	if err != nil || len(types) != 3 || types[0].DatabaseTypeName() != "INTEGER" || types[1].DatabaseTypeName() != "TEXT" || types[2].DatabaseTypeName() != "BLOB" {
		t.Fatal(types, err)
	}
	if !rows.Next() {
		t.Fatal(rows.Err())
	}
	var n int
	var s string
	var b []byte
	if err = rows.Scan(&n, &s, &b); err != nil || n != 7 || s != "private" || len(b) != 2 {
		t.Fatal(n, s, b, err)
	}
	cancel()
	rows.Close()
	if got := trace.snapshot(); len(got.Active) != 0 {
		t.Fatal("cancelled cursor still marked active", got)
	}
	// Exercise transaction rollback and commit using the same driver and pool.
	for _, commit := range []bool{false, true} {
		tx, err := w.shortDB().BeginNamedTx(ctx, nil, "trace-transaction")
		if err != nil {
			t.Fatal(err)
		}
		if len(trace.snapshot().Active) != 1 || trace.snapshot().Active[0].Kind != "transaction" {
			t.Fatal("transaction owner missing")
		}
		if _, err = tx.ExecContext(ctx, "UPDATE trace_test SET n=n+1"); err != nil {
			t.Fatal(err)
		}
		if commit {
			err = tx.Commit()
		} else {
			err = tx.Rollback()
		}
		if err != nil || len(trace.snapshot().Active) != 0 {
			t.Fatal(err, trace.snapshot())
		}
	}
	if err = db.QueryRow("SELECT n FROM trace_test").Scan(&n); err != nil || n != 8 {
		t.Fatal("transaction semantics changed", n, err)
	}
	if db.Stats().MaxOpenConnections != 1 || w.research.Stats().MaxOpenConnections != 1 {
		t.Fatal("connection budget changed")
	}
}

func TestResearchFailureHistoryAndActiveTraceAreBounded(t *testing.T) {
	w := testStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for i := 0; i < researchFailureHistoryLimit+3; i++ {
		_, _ = w.shortDB().ExecContext(ctx, "DELETE FROM sf_records WHERE kind='test'")
	}
	before := w.research.writer.snapshot()
	if before.Failures != researchFailureHistoryLimit+3 || len(before.RecentFailures) != researchFailureHistoryLimit {
		t.Fatal(before)
	}
	if before.RecentFailures[0].EntryBudgetMS != nil {
		t.Fatal("unknown deadline became zero")
	}
	_, _ = w.shortDB().ExecContext(ctx, "DELETE FROM sf_records WHERE kind='test'")
	after := w.research.writer.snapshot()
	if !before.RecentFailures[len(before.RecentFailures)-1].At.Before(after.RecentFailures[len(after.RecentFailures)-1].At) {
		t.Fatal("returned history mutated after snapshot")
	}
	trace := &researchConnectionTrace{}
	var close []func()
	for i := 0; i < researchTraceLimit+2; i++ {
		close = append(close, trace.start("rows", "test"))
	}
	if got := trace.snapshot(); len(got.Active) != researchTraceLimit || !got.Truncated {
		t.Fatal(got)
	}
	for _, done := range close {
		done()
		done()
	}
	if got := trace.snapshot(); len(got.Active) != 0 || got.Truncated {
		t.Fatal(got)
	}
}
