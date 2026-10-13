package datahub

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Reopening a history partition used to attempt journal_mode before installing
// any busy handler. A short-lived lock therefore failed at the first statement.
func TestDatabaseOpenRecoversFromShortLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "locked.sqlite")
	other, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	other.SetMaxOpenConns(1)
	if _, err = other.Exec("CREATE TABLE fixture(value INTEGER); BEGIN EXCLUSIVE; INSERT INTO fixture VALUES(7)"); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		time.Sleep(80 * time.Millisecond)
		_, err := other.Exec("COMMIT")
		done <- err
	}()
	start := time.Now()
	db, err := database(path)
	if unlock := <-done; unlock != nil {
		t.Fatal(unlock)
	}
	if err != nil {
		t.Fatal("short lock prevented database open", err)
	}
	defer db.Close()
	var value, busy int
	if err = db.QueryRow("SELECT value FROM fixture").Scan(&value); err != nil || value != 7 {
		t.Fatal(value, err)
	}
	if err = db.QueryRow("PRAGMA busy_timeout").Scan(&busy); err != nil || busy != 5000 {
		t.Fatal(busy, err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("open exceeded phase budget")
	}
}

type replaySQLiteError int

func (e replaySQLiteError) Error() string { return fmt.Sprintf("database is locked (%d)", e) }
func (e replaySQLiteError) Code() int     { return int(e) }

type recoveryExecutor struct{ failures, calls, code int }

func (e *recoveryExecutor) ExecContext(_ context.Context, q string, _ ...any) (sql.Result, error) {
	if strings.HasPrefix(q, "PRAGMA") {
		return nil, nil
	}
	e.calls++
	if e.calls <= e.failures {
		return nil, fmt.Errorf("wrapped: %w", replaySQLiteError(e.code))
	}
	return nil, nil
}

func TestSQLiteRecoveryRetryUsesCodeAndOriginalDeadline(t *testing.T) {
	for _, code := range []int{5, 261, 773} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			e := &recoveryExecutor{failures: 1, code: code}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if _, err := retryResearchExec(ctx, e, "INSERT INTO fixture VALUES(1)"); err != nil || e.calls != 2 {
				t.Fatal(e.calls, err)
			}
		})
	}
	for _, code := range []int{517, 6, 10} {
		e := &recoveryExecutor{failures: 5, code: code}
		if _, err := retryResearchExec(context.Background(), e, "INSERT INTO fixture VALUES(1)"); err == nil || e.calls != 1 {
			t.Fatal("retried non-recoverable operation", code, e.calls, err)
		}
	}
	e := &recoveryExecutor{failures: 10, code: 261}
	if _, err := retryResearchExec(context.Background(), e, "INSERT INTO fixture VALUES(1)"); err == nil || e.calls != 4 {
		t.Fatal("unbounded retry", e.calls, err)
	}
	e = &recoveryExecutor{failures: 10, code: 261}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := retryResearchExec(ctx, e, "INSERT INTO fixture VALUES(1)"); !errors.Is(err, context.Canceled) || e.calls != 0 {
		t.Fatal("canceled call executed", e.calls, err)
	}
	e = &recoveryExecutor{failures: 10, code: 261}
	ctx, cancel = context.WithTimeout(context.Background(), 8*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := retryResearchExec(ctx, e, "INSERT INTO fixture VALUES(1)"); !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 200*time.Millisecond || e.calls != 1 {
		t.Fatal("retry escaped original deadline", e.calls, err)
	}
}

func TestFactIngestDoesNotQueueBehindFormalHistory(t *testing.T) {
	w := testStore(t)
	busy, err := w.research.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close()
	now := time.Now().UTC().Truncate(time.Minute)
	d, _ := FindDataset(ID("candles", "BTC", "Binance", "spot"))
	o := Observation{Dataset: d.ID, Source: d.Source, ObservedAt: &now, FetchedAt: now, Resolution: 300, Quality: "valid", Payload: Payload{Candle: &Candle{Open: 80000, High: 80100, Low: 79900, Close: 80000, Volume: 1}}}
	done := make(chan error, 1)
	go func() { _, err := w.Ingest(d, o); done <- err }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(300 * time.Millisecond):
		busy.Close()
		<-done
		t.Fatal("live fact ingestion waited for long history connection")
	}
	var n int
	if err = w.shortDB().QueryRow("SELECT count(*) FROM facts WHERE dataset=?", d.ID).Scan(&n); err != nil || n != 1 {
		t.Fatal(n, err)
	}
}
