package datahub

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestResearchWriteFailureDistinguishesQueueAndConnection(t *testing.T) {
	w := testStore(t)
	ctx := context.Background()
	for _, phase := range []string{"writer_queue", "connection"} {
		t.Run(phase, func(t *testing.T) {
			var release func()
			if phase == "writer_queue" {
				tx, err := w.research.BeginNamedTx(ctx, nil, "test-holder")
				if err != nil {
					t.Fatal(err)
				}
				release = func() { _ = tx.Rollback() }
			} else {
				conn, err := w.shortDB().DB.Conn(ctx)
				if err != nil {
					t.Fatal(err)
				}
				release = func() { _ = conn.Close() }
			}
			defer release()
			short, cancel := context.WithTimeout(ctx, 40*time.Millisecond)
			_, err := w.shortDB().ExecContext(short, "INSERT INTO sf_records VALUES('test',?,0,?)", "private-identity", "private-payload")
			cancel()
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(err)
			}
			failed := w.research.writer.snapshot()
			if failed.LastFailurePhase != phase {
				t.Fatal(failed)
			}
			if phase == "connection" && failed.LastFailureConnWaitMS < 20 {
				t.Fatal("connection wait missing", failed)
			}
			if phase == "writer_queue" && (failed.LastFailureGateWaitMS < 20 || failed.LastFailureConnWaitMS != 0) {
				t.Fatal("queue wait misattributed", failed)
			}
			b, _ := json.Marshal(failed)
			if strings.Contains(string(b), "private-") {
				t.Fatal("arguments leaked into diagnostics")
			}
			release()
			if _, err = w.shortDB().ExecContext(ctx, "DELETE FROM sf_records WHERE kind='test'"); err != nil {
				t.Fatal(err)
			}
			after := w.research.writer.snapshot()
			if after.Failures != failed.Failures || after.LastFailurePhase != phase || !after.LastFailureAt.Equal(*failed.LastFailureAt) {
				t.Fatal("recovery erased failure")
			}
		})
	}
}

func TestResearchNamedTransactionAndStatementFailure(t *testing.T) {
	w := testStore(t)
	ctx := context.Background()
	tx, err := w.research.BeginNamedTx(ctx, nil, "baseline-test")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO no_such_table VALUES(?)", "do-not-expose"); err == nil {
		t.Fatal("expected failure")
	}
	_ = tx.Rollback()
	s := w.research.writer.snapshot()
	if s.LastFailureOperation != "baseline-test" || s.LastFailurePhase != "statement" || strings.Contains(s.LastFailure, "do-not-expose") {
		t.Fatal(s)
	}
	conn, err := w.shortDB().DB.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	short, cancel := context.WithTimeout(ctx, 40*time.Millisecond)
	_, err = w.research.BeginNamedTx(short, nil, "book-publication-test")
	cancel()
	conn.Close()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	s = w.research.writer.snapshot()
	if s.LastFailureOperation != "book-publication-test" || s.LastFailurePhase != "connection" {
		t.Fatal(s)
	}
}

func TestBookReadTimeoutNamesPhaseWithoutRequestPayload(t *testing.T) {
	h, err := Open(Config{Root: t.TempDir(), Offline: true})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Store.Close()
	conn, err := h.Store.shortDB().DB.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	_, err = h.bookFlowView(ctx, url.Values{"id": {"private-request-id"}}, time.Now())
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "book-flow read state:") || strings.Contains(err.Error(), "private-request-id") {
		t.Fatal(err)
	}
}
