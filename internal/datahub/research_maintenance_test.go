package datahub

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestResearchMaintenanceBoundedAndResumable(t *testing.T) {
	w := testStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	old := now.Add(-100 * 24 * time.Hour)
	tx, err := w.research.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	exec := func(q string, args ...any) {
		t.Helper()
		if _, e := tx.ExecContext(ctx, q, args...); e != nil {
			t.Fatal(e)
		}
	}
	exec("INSERT INTO study_inputs VALUES('expired-test','failed',0,0,0,'{}')")
	exec("INSERT INTO study_inputs VALUES('ready-test','ready',0,0,0,'{}')")
	exec("INSERT INTO study_input_chunks VALUES('ready-test',0,'x',0,0,'hash','evidence')")
	exec("INSERT INTO mail_batches VALUES('expired-test',?)", old.UnixNano())
	exec("INSERT INTO mail_batches VALUES('recent-test',?)", now.UnixNano())
	exec("INSERT INTO mail_batch_items VALUES('recent-test','recent-test')")
	exec("INSERT INTO mail_batch_topics VALUES('expired-test','book-flow')")
	exec("INSERT INTO mail_results VALUES('expired-test',?,'sent','')", old.UnixNano())
	for i := 0; i < 520; i++ {
		exec("INSERT INTO lz_records(kind,id,asset,at,payload) VALUES('maintenance-test',?,'BTC',?,'{}')", fmt.Sprint(i), old.UnixNano())
		exec("INSERT INTO vix_minute VALUES(?,'old','20',?)", old.Unix()+int64(i), old.Unix())
		exec("INSERT INTO mail_batch_items VALUES(?,'expired-test')", fmt.Sprint(i))
		if i < 130 {
			exec("INSERT INTO study_input_chunks VALUES('expired-test',?,'x',0,0,'hash','evidence')", i)
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	count := func(q string) int {
		t.Helper()
		var n int
		if e := w.research.QueryRow(q).Scan(&n); e != nil {
			t.Fatal(e)
		}
		return n
	}
	for round := 1; round <= 3; round++ {
		if err = w.pruneLiquidations(ctx, now); err != nil {
			t.Fatal(err)
		}
		if err = w.pruneStudySnapshots(ctx, now); err != nil {
			t.Fatal(err)
		}
		if err = w.maintainVIX(ctx, now); err != nil {
			t.Fatal(err)
		}
		remaining := max(0, 520-round*256)
		for _, q := range []string{"SELECT count(*) FROM lz_records WHERE kind='maintenance-test'", "SELECT count(*) FROM vix_minute", "SELECT count(*) FROM mail_batch_items WHERE batch_id='expired-test'"} {
			if n := count(q); n != remaining {
				t.Fatalf("round %d: %s: %d != %d", round, q, n, remaining)
			}
		}
		if n := count("SELECT count(*) FROM study_input_chunks WHERE id='expired-test'"); n != max(0, 130-round*64) {
			t.Fatal("unbounded snapshot cleanup", round, n)
		}
		if count("SELECT count(*) FROM mail_batch_items i WHERE NOT EXISTS(SELECT 1 FROM mail_batches b WHERE b.id=i.batch_id)") != 0 {
			t.Fatal("orphaned partially pruned mail batch")
		}
		if count("SELECT count(*) FROM study_input_chunks WHERE id='ready-test'") != 1 || count("SELECT count(*) FROM mail_batches WHERE id='recent-test'") != 1 || count("SELECT count(*) FROM lz_records WHERE kind='origin'") != 1 {
			t.Fatal("retained evidence erased")
		}
	}
	if count("SELECT count(*) FROM mail_batches WHERE id='expired-test'") != 0 {
		t.Fatal("cleanup did not finish")
	}
	var recorded, actual int
	if err = w.research.QueryRow("SELECT bytes FROM study_input_budget WHERE id=1").Scan(&recorded); err != nil {
		t.Fatal(err)
	}
	if err = w.research.QueryRow("SELECT coalesce(sum(length(payload)+256),0) FROM study_input_chunks").Scan(&actual); err != nil {
		t.Fatal(err)
	}
	if recorded != actual {
		t.Fatal("cleanup counter mismatch", recorded, actual)
	}
}

func TestResearchMaintenanceDefersToLiveTransaction(t *testing.T) {
	w := testStore(t)
	tx, err := w.research.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err = w.maintainResearch(ctx, time.Now(), 30); err != nil {
		t.Fatal(err)
	}
	if err = w.maintainVIX(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 50*time.Millisecond {
		t.Fatal("cleanup waited behind live transaction")
	}
}

func TestResearchRetentionSeeksTimeWithoutScanningAllFacts(t *testing.T) {
	w := testStore(t)
	ctx := context.Background()
	// LIMIT bounds deleted rows, not inspected rows. The production file has
	// 331,447 facts and no expired facts: this must seek on time rather than
	// inspect every retained revision while holding the shared writer.
	rows, err := w.research.QueryContext(ctx, `EXPLAIN QUERY PLAN SELECT dataset,ts,res,revision FROM facts WHERE ts<? AND NOT EXISTS(SELECT 1 FROM study_inputs i WHERE i.state='building' AND facts.ts>=i.from_ts AND facts.ts<i.to_ts) LIMIT 256`, time.Now().Add(-90*24*time.Hour).Unix())
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	sought := false
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err = rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(detail, "SCAN facts") {
			t.Fatal("unbounded retained-fact scan", detail)
		}
		if strings.Contains(detail, "SEARCH facts") && strings.Contains(detail, "ts<?") {
			sought = true
		}
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !sought {
		t.Fatal("retention did not seek its expiry boundary")
	}
}

func TestResearchUsageRestoredBeforeDeferredMaintenance(t *testing.T) {
	root := t.TempDir()
	w, err := OpenWarehouse(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = w.research.Exec("INSERT INTO documents VALUES('budget-test','retained','BTC',?,zeroblob(1048576))", time.Now().Unix()); err != nil {
		w.Close()
		t.Fatal(err)
	}
	w.Close()
	w, err = OpenWarehouse(root)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	before := w.Status().ResearchBytes
	if before < 1<<20 {
		t.Fatal("restarted budget forgot retained data", before)
	}
	tx, err := w.research.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = w.maintainResearch(ctx, time.Now(), 90); err != nil {
		t.Fatal(err)
	}
	if w.Status().ResearchBytes < before {
		t.Fatal("deferred maintenance reset budget")
	}
}
