package datahub

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestShortCheckpointUsesRemainingBudgetAndRetainsFailure(t *testing.T) {
	now := time.Now().UTC()
	h := shortTestHub(t, now)
	w := h.Store
	trace := &shortTrace{Operations: map[string]time.Duration{"baseline_page": time.Millisecond}}
	before := w.shortDB().writer.snapshot()
	release, err := w.shortDB().writer.acquire(context.Background(), "test-held-writer")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		time.Sleep(180 * time.Millisecond)
		release(nil, 0)
		close(done)
	}()
	// The fast primary phase left time in its original two-second cycle.
	ctx, cancel := context.WithTimeout(context.Background(), 1800*time.Millisecond)
	w.recordShortRuntimeContext(ctx, "baseline", now, time.Millisecond, trace, nil)
	cancel()
	<-done
	r := w.shortRuntimeView()
	if r.PersistenceError != "" || w.shortDB().writer.snapshot().Failures != before.Failures || r.Stages["baseline"].Errors != 0 {
		t.Fatal("available cycle budget was not used", r)
	}

	release, err = w.shortDB().writer.acquire(context.Background(), "test-held-writer")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 40*time.Millisecond)
	started := time.Now()
	primaryError := errors.New("primary stage failed before checkpoint")
	w.recordShortRuntimeContext(ctx, "baseline", now.Add(time.Second), time.Millisecond, trace, primaryError)
	cancel()
	elapsed := time.Since(started)
	release(nil, 0)
	r = w.shortRuntimeView()
	failed := w.shortDB().writer.snapshot()
	if elapsed > 500*time.Millisecond || r.PersistenceFailures != 1 || r.PersistenceError == "" || r.LastPersistenceFailureAt == nil || r.PersistenceRecoveredAt != nil || r.Stages["baseline"].Errors != 1 {
		t.Fatal("deadline/failure history lost", elapsed, r)
	}
	if failed.Failures != before.Failures+1 || failed.LastFailureOperation != "short-flow/runtime-v2: INSERT INTO sf_records VALUES('state',?,?,?) ON" {
		t.Fatal("failed state key missing", failed)
	}
	var stored ShortRuntime
	if err := w.shortLoad(context.Background(), "state", "short-flow/runtime-v2", &stored); err != nil || !stored.At.Equal(now) {
		t.Fatal("failed checkpoint changed durable state", err, stored)
	}
	w.recordShortRuntime("baseline", now.Add(2*time.Second), time.Millisecond, trace, nil)
	if err := w.shortLoad(context.Background(), "state", "short-flow/runtime-v2", &stored); err != nil {
		t.Fatal(err)
	}
	if stored.PersistenceError != "" || stored.PersistenceFailures != 1 || stored.LastPersistenceFailureAt == nil || stored.PersistenceRecoveredAt == nil || stored.Stages["baseline"].Errors != 1 || stored.Stages["baseline"].LastFailure != primaryError.Error() {
		t.Fatal("recovery erased checkpoint/primary failure", stored)
	}
	if w.shortDB().writer.snapshot().Failures != failed.Failures {
		t.Fatal("recovery reset writer history")
	}
}

func TestShortStateFailureIdentifiesOnlyInternalKey(t *testing.T) {
	h := shortTestHub(t, time.Now().UTC())
	for _, key := range []string{"short-flow/current", "short-flow/report", "short-flow/gap", "short-flow/runtime-v2"} {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := h.Store.shortSaveState(ctx, key, map[string]string{"private": "never-log-payload"}); err == nil {
			t.Fatal("cancelled write succeeded", key)
		}
		op := h.Store.shortDB().writer.snapshot().LastFailureOperation
		if !strings.HasPrefix(op, key+": ") || strings.Contains(op, "never-log-payload") {
			t.Fatal(op)
		}
	}
}
