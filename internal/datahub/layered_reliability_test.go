package datahub

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestShortProjectionAndRegistrationDoNotWaitForHubConnection(t *testing.T) {
	h := shortTestHub(t, time.Now().UTC())
	ctx := context.Background()
	conn, err := h.Store.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	step, stop := context.WithTimeout(ctx, 150*time.Millisecond)
	defer stop()
	s := ShortObservation{At: time.Now().UTC(), Registration: "registered"}
	if err = h.Store.shortSaveState(step, "short-flow/current", s); err != nil {
		t.Fatal("display still blocked by hub", err)
	}
	var got ShortObservation
	if err = h.Store.shortStateResult(step, "short-flow/current", &got); err != nil || got.Registration != "registered" {
		t.Fatal(got, err)
	}
	// A full research sub-budget cannot suppress the explicit failure state.
	if _, err = h.Store.shortDB().Exec("UPDATE sf_budget SET used=?", shortFlowBudget); err != nil {
		t.Fatal(err)
	}
	h.Store.shortGap(time.Now(), context.DeadlineExceeded)
	var gap shortGap
	if err = h.Store.shortStateResult(step, "short-flow/gap", &gap); err != nil || !gap.Paused {
		t.Fatal("lost failure while capacity full", gap, err)
	}
}

func TestPaperRealProducerQueueFreezesBlockingStage(t *testing.T) {
	// Independent producer exercises exactly the production 256-message queue.
	// The deliberately blocked consumer must not silently drop offered messages.
	d := &paperDiagnostics{}
	queue := make(chan paperMessage, 256)
	unblock := d.stage("ledger_begin")
	done := make(chan *paperDiagnosticView, 1)
	go func() {
		var failure *paperDiagnosticView
		for i := 0; i < 257; i++ {
			if x := paperEnqueueStream(queue, paperMessage{Kind: "stream", At: time.Now().UTC()}, d); x != nil {
				failure = x
			}
		}
		done <- failure
	}()
	failure := <-done
	unblock()
	if failure == nil || failure.Active != "ledger_begin" || failure.Received != 257 || failure.Enqueued != 256 || failure.Overflows != 1 {
		t.Fatalf("missing offered/accepted/failure evidence: %+v", failure)
	}
	for len(queue) > 0 {
		m := <-queue
		d.processed(m.At)
	}
	v := d.snapshot()
	if v.Processed != 256 || v.HighWater != 256 || failure.Processed != 0 {
		t.Fatal("diagnostic mutated historical evidence", v)
	}
	// Published snapshots own maps; API encoding cannot race with producers.
	encoded, err := json.Marshal(failure)
	if err != nil || len(encoded) > 32<<10 {
		t.Fatal("diagnostics not bounded", len(encoded), err)
	}
}

func TestPaperDrawdownLowerBoundRetainsKnownPeakAcrossGap(t *testing.T) {
	p, now := paperFixture(t)
	s := p.snapshot()
	origin := now.Add(-5 * time.Minute)
	s.Origin = &origin
	values := []*string{flowPtr("10020"), nil, flowPtr("9990"), flowPtr("9980")}
	batch := paperBatch{}
	for i, v := range values {
		e := paperEquity{Group: "risk", At: now.Add(time.Duration(i-4) * time.Minute)}
		if v != nil {
			x := pd(*v)
			e.BeforeFunding = &x
		}
		batch.Equity = append(batch.Equity, e)
	}
	if err := p.commit(context.Background(), s, batch); err != nil {
		t.Fatal(err)
	}
	curve, err := p.equityCurve(context.Background(), s, "risk", now)
	if err != nil {
		t.Fatal(err)
	}
	paperAssertDecimal(t, *curve.DrawdownLowerBound, "40")
	paperAssertDecimal(t, *curve.MaximumDrawdown, "10")
	if curve.DrawdownComplete || curve.CompleteDrawdown != nil || curve.Points[1].Value != nil {
		t.Fatal("gap was fabricated as complete", curve)
	}
}

func TestSignalClocksAreObservedAfterCommitAndNeverBackfilled(t *testing.T) {
	h, err := Open(Config{Root: t.TempDir(), Offline: true})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Store.Close()
	ctx := context.Background()
	now := time.Now().UTC()
	sig := Signal{ID: "clock-event", Asset: "BTC", Rules: MultifactorRules, At: now, Direction: "sell"}
	if err = h.commitSignals(ctx, "BTC", signalState{}, []Signal{sig}, map[string]string{sig.ID: "anomaly"}, now); err != nil {
		t.Fatal(err)
	}
	var first Signal
	var clock struct {
		FirstReadable time.Time `json:"firstReadableAt"`
	}
	if err = h.Store.document(ctx, "signal", sig.ID, &first); err != nil {
		t.Fatal(err)
	}
	if err = h.Store.document(ctx, "signal-clock", sig.ID, &clock); err != nil {
		t.Fatal(err)
	}
	if first.ComputedAt == nil || clock.FirstReadable.Before(*first.ComputedAt) || first.CoreInputAvailableAt != nil {
		t.Fatal("fabricated source availability", first.ComputedAt, clock)
	}
	if err = h.commitSignals(ctx, "BTC", signalState{}, []Signal{sig}, map[string]string{sig.ID: "anomaly"}, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	var after Signal
	h.Store.document(ctx, "signal", sig.ID, &after)
	if !after.ComputedAt.Equal(*first.ComputedAt) {
		t.Fatal("duplicate event rewrote compute clock")
	}
}
