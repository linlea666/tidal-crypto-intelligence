package datahub

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestPaperActualConsumerCountsEverySubmittedMessage(t *testing.T) {
	h, e := Open(Config{Root: t.TempDir(), Offline: true, PaperMode: "collect"})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	reader, e := openPaperPublicationReader(h.Store.root)
	if e != nil {
		t.Fatal(e)
	}
	defer reader.Close()
	p := h.Store.paper
	out := make(chan paperMessage, 256)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); h.paperConsumeLoop(ctx, p, reader, out) }()
	// Independent producer with an exact message quota and absolute deadlines.
	// A late timer catches up all messages; it cannot discard ticker beats.
	const offered = 1200
	start := time.Now()
	accepted, dropped := 0, 0
	for i := 1; i <= offered; i++ {
		deadline := start.Add(time.Duration(i) * time.Millisecond)
		if delay := time.Until(deadline); delay > 0 {
			time.Sleep(delay)
		}
		at := time.Now().UTC()
		raw := []byte(fmt.Sprintf(`{"e":"bookTicker","s":"BTCUSDT","st":1,"u":%d,"E":%d,"b":"80000","a":"80001","B":"1","A":"1"}`, i, at.UnixMilli()))
		failure := paperEnqueueStream(out, paperMessage{Kind: "stream", At: at, Raw: raw}, &p.diagnostics)
		if failure == nil {
			accepted++
		} else {
			dropped++
		}
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && p.diagnostics.snapshot().Processed < uint64(accepted) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
	d := p.diagnostics.snapshot()
	if d.Received != offered || int(d.Enqueued) != accepted || int(d.Processed) != accepted || int(d.Overflows) != dropped || accepted+dropped != offered {
		t.Fatal("message conservation", offered, accepted, dropped, d)
	}
	if dropped != 0 {
		t.Fatal("paced baseline unexpectedly overflowed", dropped)
	}
	if p.lastQuoteID != offered {
		t.Fatal("last actual wire message not applied", p.lastQuoteID)
	}
	if d.Stages["source_read"].Count == 0 || d.Stages["heartbeat"].Count == 0 || d.Stages["publish_feed"].Count == 0 || d.Stages["consumer_wait"].Count == 0 {
		t.Fatal("replay bypassed production loop", d.Stages)
	}
	if len(d.ArrivalIntervalsUS) != 512 || d.ArrivalThrough == nil || d.LastDequeuedAt == nil {
		t.Fatal("arrival evidence unbounded or absent")
	}
	if e = p.verifyRecovery(); e != nil {
		t.Fatal(e)
	}
	t.Logf("production queue baseline: offered=%d accepted=%d processed=%d lost=%d elapsed=%s highwater=%d", offered, accepted, d.Processed, dropped, time.Since(start), d.HighWater)
}

func TestPaperBurstFailureFreezesConsumerWaitAndArrivalProfile(t *testing.T) {
	var d paperDiagnostics
	end := d.stage("consumer_loop")
	defer end()
	idle := d.stage("consumer_wait")
	out := make(chan paperMessage, 256)
	for i := 0; i < 256; i++ {
		if f := paperEnqueueStream(out, paperMessage{Kind: "stream", At: time.Now().UTC()}, &d); f != nil {
			t.Fatal("early overflow")
		}
	}
	f := paperEnqueueStream(out, paperMessage{Kind: "stream", At: time.Now().UTC()}, &d)
	idle()
	if f == nil || f.Active != "consumer_wait" || len(f.ArrivalIntervalsUS) != 256 || f.ArrivalThrough == nil || f.LastDequeuedAt != nil {
		t.Fatal("cannot distinguish unscheduled consumer from SQL wait", f)
	}
	for len(out) > 0 {
		m := <-out
		d.processed(m.At)
	}
	if f.Processed != 0 || f.Active != "consumer_wait" {
		t.Fatal("failure evidence mutated after drain")
	}
	if d.snapshot().Processed != 256 {
		t.Fatal("lost accepted message")
	}
}

// Production 2026-10-08T19:25:17Z captured 257 arrivals in 1.784ms
// with no dequeue for 14.656ms. Hold the consumer for that bounded scheduling
// delay; synthetic prices are isolated and every message uses the real loop.
func TestPaperCapturedSchedulingBurstKeepsEveryQuote(t *testing.T) {
	h, err := Open(Config{Root: t.TempDir(), Offline: true, PaperMode: "collect"})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Store.Close()
	reader, err := openPaperPublicationReader(h.Store.root)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	p := h.Store.paper
	out := make(chan paperMessage, 256)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		time.Sleep(14656 * time.Microsecond)
		h.paperConsumeLoop(ctx, p, reader, out)
	}()
	defer func() { cancel(); <-done }()
	start := time.Now()
	for i := 1; i <= 257; i++ {
		if wait := time.Until(start.Add(time.Duration(i) * 1784 * time.Microsecond / 257)); wait > 0 {
			time.Sleep(wait)
		}
		at := time.Now().UTC()
		raw := []byte(fmt.Sprintf(`{"e":"bookTicker","s":"BTCUSDT","u":%d,"E":%d,"b":"80000","a":"80001","B":"1","A":"1"}`, i, at.UnixMilli()))
		if failure := paperEnqueueStream(out, paperMessage{Kind: "stream", At: at, Raw: raw}, &p.diagnostics); failure != nil {
			t.Fatal("short scheduling stall became a data gap", failure.Active)
		}
	}
	until := time.Now().Add(2 * time.Second)
	for p.diagnostics.snapshot().Processed < 257 && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
	// Avoid a second wait consuming anything: closing a channel is repeatable.
	d := p.diagnostics.snapshot()
	if d.Received != 257 || d.Enqueued != 257 || d.Processed != 257 || d.Overflows != 0 || p.lastQuoteID != 257 {
		t.Fatal("burst lost an observation", d)
	}
	if d.Stages["enqueue_backpressure"].Count == 0 {
		t.Fatal("test missed full queue")
	}
	if err = p.verifyRecovery(); err != nil {
		t.Fatal(err)
	}
}

func TestPaperBlockedWriterStillTimesOutAndKeepsGap(t *testing.T) {
	h, err := Open(Config{Root: t.TempDir(), Offline: true, PaperMode: "collect"})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Store.Close()
	reader, err := openPaperPublicationReader(h.Store.root)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	p := h.Store.paper
	conn, err := p.db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	out := make(chan paperMessage, 256)
	go func() { defer close(done); h.paperConsumeLoop(ctx, p, reader, out) }()
	defer func() { conn.Close(); cancel(); <-done }()
	until := time.Now().Add(time.Second)
	for p.diagnostics.snapshot().Active != "ledger_begin" && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	var failure *paperDiagnosticView
	at := time.Now().UTC()
	for i := 0; i < 257; i++ {
		failure = paperEnqueueStream(out, paperMessage{Kind: "stream", At: at, Raw: []byte(`{}`)}, &p.diagnostics)
	}
	elapsed := time.Since(at)
	if failure == nil || failure.Active != "ledger_begin" || failure.Enqueued != 256 || failure.Overflows != 1 || elapsed < 900*time.Millisecond || elapsed > 1500*time.Millisecond {
		t.Fatal("unbounded or suppressed writer gap", elapsed, failure)
	}
	conn.Close()
	// Explicit failure is delivered through the same path as the live producer.
	paperSend(ctx, out, paperMessage{Kind: "gap", At: time.Now().UTC(), Err: fmt.Errorf("perpetual quote queue overflow"), Diagnostics: failure})
	until = time.Now().Add(3 * time.Second)
	for p.snapshot().LastFailure != "stream: perpetual quote queue overflow" && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
	s := p.snapshot()
	if !s.Gap || s.Accounts[0].Position != nil || s.Accounts[1].Position != nil {
		t.Fatal("writer stall fabricated a valid path")
	}
	var n int
	if err = p.db.QueryRow("SELECT count(*) FROM paper_events WHERE kind='fetch_failure'").Scan(&n); err != nil || n == 0 {
		t.Fatal("failure evidence missing", n, err)
	}
}
