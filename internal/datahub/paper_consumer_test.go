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
