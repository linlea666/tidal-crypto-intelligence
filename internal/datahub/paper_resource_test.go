package datahub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// A producer with absolute deadlines supplies every scheduled message through
// the production queue and consumer. Synthetic prices stay in isolated tests;
// count conservation, actual fills, maintenance and recovery are all checked.
func paperResourceStart(t *testing.T, h *Hub) func() {
	t.Helper()
	p, err := openPaper(h.Store.root, "run")
	if err != nil {
		t.Fatal(err)
	}
	h.Store.paper = p
	reader, err := openPaperPublicationReader(h.Store.root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reader.Close() })
	// No settlement occurs during this synthetic, at-most-six-hour fixture.
	s := p.snapshot()
	s.FundingThrough = time.Now().UTC().Add(24 * time.Hour)
	if err = p.commit(context.Background(), s, paperBatch{}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	producerCtx, stopProducers := context.WithCancel(ctx)
	out := make(chan paperMessage, 256)
	consumerDone := make(chan struct{})
	go func() { defer close(consumerDone); h.paperConsumeLoop(ctx, p, reader, out) }()
	producerDone := make(chan error, 1)
	var offered, accepted, bursts uint64
	var maxLateness time.Duration
	go func() {
		start := time.Now()
		var lastMark, lastMetadata time.Time
		var quoteID int64
		for id := int64(1); ; id++ {
			deadline := start.Add(time.Duration(id) * 5 * time.Millisecond)
			timer := time.NewTimer(max(time.Duration(0), time.Until(deadline)))
			select {
			case <-producerCtx.Done():
				timer.Stop()
				producerDone <- nil
				return
			case <-timer.C:
			}
			now := time.Now().UTC()
			maxLateness = max(maxLateness, time.Since(deadline))
			if !now.Truncate(5 * time.Minute).Equal(lastMetadata.Truncate(5 * time.Minute)) {
				instrument := []byte(`{"symbols":[{"symbol":"BTCUSDT","status":"TRADING","contractType":"PERPETUAL","marginAsset":"USDT","filters":[{"filterType":"LOT_SIZE","stepSize":"0.001","minQty":"0.001","maxQty":"1000"},{"filterType":"MIN_NOTIONAL","notional":"5"}]}]}`)
				if !paperSend(producerCtx, out, paperMessage{Kind: "instrument", At: now, Raw: instrument}) {
					producerDone <- nil
					return
				}
				rows := [][]any{}
				through := now.Truncate(5 * time.Minute)
				for at := through.Add(-17 * time.Hour); at.Before(through); at = at.Add(5 * time.Minute) {
					rows = append(rows, []any{at.UnixMilli(), "80000", "80250", "79750", "80000", "1", at.Add(5*time.Minute).UnixMilli() - 1})
				}
				raw, _ := json.Marshal(rows)
				if !paperSend(producerCtx, out, paperMessage{Kind: "candles", At: now, Raw: raw}) {
					producerDone <- nil
					return
				}
				lastMetadata = now
			}
			quoteID++
			messages := []string{fmt.Sprintf(`{"e":"bookTicker","s":"BTCUSDT","st":1,"u":%d,"E":%d,"b":"80000","a":"80001","B":"1","A":"1"}`, quoteID, now.UnixMilli())}
			if now.Sub(lastMark) >= time.Second {
				messages = append(messages, fmt.Sprintf(`{"e":"markPriceUpdate","s":"BTCUSDT","E":%d,"p":"80000","T":0}`, now.UnixMilli()))
				lastMark = now
			}
			for _, raw := range messages {
				offered++
				f := paperEnqueueStream(out, paperMessage{Kind: "stream", At: now, Raw: []byte(raw)}, &p.diagnostics)
				if f != nil {
					producerDone <- fmt.Errorf("queue overflow: stage=%s duration=%.3fms", f.Active, f.ActiveMS)
					return
				}
				accepted++
			}
			// Sanitized envelope of the captured 257-message / 1.784ms burst.
			// Repeat during concurrent maintenance; no ticker beats can vanish.
			if id%12000 == 0 {
				bursts++
				burstAt := time.Now()
				for n := 1; n <= 257; n++ {
					if wait := time.Until(burstAt.Add(time.Duration(n) * 1784 * time.Microsecond / 257)); wait > 0 {
						time.Sleep(wait)
					}
					at := time.Now().UTC()
					quoteID++
					offered++
					raw := []byte(fmt.Sprintf(`{"e":"bookTicker","s":"BTCUSDT","u":%d,"E":%d,"b":"80000","a":"80001","B":"1","A":"1"}`, quoteID, at.UnixMilli()))
					if f := paperEnqueueStream(out, paperMessage{Kind: "stream", At: at, Raw: raw}, &p.diagnostics); f != nil {
						producerDone <- fmt.Errorf("captured burst overflow: %s", f.Active)
						return
					}
					accepted++
				}
			}
		}
	}()
	signalDone := make(chan error, 1)
	go func() {
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		var last time.Time
		var seq int64
		for {
			select {
			case <-producerCtx.Done():
				signalDone <- nil
				return
			case <-tick.C:
				now := time.Now().UTC()
				s := p.snapshot()
				if s.Origin == nil || s.Gap || now.Sub(last) < time.Minute {
					continue
				}
				seq++
				side := "buy"
				if seq%2 == 0 {
					side = "sell"
				}
				sig := Signal{ID: fmt.Sprintf("resource-%d", seq), Asset: "BTC", Rules: MultifactorRules, Direction: side, At: now}
				write, stop := context.WithTimeout(producerCtx, 2*time.Second)
				err := h.commitSignals(write, "BTC", signalState{}, []Signal{sig}, map[string]string{sig.ID: "anomaly"}, now)
				stop()
				if err != nil {
					signalDone <- err
					return
				}
				last = now
			}
		}
	}()
	return func() {
		stopProducers()
		producerErr := <-producerDone
		signalErr := <-signalDone
		drainUntil := time.Now().Add(2 * time.Second)
		for p.diagnostics.snapshot().Processed < accepted && time.Now().Before(drainUntil) {
			time.Sleep(time.Millisecond)
		}
		cancel()
		<-consumerDone
		d := p.diagnostics.snapshot()
		if producerErr != nil {
			t.Error("paper resource producer", producerErr)
		}
		if signalErr != nil && !errors.Is(signalErr, context.Canceled) {
			t.Error("paper resource publication", signalErr)
		}
		if d.Received != offered || d.Enqueued != accepted || d.Processed != accepted || d.Overflows != 0 {
			t.Error("paper resource message conservation", offered, accepted, d.Processed, d.Overflows)
		}
		if err := p.verifyRecovery(); err != nil {
			t.Error("paper resource ledger", err)
		}
		var fills int
		if err := p.db.QueryRow("SELECT count(*) FROM paper_fills").Scan(&fills); err != nil || fills == 0 {
			t.Error("paper replay produced no actual fills", fills, err)
		}
		state := p.snapshot()
		var gaps, failures, closed, complete int
		for _, q := range []struct {
			sql  string
			dest *int
		}{
			{"SELECT count(*) FROM paper_events WHERE kind='gap' AND json_extract(payload,'$.reason')<>'restart_gap'", &gaps},
			{"SELECT count(*) FROM paper_events WHERE kind IN ('failure','fetch_failure')", &failures},
			{"SELECT count(*) FROM paper_trades WHERE exited IS NOT NULL", &closed},
			{"SELECT count(*) FROM paper_trades WHERE exited IS NOT NULL AND coalesce(json_array_length(payload,'$.quality'),0)=0", &complete},
		} {
			if err := p.db.QueryRow(q.sql).Scan(q.dest); err != nil {
				t.Error(err)
			}
		}
		coverage := 0.0
		if state.ObservedSeconds > 0 {
			coverage = 100 * float64(state.CoveredSeconds) / float64(state.ObservedSeconds)
		}
		report := map[string]any{"offered": offered, "accepted": accepted, "consumed": d.Processed, "overflows": d.Overflows, "bursts": bursts, "maxProducerLatenessMs": float64(maxLateness) / float64(time.Millisecond), "diagnostics": d, "bytes": p.size(), "cursor": state.Cursor, "fills": fills, "closed": closed, "completeClosed": complete, "gapsAfterRestart": gaps, "failures": failures, "observedSeconds": state.ObservedSeconds, "coveredSeconds": state.CoveredSeconds, "coveragePercent": coverage, "accounts": state.Accounts}
		if dir := os.Getenv("TIDAL_RESOURCE_OUTPUT"); dir != "" {
			raw, err := json.MarshalIndent(report, "", "  ")
			if err == nil {
				err = os.WriteFile(filepath.Join(dir, "paper.json"), raw, 0644)
			}
			if err != nil {
				t.Error(err)
			}
		}
		if gaps != 0 || failures != 0 {
			t.Error("paper replay quality failed", gaps, failures)
		}
		if state.ObservedSeconds >= 60 && coverage < 95 {
			t.Error("paper replay coverage below 95%", coverage)
		}
		t.Logf("paper production replay offered=%d accepted=%d consumed=%d overflow=%d bursts=%d maxLateness=%s bytes=%d cursor=%d fills=%d", offered, accepted, d.Processed, d.Overflows, bursts, maxLateness, p.size(), p.snapshot().Cursor, fills)
	}
}

func TestPaperResourceActorRecordsFills(t *testing.T) {
	h, err := Open(Config{Root: t.TempDir(), Offline: true})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Store.Close()
	stop := paperResourceStart(t, h)
	defer stop()
	deadline := time.NewTimer(40 * time.Second)
	defer deadline.Stop()
	poll := time.NewTicker(20 * time.Millisecond)
	defer poll.Stop()
	for {
		select {
		case <-deadline.C:
			t.Fatal("public-wire resource actor did not consume and fill its first signal")
		case <-poll.C:
			s := h.Store.paper.snapshot()
			if s.Cursor > 0 && s.Accounts[0].Position != nil && s.Accounts[1].Position != nil {
				return
			}
		}
	}
}
