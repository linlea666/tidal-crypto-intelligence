package datahub

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// Adds a 200 quote/s paper path to the existing constrained long replay.
// Generated quotes are confined to _test.go and never enter a live database.
func paperResourceStart(t *testing.T, h *Hub) func() {
	t.Helper()
	p, err := openPaper(h.Store.root, "run")
	if err != nil {
		t.Fatal(err)
	}
	h.Store.paper = p
	now := time.Now().UTC()
	origin := now.Add(-time.Second)
	s := p.snapshot()
	s.Origin = &origin
	s.GoodSince = now.Add(-time.Minute)
	s.Gap = false
	s.Pause = ""
	s.FundingThrough = now.Add(24 * time.Hour)
	if err = p.commit(context.Background(), s, paperBatch{}); err != nil {
		t.Fatal(err)
	}
	p.instrument = paperInstrument{At: now, Status: "TRADING", Step: pd(".001"), Minimum: pd(".001"), Maximum: pd("1000"), MinNotional: pd("5")}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		tick := time.NewTicker(5 * time.Millisecond)
		defer tick.Stop()
		var id, seq int64
		var lastSecond, lastSignal time.Time
		candles := map[int64]Candle{}
		for {
			select {
			case <-ctx.Done():
				done <- nil
				return
			case now := <-tick.C:
				now = now.UTC()
				id++
				p.markAt = now
				p.sourceAt = now
				raw := []byte(fmt.Sprintf(`{"e":"bookTicker","s":"BTCUSDT","st":1,"u":%d,"E":%d,"b":"80000","a":"80001","B":"1","A":"1"}`, id, now.UnixMilli()))
				if err := p.streamMessage(ctx, paperMessage{At: now, Raw: raw}, candles, false); err != nil {
					done <- err
					return
				}
				if now.Sub(lastSecond) >= time.Second {
					p.atr = &paperIntent{ATR: pd("500"), ATRThrough: now.Truncate(time.Hour)}
					if err := p.heartbeat(ctx, now, false); err != nil {
						done <- err
						return
					}
					p.publishFeed()
					lastSecond = now
				}
				if now.Sub(lastSignal) >= time.Minute {
					seq++
					side := "buy"
					if seq%2 == 0 {
						side = "sell"
					}
					signal := Signal{ID: fmt.Sprintf("resource-%d", seq), Asset: "BTC", Rules: MultifactorRules, Direction: side, At: now}
					if err := p.consume(ctx, []paperPublication{{Seq: seq, At: now, Signal: signal}}, now, false); err != nil {
						done <- err
						return
					}
					lastSignal = now
				}
			}
		}
	}()
	return func() {
		cancel()
		if err := <-done; err != nil && err != context.Canceled {
			t.Error("paper concurrent resource path", err)
		}
		if err := p.verifyRecovery(); err != nil {
			t.Error("paper resource ledger", err)
		}
		t.Logf("paper replay bytes=%d cursor=%d", p.size(), p.snapshot().Cursor)
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
	deadline := time.NewTimer(3 * time.Second)
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
