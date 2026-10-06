package datahub

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Explicit local-browser fixture, test binary only. Never used by Open or Run.
func TestPaperBrowserFixture(t *testing.T) {
	dest := os.Getenv("TIDAL_PAPER_UI_FIXTURE")
	if dest == "" {
		t.Skip("opt-in disposable browser fixture")
	}
	abs, err := filepath.Abs(dest)
	if err != nil || !strings.Contains(abs, "/tmp/paper-ui-") {
		t.Fatal("fixture must be in a disposable tmp/paper-ui- directory")
	}
	p, _ := paperFixture(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Minute)
	start := now.Add(-32 * time.Minute)
	origin := start.Add(-time.Minute)
	s := p.snapshot()
	s.Generation = "UI-test-only"
	s.Origin = &origin
	s.GoodSince = start.Add(-time.Minute)
	s.FundingThrough = now.Add(time.Hour)
	s.LastTick = start.Add(-time.Second)
	if err = p.commit(ctx, s, paperBatch{}); err != nil {
		t.Fatal(err)
	}
	p.instrument.At = start
	p.quote.At = start
	p.quote.EventAt = start
	var seq int64
	for i := 0; i < 1920; i++ {
		at := start.Add(time.Duration(i) * time.Second)
		p.sourceAt = at
		p.markAt = at
		p.atr = &paperIntent{ATR: pd("100"), ATRThrough: at.Truncate(time.Hour)}
		bid, ask := "9999", "10000"
		if i%120 >= 20 && i%120 < 25 {
			bid, ask = "10300", "10301"
		}
		paperTick(t, p, int64(i+2), at, bid, ask, "1")
		if i%120 == 0 {
			seq++
			side := "buy"
			if seq%2 == 0 {
				side = "sell"
			}
			paperSignal(t, p, seq, at, side)
		}
		if err = p.heartbeat(ctx, at, false); err != nil {
			t.Fatal(err)
		}
	}
	if err = p.settle(ctx, []paperFunding{{At: start.Add(10 * time.Minute), Acquired: now, Rate: pd(".0001"), Mark: pd("10000")}}, now); err != nil {
		t.Fatal(err)
	}
	if err = p.verifyRecovery(); err != nil {
		t.Fatal(err)
	}
	if err = BackupFile(ctx, p.path, abs); err != nil {
		t.Fatal(err)
	}
	t.Log("Disposable local paper fixture saved; not forward performance")
}
