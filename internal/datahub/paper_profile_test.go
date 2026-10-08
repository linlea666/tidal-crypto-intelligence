package datahub

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"
)

// Opt-in replay consumes a private, bounded timing profile from a production
// overflow diagnostic. Only arrival intervals are used; no live API or account.
func TestPaperCapturedBurstProductionPath(t *testing.T) {
	name := os.Getenv("TIDAL_PAPER_PROFILE_FILE")
	if name == "" {
		t.Skip("requires actual captured arrival intervals")
	}
	raw, e := os.ReadFile(name)
	if e != nil {
		t.Fatal(e)
	}
	if len(raw) > 256<<10 {
		t.Fatal("profile too large")
	}
	var profile struct {
		Intervals       []int64 `json:"arrivalIntervalsUs"`
		ConsumerDelayUS int64   `json:"consumerDelayUs"`
	}
	if e = json.Unmarshal(raw, &profile); e != nil {
		t.Fatal(e)
	}
	if len(profile.Intervals) < 256 || len(profile.Intervals) > 512 {
		t.Fatal("expected bounded captured window")
	}
	var span time.Duration
	for _, v := range profile.Intervals {
		if v < 0 || v > 5e6 {
			t.Fatal("invalid interval")
		}
		span += time.Duration(v) * time.Microsecond
	}
	if span > 15*time.Second {
		t.Fatal("profile exceeds bounded replay duration")
	}
	if n, _ := strconv.Atoi(os.Getenv("TIDAL_PAPER_REPLAY_PROCS")); n > 0 && n <= 2 {
		old := runtime.GOMAXPROCS(n)
		defer runtime.GOMAXPROCS(old)
	}
	if profile.ConsumerDelayUS < 0 || profile.ConsumerDelayUS > 100000 {
		t.Fatal("consumer delay outside captured bounded range")
	}
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
	go func() {
		defer close(done)
		if profile.ConsumerDelayUS > 0 {
			timer := time.NewTimer(time.Duration(profile.ConsumerDelayUS) * time.Microsecond)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
			}
		}
		h.paperConsumeLoop(ctx, p, reader, out)
	}()
	defer func() { cancel(); <-done }()
	// Include steady-state heartbeat/source/maintenance before replay, avoiding
	// attribution of a startup lock to the captured steady-state production gap.
	ready := time.Now().Add(4 * time.Second)
	for profile.ConsumerDelayUS == 0 && p.diagnostics.snapshot().Stages["source_read"].Count < 2 && time.Now().Before(ready) {
		time.Sleep(time.Millisecond)
	}
	start := time.Now()
	deadline := start
	maxLate := time.Duration(0)
	offered, accepted := 0, 0
	var failure *paperDiagnosticView
	for i, us := range profile.Intervals {
		deadline = deadline.Add(time.Duration(us) * time.Microsecond)
		if wait := time.Until(deadline); wait > 0 {
			time.Sleep(wait)
		}
		at := time.Now().UTC()
		maxLate = max(maxLate, time.Since(deadline))
		offered++
		raw := []byte(fmt.Sprintf(`{"e":"bookTicker","s":"BTCUSDT","st":1,"u":%d,"E":%d,"b":"80000","a":"80001","B":"1","A":"1"}`, i+1, at.UnixMilli()))
		failure = paperEnqueueStream(out, paperMessage{Kind: "stream", At: at, Raw: raw}, &p.diagnostics)
		if failure != nil {
			break
		}
		accepted++
	}
	drained := time.Now().Add(2 * time.Second)
	for p.diagnostics.snapshot().Processed < uint64(accepted) && time.Now().Before(drained) {
		time.Sleep(time.Millisecond)
	}
	d := p.diagnostics.snapshot()
	report := map[string]any{"consumerDelayUs": profile.ConsumerDelayUS, "profileMessages": len(profile.Intervals), "profileSpanUs": span.Microseconds(), "offered": offered, "accepted": accepted, "processed": d.Processed, "overflows": d.Overflows, "maxReplayLatenessUs": maxLate.Microseconds(), "elapsedUs": time.Since(start).Microseconds(), "failure": failure, "diagnostics": d, "gomaxprocs": runtime.GOMAXPROCS(0)}
	b, _ := json.MarshalIndent(report, "", "  ")
	if dest := os.Getenv("TIDAL_PAPER_PROFILE_OUTPUT"); dest != "" {
		if e = os.WriteFile(dest, b, 0600); e != nil {
			t.Fatal(e)
		}
	}
	t.Logf("captured burst: input=%d submitted=%d accepted=%d consumed=%d failures=%d span=%s late<=%s", len(profile.Intervals), offered, accepted, d.Processed, d.Overflows, span, maxLate)
	if int(d.Processed) != accepted || int(d.Enqueued) != accepted || int(d.Received) != offered {
		t.Fatal("message conservation failed")
	}
	if failure != nil {
		t.Fatalf("captured burst overflow reproduced: active=%s elapsed=%.3fms", failure.Active, failure.ActiveMS)
	}
	if offered != len(profile.Intervals) {
		t.Fatal("did not submit complete profile")
	}
}
