package datahub

// Offline acceptance only. These fixtures are compiled exclusively into tests.
import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func seedBookFlowBaseline(t *testing.T, h *Hub, now time.Time) {
	t.Helper()
	s := bookFlowState{Parameters: bookFlowParameters(), Origin: now.Add(-31 * time.Minute), Last: now, History: map[string][]bookPoint{}, Current: map[string]bookPoint{}, FootThrough: map[string]time.Time{}, Days: map[string]bookFlowDay{}, Coverage: map[int64]uint8{}}
	for _, venue := range []string{"Binance", "OKX"} {
		s.History[venue] = bookPrior(now.Truncate(time.Minute), "10")
		s.Current[venue] = compactBookPoint(s.History[venue][len(s.History[venue])-1])
		c := minuteContract{Venue: venue, Consecutive: 3, VerifiedAt: flowPtr(now.Add(-time.Minute)), Cutover: flowPtr(now.Truncate(5 * time.Minute).Add(5 * time.Minute))}
		if err := h.Store.bookFlowSave(context.Background(), "contract", minuteFootID(venue), now, c, false); err != nil {
			t.Fatal(err)
		}
	}
	if err := h.commitBookFlow(context.Background(), s, nil); err != nil {
		t.Fatal(err)
	}
}
func bookFlowResourceInput(t *testing.T, h *Hub, now time.Time) {
	t.Helper()
	for _, venue := range []string{"Binance", "OKX"} {
		d, _ := h.Dataset(ID("book", "BTC", venue, "spot"))
		o := bookFixture(now.Truncate(time.Minute), "30")
		o.Dataset = d.ID
		o.FetchedAt = now
		if _, err := h.Store.Ingest(d, o); err != nil {
			t.Fatal(err)
		}
		md, _ := h.Dataset(minuteFootID(venue))
		raw := [][]any{}
		for i := 10; i > 0; i-- {
			at := now.Truncate(time.Minute).Add(-time.Duration(i) * time.Minute)
			raw = append(raw, []any{at.Unix(), [][]any{{80300, 80400, "1", "3", "80350", "241050", "80350", "241050", 10, 30}}})
		}
		b, _ := json.Marshal(map[string]any{"code": "0", "data": raw})
		rows, err := Normalize(md, b, now)
		if err != nil {
			t.Fatal(err)
		}
		for _, o := range rows {
			if _, err = h.Store.Ingest(md, o); err != nil {
				t.Fatal(err)
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err = h.Scheduler.processMinuteContract(ctx, md, rows, now, nil)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
	}
}
func bookFlowResourceStart(t *testing.T, h *Hub) func() {
	t.Helper()
	now := time.Now().UTC()
	seedBookFlowBaseline(t, h, now)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); h.bookFlowWorker(ctx) }()
	return func() {
		cancel()
		<-done
		h.mu.RLock()
		runtime := h.bookFlowRuntime
		h.mu.RUnlock()
		if runtime.Failures != 0 {
			t.Errorf("book flow resource worker failures: %+v", runtime)
		}
		var events, clocks, absorption int
		if err := h.Store.shortDB().QueryRow("SELECT count(*) FROM book_flow WHERE kind='first'").Scan(&events); err != nil {
			t.Error(err)
		}
		if err := h.Store.shortDB().QueryRow("SELECT count(*) FROM book_flow WHERE kind='clock'").Scan(&clocks); err != nil {
			t.Error(err)
		}
		if events < 2 || clocks < 2 {
			t.Errorf("book flow worker did not register real test-path events: %d/%d", events, clocks)
		}
		if err := h.Store.shortDB().QueryRow("SELECT count(*) FROM book_flow WHERE kind='clock' AND id LIKE '%/absorption'").Scan(&absorption); err != nil {
			t.Error(err)
		}
		if time.Since(now) > 6*time.Minute && absorption < 2 {
			t.Errorf("minute trade / raw depth proof path was not exercised: %d", absorption)
		}
		if out := os.Getenv("TIDAL_RESOURCE_OUTPUT"); out != "" {
			v, err := h.bookFlowView(context.Background(), nil, time.Now().UTC())
			if err != nil {
				t.Error(err)
			} else {
				b, _ := json.Marshal(v)
				if err = os.WriteFile(filepath.Join(out, "book-flow.json"), b, 0644); err != nil {
					t.Error(err)
				}
			}
		}
	}
}
func TestBookFlowBrowserFixture(t *testing.T) {
	root := os.Getenv("TIDAL_BOOK_QA_ROOT")
	if root == "" {
		t.Skip("manual isolated browser fixture")
	}
	if !filepath.IsAbs(root) || !strings.Contains(root, "/tmp/book-flow-qa/") {
		t.Fatal("isolated QA directory required")
	}
	h, err := Open(Config{Root: root, Offline: true, BookFlowMode: "run"})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Store.Close()
	now := time.Now().UTC()
	seedBookFlowBaseline(t, h, now.Add(-10*time.Minute))
	h.boot = now.Add(-time.Hour)
	for i := 9; i >= 0; i-- {
		at := now.Add(-time.Duration(i) * time.Minute)
		fd, _ := h.Dataset("fx.usd.kraken")
		if _, err = h.Store.Ingest(fd, Observation{Dataset: fd.ID, Source: fd.Source, ObservedAt: &at, FetchedAt: at, Quality: "valid", Payload: Payload{Rates: []Rate{{Quote: "USDT", USD: "1"}}}}); err != nil {
			t.Fatal(err)
		}
		bookFlowResourceInput(t, h, at)
		if err = h.bookFlowStep(context.Background(), at); err != nil {
			t.Fatal(err)
		}
	}
	cd, _ := h.Dataset(ID("candles", "BTC", "Binance", "spot"))
	for at := now.Truncate(5 * time.Minute).Add(-48 * time.Hour); at.Before(now.Truncate(5 * time.Minute)); at = at.Add(5 * time.Minute) {
		v := 80500.0 + float64(at.Minute())*3
		if _, err = h.Store.Ingest(cd, Observation{Dataset: cd.ID, Source: cd.Source, ObservedAt: &at, FetchedAt: now, Resolution: 300, Quality: "valid", Payload: Payload{Candle: &Candle{Open: v, High: v + 70, Low: v - 70, Close: v + 10, Volume: 100}}}); err != nil {
			t.Fatal(err)
		}
	}
	if err = h.advanceBookTrials(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	h.bookFlowRuntime = bookFlowRuntime{At: &now, Success: &now, Registered: true}
	if err = h.Store.SaveState("book-flow/runtime", h.bookFlowRuntime); err != nil {
		t.Fatal(err)
	}
	t.Log("offline fixture only; no upstream calls, orders or email")
}

// Production core tasks use minute phases. An elapsed-since-last-input check
// accumulates serial maintenance time and can skip a source minute entirely.
func bookFlowResourceDue(now, last time.Time) bool {
	return now.Truncate(time.Minute).After(last.Truncate(time.Minute))
}
func TestBookFlowResourceMaintenancePhaseDoesNotSkipMinute(t *testing.T) {
	times := []string{"2026-10-08T23:52:58.410885549Z", "2026-10-08T23:53:22.143700722Z", "2026-10-08T23:53:54.550488934Z", "2026-10-08T23:54:04.550488934Z"}
	last, oldLast := time.Time{}, time.Time{}
	minutes, oldMinutes := []int{}, []int{}
	for _, v := range times {
		at, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			t.Fatal(err)
		}
		if bookFlowResourceDue(at, last) {
			minutes = append(minutes, at.Minute())
			last = at
		}
		if at.Sub(oldLast) >= time.Minute {
			oldMinutes = append(oldMinutes, at.Minute())
			oldLast = at
		}
	}
	if len(oldMinutes) != 2 || oldMinutes[0] != 52 || oldMinutes[1] != 54 {
		t.Fatal("old defect not reproduced", oldMinutes)
	}
	if len(minutes) != 3 || minutes[0] != 52 || minutes[1] != 53 || minutes[2] != 54 {
		t.Fatal("source minute lost", minutes)
	}
}
