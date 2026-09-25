package datahub

// Synthetic facts are confined to this opt-in offline test. The fixture includes
// the 30-day hourly books omitted by the original resource replay.
import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"testing"
	"time"
)

func seedBaselineReplay(t *testing.T, h *Hub, now time.Time, days, levels int) {
	t.Helper()
	fd, _ := h.Dataset("fx.usd.kraken")
	for at := now.Add(-time.Duration(days) * 24 * time.Hour); at.Before(now); at = at.Add(time.Hour) {
		if at.Hour() == 0 {
			t.Logf("hourly books: %s", at.Format("2006-01-02"))
		}
		for _, d := range Registry() {
			if d.Kind != "book" {
				continue
			}
			mid := 80000
			if d.Asset == "ETH" {
				mid = 3000
			}
			b := &Book{Low: float64(mid - levels), High: float64(mid + levels)}
			for i := 0; i < levels; i++ {
				b.Bids = append(b.Bids, Level{fmt.Sprint(mid - i), fmt.Sprint(1 + i%5)})
				b.Asks = append(b.Asks, Level{fmt.Sprint(mid + 1 + i), fmt.Sprint(1 + i%5)})
			}
			o := Observation{Dataset: d.ID, Source: d.Source, ObservedAt: &at, FetchedAt: now, Resolution: 3600, Quality: "valid", Payload: Payload{Book: b}}
			if _, e := h.Store.Ingest(d, o); e != nil {
				t.Fatal(e)
			}
		}
		o := Observation{Dataset: fd.ID, Source: fd.Source, ObservedAt: &at, FetchedAt: now, Resolution: 3600, Quality: "valid", Payload: Payload{Rates: []Rate{{Quote: "USDT", USD: "1.0001"}}}}
		if _, e := h.Store.Ingest(fd, o); e != nil {
			t.Fatal(e)
		}
	}
}

func TestBaselineResource(t *testing.T) {
	if os.Getenv("TIDAL_BASELINE_REPLAY") != "1" {
		t.Skip("opt-in baseline profile")
	}
	h, e := Open(Config{Root: t.TempDir(), Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	seedBaselineReplay(t, h, time.Now().UTC().Truncate(time.Hour), 30, 1000)
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				t.Logf("baseline heap=%.1f MiB heapSys=%.1f MiB", float64(m.HeapAlloc)/(1<<20), float64(m.HeapSys)/(1<<20))
			}
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	start := time.Now()
	if os.Getenv("TIDAL_BASELINE_REFERENCE") == "1" {
		e = h.referenceBaselines(ctx)
	} else {
		e = h.BuildBaselines(ctx)
	}
	close(done)
	<-stopped
	runtime.ReadMemStats(&after)
	t.Logf("baseline elapsed=%s error=%v allocated=%.1f MiB", time.Since(start), e, float64(after.TotalAlloc-before.TotalAlloc)/(1<<20))
	if out := os.Getenv("TIDAL_RESOURCE_OUTPUT"); out != "" {
		f, e := os.Create(filepath.Join(out, "baseline-heap.pprof"))
		if e != nil {
			t.Fatal(e)
		}
		pprof.WriteHeapProfile(f)
		f.Close()
	}
}
