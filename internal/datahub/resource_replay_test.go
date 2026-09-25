package datahub

// Opt-in, offline test-only replay. Nothing in this file can seed production.
import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sync"
	"testing"
	"time"
)

func TestResourceReplay(t *testing.T) {
	if os.Getenv("TIDAL_RESOURCE_REPLAY") != "1" {
		t.Skip("opt-in Linux resource gate")
	}
	h, e := Open(Config{Root: t.TempDir(), Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Hour)
	from := now.Add(-90 * 24 * time.Hour).Add(time.Hour)
	flow, _ := FindDataset("flow.btc..spot")
	cd, _ := FindDataset("candles.btc.binance.spot")
	ingest := func(d Dataset, o Observation) {
		t.Helper()
		if _, e := h.Store.Ingest(d, o); e != nil {
			t.Fatal(e)
		}
	}
	t.Log("phase: 90-day history backfill through normalized warehouse")
	for at := from; at.Before(now); at = at.Add(5 * time.Minute) {
		o := Observation{Dataset: flow.ID, Source: flow.Source, ObservedAt: &at, FetchedAt: now, TimeBasis: "source", Resolution: 300, Quality: "valid", Payload: Payload{Flow: &Flow{fmt.Sprint(1000000 + at.Minute()*10000), "990000"}}}
		ingest(flow, o)
		o.Dataset = cd.ID
		o.Source = cd.Source
		o.Payload = Payload{Candle: &Candle{Open: 80000, High: 80100, Low: 79900, Close: 80000, Volume: 10}}
		ingest(cd, o)
		if at.Hour() == 0 && at.Minute() == 0 {
			t.Logf("backfill through %s", at.Format("2006-01-02"))
		}
	}
	t.Log("phase: current books, backfill rollups and local readers")
	for i := 0; i < 60; i++ {
		at := now.Add(-time.Duration(60-i) * 2 * time.Minute)
		for _, d := range Registry() {
			if d.Kind != "book" {
				continue
			}
			book := &Book{}
			for n := 0; n < 1000; n++ {
				book.Bids = append(book.Bids, Level{fmt.Sprint(80000 - n), fmt.Sprint(1 + n%5)})
				book.Asks = append(book.Asks, Level{fmt.Sprint(80001 + n), fmt.Sprint(1 + n%5)})
			}
			ingest(d, Observation{Dataset: d.ID, Source: d.Source, ObservedAt: &at, FetchedAt: now, Resolution: 60, Quality: "valid", Payload: Payload{Book: book}})
		}
		if i%10 == 0 {
			if e := h.Store.Rollup(ctx, h.datasets(), now); e != nil {
				t.Fatal(e)
			}
		}
	}
	t.Log("phase: checkpointed full strategy computation")
	s := Study{ID: "resource-replay", Asset: "BTC", From: now.Add(-90 * 24 * time.Hour), To: now, Created: now, Pipeline: studyPipeline}
	for i := 0; i < 24; i++ {
		r, e := h.evaluateStudy(ctx, s, time.Now())
		if e != nil {
			t.Fatal(e)
		}
		if r.CoreCalculated {
			t.Log("full strategy calculation complete")
			break
		}
		if r.StrategyState != "calculating" {
			t.Fatalf("unexpected data gap: %+v", r.Missing)
		}
		if i == 23 {
			t.Fatal("checkpoint did not finish")
		}
	}
	t.Log("phase: repeated current writes, signals, rollups and concurrent GETs")
	for cycle := 0; cycle < 12; cycle++ {
		live := time.Now().UTC().Truncate(time.Minute).Add(-5 * time.Minute)
		ingest(flow, Observation{Dataset: flow.ID, Source: flow.Source, ObservedAt: &live, FetchedAt: time.Now().UTC(), Resolution: 60, Quality: "valid", Payload: Payload{Flow: &Flow{"1200000", "1000000"}}})
		var wg sync.WaitGroup
		for k := 0; k < 2; k++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for n := 0; n < 20; n++ {
					if _, e := h.Read(ctx, "activity", url.Values{"asset": {"BTC"}, "hours": {"1"}}); e != nil {
						t.Error(e)
					}
				}
			}()
		}
		if e := h.Store.Rollup(ctx, h.datasets(), time.Now()); e != nil {
			t.Fatal(e)
		}
		if e := h.processSignals(ctx, time.Now()); e != nil {
			t.Fatal(e)
		}
		wg.Wait()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		t.Logf("cycle %d heap=%.1f MiB sys=%.1f MiB alloc=%.1f MiB", cycle, float64(m.HeapAlloc)/(1<<20), float64(m.Sys)/(1<<20), float64(m.TotalAlloc)/(1<<20))
		time.Sleep(10 * time.Second)
	}
	if h.Scheduler.quota.Calls != 0 {
		t.Fatal("local replay invoked upstream")
	}
	if out := os.Getenv("TIDAL_RESOURCE_OUTPUT"); out != "" {
		f, e := os.Create(filepath.Join(out, "heap.pprof"))
		if e != nil {
			t.Fatal(e)
		}
		pprof.WriteHeapProfile(f)
		f.Close()
	}
}
