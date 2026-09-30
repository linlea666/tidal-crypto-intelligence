package datahub

// Opt-in, offline test-only replay. Nothing in this file can seed production.
import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
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
	perp, _ := FindDataset(ID("flow", "BTC", "", "futures"))
	coinOI, _ := FindDataset(ID("oi-coin-history", "BTC", "", "futures"))
	liquidations, _ := FindDataset(ID("liquidations", "BTC", "", "futures"))
	funding, _ := FindDataset(ID("funding", "ALL", "", "futures"))
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
		o.Dataset, o.Source = perp.ID, perp.Source
		o.Payload = Payload{Flow: &Flow{"5000000", "4000000"}}
		ingest(perp, o)
		o.Dataset, o.Source = coinOI.ID, coinOI.Source
		o.Payload = Payload{OI: []Interest{{Base: "500000"}}}
		ingest(coinOI, o)
		o.Dataset, o.Source = liquidations.ID, liquidations.Source
		o.Payload = Payload{Liquidation: &Liquidation{Long: "100000", Short: "120000"}}
		ingest(liquidations, o)
		if at.Minute()%10 == 0 {
			o.Dataset, o.Source, o.FetchedAt = funding.ID, funding.Source, at
			o.Payload = Payload{Funding: []Funding{}}
			for _, venue := range []string{"Binance", "OKX", "Bybit"} {
				for _, margin := range []string{"stablecoin", "coin"} {
					o.Payload.Funding = append(o.Payload.Funding, Funding{Asset: "BTC", Venue: venue, Margin: margin, RatePercent: "0.01", RateKind: "predicted", Hours: flowPtr(8.0)})
				}
			}
			ingest(funding, o)
		}
		if at.Hour() == 0 && at.Minute() == 0 {
			t.Logf("backfill through %s", at.Format("2006-01-02"))
		}
	}
	t.Log("phase: native one-minute flow overlapping the recent 48h of five-minute history")
	for at := now.Add(-48 * time.Hour); at.Before(now); at = at.Add(time.Minute) {
		bucket := at.Truncate(5 * time.Minute)
		ingest(flow, Observation{Dataset: flow.ID, Source: flow.Source, ObservedAt: &at, FetchedAt: now, Resolution: 60, Quality: "valid", Payload: Payload{Flow: &Flow{Buy: fmt.Sprint((1000000 + bucket.Minute()*10000) / 5), Sell: "198000"}}})
	}
	t.Log("phase: 30-day hourly books and production maintenance")
	seedBaselineReplay(t, h, now, 30, 1000)
	t.Log("phase: current books, backfill rollups and local readers")
	for i := 0; i < 60; i++ {
		at := now.Add(-time.Duration(60-i) * 2 * time.Minute)
		for _, d := range Registry() {
			if d.Kind != "book" {
				continue
			}
			mid := 80000
			if d.Asset == "ETH" {
				mid = 3000
			}
			book := &Book{Low: float64(mid - 1000), High: float64(mid + 1000)}
			for n := 0; n < 1000; n++ {
				book.Bids = append(book.Bids, Level{fmt.Sprint(mid - n), fmt.Sprint(1 + n%5)})
				book.Asks = append(book.Asks, Level{fmt.Sprint(mid + 1 + n), fmt.Sprint(1 + n%5)})
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
	for i := 0; i < 40; i++ {
		r, e := h.evaluateStudy(ctx, s, time.Now())
		if e != nil {
			t.Fatal(e)
		}
		if s.Result != nil && s.Result.CoreCalculated && !r.CoreCalculated {
			t.Fatal("finished control restarted while multifactor checkpoint advanced")
		}
		s.Result, s.InputVersion = r, h.studyInputVersion(ctx, s)
		if r.CoreCalculated && r.MultifactorComparison != nil && r.MultifactorComparison.State == "association_calculated" {
			s.Result = r
			s.State = "complete"
			if e = h.Store.saveDocument("study", s.ID, s.Asset, s.Created, s); e != nil {
				t.Fatal(e)
			}
			t.Log("full strategy calculation complete")
			break
		}
		if !r.CoreCalculated && r.StrategyState != "calculating" {
			t.Fatalf("unexpected data gap: %+v", r.Missing)
		}
		if i == 39 {
			t.Fatal("checkpoint did not finish")
		}
	}
	t.Log("phase: liquidation immutable episodes and 1h/4h outcome evaluation")
	for i := 1; i <= 8; i++ {
		side := "short"
		if i%2 == 0 {
			side = "long"
		}
		start := now.Add(-time.Duration(i*4) * time.Hour)
		ev := LiquidationStudyEvent{ID: fmt.Sprintf("resource-liquidation-%d", i), Side: side, Rule: LiquidationRules, Selected: start.Add(-time.Minute), Start: start, Price: 80000, ATR: 500, Zone: LiquidationZone{Side: side, Low: 79000, High: 79250}, Outcomes: map[string]LiquidationOutcome{"1": {State: "observing"}, "4": {State: "observing"}}}
		if e = h.Store.liquidationPut(ctx, "event", ev.ID, "BTC", ev.Selected, ev); e != nil {
			t.Fatal(e)
		}
	}
	if e = h.advanceLiquidationStudy(ctx, time.Now().UTC()); e != nil {
		t.Fatal(e)
	}
	t.Log("phase: independent short-flow baseline, frozen trials and bounded progress")
	if e = h.Store.shortPut(ctx, "origin", ShortFlowRules, now, now.Add(-48*time.Hour)); e != nil {
		t.Fatal(e)
	}
	if e = h.shortObservationStep(ctx, now.Add(time.Minute)); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 16; i++ {
		step, cancel := context.WithTimeout(ctx, 2*time.Second)
		e = h.shortBaselineStep(step, time.Now().UTC())
		cancel()
		if e != nil {
			t.Fatal("short baseline batch", e)
		}
	}
	var shortBase ShortBaseline
	if e = h.Store.shortLoad(ctx, "baseline", ShortFlowRules, &shortBase); e != nil || !shortBase.Valid {
		t.Fatal("short baseline cold checkpoint", e)
	}
	for i := 1; i <= 8; i++ {
		// All trial windows must already be past the 20-minute late-candle
		// allowance, independently of the runner's minute within the hour.
		at := now.Add(-time.Duration(i*4+1) * time.Hour)
		tr := newShortTrial(fmt.Sprint("replay-short-", i), "short-5", "buy", at, at, nil)
		v := ShortEpisode{ID: tr.ID, Direction: "buy", At: at, Updated: at, Trials: map[string]*ShortTrial{"5": tr}}
		if e = h.Store.shortPut(ctx, "episode", v.ID, at, v); e != nil {
			t.Fatal(e)
		}
	}
	t.Log("phase: repeated full maintenance, current writes and concurrent local GETs")
	duration := 2 * time.Minute
	if configured := os.Getenv("TIDAL_REPLAY_DURATION"); configured != "" {
		duration, e = time.ParseDuration(configured)
		if e != nil {
			t.Fatal(e)
		}
	}
	phaseStarted := time.Now()
	until := phaseStarted.Add(duration)
	lastMaintenance, lastBooks := time.Time{}, time.Time{}
	for cycle := 0; time.Now().Before(until); cycle++ {
		current := time.Now().UTC()
		live := current.Truncate(time.Minute).Add(-time.Minute)
		ingest(flow, Observation{Dataset: flow.ID, Source: flow.Source, ObservedAt: &live, FetchedAt: current, Resolution: 60, Quality: "valid", Payload: Payload{Flow: &Flow{"1200000", "1000000"}}})
		fx, _ := h.Dataset("fx.usd.kraken")
		ingest(fx, Observation{Dataset: fx.ID, Source: fx.Source, ObservedAt: &current, FetchedAt: current, Quality: "valid", Payload: Payload{Rates: []Rate{{"USDT", "1.0001"}}}})
		for _, a := range Assets() {
			pd, _ := h.Dataset(ID("price", a, "Binance", "spot"))
			value := "80000"
			if a == "ETH" {
				value = "3000"
			}
			ingest(pd, Observation{Dataset: pd.ID, Source: pd.Source, ObservedAt: &current, FetchedAt: current, Quality: "valid", Payload: Payload{Price: &Price{value, "USDT"}}})
		}
		if current.Sub(lastBooks) >= 2*time.Minute {
			for _, asset := range Assets() {
				d, o := liquidationReplayFixture(asset, current)
				ingest(d, o)
			}
			h.processLiquidations(ctx, current)
		}
		if current.Sub(lastBooks) >= 2*time.Minute {
			for _, d := range Registry() {
				if d.Kind == "book" {
					book := &Book{}
					mid := 80000
					if d.Asset == "ETH" {
						mid = 3000
					}
					book.Low, book.High = float64(mid-1000), float64(mid+1000)
					for n := 0; n < 1000; n++ {
						q := fmt.Sprint(1 + (cycle+n)%5)
						book.Bids = append(book.Bids, Level{fmt.Sprint(mid - n), q})
						book.Asks = append(book.Asks, Level{fmt.Sprint(mid + 1 + n), q})
					}
					ingest(d, Observation{Dataset: d.ID, Source: d.Source, ObservedAt: &current, FetchedAt: current, Resolution: 60, Quality: "valid", Payload: Payload{Book: book}})
				}
				if d.Kind == "large" {
					o := orderObservation(d, current, orderFixture(current))
					o.Payload.Large = nil
					for n := 0; n < 100; n++ {
						r := orderFixture(current)
						r.ID = fmt.Sprint(n)
						r.Quantity = fmt.Sprint(1 + n)
						r.Side = "bid"
						if n%2 == 0 {
							r.Side = "ask"
						}
						o.Payload.Large = append(o.Payload.Large, r)
					}
					ingest(d, o)
				}
			}
			lastBooks = current
		}
		var wg sync.WaitGroup
		for k := 0; k < 2; k++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for _, path := range []string{"activity", "levels", "large-orders", "signals", "studies", "liquidations", "liquidation-study"} {
					for n := 0; n < 4; n++ {
						if _, err := h.Read(ctx, path, url.Values{"asset": {"BTC"}, "hours": {"1"}, "layout": {"split"}}); err != nil {
							t.Error(err)
						}
					}
				}
			}()
		}
		if current.Sub(lastMaintenance) >= time.Minute {
			h.maintain(ctx)
			h.processLiquidations(ctx, time.Now().UTC())
			if e = h.processSignals(ctx, time.Now().UTC()); e != nil {
				t.Fatal(e)
			}
			lastMaintenance = current
		}
		// Reproduce a formal reader holding the original pool while short work
		// publishes and commits. The ordinary reader pool must not be enlarged.
		formalReader, err := h.Store.research.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, phase := range []func(context.Context, time.Time) error{h.shortObservationStep, h.shortBaselineStep, h.shortStudyStep} {
			step, cancel := context.WithTimeout(ctx, 2*time.Second)
			err := phase(step, time.Now().UTC())
			cancel()
			if err != nil {
				formalReader.Close()
				t.Fatal("bounded short-flow replay", err)
			}
		}
		formalReader.Close()
		wg.Wait()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		sample := map[string]any{"at": time.Now().UTC(), "cycle": cycle, "heapBytes": m.HeapAlloc, "heapSysBytes": m.HeapSys, "heapReleasedBytes": m.HeapReleased, "totalAllocBytes": m.TotalAlloc, "sysBytes": m.Sys, "goroutines": runtime.NumGoroutine()}
		if b, err := os.ReadFile("/proc/self/status"); err == nil {
			for _, line := range strings.Split(string(b), "\n") {
				if strings.HasPrefix(line, "VmRSS:") {
					sample["processRSS"] = strings.TrimSpace(strings.TrimPrefix(line, "VmRSS:"))
				}
			}
		}
		for _, f := range []string{"memory.current", "memory.stat"} {
			if b, err := os.ReadFile("/sys/fs/cgroup/" + f); err == nil {
				sample[f] = strings.TrimSpace(string(b))
			}
		}
		if out := os.Getenv("TIDAL_RESOURCE_OUTPUT"); out != "" {
			// Synthetic telemetry must also be readable by the host CI runner.
			f, err := os.OpenFile(filepath.Join(out, "runtime.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
			if err != nil {
				t.Fatal(err)
			}
			json.NewEncoder(f).Encode(sample)
			f.Close()
		}
		if cycle%6 == 0 {
			t.Logf("cycle %d heap=%.1f MiB heapSys=%.1f MiB elapsed=%s", cycle, float64(m.HeapAlloc)/(1<<20), float64(m.HeapSys)/(1<<20), duration-time.Until(until))
		}
		time.Sleep(10 * time.Second)
	}
	var built time.Time
	if !h.Store.LoadState("baselineComputed", &built) {
		t.Fatal("baseline maintenance never completed")
	}
	for _, asset := range Assets() {
		var zones LiquidationMapSnapshot
		if err := h.Store.liquidationLoad(ctx, "state", ID("map", asset, "", "futures"), &zones); err != nil || len(zones.Zones) < 321 {
			t.Fatalf("liquidation state did not advance for %s: zones=%d error=%v", asset, len(zones.Zones), err)
		}
	}
	var completed int
	if err := h.Store.research.QueryRowContext(ctx, "SELECT count(*) FROM lz_records WHERE kind='event' AND done=1").Scan(&completed); err != nil || completed < 6 {
		t.Fatalf("liquidation outcome checkpoints did not complete: %d error=%v", completed, err)
	}
	var liquidationStatus liquidationGap
	if h.Store.LoadState("liquidation/gap", &liquidationStatus) && liquidationStatus.Paused {
		t.Fatalf("liquidation worker remains paused at end of replay: %s", liquidationStatus.Reason)
	}

	if h.Scheduler.quota.Calls != 0 {
		t.Fatal("local replay invoked upstream")
	}
	var shortDone int
	if e = h.Store.research.QueryRow("SELECT count(*) FROM sf_records WHERE kind='episode' AND json_extract(payload,'$.done')=1").Scan(&shortDone); e != nil || shortDone < 8 {
		t.Fatal("short study checkpoints did not complete", shortDone, e)
	}
	if out := os.Getenv("TIDAL_RESOURCE_OUTPUT"); out != "" {
		phase, err := json.Marshal(map[string]any{"startedAt": phaseStarted.UTC(), "endedAt": time.Now().UTC(), "elapsedSeconds": time.Since(phaseStarted).Seconds()})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(out, "phase.json"), phase, 0644); err != nil {
			t.Fatal(err)
		}
		f, e := os.Create(filepath.Join(out, "heap.pprof"))
		if e != nil {
			t.Fatal(e)
		}
		pprof.WriteHeapProfile(f)
		f.Close()
	}
}
