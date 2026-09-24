package datahub

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testStore(t *testing.T) *Warehouse {
	t.Helper()
	w, e := OpenWarehouse(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { w.Close() })
	return w
}
func testObservation(d Dataset, at time.Time, buy string) Observation {
	return Observation{Dataset: d.ID, Source: d.Source, ObservedAt: &at, FetchedAt: at, TimeBasis: "source", Resolution: d.Resolution, Quality: "valid", Payload: Payload{Flow: &Flow{buy, "1"}}}
}
func TestWarehouseDuplicatesCorrectionsAndRestore(t *testing.T) {
	w := testStore(t)
	d, _ := FindDataset("flow.btc..spot")
	at := time.Now().UTC().Truncate(time.Minute).Add(-time.Hour)
	o := testObservation(d, at, "10.005")
	if _, e := w.Ingest(d, o); e != nil {
		t.Fatal(e)
	}
	o.FetchedAt = at.Add(10 * time.Minute)
	if changed, e := w.Ingest(d, o); e != nil || changed {
		t.Fatalf("duplicate: %v %v", changed, e)
	}
	if latest, _ := w.Latest(d.ID); latest.Fresh(d, at.Add(13*time.Minute)) {
		t.Fatal("fetch rejuvenated stale source timestamp")
	}
	next := testObservation(d, at.Add(time.Minute), "20")
	w.Ingest(d, next)
	correct := testObservation(d, at, "12")
	correct.FetchedAt = at.Add(2 * time.Minute)
	w.Ingest(d, correct)
	latest, _ := w.Latest(d.ID)
	if latest.Payload.Flow.Buy != "20" {
		t.Fatal("late correction overwrote latest")
	}
	rows, _, e := w.Query(context.Background(), d, 60, at, at.Add(2*time.Minute), 100)
	if e != nil || len(rows) != 2 {
		t.Fatalf("history %d %v", len(rows), e)
	}
	if rows[0].Payload.Flow.Buy != "12" {
		t.Fatal("history correction missing")
	}
	if e = w.Rollup(context.Background(), map[string]Dataset{d.ID: d}, time.Now()); e != nil {
		t.Fatal(e)
	}
	restored, e := OpenWarehouse(w.Root())
	if e != nil {
		t.Fatal(e)
	}
	defer restored.Close()
	latest, _ = restored.Latest(d.ID)
	if latest.Payload.Flow.Buy != "20" {
		t.Fatal("restore missing")
	}
}
func TestAggregationStockFlowAndVWAPInputs(t *testing.T) {
	at := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)
	d, _ := FindDataset("flow.btc..spot")
	rows := []Observation{testObservation(d, at, "10.005"), testObservation(d, at.Add(time.Minute), "20.005")}
	o := Aggregate(d, rows, at, at.Add(15*time.Minute), 900)
	if o.Payload.Flow.Buy != "30.01" || o.Payload.Flow.Sell != "2" {
		t.Fatal("flow sum")
	}
	d.Kind = "oi"
	rows[0].Payload = Payload{OI: []Interest{{"All", "100", "1"}}}
	rows[1].Payload = Payload{OI: []Interest{{"All", "110", "1"}}}
	o = Aggregate(d, rows, at, at.Add(time.Hour), 3600)
	if o.Payload.OI[0].USD != "110" {
		t.Fatal("stock summed")
	}
	d.Kind = "footprint"
	rows[0].Payload = Payload{Foot: []Foot{{Low: "100", High: "110", BuyBase: "2", BuyQuote: "200"}}}
	rows[1].Payload = Payload{Foot: []Foot{{Low: "100", High: "110", BuyBase: "1", BuyQuote: "110"}}}
	o = Aggregate(d, rows, at, at.Add(time.Hour), 3600)
	f := o.Payload.Foot[0]
	if f.BuyBase != "3" || f.BuyQuote != "310" {
		t.Fatal("VWAP weighted inputs not preserved")
	}
}
func TestQuotaRollingWindowAndRestartLedger(t *testing.T) {
	q := Quota{}
	at := time.Now()
	starts := []time.Time{}
	for i := 0; i < 1800; i++ {
		now := at.Add(time.Duration(i) * 100 * time.Millisecond)
		if q.available(now) {
			q.Starts = append(q.Starts, now)
			starts = append(starts, now)
		}
	}
	for _, end := range starts {
		n := 0
		for _, v := range starts {
			if v.After(end.Add(-time.Minute)) && !v.After(end) {
				n++
			}
		}
		if n > 12 {
			t.Fatalf("rolling limit exceeded %d", n)
		}
	}
	w := testStore(t)
	w.SaveState("quota", q)
	s := NewScheduler(w, Registry(), nil, true, time.Now())
	if len(s.quota.Starts) != len(q.Starts) {
		t.Fatal("quota ledger not restored")
	}
	s.quota.Cooldown = time.Now().Add(time.Minute)
	if s.quota.available(time.Now()) {
		t.Fatal("429 cooldown bypass")
	}
}
func TestGetReadsNeverSpendQuotaAndConcurrentRequestsCoalesce(t *testing.T) {
	h, e := Open(Config{Root: t.TempDir(), Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	var calls atomic.Int64
	h.Scheduler.fetch = func(context.Context, Dataset) ([]byte, error) { calls.Add(1); return nil, ErrNoData }
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, e := h.Read(context.Background(), "whales", url.Values{"asset": {"BTC"}}); e != nil {
				t.Error(e)
			}
		}()
	}
	wg.Wait()
	if calls.Load() != 0 {
		t.Fatal("GET invoked upstream")
	}
	req := DataRequest{Dataset: "map.btc..futures", Range: "7d"}
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, e := h.Request(req); e != nil {
				t.Error(e)
			}
		}()
	}
	wg.Wait()
	n := 0
	for _, j := range h.Scheduler.jobs {
		if j.Mode == "on_demand" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("want one shared job, got %d", n)
	}
}
func TestNullFootprintAndLiquidation(t *testing.T) {
	d, _ := FindDataset("footprint.btc.binance.spot")
	now := time.Now().UTC()
	raw := []byte(fmt.Sprintf(`{"code":"0","data":[[%d,null]]}`, now.Add(-10*time.Minute).Unix()))
	o, e := Normalize(d, raw, now)
	if e != nil || len(o) != 1 || o[0].Quality != "missing" {
		t.Fatal("null footprint became zero")
	}
	d, _ = FindDataset("whales.all.hyperliquid.futures")
	raw = []byte(fmt.Sprintf(`{"code":"0","data":[{"user":"0x0000000000000000000000000000000000000001","symbol":"BTC","position_size":-2,"entry_price":100,"mark_price":110,"liq_price":0,"leverage":2,"position_value_usd":220,"unrealized_pnl":-20,"margin_balance":100,"funding_fee":0,"margin_mode":"cross","update_time":%d}]}`, now.UnixMilli()))
	o, e = Normalize(d, raw, now)
	if e != nil {
		t.Fatal(e)
	}
	if o[0].Payload.Whales[0].Liquidation != nil {
		t.Fatal("zero liquidation not null")
	}
}
func TestBookUSDConversionAndStaleFX(t *testing.T) {
	h, e := Open(Config{Root: t.TempDir(), Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	now := time.Now().UTC()
	fd, _ := h.Dataset("fx.usd.kraken")
	h.Store.Ingest(fd, Observation{Dataset: fd.ID, Source: "kraken", FetchedAt: now, Quality: "valid", Payload: Payload{Rates: []Rate{{"USDT", "0.98"}, {"USDC", "1.001"}}}})
	d, _ := h.Dataset("book.btc.binance.spot")
	raw := []byte(fmt.Sprintf(`{"code":"0","data":[[%d,[[100,2]],[[110,3]]]]}`, now.Unix()))
	rows, e := Normalize(d, raw, now)
	if e != nil {
		t.Fatal(e)
	}
	h.Store.Ingest(d, rows[0])
	f := h.rawFrame("BTC", 25, now)
	if len(f.Zones) != 2 {
		t.Fatal("missing converted zones")
	}
	var bid int64
	for _, z := range f.Zones {
		if z.Side == "bid" {
			bid = z.USD
		}
	}
	if bid != 19600 {
		t.Fatalf("USD value %d", bid)
	}
	f = h.rawFrame("BTC", 25, now.Add(31*time.Second))
	if len(f.Zones) != 0 {
		t.Fatal("stale FX kept in USD total")
	}
}
func TestModelDoesNotSumTimeSlices(t *testing.T) {
	d, _ := FindDataset("heatmap.btc..futures")
	now := time.Now().UTC()
	raw := []byte(fmt.Sprintf(`{"code":"0","data":{"y_axis":[100,110],"price_candlesticks":[[%d],[%d]],"liquidation_leverage_data":[[0,0,10],[1,1,20]],"update_time":%d}}`, now.Add(-time.Minute).Unix(), now.Unix(), now.UnixMilli()))
	rows, e := Normalize(d, raw, now)
	if e != nil {
		t.Fatal(e)
	}
	if len(rows[0].Payload.Model.Bins) != 1 || rows[0].Payload.Model.Bins[0].Strength != 20 {
		t.Fatal("heatmap summed across time")
	}
}
func TestPersistenceUsesTimeAndBreaksOnGaps(t *testing.T) {
	now := time.Now()
	samples := []wallSample{}
	for i := 34; i >= 0; i -= 2 {
		samples = append(samples, wallSample{At: now.Add(-time.Duration(i) * time.Minute), Large: true})
	}
	occ, seconds, _ := persistence(samples, now)
	if occ < .99 || seconds < 1800 {
		t.Fatal("stable sampled wall failed time weighting")
	}
	samples = samples[:len(samples)-3]
	_, seconds, _ = persistence(samples, now)
	if seconds != 0 {
		t.Fatal("gap falsely counted as continuity")
	}
}
func TestOptionalPrivateProbeContracts(t *testing.T) {
	dir := os.Getenv("TIDAL_PROBE_DIR")
	if dir == "" {
		t.Skip("optional private, ignored response contracts")
	}
	cases := map[string]string{"book": "book.btc.binance.spot", "large": "large.btc.binance.spot", "large-history": "large-history.btc.binance.spot", "spot-flow": "flow.btc..spot", "perp-flow": "flow.btc..futures", "oi": "oi.btc..futures", "funding": "funding.all..futures", "liquidation": "liquidations.btc..futures", "footprint": "footprint.btc.binance.spot", "perp-footprint": "footprint.btc.okx.futures", "map": "map.btc..futures", "heatmap": "heatmap.btc..futures", "whales": "whales.all.hyperliquid.futures"}
	for name, id := range cases {
		t.Run(name, func(t *testing.T) {
			d, _ := FindDataset(id)
			b, e := os.ReadFile(filepath.Join(dir, name+".json"))
			if e != nil {
				t.Fatal(e)
			}
			rows, e := Normalize(d, b, time.Now().UTC())
			if e != nil {
				t.Fatal(e)
			}
			if len(rows) == 0 {
				t.Fatal("no observations")
			}
			_, e = json.Marshal(rows)
			if e != nil {
				t.Fatal(e)
			}
		})
	}
}

func TestClosedFootprintOnly(t *testing.T) {
	d, _ := FindDataset("footprint.btc.binance.spot")
	now := time.Now().UTC()
	raw := []byte(fmt.Sprintf(`{"code":"0","data":[[%d,[[100,101,1,2,100,200,100,200,1,2]]]]}`, now.Unix()))
	if rows, err := Normalize(d, raw, now); err == nil || len(rows) > 0 {
		t.Fatal("forming footprint accepted as completed evidence")
	}
}
func TestCoarseBackfillSurvivesPartialFineRollup(t *testing.T) {
	w := testStore(t)
	d, _ := FindDataset("flow.btc..spot")
	at := time.Now().UTC().Truncate(time.Hour).Add(-2 * time.Hour)
	full := testObservation(d, at, "1000")
	full.Resolution = 3600
	w.Ingest(d, full)
	w.Ingest(d, testObservation(d, at.Add(time.Minute), "10"))
	if e := w.Rollup(context.Background(), map[string]Dataset{d.ID: d}, time.Now()); e != nil {
		t.Fatal(e)
	}
	rows, _, e := w.Query(context.Background(), d, 3600, at, at.Add(time.Hour), 10)
	if e != nil || len(rows) != 1 || rows[0].Payload.Flow.Buy != "1000" {
		t.Fatal("partial fine tail replaced authoritative hourly interval", e)
	}
}
func TestOnlineBackupRestoresWAL(t *testing.T) {
	w := testStore(t)
	d, _ := FindDataset("flow.btc..spot")
	at := time.Now().UTC()
	w.Ingest(d, testObservation(d, at, "123.45"))
	dest := filepath.Join(t.TempDir(), "hub.sqlite")
	if e := BackupFile(context.Background(), filepath.Join(w.Root(), "hub.sqlite"), dest); e != nil {
		t.Fatal(e)
	}
	restored, e := OpenWarehouse(filepath.Dir(dest))
	if e != nil {
		t.Fatal(e)
	}
	defer restored.Close()
	o, ok := restored.Latest(d.ID)
	if !ok || o.Payload.Flow.Buy != "123.45" {
		t.Fatal("committed WAL data absent from restored backup")
	}
}
func TestDatasetRetentionDoesNotDeletePendingPeer(t *testing.T) {
	w := testStore(t)
	now := time.Now().UTC()
	at := now.Truncate(24 * time.Hour).Add(-10 * 24 * time.Hour)
	flow, _ := FindDataset("flow.btc..spot")
	book, _ := FindDataset("book.btc.binance.spot")
	w.Ingest(flow, testObservation(flow, at, "10"))
	o := testObservation(book, at, "1")
	w.Ingest(book, o)
	path := w.partition(60, at, false)
	reg := map[string]Dataset{flow.ID: flow, book.ID: book}
	if e := w.cleanPartition(context.Background(), path, at, 60, reg, now, 90, false); e != nil {
		t.Fatal(e)
	}
	r, _, _ := w.Query(context.Background(), book, 60, at, at.Add(time.Hour), 5)
	if len(r) != 1 {
		t.Fatal("unrolled book removed")
	}
	if e := w.Rollup(context.Background(), reg, now); e != nil {
		t.Fatal(e)
	}
	if e := w.cleanPartition(context.Background(), path, at, 60, reg, now, 90, false); e != nil {
		t.Fatal(e)
	}
	r, _, _ = w.Query(context.Background(), book, 60, at, at.Add(time.Hour), 5)
	if len(r) != 0 {
		t.Fatal("expired book retained")
	}
	r, _, _ = w.Query(context.Background(), flow, 60, at, at.Add(time.Hour), 5)
	if len(r) != 1 {
		t.Fatal("30-day flow deleted with 7-day book")
	}
}
func Test429AndAuthenticationPauseCountFailedCalls(t *testing.T) {
	for _, status := range []int{429, 401} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			w := testStore(t)
			d, _ := FindDataset("book.btc.binance.spot")
			now := time.Now().UTC()
			s := NewScheduler(w, []Dataset{d}, func(context.Context, Dataset) ([]byte, error) {
				return nil, &FetchError{Status: status, RetryAfter: time.Minute}
			}, true, now)
			s.jobs[d.ID].Next = now.Add(-time.Second)
			if !s.Step(context.Background(), now) {
				t.Fatal("request not started")
			}
			s.wg.Wait()
			if s.quota.Calls != 1 {
				t.Fatal("failed call not counted")
			}
			if status == 401 && !s.quota.AuthFailed {
				t.Fatal("auth not paused")
			}
			if status == 429 && s.quota.Cooldown.Before(now.Add(50*time.Second)) {
				t.Fatal("no shared cooldown")
			}
			if s.Step(context.Background(), now.Add(20*time.Second)) {
				t.Fatal("paused scheduler sent request")
			}
		})
	}
}
