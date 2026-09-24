package datahub

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func ptr(s string) *string { return &s }
func orderObservation(d Dataset, at time.Time, r LargeOrder) Observation {
	return Observation{Dataset: d.ID, Source: d.Source, FetchedAt: at, Quality: "valid", Payload: Payload{Large: []LargeOrder{r}}}
}
func orderFixture(at time.Time) LargeOrder {
	return LargeOrder{ID: "test-order", Side: "bid", Price: "80000", Quantity: "10", ReportedUSD: "800000", ExecutedUSD: "400000", ExecutedQuantity: ptr("5"), InitialQuantity: ptr("15"), RawState: 1, Changed: &at}
}
func TestOrderLifecycleAndCorrections(t *testing.T) {
	w := testStore(t)
	ctx := context.Background()
	d, _ := FindDataset("large.btc.binance.spot")
	hd, _ := FindDataset("large-history.btc.binance.spot")
	at := time.Now().UTC().Add(-time.Hour)
	r := orderFixture(at)
	ingest := func(d Dataset, r LargeOrder, at time.Time) {
		t.Helper()
		if _, err := w.Ingest(d, orderObservation(d, at, r)); err != nil {
			t.Fatal(err)
		}
	}
	events := func() []OrderEvent {
		t.Helper()
		es, _, err := w.OrderEvents(ctx, "BTC", at.Add(-time.Minute), at.Add(2*time.Hour), 100, 0)
		if err != nil {
			t.Fatal(err)
		}
		return es
	}
	ingest(d, r, at)
	ingest(d, r, at.Add(time.Minute))
	if es := events(); len(es) != 1 || es[0].ExecutedDelta != nil {
		t.Fatal("first/duplicate attributed cumulative execution")
	}
	later := at.Add(5 * time.Minute)
	r.Changed = &later
	r.Quantity = "11"
	r.ExecutedQuantity = ptr("6")
	r.ExecutedUSD = "480000"
	ingest(d, r, later)
	es := events()
	if len(es) != 2 || es[0].ExecutedQuantityDelta == nil || *es[0].ExecutedQuantityDelta != "1" || *es[0].QuantityDelta != "1" {
		t.Fatal("refill/execution not kept separate")
	}
	end := later.Add(time.Minute)
	r.End = &end
	r.RawState = 2
	ingest(hd, r, end)
	ingest(hd, r, end.Add(time.Minute))
	if len(events()) != 3 {
		t.Fatal("history duplicate")
	}
	ended, _, err := w.EndedOrders(ctx, "BTC", 100, 0)
	if err != nil || len(ended) != 1 || ended[0].Order.Quantity != "11" {
		t.Fatal("ended remaining was invented as zero")
	}
	// Out-of-order active response cannot resurrect or add an execution.
	ingest(d, orderFixture(at), end.Add(2*time.Minute))
	if len(events()) != 3 {
		t.Fatal("old active response changed lifecycle")
	}
	// Cumulative rollback is a correction, not negative trading volume.
	newer := end.Add(5 * time.Minute)
	r.Changed = &newer
	r.ExecutedQuantity = ptr("4")
	r.ExecutedUSD = "320000"
	ingest(hd, r, newer)
	es = events()
	if es[0].Kind != "correction" || es[0].ExecutedDelta != nil {
		t.Fatal("rollback treated as flow")
	}
	for _, e := range es {
		if e.ExecutedDelta != nil {
			t.Fatal("old invalid execution was not retracted")
		}
	}
	// Empty current list must not manufacture a cancellation event.
	if _, err = w.Ingest(d, Observation{Dataset: d.ID, Source: d.Source, FetchedAt: newer.Add(time.Minute), Quality: "valid"}); err != nil {
		t.Fatal(err)
	}
	if len(events()) != len(es) {
		t.Fatal("disappearance implies cancellation")
	}
}
func TestLargeContractsAndEmpty(t *testing.T) {
	d, _ := FindDataset("large.btc.binance.spot")
	now := time.Now().UTC()
	raw := fmt.Sprintf(`{"code":"0","data":[{"id":"x","order_side":2,"order_state":1,"limit_price":80000,"current_quantity":5,"current_usd_value":400000,"executed_usd_value":200000,"executed_volume":2.5,"start_quantity":7.5,"current_time":%d}]}`, now.UnixMilli())
	rows, err := Normalize(d, []byte(raw), now)
	if err != nil || len(rows) != 1 || rows[0].Payload.Large[0].Side != "bid" || *rows[0].Payload.Large[0].ExecutedQuantity != "2.5" {
		t.Fatalf("contract %v", err)
	}
	rows, err = Normalize(d, []byte(`{"code":"0","data":[]}`), now)
	if err != nil || len(rows) != 1 || len(rows[0].Payload.Large) != 0 {
		t.Fatal("valid empty not recognized")
	}
}
func TestHistorySaturationAndDurableCursor(t *testing.T) {
	w := testStore(t)
	d, _ := FindDataset("large-history.btc.binance.spot")
	s := NewScheduler(w, []Dataset{d}, nil, true, time.Now())
	j := s.jobs[d.ID]
	from := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	to := from.Add(30 * time.Minute)
	j.OrderRanges = []OrderRange{{from, to}}
	j.InFlight = true
	s.inflight = 1
	s.fetch = func(_ context.Context, d Dataset) ([]byte, error) {
		if d.Params["state"] != "2" || d.Params["start_time"] != strconv.FormatInt(from.UnixMilli(), 10) {
			t.Fatal("missing query bounds/state")
		}
		rs := []map[string]any{}
		for i := 0; i < 100; i++ {
			rs = append(rs, map[string]any{"id": fmt.Sprint(i), "order_side": 1, "order_state": 2, "limit_price": 81000, "current_quantity": 1, "current_usd_value": 81000, "executed_usd_value": 0, "order_end_time": from.Add(time.Minute).UnixMilli()})
		}
		return json.Marshal(map[string]any{"code": "0", "data": rs})
	}
	s.runOrderHistory(context.Background(), *j)
	if len(j.OrderRanges) != 2 || j.OrderThrough != nil {
		t.Fatal("truncated history incorrectly marked complete")
	}
	restored := NewScheduler(w, []Dataset{d}, nil, true, time.Now())
	j = restored.jobs[d.ID]
	if len(j.OrderRanges) != 2 {
		t.Fatal("split queue lost on restart")
	}
	restored.fetch = func(context.Context, Dataset) ([]byte, error) { return []byte(`{"code":"0","data":[]}`), nil }
	j.InFlight = true
	restored.inflight = 1
	restored.runOrderHistory(context.Background(), *j)
	if j.OrderThrough == nil || !j.OrderThrough.Equal(from.Add(15*time.Minute)) || len(j.OrderRanges) != 1 {
		t.Fatal("empty page failed to advance exactly one window")
	}
}
func TestWindowEvidenceAndNoDoubleCounting(t *testing.T) {
	h, err := Open(Config{Root: t.TempDir(), Offline: true})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Store.Close()
	d, _ := h.Dataset("flow.btc..spot")
	from := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Hour)
	to := from.Add(time.Hour)
	for i := 0; i < 60; i++ {
		if i == 20 {
			continue
		}
		o := testObservation(d, from.Add(time.Duration(i)*time.Minute), "3")
		if _, err = h.Store.Ingest(d, o); err != nil {
			t.Fatal(err)
		}
	}
	s, err := h.flowWindow(context.Background(), d, from, to, from, 60)
	if err != nil {
		t.Fatal(err)
	}
	if s.Buy != 17700 || s.Sell != 5900 || !s.Boundaries || s.Coverage < .98 || !s.Partial {
		t.Fatalf("invalid window: %+v", s)
	}
	bias, _ := activityBias(s, true, true)
	if bias != "买方较主动" {
		t.Fatal(bias)
	}
	s.Boundaries = false
	bias, _ = activityBias(s, true, true)
	if bias != "证据不足" {
		t.Fatal("missing endpoint guessed")
	}
	s.Boundaries = true
	s.Coverage = .89
	bias, _ = activityBias(s, true, true)
	if bias != "证据不足" {
		t.Fatal("incomplete window guessed")
	}
	var last int64
	for _, p := range s.Series {
		if n, ok := p["cvdCents"].(int64); ok {
			last = n
		}
	}
	if last != s.Buy-s.Sell {
		t.Fatal("CVD and net diverge")
	}
	for i := 0; i < 100; i++ {
		if _, err = h.Read(context.Background(), "activity", url.Values{"asset": {"BTC"}}); err != nil {
			t.Fatal(err)
		}
	}
	if h.Scheduler.quota.Calls != 0 {
		t.Fatal("local queries spent upstream quota")
	}
}
func TestWhalePolicyAndRegistryBudget(t *testing.T) {
	now := time.Now().UTC()
	for _, tc := range []struct {
		age   time.Duration
		fresh bool
	}{{7 * time.Minute, true}, {8 * time.Minute, true}, {481 * time.Second, false}, {-time.Minute, false}} {
		if freshWhale(Whale{At: now.Add(-tc.age)}, now) != tc.fresh {
			t.Fatalf("whale age %v", tc.age)
		}
	}
	budget := 0.0
	for _, d := range Registry() {
		if d.Source == "coinglass" {
			budget += 60.0 / float64(d.Refresh)
		}
		if d.Kind == "whales" && (d.Refresh != 300 || d.TTL != 480) {
			t.Fatal("policy mismatch")
		}
	}
	if budget > 10.65 || budget < 10.6 {
		t.Fatalf("budget %.3f", budget)
	}
}
func TestOrderRetentionBudgetAndBackup(t *testing.T) {
	w := testStore(t)
	d, _ := FindDataset("large.btc.binance.spot")
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Hour)
	at := now.Add(-31 * 24 * time.Hour)
	r := orderFixture(at)
	if _, err := w.Ingest(d, orderObservation(d, at, r)); err != nil {
		t.Fatal(err)
	}
	if err := w.maintainOrders(ctx, now, 90); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := w.db.QueryRow("SELECT count(*) FROM order_hours").Scan(&n); err != nil || n != 1 {
		t.Fatal("hourly summary absent", err)
	}
	if err := w.db.QueryRow("SELECT count(*) FROM order_events").Scan(&n); err != nil || n != 0 {
		t.Fatal("old details retained")
	}
	// Simulate hard pressure; latest still changes, no unbounded detail writes.
	w.mu.Lock()
	w.status.OrderPaused = true
	w.mu.Unlock()
	r = orderFixture(now)
	r.ID = "budget"
	if _, err := w.Ingest(d, orderObservation(d, now, r)); err != nil {
		t.Fatal(err)
	}
	o, ok := w.Latest(d.ID)
	if !ok || len(o.Payload.Large) != 1 {
		t.Fatal("latest stopped under pressure")
	}
	dest := filepath.Join(t.TempDir(), "hub.sqlite")
	if err := BackupFile(ctx, filepath.Join(w.root, "hub.sqlite"), dest); err != nil {
		t.Fatal(err)
	}
	if st, err := os.Stat(dest); err != nil || st.Size() == 0 {
		t.Fatal("backup absent")
	}
	restored, err := OpenWarehouse(filepath.Dir(dest))
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if err := restored.db.QueryRow("SELECT count(*) FROM order_hours").Scan(&n); err != nil || n != 1 {
		t.Fatal("new lifecycle summary absent after restore", err)
	}
	gap, err := restored.OrderGap(ctx, "BTC", now, now.Add(time.Hour))
	if err != nil || !gap {
		t.Fatal("capacity gap lost after restore", err)
	}
}

func TestRangeTotalsAcrossAssetBucketAndFX(t *testing.T) {
	h, err := Open(Config{Root: t.TempDir(), Offline: true})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Store.Close()
	now := time.Now().UTC()
	fx, _ := h.Dataset("fx.usd.kraken")
	h.Store.Ingest(fx, Observation{Dataset: fx.ID, Source: fx.Source, FetchedAt: now, ObservedAt: &now, Quality: "valid", Payload: Payload{Rates: []Rate{{"USDT", "0.98"}}}})
	for _, asset := range Assets() {
		center := 80000.
		if asset == "ETH" {
			center = 2000
		}
		pd, _ := h.Dataset(ID("price", asset, "Binance", "spot"))
		h.Store.Ingest(pd, Observation{Dataset: pd.ID, Source: pd.Source, FetchedAt: now, ObservedAt: &now, Quality: "valid", Payload: Payload{Price: &Price{fmt.Sprint(center / .98), "USDT"}}})
		wantBid, wantAsk := int64(0), int64(0)
		for i, v := range []string{"Binance", "OKX", "Coinbase", "Kraken", "Bitfinex"} {
			d, _ := h.Dataset(ID("book", asset, v, "spot"))
			rate := 1.
			if d.Quote == "USDT" {
				rate = .98
			}
			bp, ap := center*.995/rate, center*1.005/rate
			qty := float64(i + 1)
			o := Observation{Dataset: d.ID, Source: d.Source, FetchedAt: now, ObservedAt: &now, Resolution: 60, Quality: "valid", Payload: Payload{Book: &Book{Bids: []Level{{fmt.Sprint(bp), fmt.Sprint(qty)}}, Asks: []Level{{fmt.Sprint(ap), fmt.Sprint(qty)}}, Low: bp, High: ap}}}
			if _, err = h.Store.Ingest(d, o); err != nil {
				t.Fatal(err)
			}
			wantBid += int64(center*.995*qty*100 + .5)
			wantAsk += int64(center*1.005*qty*100 + .5)
		}
		for _, step := range []float64{baseStep(asset), baseStep(asset) * 4, baseStep(asset) * 10} {
			for _, span := range []float64{10, 1000} {
				f := h.Overview(context.Background(), asset, step, span, 0)
				if f.Summary.BidCents != wantBid || f.Summary.AskCents != wantAsk || !f.Summary.HasData {
					t.Fatalf("%s totals wrong for %.0f/%v: %+v", asset, step, span, f.Summary)
				}
				for _, z := range f.Zones {
					var sum int64
					for _, n := range z.Sources {
						sum += n
					}
					if sum != z.USD {
						t.Fatal("contributions != bucket")
					}
				}
				filtered := h.Overview(context.Background(), asset, step, span, 86400)
				if filtered.Summary != f.Summary { // time pointers differ; compare monetary values below
					if filtered.Summary.BidCents != wantBid || filtered.Summary.AskCents != wantAsk {
						t.Fatal("minimum age changed range total")
					}
				}
			}
		}
	}
}
func TestDerivativesReadsNativeStockResolution(t *testing.T) {
	h, err := Open(Config{Root: t.TempDir(), Offline: true})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Store.Close()
	to := time.Now().UTC().Truncate(5 * time.Minute)
	from := to.Add(-time.Hour)
	d, _ := h.Dataset("oi.btc..futures")
	for _, p := range []struct {
		at  time.Time
		usd string
	}{{from, "1000"}, {to.Add(-5 * time.Minute), "1200"}} {
		o := Observation{Dataset: d.ID, Source: d.Source, FetchedAt: p.at, Quality: "valid", Payload: Payload{OI: []Interest{{Venue: "All", USD: p.usd}}}}
		if _, err = h.Store.Ingest(d, o); err != nil {
			t.Fatal(err)
		}
	}
	v, err := h.derivativesAt(context.Background(), "BTC", 1, to)
	if err != nil {
		t.Fatal(err)
	}
	m := v.(map[string]any)
	if m["oiChangeCents"].(*int64) == nil || *m["oiChangeCents"].(*int64) != 20000 {
		t.Fatal("OI stock history lost its native granularity")
	}
}

func TestHistoryNeverPreemptsDueCurrentAndErrorsDoNotAdvance(t *testing.T) {
	w := testStore(t)
	book, _ := FindDataset("book.btc.binance.spot")
	hist, _ := FindDataset("large-history.btc.binance.spot.revoked")
	now := time.Now().UTC()
	called := make(chan string, 3)
	s := NewScheduler(w, []Dataset{book, hist}, func(_ context.Context, d Dataset) ([]byte, error) {
		called <- d.Kind
		return nil, &FetchError{Status: 429, RetryAfter: time.Minute}
	}, true, now)
	s.jobs[book.ID].Next = now
	s.jobs[hist.ID].Next = now.Add(-24 * time.Hour)
	if !s.Step(context.Background(), now) {
		t.Fatal("current not scheduled")
	}
	s.wg.Wait()
	if <-called != "book" {
		t.Fatal("old history preempted current")
	}
	j := s.jobs[hist.ID]
	j.InFlight = true
	s.inflight = 1
	s.runOrderHistory(context.Background(), *j)
	if j.OrderThrough != nil || len(j.OrderRanges) != 1 || s.quota.RateLimited != 2 || s.quota.Cooldown.Before(now.Add(59*time.Second)) {
		t.Fatal("429 lost range/progress or global cooldown")
	}
	s.fetch = func(context.Context, Dataset) ([]byte, error) { return nil, &FetchError{Status: 401} }
	j.InFlight = true
	s.inflight = 1
	s.runOrderHistory(context.Background(), *j)
	if !s.quota.AuthFailed {
		t.Fatal("history authorization did not pause shared quota")
	}
}
func TestOrderTimestampExactMilliseconds(t *testing.T) {
	for _, ms := range []int64{1790260740001, 1790260740999} {
		v := timestamp(json.Number(strconv.FormatInt(ms, 10)))
		if v == nil || v.UnixMilli() != ms {
			t.Fatal("history boundary lost precision")
		}
	}
}

func TestHistoryFilterMismatchPreservesRawFactAndGap(t *testing.T) {
	w := testStore(t)
	d, _ := FindDataset("large-history.btc.binance.spot.revoked")
	now := time.Now().UTC().Truncate(time.Millisecond)
	s := NewScheduler(w, []Dataset{d}, func(context.Context, Dataset) ([]byte, error) {
		return []byte(fmt.Sprintf(`{"code":"0","data":[{"id":"ended","limit_price":80000,"current_quantity":1,"current_usd_value":80000,"executed_usd_value":0,"order_side":2,"order_state":2,"order_end_time":%d}]}`, now.Add(-time.Minute).UnixMilli())), nil
	}, true, now)
	j := s.jobs[d.ID]
	j.OrderRanges = []OrderRange{{now.Add(-30 * time.Minute), now}}
	j.InFlight, s.inflight = true, 1
	s.runOrderHistory(context.Background(), *j)
	if j.OrderThrough == nil || j.OrderGaps != 1 || j.Error == "" || j.Failures != 0 {
		t.Fatal("filter mismatch either hidden or retried forever")
	}
	rows, _, err := w.EndedOrders(context.Background(), "BTC", 100, 0)
	if err != nil || len(rows) != 1 || rows[0].Order.RawState != 2 {
		t.Fatal("query filter manufactured a cancellation")
	}
}

func TestActivityCandleCoverageDoesNotRoundDownThreshold(t *testing.T) {
	h, err := Open(Config{Root: t.TempDir(), Offline: true})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Store.Close()
	now := time.Now().UTC()
	to := now.Truncate(5 * time.Minute)
	from := to.Add(-time.Hour)
	fd, _ := h.Dataset("flow.btc..spot")
	cd, _ := h.Dataset("candles.btc.binance.spot")
	for i := 0; i < 60; i++ {
		h.Store.Ingest(fd, testObservation(fd, from.Add(time.Duration(i)*time.Minute), "3"))
	}
	for _, d := range []Dataset{mustDataset(t, "price.btc.binance.spot"), mustDataset(t, "fx.usd.kraken")} {
		h.Store.Ingest(d, Observation{Dataset: d.ID, Source: d.Source, ObservedAt: &now, FetchedAt: now, Quality: "valid", Payload: Payload{Price: &Price{"80000", "USDT"}, Rates: []Rate{{"USDT", "1"}}}})
	}
	for i := 0; i < 12; i++ {
		if i == 4 || i == 5 {
			continue
		}
		at := from.Add(time.Duration(i) * 5 * time.Minute)
		h.Store.Ingest(cd, Observation{Dataset: cd.ID, Source: cd.Source, ObservedAt: &at, FetchedAt: now, Quality: "valid", Resolution: 300, Payload: Payload{Candle: &Candle{Open: 80000, Close: 80100, High: 80200, Low: 79900}}})
	}
	v, err := h.ActivityView(context.Background(), "BTC", 1, 5)
	if err != nil {
		t.Fatal(err)
	}
	if v.(map[string]any)["bias"] != "证据不足" {
		t.Fatal("10 of 12 candles treated as 90% coverage")
	}
}

func mustDataset(t *testing.T, id string) Dataset {
	t.Helper()
	d, err := FindDataset(id)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
