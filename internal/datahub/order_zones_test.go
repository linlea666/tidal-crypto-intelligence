package datahub

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/shopspring/decimal"
	"net/url"
	"path/filepath"
	"testing"
	"time"
)

func zoneHub(t *testing.T) *Hub {
	t.Helper()
	h, e := Open(Config{Root: t.TempDir(), Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { h.Store.Close() })
	return h
}
func zoneIngest(t *testing.T, h *Hub, d Dataset, o Observation) {
	t.Helper()
	if _, e := h.Store.Ingest(d, o); e != nil {
		t.Fatal(e)
	}
}
func zonePrice(t *testing.T, h *Hub, a, rate, p string, at time.Time) {
	t.Helper()
	fd, _ := h.Dataset("fx.usd.kraken")
	zoneIngest(t, h, fd, Observation{Dataset: fd.ID, ObservedAt: &at, FetchedAt: at, Quality: "valid", Resolution: 60, Payload: Payload{Rates: []Rate{{Quote: "USDT", USD: rate}}}})
	pd, _ := h.Dataset(ID("price", a, "Binance", "spot"))
	zoneIngest(t, h, pd, Observation{Dataset: pd.ID, ObservedAt: &at, FetchedAt: at, Quality: "valid", Payload: Payload{Price: &Price{Value: p, Quote: "USDT"}}})
}
func TestOrderZoneFullSetFXAndCenteredBoundaries(t *testing.T) {
	h := zoneHub(t)
	now := time.Now().UTC()
	zonePrice(t, h, "BTC", "1.002", "85000", now)
	var expected int64
	for _, v := range []string{"Coinbase", "Binance"} {
		d, _ := h.Dataset(ID("large", "BTC", v, "spot"))
		o := orderObservation(d, now, orderFixture(now))
		o.Payload.Large = nil
		for i := 0; i < 267; i++ {
			r := orderFixture(now)
			r.ID = fmt.Sprint(i)
			r.Price = "82000"
			r.Quantity = "0.12345678"
			o.Payload.Large = append(o.Payload.Large, r)
			rate := "1"
			if v == "Binance" {
				rate = "1.002"
			}
			expected += money(multiply(multiply(r.Price, r.Quantity), rate))
		}
		zoneIngest(t, h, d, o)
	}
	z, e := h.orderZones(context.Background(), "BTC", url.Values{}, now)
	if e != nil {
		t.Fatal(e)
	}
	if len(z.Zones) != 2 || z.Valid != 534 || z.Bid == nil || *z.Bid != expected {
		t.Fatalf("paginated/misconverted: %+v", z)
	}
	if z.Zones[0].Center != 82250 || z.Zones[1].Center != 82000 {
		t.Fatal("native USDT was bucketed before FX")
	}
	for _, b := range z.Zones {
		var sum int64
		var qs string
		for _, r := range b.Items {
			sum += int64(num(r["usdCents"]))
			qs = dec(qs).Add(dec(str(r["quantity"]))).String()
		}
		if sum != b.USD || qs != b.Quantity || b.Sources[0].USD != sum {
			t.Fatal("hierarchical reconciliation")
		}
	}
	for _, tc := range []struct {
		p    string
		s, w float64
	}{{"81874.999999", 250, 81750}, {"81875", 250, 82000}, {"82125", 250, 82250}, {"2994.999999", 10, 2990}, {"2995", 10, 3000}, {"3005", 10, 3010}} {
		if g := orderZoneCenter(tc.p, tc.s); g != tc.w {
			t.Fatalf("boundary %s: %v", tc.p, g)
		}
	}
	if z.Zones[0].ExecutedQuantity != nil {
		t.Fatal("first discovery counted old cumulative execution")
	}
}
func TestOrderZoneUnknownAndFrozenFacts(t *testing.T) {
	h := zoneHub(t)
	now := time.Now().UTC()
	zonePrice(t, h, "ETH", "0.9999", "3300", now)
	d, _ := h.Dataset(ID("large", "ETH", "Binance", "spot"))
	r := orderFixture(now)
	r.Price = "3200.001"
	r.Quantity = "2.345678901"
	zoneIngest(t, h, d, orderObservation(d, now, r))
	original, e := h.orderZones(context.Background(), "ETH", url.Values{}, now)
	if e != nil || len(original.Zones) != 1 {
		t.Fatalf("%v %+v", e, original)
	}
	before, _ := json.Marshal(original)
	expired, e := h.orderZones(context.Background(), "ETH", url.Values{}, now.Add(time.Minute))
	if e != nil || expired.Bid != nil || len(expired.Zones) != 0 || !expired.Partial {
		t.Fatal("expired FX/price was money")
	}
	after, _ := json.Marshal(original)
	if string(before) != string(after) {
		t.Fatal("old view mutated")
	}
	// A terminal history fact excludes a still-returned active row.
	end := now.Add(time.Second)
	r.RawState = 2
	r.End = &end
	r.Changed = &end
	hd, _ := h.Dataset(ID("large-history", "ETH", "Binance", "spot"))
	zoneIngest(t, h, hd, orderObservation(hd, end, r))
	ended, e := h.orderZones(context.Background(), "ETH", url.Values{}, end)
	if e != nil || ended.Valid != 0 {
		t.Fatal("terminal order still ranks")
	}
}
func TestOrderZoneCandidatesAreUnion(t *testing.T) {
	zs := []OrderZone{}
	for i := 1; i <= 9; i++ {
		zs = append(zs, OrderZone{ID: fmt.Sprint(i), Side: "bid", Center: float64(i), USD: int64(10 - i)})
	}
	rankOrderZones(zs, 10)
	n := 0
	for _, z := range zs {
		if z.Focus {
			n++
		}
	}
	if n != 6 {
		t.Fatal(n)
	}
}
func TestOrderZoneObservationRecoveryCapsAndNoBackfill(t *testing.T) {
	root := t.TempDir()
	h, e := Open(Config{Root: root, Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	now := time.Now().UTC()
	d, _ := h.Dataset(ID("large", "BTC", "Coinbase", "spot"))
	r := orderFixture(now)
	old := now.Add(-8 * 24 * time.Hour)
	r.Start = &old
	for _, dt := range []time.Duration{-30 * time.Minute, -25 * time.Minute, -5 * time.Minute} {
		at := now.Add(dt)
		zoneIngest(t, h, d, orderObservation(d, at, r))
	}
	// An exact retry cannot evict older observations or grow the ledger.
	var pre int64
	h.Store.db.QueryRow("SELECT bytes FROM order_zone_storage WHERE id=1").Scan(&pre)
	zoneIngest(t, h, d, orderObservation(d, now.Add(-5*time.Minute), r))
	var post int64
	h.Store.db.QueryRow("SELECT bytes FROM order_zone_storage WHERE id=1").Scan(&post)
	if pre != post {
		t.Fatal("duplicate grew storage")
	}
	backup := filepath.Join(t.TempDir(), "hub.sqlite")
	if e = BackupFile(context.Background(), filepath.Join(root, "hub.sqlite"), backup); e != nil {
		t.Fatal(e)
	}
	h.Store.Close()
	h, e = Open(Config{Root: root, Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	zonePrice(t, h, "BTC", "1", "85000", now)
	q := url.Values{"layer": {"orders"}, "period": {"24h"}, "order": {orderKey(d, r)}}
	hist, e := h.orderZoneHistory(context.Background(), "BTC", q, now)
	if e != nil {
		t.Fatal(e)
	}
	if hist.ActualFrom == nil || !hist.ActualFrom.Equal(now.Add(-30*time.Minute)) || len(hist.Track) != 4 {
		t.Fatalf("inferred lifetime or gap lost: %+v", hist.Track)
	}
	if hist.Track[2].State != "gap" || hist.StorageBytes != pre {
		t.Fatal("gap/counter lost after recovery")
	}
	bdb, e := database(backup)
	if e != nil {
		t.Fatal(e)
	}
	var n int
	bdb.QueryRow("SELECT count(*) FROM order_zone_samples").Scan(&n)
	bdb.Close()
	if n != 3 {
		t.Fatal("backup omitted observations")
	}
	tx, _ := h.Store.db.Begin()
	fit, e := trimOrderZoneSamples(tx, 100, 100, OrderHistoryLimit)
	if e != nil || !fit {
		t.Fatal(e)
	}
	tx.Commit()
	h.Store.db.QueryRow("SELECT count(*) FROM order_zone_samples").Scan(&n)
	if n != 0 {
		t.Fatal("cap not enforced")
	}
	tx, _ = h.Store.db.Begin()
	fit, e = trimOrderZoneSamples(tx, 101, 100, OrderHistoryLimit)
	tx.Rollback()
	if e != nil || fit {
		t.Fatal("oversize fit")
	}
}
func TestOrderZoneHeatCoverageAndFootprint(t *testing.T) {
	h := zoneHub(t)
	now := time.Now().UTC()
	zonePrice(t, h, "BTC", "1", "85000", now)
	at := now.Truncate(5 * time.Minute).Add(-5 * time.Minute)
	d, _ := h.Dataset(ID("book", "BTC", "Coinbase", "spot"))
	b := &Book{Low: 80000, High: 89000, Bids: []Level{{Price: "82000", Quantity: "10"}}, Asks: []Level{{Price: "88000", Quantity: "20"}}}
	zoneIngest(t, h, d, Observation{Dataset: d.ID, FetchedAt: now, ObservedAt: &at, Resolution: 300, Quality: "valid", Payload: Payload{Book: b}})
	old, e := h.HistoryView(context.Background(), "BTC", 4, 0, 250, 10, true)
	if e != nil {
		t.Fatal(e)
	}
	points := old.(map[string]any)["points"].([]map[string]any)
	if len(points) != 1 || num(points[0]["sources"]) != 1 {
		t.Fatalf("zero heat coverage: %+v", points)
	}
	hist, e := h.orderZoneHistory(context.Background(), "BTC", url.Values{"period": {"24h"}}, now)
	if e != nil {
		t.Fatal(e)
	}
	found := false
	for _, p := range hist.Points {
		if len(p.Cells) > 0 {
			found = true
			if p.Cells[0][4].(int) != 4 || p.Cells[0][5].(int) != 4 {
				t.Fatal("per-cell venue coverage", p.Cells)
			}
		}
	}
	if !found {
		t.Fatal("missing book history")
	}
	fd, _ := h.Dataset(ID("fx", "USD", "Kraken", ""))
	_ = fd
	fx, _ := h.Dataset("fx.usd.kraken")
	zoneIngest(t, h, fx, Observation{Dataset: fx.ID, FetchedAt: now, ObservedAt: &at, Resolution: 60, Quality: "valid", Payload: Payload{Rates: []Rate{{Quote: "USDT", USD: "1.002"}}}})
	ft, _ := h.Dataset(ID("footprint", "BTC", "Bybit", "spot"))
	zoneIngest(t, h, ft, Observation{Dataset: ft.ID, FetchedAt: now, ObservedAt: &at, Resolution: 300, Quality: "valid", Payload: Payload{Foot: []Foot{{Low: "82000", High: "82100", BuyBase: "2", SellBase: "3", BuyQuote: "164000", SellQuote: "246000"}}}})
	trades, e := h.orderZoneTrades(context.Background(), "BTC", 1, 250, 10, 85000, true, now, nil)
	if e != nil || len(trades.Zones) != 1 {
		t.Fatalf("%v %+v", e, trades)
	}
	if trades.Zones[0].BuyCents != 16432800 || trades.Zones[0].Buy != "2" || !trades.Partial {
		t.Fatal("flow conversion/coverage", trades)
	}
}
func TestOrderZoneReadIsLocalOnly(t *testing.T) {
	h := zoneHub(t)
	now := time.Now().UTC()
	zonePrice(t, h, "BTC", "1", "85000", now)
	before, _ := json.Marshal(h.Scheduler.State())
	for i := 0; i < 100; i++ {
		q := url.Values{"asset": {"BTC"}, "range": {fmt.Sprint(i%25 + 1)}}
		if _, e := h.Read(context.Background(), "large-order-zones", q); e != nil {
			t.Fatal(e)
		}
	}
	after, _ := json.Marshal(h.Scheduler.State())
	if string(before) != string(after) {
		t.Fatal("GET scheduled upstream work")
	}
}

func TestOrderZoneExecutionWindowAndCorrection(t *testing.T) {
	h := zoneHub(t)
	now := time.Now().UTC()
	d, _ := h.Dataset(ID("large", "BTC", "Coinbase", "spot"))
	from := now.Truncate(5 * time.Minute).Add(-time.Hour)
	r := orderFixture(from)
	r.Changed = nil
	zoneIngest(t, h, d, orderObservation(d, from.Add(-time.Minute), r))
	r.Quantity = "9"
	r.ExecutedQuantity = ptr("6")
	r.ExecutedUSD = "480000"
	zoneIngest(t, h, d, orderObservation(d, from.Add(time.Minute), r))
	r.Quantity = "7"
	r.ExecutedQuantity = ptr("8")
	r.ExecutedUSD = "640000"
	zoneIngest(t, h, d, orderObservation(d, from.Add(10*time.Minute), r))
	zoneIngest(t, h, d, orderObservation(d, now, r))
	zonePrice(t, h, "BTC", "1", "85000", now)
	v, e := h.orderZones(context.Background(), "BTC", url.Values{"hours": {"1"}}, now)
	if e != nil || len(v.Zones) != 1 {
		t.Fatal(e)
	}
	z := v.Zones[0]
	if z.ExecutedQuantity == nil || *z.ExecutedQuantity != "2" || z.UncertainExecutedQuantity == nil || *z.UncertainExecutedQuantity != "1" {
		t.Fatalf("crossing cumulative delta: %+v", z)
	}
	if z.ChangeQuantity == nil || *z.ChangeQuantity != "-2" {
		t.Fatal("native quantity change", z.ChangeQuantity)
	}
	r.ExecutedQuantity = ptr("4")
	r.ExecutedUSD = "320000"
	zoneIngest(t, h, d, orderObservation(d, now.Add(time.Second), r))
	corrected, e := h.orderZones(context.Background(), "BTC", url.Values{"hours": {"1"}}, now.Add(2*time.Second))
	if e != nil || corrected.Zones[0].ExecutedQuantity != nil {
		t.Fatal("old execution survived correction", e)
	}
}
func TestOrderZoneRetentionAndPriority(t *testing.T) {
	h := zoneHub(t)
	now := time.Now().UTC()
	d, _ := h.Dataset(ID("large", "BTC", "Coinbase", "spot"))
	r := orderFixture(now)
	old := now.Add(-8 * 24 * time.Hour)
	r.Changed = nil
	zoneIngest(t, h, d, orderObservation(d, old, r))
	zoneIngest(t, h, d, orderObservation(d, now, r))
	var n int
	h.Store.db.QueryRow("SELECT count(*) FROM order_zone_samples").Scan(&n)
	if n != 1 {
		t.Fatal("7 day retention", n)
	}
	// Fill only the optional-history charge to reproduce shared-ledger pressure.
	var size int64
	h.Store.db.QueryRow("SELECT bytes FROM order_zone_storage").Scan(&size)
	h.Store.db.Exec("UPDATE order_storage SET bytes=?", OrderHistoryLimit-size/2)
	r.ID = "priority-new"
	zoneIngest(t, h, d, orderObservation(d, now.Add(time.Second), r))
	var payload []byte
	if err := h.Store.db.QueryRow("SELECT payload FROM tracked_orders WHERE k=?", orderKey(d, r)).Scan(&payload); err != nil {
		t.Fatal("new history starved lifecycle", err)
	}
	// Simulated other-feature charge is deliberately not removed by the trimmer.
	h.Store.db.Exec("UPDATE order_storage SET bytes=?", OrderHistoryLimit)
	tx, _ := h.Store.db.Begin()
	fit, e := trimOrderZoneSamples(tx, 1024, orderZoneLimit, OrderHistoryLimit)
	tx.Commit()
	if e != nil {
		t.Fatal(e)
	}
	if fit {
		var bytes int64
		h.Store.db.QueryRow("SELECT bytes FROM order_storage").Scan(&bytes)
		if bytes+1024 > OrderHistoryLimit {
			t.Fatal("shared budget not enforced")
		}
	}
}
func TestOrderZoneHistoryDoesNotSumTemporalFrames(t *testing.T) {
	h := zoneHub(t)
	now := time.Now().UTC()
	zonePrice(t, h, "BTC", "1", "85000", now)
	d, _ := h.Dataset(ID("book", "BTC", "Coinbase", "spot"))
	start := now.Truncate(30 * time.Minute).Add(-time.Hour)
	for i := 0; i < 3; i++ {
		at := start.Add(time.Duration(i) * 5 * time.Minute)
		zoneIngest(t, h, d, Observation{Dataset: d.ID, ObservedAt: &at, FetchedAt: now, Resolution: 300, Quality: "valid", Payload: Payload{Book: &Book{Low: 81000, High: 88000, Bids: []Level{{Price: "82000", Quantity: fmt.Sprint(i + 1)}}, Asks: []Level{{Price: "88000", Quantity: "1"}}}}})
	}
	hist, e := h.orderZoneHistory(context.Background(), "BTC", url.Values{}, now)
	if e != nil {
		t.Fatal(e)
	}
	for _, p := range hist.Points {
		for _, c := range p.Cells {
			if c[0].(float64) == 82000 && c[3].(string) != "3" {
				t.Fatal("summed stock over time", c)
			}
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e = h.orderZoneHistory(ctx, "BTC", url.Values{}, now); e == nil {
		t.Fatal("canceled unbounded read")
	}
}

func TestOrderZoneBusyWriterHonorsDeadline(t *testing.T) {
	h := zoneHub(t)
	h.Store.write.Lock()
	defer h.Store.write.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, e := h.orderZones(ctx, "BTC", url.Values{}, time.Now()); e != context.DeadlineExceeded {
		t.Fatal(e)
	}
}

func TestOrderZoneDuplicateIdentityNeverAddsLiquidity(t *testing.T) {
	h := zoneHub(t)
	now := time.Now().UTC()
	zonePrice(t, h, "BTC", "1", "85000", now)
	d, _ := h.Dataset(ID("large", "BTC", "Coinbase", "spot"))
	r := orderFixture(now)
	o := orderObservation(d, now, r)
	o.Payload.Large = append(o.Payload.Large, r)
	zoneIngest(t, h, d, o)
	v, e := h.orderZones(context.Background(), "BTC", url.Values{}, now)
	if e != nil || len(v.Zones) != 1 || v.Zones[0].Quantity != "10" {
		t.Fatal("duplicate amount", e)
	}
	o.FetchedAt = now.Add(time.Second)
	o.Payload.Large[1].Quantity = "20"
	zoneIngest(t, h, d, o)
	v, e = h.orderZones(context.Background(), "BTC", url.Values{}, now.Add(time.Second))
	if e != nil || len(v.Zones) != 0 || !v.Partial {
		t.Fatal("conflicting duplicates ranked", e)
	}
}

func TestOrderZoneFastIndexPreservesExactBoundaries(t *testing.T) {
	for _, step := range []float64{1, 10, 25, 250, 1000} {
		sd, half := decimal.NewFromFloat(step), decimal.NewFromFloat(step/2)
		for _, rate := range []string{"1", "1.0000000000000000003", "0.99871432", "1.05237854"} {
			for i := 0; i < 2500; i++ {
				raw := fmt.Sprintf("%d.%012d", 80000+i, i*i%1000000)
				pd := dec(raw).Mul(dec(rate))
				want := centeredOrderPrice(pd, sd, half)
				got := historyOrderCenter(pd, num(raw)*num(rate), step, sd, half)
				if got != want {
					t.Fatalf("index changed: %s %s %v: %v != %v", raw, rate, step, got, want)
				}
			}
		}
	}
	if g := orderZoneCenter("81874.9999999999999999", 250); g != 81750 {
		t.Fatal("half-open boundary rounded", g)
	}
}

func TestOrderZoneHistoryCachedAmountsFollowQuantityAndFX(t *testing.T) {
	h := zoneHub(t)
	now := time.Now().UTC()
	zonePrice(t, h, "BTC", "1", "85000", now)
	fd, _ := h.Dataset("fx.usd.kraken")
	bd, _ := h.Dataset(ID("book", "BTC", "Binance", "spot"))
	type want struct{ quantity, rate string }
	expected := map[int64]want{}
	for i, x := range []want{{"1.25", "1"}, {"2.75", "1"}, {"2.75", "1.002"}, {"1.25", "0.9995"}} {
		at := now.Truncate(30 * time.Minute).Add(time.Duration(i-6) * 30 * time.Minute)
		zoneIngest(t, h, fd, Observation{Dataset: fd.ID, ObservedAt: &at, FetchedAt: now, Resolution: 60, Quality: "valid", Payload: Payload{Rates: []Rate{{Quote: "USDT", USD: x.rate}}}})
		zoneIngest(t, h, bd, Observation{Dataset: bd.ID, ObservedAt: &at, FetchedAt: now, Resolution: 300, Quality: "valid", Payload: Payload{Book: &Book{Low: 81000, High: 89000, Bids: []Level{{Price: "82000", Quantity: x.quantity}}, Asks: []Level{{Price: "88000", Quantity: "3"}}}}})
		expected[at.Unix()] = x
	}
	hist, e := h.orderZoneHistory(context.Background(), "BTC", url.Values{"period": {"24h"}}, now)
	if e != nil {
		t.Fatal(e)
	}
	seen := 0
	for _, p := range hist.Points {
		x, ok := expected[p.Time]
		if !ok {
			continue
		}
		for _, c := range p.Cells {
			if c[1].(int) != 0 {
				continue
			}
			seen++
			dollarPrice := multiply("82000", x.rate)
			if c[0].(float64) != orderZoneCenter(dollarPrice, 250) || c[2].(int64) != money(multiply(dollarPrice, x.quantity)) || c[3].(string) != x.quantity {
				t.Fatalf("stale cached value: %v, want %+v", c, x)
			}
		}
	}
	if seen != len(expected) {
		t.Fatalf("observations %d != %d", seen, len(expected))
	}
}

func TestOrderZoneSampledReaderOwnsFramesAndStops(t *testing.T) {
	h := zoneHub(t)
	d, _ := h.Dataset(ID("book", "BTC", "Coinbase", "spot"))
	now := time.Now().UTC().Truncate(5 * time.Minute)
	for i := 0; i < 5; i++ {
		at := now.Add(time.Duration(i-6) * 5 * time.Minute)
		o := Observation{Dataset: d.ID, ObservedAt: &at, FetchedAt: now, Resolution: 300, Quality: "valid", Payload: Payload{Book: &Book{Bids: []Level{{Price: "82000", Quantity: fmt.Sprint(i + 1)}}}}}
		if i == 2 {
			o.Payload.Book = nil
			o.Quality = "missing"
		}
		zoneIngest(t, h, d, o)
	}
	var got []Observation
	if e := h.Store.visitSampled(context.Background(), d, 300, now.Add(-time.Hour), now, 300, func(o Observation) error { got = append(got, o); return nil }); e != nil {
		t.Fatal(e)
	}
	if len(got) != 5 || got[0].Payload.Book.Bids[0].Quantity != "1" || got[2].Payload.Book != nil || got[4].Payload.Book.Bids[0].Quantity != "5" {
		t.Fatal("retained frames were overwritten or missing data carried forward")
	}
	stop := fmt.Errorf("stop after first frame")
	calls := 0
	e := h.Store.visitSampled(context.Background(), d, 300, now.Add(-time.Hour), now, 300, func(o Observation) error { calls++; return stop })
	if e != stop || calls != 1 {
		t.Fatalf("callback after stop: %v / %d", e, calls)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if e = h.Store.visitSampled(ctx, d, 300, now.Add(-time.Hour), now, 300, func(Observation) error { t.Fatal("callback after cancellation"); return nil }); e == nil {
		t.Fatal("cancel ignored")
	}
}
