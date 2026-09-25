package datahub

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestPreparedZonesPreserveFinancialReference(t *testing.T) {
	now := time.Now().UTC()
	registry := map[string]Dataset{}
	books := map[string]Observation{}
	for _, a := range Assets() {
		for i, d := range Registry() {
			if d.Kind != "book" || d.Asset != a {
				continue
			}
			registry[d.ID] = d
			at := now.Add(-time.Duration(i) * time.Second)
			b := &Book{Low: 50, High: 220000}
			for n := 0; n < 120; n++ {
				p := 80000 + n*3
				if a == "ETH" {
					p = 3000 + n
				}
				b.Bids = append(b.Bids, Level{fmt.Sprintf("%d.123456", p-250), fmt.Sprintf("0.%07d", n+1)})
				b.Asks = append(b.Asks, Level{fmt.Sprintf("%d.876543", p), "17.99382301"})
			}
			books[d.ID] = Observation{Dataset: d.ID, ObservedAt: &at, FetchedAt: now, Quality: "valid", Payload: Payload{Book: b}}
		}
		for _, rates := range []map[string]string{{"USD": "1", "USDT": "1.000037"}, {"USD": "1"}} {
			for _, step := range steps(a) {
				got, cg := makeZones(a, step, 80000, books, registry, rates, now, true)
				want, cw := referenceZones(a, step, 80000, books, registry, rates, now, true)
				for i := range want {
					want[i].Evidence = "尚无匹配触及／成交证据"
				}
				if !reflect.DeepEqual(got, want) || !reflect.DeepEqual(cg, cw) {
					t.Fatalf("financial reference mismatch %s step %v", a, step)
				}
			}
		}
		books = map[string]Observation{}
		registry = map[string]Dataset{}
	}
}

func TestBaselineCheckpointMatchesFullHistory(t *testing.T) {
	h, e := Open(Config{Root: t.TempDir(), Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	seedBaselineReplay(t, h, time.Now().UTC().Truncate(time.Hour), 2, 100)
	if e = h.referenceBaselines(context.Background()); e != nil {
		t.Fatal(e)
	}
	want := h.baselines
	// Checkpoint recovery after a canceled attempt must not publish partial data.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if e = h.BuildBaselines(ctx); e == nil {
		t.Fatal("expected cancellation")
	}
	if e = h.BuildBaselines(context.Background()); e != nil {
		t.Fatal(e)
	}
	got := h.baselines
	for k, v := range got {
		v.At = time.Time{}
		got[k] = v
	}
	for k, v := range want {
		v.At = time.Time{}
		want[k] = v
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatal("checkpoint changed baseline cohorts/values/counts")
	}
}

func TestSplitBoardIndependentFiltersSharedVersionScale(t *testing.T) {
	h, e := Open(Config{Root: t.TempDir(), Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	now := time.Now().UTC()
	d, _ := FindDataset("large.btc.coinbase.spot")
	o := orderObservation(d, now, orderFixture(now))
	o.Payload.Large = nil
	for i := 0; i < 267; i++ {
		r := orderFixture(now)
		r.ID = fmt.Sprint(i)
		r.Quantity = fmt.Sprint(i + 1)
		r.Side = "bid"
		if i%2 == 0 {
			r.Side = "ask"
		}
		o.Payload.Large = append(o.Payload.Large, r)
	}
	if _, e = h.Store.Ingest(d, o); e != nil {
		t.Fatal(e)
	}
	q := url.Values{"layout": {"split"}}
	first := boardWire(t, h, q)
	q.Set("version", str(first["version"]))
	seen := map[string]bool{}
	for page := 0; page < 3; page++ {
		q.Set("bid_offset", fmt.Sprint(page*50))
		q.Set("ask_offset", fmt.Sprint(page*50))
		m := boardWire(t, h, q)
		if m["scaleMaxCents"] != first["scaleMaxCents"] {
			t.Fatal("scale changes across page")
		}
		for _, side := range []string{"bid", "ask"} {
			for _, raw := range m[side].(map[string]any)["items"].([]any) {
				r := raw.(map[string]any)
				if seen[str(r["id"])] {
					t.Fatal("duplicate")
				}
				seen[str(r["id"])] = true
			}
		}
	}
	if len(seen) != 267 {
		t.Fatalf("lost rows %d", len(seen))
	}
	q.Set("bid_minUsd", "999999999999")
	q.Set("bid_offset", "0")
	q.Set("ask_offset", "50")
	m := boardWire(t, h, q)
	if m["bid"].(map[string]any)["total"] != float64(0) || m["ask"].(map[string]any)["offset"] != float64(50) {
		t.Fatal("side filter leaked")
	}
	// New absent rows invalidate old facts without changing pinned page membership.
	o.FetchedAt = now.Add(time.Second)
	o.Payload.Large = o.Payload.Large[:1]
	h.Store.Ingest(d, o)
	q.Set("bid_minUsd", "0")
	m = boardWire(t, h, q)
	for _, raw := range m["bid"].(map[string]any)["items"].([]any) {
		r := raw.(map[string]any)
		if r["presenceState"] != "unreturned" || r["valid"] != false {
			t.Fatal("missing list ID remained valid")
		}
	}
	if m["bid"].(map[string]any)["total"] != float64(133) {
		t.Fatal("new list changed pinned membership")
	}
	// An explicit ended fact overrides a formerly active pinned row, not its amount.
	r := o.Payload.Large[0]
	r.RawState = 3
	ended := now.Add(2 * time.Second)
	r.End = &ended
	o.FetchedAt = ended
	o.Payload.Large = []LargeOrder{r}
	h.Store.Ingest(d, o)
	q.Set("ask_offset", "0")
	q.Set("ask_sort", "amount_asc")
	m = boardWire(t, h, q)
	row := m["ask"].(map[string]any)["items"].([]any)[0].(map[string]any)
	if row["presenceState"] != "revoked" || row["valid"] != false {
		t.Fatal("terminal fact not reflected")
	}
	for i := 0; i < 100; i++ {
		if _, e = h.Read(context.Background(), "large-orders", q); e != nil {
			t.Fatal(e)
		}
	}
	if h.Scheduler.quota.Calls != 0 {
		t.Fatal("GET used upstream")
	}
}

func TestNativeLiquidityDoesNotCallDecreasesCancellations(t *testing.T) {
	d, _ := FindDataset("book.btc.binance.spot")
	at := time.Now().UTC().Add(-2 * time.Minute)
	nextAt := at.Add(2 * time.Minute)
	o := Observation{ObservedAt: &at, FetchedAt: at, Revision: "a", Payload: Payload{Book: &Book{Bids: []Level{{"80000", "10"}}, Asks: []Level{{"80100", "5"}}, Low: 70000, High: 90000}}}
	old := nativeLiquidity(o, d, "1")
	n := o
	n.ObservedAt = &nextAt
	n.FetchedAt = nextAt
	n.Revision = "b"
	n.Payload.Book = &Book{Bids: []Level{{"80000", "4"}}, Asks: []Level{{"80100", "5"}}, Low: 70000, High: 90000}
	next := nativeLiquidity(n, d, "1.00001")
	events := compareLiquidity(d, old, next, 80001, true)
	found := false
	for _, e := range events {
		if e.Side == "bid" && e.Step == 100 {
			found = true
			if e.Kind != "decrease" || e.Decrease == nil || *e.Decrease != 60 {
				t.Fatalf("wrong native change %+v", e)
			}
			if e.TradeNote != "" {
				t.Fatal("invented trade")
			}
		}
	}
	if !found {
		t.Fatal("missing change")
	}
	n.Payload.Book.Bids = []Level{{"80100", "2"}}
	next = nativeLiquidity(n, d, "1")
	for _, e := range compareLiquidity(d, old, next, 80001, true) {
		if e.Side == "bid" && e.Step == 100 && e.Kind != "unreturned" {
			t.Fatal("missing level inferred zero/cancel")
		}
	}
	n.ObservedAt = nil
	for _, e := range compareLiquidity(d, old, nativeLiquidity(n, d, "1"), 80001, true) {
		if e.Kind != "uncomparable" || e.Decrease != nil {
			t.Fatal("unknown time inferred change")
		}
	}
	n.ObservedAt = &at
	for _, e := range compareLiquidity(d, old, nativeLiquidity(n, d, "1"), 80001, true) {
		if e.Kind != "uncomparable" {
			t.Fatal("same timestamp revision counted new flow")
		}
	}
}

func TestLiquidityPersistenceRevisionsAndIsolation(t *testing.T) {
	h, e := Open(Config{Root: t.TempDir(), Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	ctx := context.Background()
	now := time.Now().UTC()
	d, _ := FindDataset("book.btc.coinbase.spot")
	pd, _ := FindDataset("price.btc.binance.spot")
	fx, _ := FindDataset("fx.usd.kraken")
	for _, v := range []struct {
		d Dataset
		p Payload
	}{{pd, Payload{Price: &Price{"80000", "USDT"}}}, {fx, Payload{Rates: []Rate{{"USDT", "1"}}}}} {
		h.Store.Ingest(v.d, Observation{Dataset: v.d.ID, ObservedAt: &now, FetchedAt: now, Quality: "valid", Payload: v.p})
	}
	old := now.Add(-2 * time.Minute)
	o := Observation{Dataset: d.ID, Source: d.Source, ObservedAt: &old, FetchedAt: old, Quality: "valid", Payload: Payload{Book: &Book{Bids: []Level{{"80000", "10"}}, Asks: []Level{{"80100", "10"}}, Low: 70000, High: 90000}}}
	h.Store.Ingest(d, o)
	if e = h.SampleLiquidity(ctx, now); e != nil {
		t.Fatal(e)
	}
	o.ObservedAt = &now
	o.FetchedAt = now
	o.Payload.Book.Bids[0].Quantity = "2"
	h.Store.Ingest(d, o)
	if e = h.SampleLiquidity(ctx, now); e != nil {
		t.Fatal(e)
	}
	rows, e := h.LiquidityEvents(ctx, "BTC", 100, old.Add(-time.Second), now.Add(time.Second), 100)
	if e != nil || len(rows) == 0 {
		t.Fatal("no persisted events", e)
	}
	before, _ := json.Marshal(rows)
	h.SampleLiquidity(ctx, now)
	rows, _ = h.LiquidityEvents(ctx, "BTC", 100, old.Add(-time.Second), now.Add(time.Second), 100)
	after, _ := json.Marshal(rows)
	if string(before) != string(after) {
		t.Fatal("same revision generated repeated facts")
	}
	// Same timestamp correction retracts former reductions, keeps unknown state.
	o.Payload.Book.Bids[0].Quantity = "9"
	h.Store.Ingest(d, o)
	h.SampleLiquidity(ctx, now)
	rows, _ = h.LiquidityEvents(ctx, "BTC", 100, old.Add(-time.Second), now.Add(time.Second), 100)
	if len(rows) != 0 {
		t.Fatal("corrected reductions remained")
	}
	wallBefore := h.rawFrame("BTC", 100, now)
	ld, _ := FindDataset("large.btc.coinbase.spot")
	large := orderFixture(now)
	large.Quantity = "999999"
	h.Store.Ingest(ld, orderObservation(ld, now, large))
	wallAfter := h.rawFrame("BTC", 100, now)
	if !reflect.DeepEqual(wallBefore, wallAfter) {
		t.Fatal("large order changed ordinary wall")
	}
	var bytes int64
	h.Store.db.QueryRow("SELECT bytes FROM order_storage WHERE id=1").Scan(&bytes)
	if bytes < 0 {
		t.Fatal("budget counter underflow")
	}
}

func TestSplitBoardMissingFXExpiryAndEmpty(t *testing.T) {
	h, e := Open(Config{Root: t.TempDir(), Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	q := url.Values{"layout": {"split"}}
	empty := boardWire(t, h, q)
	if empty["bid"].(map[string]any)["validCents"] != nil {
		t.Fatal("unknown coverage became zero")
	}
	now := time.Now().UTC()
	d, _ := FindDataset("large.btc.binance.spot")
	h.Store.Ingest(d, orderObservation(d, now, orderFixture(now)))
	unknown := boardWire(t, h, q)
	bid := unknown["bid"].(map[string]any)
	if bid["snapshotCents"] != nil || bid["validCents"] != nil {
		t.Fatal("missing FX became dollars")
	}
	q.Set("version", str(unknown["version"]))
	q.Set("bid_minUsd", "1")
	filtered := boardWire(t, h, q)["bid"].(map[string]any)
	if filtered["excludedFX"] != float64(1) || filtered["total"] != float64(0) {
		t.Fatal("FX filter exclusions missing")
	}
	fd, _ := FindDataset("fx.usd.kraken")
	h.Store.Ingest(fd, Observation{Dataset: fd.ID, FetchedAt: now, ObservedAt: &now, Quality: "valid", Payload: Payload{Rates: []Rate{{"USDT", "1.002"}}}})
	q.Del("version")
	q.Del("bid_minUsd")
	v := boardWire(t, h, q)
	row := v["bid"].(map[string]any)["items"].([]any)[0].(map[string]any)
	amount := row["usdCents"]
	if amount == nil {
		t.Fatal("valid FX missing")
	}
	if e = h.reconcileOrderRows(context.Background(), "BTC", []any{row}, now.Add(31*time.Second)); e != nil {
		t.Fatal(e)
	}
	if row["valid"] != false || row["usdCents"] != amount {
		t.Fatal("FX expiry mutated pinned amount or stayed valid")
	}
	q.Set("version", str(v["version"]))
	h.viewMu.Lock()
	h.views = map[string]cachedView{}
	h.viewBytes = 0
	h.viewMu.Unlock()
	if boardWire(t, h, q)["snapshotExpired"] != true {
		t.Fatal("evicted snapshot silently switched")
	}
}

func TestLiquidityLateFootprintBackupBudgetAndGET(t *testing.T) {
	h, e := Open(Config{Root: t.TempDir(), Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	ctx := context.Background()
	now := time.Now().UTC()
	from := now.Add(-3 * time.Minute)
	at := now.Add(-time.Minute)
	event := LiquidityEvent{Key: "test-event", Dataset: "book.btc.binance.spot", Venue: "Binance", Side: "bid", Quote: "USDT", Low: 80000, Step: 100, From: &from, At: at, KnownAt: at, Kind: "decrease", Before: ptr("10"), After: ptr("4")}
	b, _ := json.Marshal(event)
	if _, e = h.Store.db.Exec("INSERT INTO liquidity_events VALUES(?,?,?,?,?)", event.Key, "BTC", event.Dataset, at.Unix(), b); e != nil {
		t.Fatal(e)
	}
	fd, _ := FindDataset("footprint.btc.binance.spot")
	footAt := from.Truncate(5 * time.Minute)
	o := Observation{Dataset: fd.ID, Source: fd.Source, ObservedAt: &footAt, FetchedAt: now, Quality: "valid", Resolution: 300, Payload: Payload{Foot: []Foot{{Low: "80000", High: "80100", SellBase: "1", BuyBase: "2"}}}}
	if _, e = h.Store.Ingest(fd, o); e != nil {
		t.Fatal(e)
	}
	if e = h.refreshLiquidityEvidence(ctx, now); e != nil {
		t.Fatal(e)
	}
	events, e := h.LiquidityEvents(ctx, "BTC", 100, from, now, 100)
	if e != nil || len(events) != 1 || !strings.HasPrefix(events[0].TradeNote, "此后补到") || !events[0].KnownAt.Equal(at) {
		t.Fatalf("late evidence incorrect: %v %v", events, e)
	}
	// A second venue has no matching footprint and must not inherit Binance evidence.
	event.Key = "coinbase"
	event.Venue = "Coinbase"
	event.Dataset = "book.btc.coinbase.spot"
	b, _ = json.Marshal(event)
	h.Store.db.Exec("INSERT INTO liquidity_events VALUES(?,?,?,?,?)", event.Key, "BTC", event.Dataset, at.Unix(), b)
	h.Store.db.Exec("DELETE FROM state WHERE key='liquidity.foot/BTC'")
	if e = h.refreshLiquidityEvidence(ctx, now); e != nil {
		t.Fatal(e)
	}
	events, _ = h.LiquidityEvents(ctx, "BTC", 100, from, now, 100)
	for _, v := range events {
		if v.Venue == "Coinbase" && v.TradeNote != "" {
			t.Fatal("cross-venue invented evidence")
		}
	}
	before, _ := json.Marshal(events)
	for i := 0; i < 100; i++ {
		if _, e = h.LiquidityEvents(ctx, "BTC", 100, from, now, 100); e != nil {
			t.Fatal(e)
		}
	}
	events, _ = h.LiquidityEvents(ctx, "BTC", 100, from, now, 100)
	after, _ := json.Marshal(events)
	if string(before) != string(after) || h.Scheduler.quota.Calls != 0 {
		t.Fatal("GET mutated facts or fetched upstream")
	}
	dest := filepath.Join(t.TempDir(), "hub.sqlite")
	if e = BackupFile(ctx, filepath.Join(h.Store.Root(), "hub.sqlite"), dest); e != nil {
		t.Fatal(e)
	}
	restored, e := OpenWarehouse(filepath.Dir(dest))
	if e != nil {
		t.Fatal(e)
	}
	defer restored.Close()
	var count int
	restored.db.QueryRow("SELECT count(*) FROM liquidity_events").Scan(&count)
	if count != 2 {
		t.Fatal("event backup lost observations")
	}
	var bytes int64
	restored.db.QueryRow("SELECT bytes FROM order_storage WHERE id=1").Scan(&bytes)
	if bytes <= 0 {
		t.Fatal("liquidity bytes omitted from shared budget")
	}
	// No extra evidence writes may exceed the history budget.
	h.Store.db.Exec("UPDATE order_storage SET bytes=? WHERE id=1", OrderHistoryLimit)
	h.Store.db.Exec("UPDATE liquidity_events SET payload=? WHERE k=?", b, event.Key)
	event.Venue = "Binance"
	b, _ = json.Marshal(event)
	h.Store.db.Exec("UPDATE liquidity_events SET payload=? WHERE k=?", b, event.Key)
	h.Store.db.Exec("UPDATE order_storage SET bytes=? WHERE id=1", OrderHistoryLimit)
	h.Store.db.Exec("DELETE FROM state WHERE key='liquidity.foot/BTC'")
	if e = h.refreshLiquidityEvidence(ctx, now); e != nil {
		t.Fatal(e)
	}
	h.Store.db.QueryRow("SELECT bytes FROM order_storage WHERE id=1").Scan(&bytes)
	if bytes > OrderHistoryLimit {
		t.Fatal("late evidence exceeded budget")
	}
	var gap time.Time
	if !h.Store.LoadState("liquidityGapAt", &gap) || gap.IsZero() {
		t.Fatal("budget gap unreported")
	}
}
