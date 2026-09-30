package datahub

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"
)

func liquidationFixture(asset string, at time.Time) (Dataset, Observation) {
	d, _ := FindDataset(ID("map", asset, "", "futures"))
	mid, step := 80000.0, 250.0
	if asset == "ETH" {
		mid, step = 3000, 10
	}
	m := &Model{Contract: LiquidationContract, CoverageComplete: true, Model: "CoinGlass aggregated-map", Range: "1d", ReferencePrice: mid, Unit: "relative", Bins: []ModelBin{}}
	for i := -8; i <= 8; i++ {
		p := mid + float64(i)*step
		n := fmt.Sprint(10000000 + (i+8)*1000000)
		for _, v := range []string{"Binance", "OKX", "Bybit"} {
			m.Bins = append(m.Bins, ModelBin{Price: p, NativePrice: fmt.Sprint(p), Strength: num(n), RawStrength: n, Venue: v, Instrument: asset + "USDT", Quote: "USDT"})
		}
	}
	return d, Observation{Dataset: d.ID, Source: d.Source, FetchedAt: at, Quality: "valid", Payload: Payload{Model: m}, Revision: "fixture"}
}
func TestLiquidationContractPrecisionAndUnits(t *testing.T) {
	d, _ := FindDataset(ID("map", "BTC", "", "futures"))
	raw := []byte(`{"code":"0","data":{"last_price":83542,"data":[{"instrument":{"exName":"Binance","instrumentId":"BTCUSDT","quoteAsset":"USDT"},"liqMapV2":{"a":[[82501,"312480691.630001",null,null],[82501,"312480691.630001",null,null]]}},{"instrument":{"exName":"OKX","instrumentId":"BTC-USDT-SWAP","quoteAsset":"USDT"},"liqMapV2":{"a":[[82503,"0.000009",null,null]]}}]}}`)
	obs, e := Normalize(d, raw, time.Now())
	if e != nil {
		t.Fatal(e)
	}
	if len(obs[0].Payload.Model.Bins) != 2 {
		t.Fatal("duplicate contribution counted")
	}
	s, e := makeLiquidationMap(d, obs[0], time.Now())
	if e != nil {
		t.Fatal(e)
	}
	if !s.Complete || s.Quote != "USDT" || len(s.Zones) != 1 || s.Zones[0].Strength != "312480691.63001" || s.Zones[0].Relative != 100 {
		t.Fatalf("bad exact grouping: %+v", s)
	}
	if s.Zones[0].Side != "long" || s.Zones[0].WhaleCents != nil || s.Zones[0].LowUSD != nil {
		t.Fatal("unit/direction contamination")
	}
	for _, replacement := range []string{`"invalid"`, `null`, `-1`} {
		bad := strings.Replace(string(raw), `"0.000009"`, replacement, 1)
		if _, e = Normalize(d, []byte(bad), time.Now()); e == nil {
			t.Fatalf("accepted %s", replacement)
		}
	}
	conflict := strings.Replace(string(raw), `[82501,"312480691.630001",null,null]]`, `[82501,"1",null,null]]`, 1)
	if _, e = Normalize(d, []byte(conflict), time.Now()); e == nil {
		t.Fatal("conflicting duplicate accepted")
	}
	obs[0].Payload.Model.Bins[1].Quote = "USD"
	s, e = makeLiquidationMap(d, obs[0], time.Now())
	if e != nil || s.Complete || len(s.Zones) != 2 {
		t.Fatal("mixed native quotes merged")
	}
}
func TestLiquidationLifecycleContinuityAndFrozenSide(t *testing.T) {
	at := time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC)
	d, o := liquidationFixture("BTC", at)
	s, _ := makeLiquidationMap(d, o, at)
	current := s.Zones[12]
	z := evolveLiquidationZone(nil, current, s)
	if z.State != "new" || z.Side != "short" {
		t.Fatal(z)
	}
	id := z.ID
	for _, m := range []int{15, 30} {
		s.Available = at.Add(time.Duration(m) * time.Minute)
		z = evolveLiquidationZone(&z, current, s)
	}
	if z.Samples != 3 || z.State != "persistent" {
		t.Fatal(z)
	}
	repeat := evolveLiquidationZone(&z, current, s)
	if repeat.Samples != 3 {
		t.Fatal("duplicate sampled")
	}
	s.Available = at.Add(45 * time.Minute)
	current.Side = "long"
	current.Strength = dec(z.Strength).Mul(dec("1.2")).String()
	z = evolveLiquidationZone(&z, current, s)
	if z.Side != "short" || z.ID != id || z.Trend != "stronger" {
		t.Fatal("direction flipped or change lost", z)
	}
	s.Available = at.Add(90 * time.Minute)
	z = evolveLiquidationZone(&z, current, s)
	if z.Samples != 1 || z.State != "new" {
		t.Fatal("gap didn't reset persistence")
	}
	s.Available = at.Add(105 * time.Minute)
	s.Comparable = "different"
	z = evolveLiquidationZone(&z, current, s)
	if z.Change != nil || z.Samples != 1 {
		t.Fatal("incomparable growth")
	}
	z.Strength = "0"
	z.Comparable = s.Comparable
	s.Available = s.Available.Add(15 * time.Minute)
	z = evolveLiquidationZone(&z, current, s)
	if z.Change != nil || z.Trend != "from_zero" {
		t.Fatal("zero denominator growth")
	}
}
func TestLiquidationCandleTimingAndAmbiguity(t *testing.T) {
	at := time.Date(2026, 9, 30, 1, 2, 0, 0, time.UTC)
	base := LiquidationZone{Low: 100, High: 110, Side: "short", First: at, State: "new"}
	z := base
	applyLiquidationCandle(&z, at.Truncate(5*time.Minute), Candle{Open: 95, High: 120, Low: 90, Close: 115})
	if z.Touch != nil {
		t.Fatal("pre-visibility touch")
	}
	first := at.Truncate(5 * time.Minute).Add(5 * time.Minute)
	applyLiquidationCandle(&z, first, Candle{Open: 99, High: 105, Low: 99, Close: 103})
	if z.Touch == nil || z.State != "touched" {
		t.Fatal(z)
	}
	applyLiquidationCandle(&z, first.Add(5*time.Minute), Candle{Open: 103, High: 112, Low: 102, Close: 111})
	applyLiquidationCandle(&z, first.Add(10*time.Minute), Candle{Open: 111, High: 113, Low: 110, Close: 112})
	if z.State != "crossed" {
		t.Fatal(z)
	}
	applyLiquidationCandle(&z, first.Add(15*time.Minute), Candle{Open: 112, High: 113, Low: 97, Close: 99})
	applyLiquidationCandle(&z, first.Add(20*time.Minute), Candle{Open: 99, High: 100, Low: 97, Close: 98})
	if z.State != "reclaimed" {
		t.Fatal(z)
	}
	z = base
	applyLiquidationCandle(&z, first, Candle{Open: 99, High: 120, Low: 95, Close: 115})
	if z.Uncertain == "" {
		t.Fatal("OHLC ambiguity erased")
	}
	z = base
	z.PreviousClose = 90
	applyLiquidationCandle(&z, first, Candle{Open: 115, High: 120, Low: 114, Close: 118})
	if z.Uncertain == "" || z.Touch != nil {
		t.Fatal("gap interpreted as confirmed touch")
	}
	z = base
	applyLiquidationCandle(&z, first, Candle{Open: 90, High: 95, Low: 89, Close: 94})
	applyLiquidationCandle(&z, first.Add(15*time.Minute), Candle{Open: 94, High: 95, Low: 89, Close: 94})
	if z.Uncertain == "" {
		t.Fatal("missing candles ignored")
	}
}
func TestLiquidationRestartHistoryMissingAndReadOnly(t *testing.T) {
	h, e := Open(Config{Root: t.TempDir(), Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	ctx := context.Background()
	now := time.Now().UTC()
	d, o := liquidationFixture("BTC", now)
	var s LiquidationMapSnapshot
	for _, m := range []int{-30, -15, 0} {
		s, e = makeLiquidationMap(d, o, now.Add(time.Duration(m)*time.Minute))
		if e != nil {
			t.Fatal(e)
		}
		s.Fetched = s.Available
		if e = h.processLiquidationMap(ctx, s); e != nil {
			t.Fatal(e)
		}
	}
	if e = h.Store.liquidationLoad(ctx, "state", d.ID, &s); e != nil {
		t.Fatal(e)
	}
	z := s.Zones[8]
	if z.Samples != 3 {
		t.Fatal(z)
	}
	// Removing an interior bucket preserves returned coverage and range.
	for _, m := range []int{15, 30} {
		next, _ := makeLiquidationMap(d, o, now.Add(time.Duration(m)*time.Minute))
		next.Zones = append(next.Zones[:8], next.Zones[9:]...)
		if e = h.processLiquidationMap(ctx, next); e != nil {
			t.Fatal(e)
		}
	}
	if e = h.Store.liquidationLoad(ctx, "state", d.ID, &s); e != nil {
		t.Fatal(e)
	}
	found := false
	for _, v := range s.Zones {
		if v.ID == z.ID {
			found = true
			if v.State != "disappeared" || v.Missing != 2 {
				t.Fatal(v)
			}
		}
	}
	if !found {
		t.Fatal("missing region lost")
	}
	restored, e := OpenWarehouse(h.Store.Root())
	if e != nil {
		t.Fatal(e)
	}
	defer restored.Close()
	var state LiquidationMapSnapshot
	if e = restored.liquidationLoad(ctx, "state", d.ID, &state); e != nil || state.Zones[0].ID != s.Zones[0].ID {
		t.Fatal("restart reset", e)
	}
	var before, after int
	h.Store.research.QueryRow("SELECT count(*) FROM lz_records").Scan(&before)
	q := url.Values{"asset": {"BTC"}, "zoneId": {z.ID}, "limit": {"2"}}
	result, e := h.liquidationHistory(ctx, "BTC", q, now.Add(time.Hour))
	if e != nil {
		t.Fatal(e)
	}
	r := result.(map[string]any)
	if len(r["items"].([]json.RawMessage)) != 2 || r["nextCursor"] == "" {
		t.Fatal("pagination failed", r)
	}
	q.Set("cursor", r["nextCursor"].(string))
	if _, e = h.liquidationHistory(ctx, "BTC", q, now.Add(time.Hour)); e != nil {
		t.Fatal(e)
	}
	for _, path := range []string{"liquidations", "liquidation-study"} {
		if _, e = h.Read(ctx, path, url.Values{"asset": {"BTC"}}); e != nil {
			t.Fatal(e)
		}
	}
	h.Store.research.QueryRow("SELECT count(*) FROM lz_records").Scan(&after)
	if before != after {
		t.Fatal("GET mutated research/upstream")
	}
}
func TestLiquidationStudyControlsAndFrozenOutcomes(t *testing.T) {
	at := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	z := LiquidationZone{ID: "main", Side: "short", Low: 110, High: 120, Relative: 90, Samples: 3, Continuous: at.Add(-30 * time.Minute), Last: at, First: at.Add(-30 * time.Minute)}
	if c := chooseLiquidationZone([]LiquidationZone{z}, "short", 115); c != nil {
		t.Fatal("selected a region already entered by live price")
	}
	weak := z
	weak.ID = "control"
	weak.Relative = 20
	weak.Low = 111
	weak.High = 121
	far := weak
	far.ID = "far"
	far.Low = 150
	far.High = 160
	if c := matchLiquidationControl([]LiquidationZone{z, far, weak}, z, 100, 10); c == nil || c.ID != "control" {
		t.Fatal("control not selected from same ATR bin")
	}
	if c := matchLiquidationControl([]LiquidationZone{z, far}, z, 100, 10); c != nil {
		t.Fatal("forced a match")
	}
	z.Touch = &at
	if c := chooseLiquidationZone([]LiquidationZone{z}, "short", 100); c != nil {
		t.Fatal("selected touched zone")
	}
	z.Touch = nil
	ev := LiquidationStudyEvent{Start: at, Price: 100, Zone: z}
	c := map[int64]Candle{}
	for i := 0; i < 12; i++ {
		c[at.Add(time.Duration(i)*5*time.Minute).Unix()] = Candle{Open: 100, High: 105, Low: 98, Close: 101}
	}
	out := evaluateLiquidationOutcome(ev, z, 1, c)
	if out.Hit == nil || *out.Hit || out.MFE == nil || *out.MFE != 5 || *out.MAE != 2 {
		t.Fatal(out)
	}
	delete(c, at.Unix())
	out = evaluateLiquidationOutcome(ev, z, 1, c)
	if out.State != "incomplete" || out.Hit != nil {
		t.Fatal("gap treated as miss", out)
	}
	c[at.Unix()] = Candle{Open: 100, High: 130, Low: 95, Close: 125}
	out = evaluateLiquidationOutcome(ev, z, 1, c)
	if out.State != "uncertain" {
		t.Fatal("ambiguous sample counted", out)
	}
	ci := liquidationWilson(15, 30)
	if ci[0] >= .5 || ci[1] <= .5 {
		t.Fatal(ci)
	}
}
func TestLiquidationBudgetFailurePreservesMarketAndOrigin(t *testing.T) {
	w := testStore(t)
	ctx := context.Background()
	var origin time.Time
	if e := w.liquidationLoad(ctx, "origin", LiquidationRules, &origin); e != nil {
		t.Fatal(e)
	}
	if e := w.liquidationPut(ctx, "state", "capacity-checkpoint", "BTC", origin, "unchanged"); e != nil {
		t.Fatal(e)
	}
	_, e := w.research.Exec("UPDATE lz_budget SET used=?", liquidationBudget)
	if e != nil {
		t.Fatal(e)
	}
	d, o := liquidationFixture("BTC", time.Now().UTC())
	if _, e = w.Ingest(d, o); e != nil {
		t.Fatal("research failure stopped market", e)
	}
	if _, ok := w.Latest(d.ID); !ok {
		t.Fatal("map snapshot lost")
	}
	var gap liquidationGap
	if !w.LoadState("liquidation/gap", &gap) || !gap.Paused {
		t.Fatal("gap not recorded")
	}
	var after time.Time
	if e = w.liquidationLoad(ctx, "origin", LiquidationRules, &after); e != nil || !origin.Equal(after) {
		t.Fatal("origin changed")
	}
	// Existing checkpoints do not consume a second copy of their allocation.
	if e = w.liquidationPut(ctx, "state", "capacity-checkpoint", "BTC", origin, "unchanged"); e != nil {
		t.Fatal("full budget rejected an existing equal-size checkpoint", e)
	}
	if e = w.liquidationPut(ctx, "state", "capacity-checkpoint", "BTC", origin, strings.Repeat("0123456789abcdefghijklmnopqrstuvwxyz", 20)); e == nil {
		t.Fatal("checkpoint growth bypassed the full sub-budget")
	}
	restored, e := OpenWarehouse(w.Root())
	if e != nil {
		t.Fatal("full liquidation budget prevented market service startup", e)
	}
	defer restored.Close()
	if e = restored.liquidationLoad(ctx, "origin", LiquidationRules, &after); e != nil || !origin.Equal(after) {
		t.Fatal("full-budget restart reset the observation origin", e)
	}
	if _, ok := restored.Latest(d.ID); !ok {
		t.Fatal("full-budget restart lost the original market snapshot")
	}
}

func TestLiquidationWhalesAllRowsAndStaleFX(t *testing.T) {
	h, e := Open(Config{Root: t.TempDir(), Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	now := time.Now().UTC()
	ctx := context.Background()
	var wd Dataset
	for _, d := range Registry() {
		if d.Kind == "whales" {
			wd = d
		}
	}
	rows := []Whale{}
	liq := "80500"
	for i := 0; i < 125; i++ {
		rows = append(rows, Whale{Address: fmt.Sprint(i), Asset: "BTC", Size: "-1", USD: "1000", Liquidation: &liq, At: now})
	}
	rows = append(rows, Whale{Address: "null", Asset: "BTC", Size: "-1", USD: "1000", At: now}, Whale{Address: "stale", Asset: "BTC", Size: "-1", USD: "1000", Liquidation: &liq, At: now.Add(-9 * time.Minute)})
	if _, e = h.Store.Ingest(wd, Observation{Dataset: wd.ID, Source: wd.Source, ObservedAt: &now, FetchedAt: now, Quality: "valid", Payload: Payload{Whales: rows}}); e != nil {
		t.Fatal(e)
	}
	zones := []LiquidationZone{{Side: "short", LowUSD: flowPtr(80000.0), HighUSD: flowPtr(81000.0)}}
	c := h.liquidationWhales(zones, "BTC", now)
	if c.Covered == nil || *c.Covered != 125 || *c.ExcludedNull != 1 || *c.ExcludedStale != 1 || zones[0].WhaleCents == nil || *zones[0].WhaleCents != 12500000 {
		t.Fatalf("list truncation/null accounting %+v %+v", c, zones)
	}
	zones = []LiquidationZone{{Side: "short"}}
	h.liquidationWhales(zones, "BTC", now)
	if zones[0].WhaleCents != nil {
		t.Fatal("missing FX became zero")
	}
	pd, _ := h.Dataset(ID("price", "BTC", "Binance", "spot"))
	h.Store.Ingest(pd, Observation{Dataset: pd.ID, ObservedAt: &now, FetchedAt: now, Quality: "valid", Payload: Payload{Price: &Price{"80000", "USDT"}}})
	fx, _ := h.Dataset("fx.usd.kraken")
	old := now.Add(-time.Minute)
	h.Store.Ingest(fx, Observation{Dataset: fx.ID, ObservedAt: &old, FetchedAt: old, Quality: "valid", Payload: Payload{Rates: []Rate{{"USDT", "1"}}}})
	ref := h.liquidationReference(ctx, "BTC", "USDT", now)
	if ref.Native == nil || ref.USD != nil || ref.FXValid {
		t.Fatal("stale FX dollar peg", ref)
	}
	ref = h.liquidationReference(ctx, "BTC", "USDT", now.Add(time.Minute))
	if ref.Native != nil {
		t.Fatal("stale price distance reference")
	}
}
func TestLiquidationRealizedCompleteWindowsAndUnknown(t *testing.T) {
	h, e := Open(Config{Root: t.TempDir(), Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	now := time.Now().UTC().Truncate(5 * time.Minute)
	for _, asset := range Assets() {
		d, _ := h.Dataset(ID("liquidations", asset, "", "futures"))
		for i := 0; i < 60; i++ {
			at := now.Add(-time.Duration(60-i) * time.Minute)
			_, e = h.Store.Ingest(d, Observation{Dataset: d.ID, ObservedAt: &at, FetchedAt: now, Quality: "valid", Resolution: 60, Payload: Payload{Liquidation: &Liquidation{Long: "1000.01", Short: "2000.02"}}})
			if e != nil {
				t.Fatal(e)
			}
		}
		window, e := h.liquidationRealized(context.Background(), asset, now)
		if e != nil || window.Long == nil || *window.Long != 6000060 || *window.Short != 12000120 || window.Coverage != 1 {
			t.Fatalf("%s: %+v %v", asset, window, e)
		}
		window, e = h.liquidationRealized(context.Background(), asset, now.Add(5*time.Minute))
		if e != nil || window.Long != nil || window.Coverage >= 1 {
			t.Fatal("partial window became complete", window, e)
		}
	}
}
func TestLiquidationOutcomeDeadlineRejectsBackfillAndFreezes(t *testing.T) {
	h, e := Open(Config{Root: t.TempDir(), Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(5 * time.Minute)
	start := now.Add(-2 * time.Hour)
	d, _ := h.Dataset(ID("candles", "BTC", "Binance", "spot"))
	for i := 0; i < 12; i++ {
		at := start.Add(time.Duration(i) * 5 * time.Minute)
		fetched := at.Add(6 * time.Minute)
		if i == 0 {
			fetched = now
		}
		_, e = h.Store.Ingest(d, Observation{Dataset: d.ID, ObservedAt: &at, FetchedAt: fetched, Quality: "valid", Resolution: 300, Payload: Payload{Candle: &Candle{Open: 100, High: 105, Low: 95, Close: 100}}})
		if e != nil {
			t.Fatal(e)
		}
	}
	c, e := h.liquidationCandles(ctx, "BTC", start, start.Add(time.Hour), start.Add(75*time.Minute))
	if e != nil || len(c) != 11 {
		t.Fatal("late backfill admitted", len(c), e)
	}
	event := LiquidationStudyEvent{ID: "frozen", Start: start, Selected: start.Add(-time.Minute), Side: "short", Price: 100, Zone: LiquidationZone{Side: "short", Low: 110, High: 120}, Outcomes: map[string]LiquidationOutcome{"1": {State: "observing"}, "4": {State: "observing"}}}
	if e = h.Store.liquidationPut(ctx, "event", event.ID, "BTC", event.Selected, event); e != nil {
		t.Fatal(e)
	}
	if e = h.advanceLiquidationStudy(ctx, now); e != nil {
		t.Fatal(e)
	}
	var saved LiquidationStudyEvent
	h.Store.liquidationLoad(ctx, "event", event.ID, &saved)
	if saved.Outcomes["1"].State != "incomplete" {
		t.Fatal(saved)
	}
	// A later fact revision cannot rerun an already frozen outcome.
	saved.Outcomes["1"] = LiquidationOutcome{State: "complete", Hit: flowPtr(false)}
	h.Store.liquidationPut(ctx, "event", event.ID, "BTC", event.Selected, saved)
	if e = h.advanceLiquidationStudy(ctx, now.Add(time.Minute)); e != nil {
		t.Fatal(e)
	}
	h.Store.liquidationLoad(ctx, "event", event.ID, &saved)
	if saved.Outcomes["1"].Hit == nil || *saved.Outcomes["1"].Hit {
		t.Fatal("frozen outcome rewritten")
	}
}

func liquidationReplayFixture(asset string, at time.Time) (Dataset, Observation) {
	d, o := liquidationFixture(asset, at)
	m := o.Payload.Model
	m.Bins = nil
	step := liquidationStep(asset)
	for i := -160; i <= 160; i++ {
		p := m.ReferencePrice + float64(i)*step
		for _, venue := range []string{"Binance", "OKX", "Bybit"} {
			n := fmt.Sprint(1000000 + (i+160)*12345)
			m.Bins = append(m.Bins, ModelBin{Price: p, NativePrice: fmt.Sprint(p), Strength: num(n), RawStrength: n, Venue: venue, Instrument: asset + "USDT", Quote: "USDT"})
		}
	}
	return d, o
}
func TestLiquidationCompressedHistoryBudget(t *testing.T) {
	now := time.Now().UTC()
	d, o := liquidationReplayFixture("BTC", now)
	s, e := makeLiquidationMap(d, o, now)
	if e != nil {
		t.Fatal(e)
	}
	for i := range s.Zones {
		s.Zones[i] = evolveLiquidationZone(nil, s.Zones[i], s)
		s.Zones[i].Contributions = nil
	}
	b, e := liquidationJSON(compactLiquidationSnapshot(s))
	if e != nil {
		t.Fatal(e)
	}
	decoded, e := decodeLiquidationHistory(b)
	if e != nil || decoded[0].ID != s.Zones[0].ID || decoded[0].Strength != s.Zones[0].Strength {
		t.Fatal("compact identity or precision lost", e)
	}
	projected := int64(len(b)+256) * 96 * 30 * 2
	if projected > liquidationBudget-8<<20 {
		t.Fatalf("30-day BTC/ETH compressed history exceeds sub-budget: %d", projected)
	}
	t.Logf("321-zone snapshot=%d bytes; 30-day two-asset history projection=%.2fMiB", len(b), float64(projected)/(1<<20))
}
func TestLiquidationNeutralAndEndedZonesDoNotBlockCursor(t *testing.T) {
	h, e := Open(Config{Root: t.TempDir(), Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(5 * time.Minute)
	d, o := liquidationFixture("BTC", now)
	s, _ := makeLiquidationMap(d, o, now)
	s.Zones = []LiquidationZone{{ID: "neutral", Side: "neutral", First: now.Add(-24 * time.Hour)}, {ID: "ended", Side: "short", Missing: 2, First: now.Add(-24 * time.Hour)}, {ID: "active", Side: "short", Low: 81000, High: 81250, First: now.Add(-time.Hour), LastBar: now.Add(-10 * time.Minute)}}
	if e = h.Store.liquidationPut(ctx, "state", d.ID, "BTC", now, s); e != nil {
		t.Fatal(e)
	}
	cd, _ := h.Dataset(ID("candles", "BTC", "Binance", "spot"))
	at := now.Add(-5 * time.Minute)
	_, e = h.Store.Ingest(cd, Observation{Dataset: cd.ID, ObservedAt: &at, FetchedAt: now, Resolution: 300, Quality: "valid", Payload: Payload{Candle: &Candle{Open: 80000, High: 80500, Low: 79500, Close: 80000}}})
	if e != nil {
		t.Fatal(e)
	}
	if e = h.advanceLiquidationStates(ctx, now); e != nil {
		t.Fatal(e)
	}
	h.Store.liquidationLoad(ctx, "state", d.ID, &s)
	if !s.Zones[2].LastBar.Equal(at) {
		t.Fatal("inactive zone blocked active cursor", s.Zones[2].LastBar)
	}
}

func TestLiquidationForwardSelectionAsOfAndEpisodeDedup(t *testing.T) {
	h, e := Open(Config{Root: t.TempDir(), Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(5 * time.Minute)
	cd, _ := h.Dataset(ID("candles", "BTC", "Binance", "spot"))
	for i := 0; i <= 180; i++ {
		at := now.Add(-time.Duration(181-i) * 5 * time.Minute)
		fetched := at.Add(6 * time.Minute)
		_, e = h.Store.Ingest(cd, Observation{Dataset: cd.ID, ObservedAt: &at, FetchedAt: fetched, Resolution: 300, Quality: "valid", Payload: Payload{Candle: &Candle{Open: 80000, High: 80100, Low: 79900, Close: 80000}}})
		if e != nil {
			t.Fatal(e)
		}
	}
	// The most recent completed candle has to be actually available at selection.
	at := now.Add(-5 * time.Minute)
	h.Store.Ingest(cd, Observation{Dataset: cd.ID, ObservedAt: &at, FetchedAt: now, Resolution: 300, Quality: "valid", Payload: Payload{Candle: &Candle{Open: 80000, High: 80100, Low: 79900, Close: 80000, Volume: 1}}})
	d, o := liquidationFixture("BTC", now)
	s, _ := makeLiquidationMap(d, o, now)
	s.MarketPrice = flowPtr(80000.0)
	for i := range s.Zones {
		z := &s.Zones[i]
		z.ID = fmt.Sprint(i)
		z.Samples = 3
		z.First = now.Add(-30 * time.Minute)
		z.Continuous = z.First
		z.Last = now
		z.State = "persistent"
	}
	if e = h.selectLiquidationStudy(ctx, s); e != nil {
		t.Fatal(e)
	}
	var count int
	h.Store.research.QueryRow("SELECT count(*) FROM lz_records WHERE kind='event'").Scan(&count)
	if count != 2 {
		t.Fatalf("want two directional episodes, got %d", count)
	}
	var before LiquidationStudyEvent
	h.Store.liquidationLoad(ctx, "episode", "short", &before)
	for i := range s.Zones {
		s.Zones[i].Strength = "999999999"
	}
	if e = h.selectLiquidationStudy(ctx, s); e != nil {
		t.Fatal(e)
	}
	var after LiquidationStudyEvent
	h.Store.liquidationLoad(ctx, "event", before.ID, &after)
	if after.Zone.Strength != before.Zone.Strength || !after.Start.After(after.Selected) {
		t.Fatal("inputs rewritten or selection leakage")
	}
	// A new zone inside four hours belongs to the same directional episode.
	for i := range s.Zones {
		s.Zones[i].ID += "-new"
	}
	if e = h.selectLiquidationStudy(ctx, s); e != nil {
		t.Fatal(e)
	}
	h.Store.research.QueryRow("SELECT count(*) FROM lz_records WHERE kind='event'").Scan(&count)
	if count != 2 {
		t.Fatal("correlated episode counted twice")
	}
	result, e := h.liquidationStudyView(ctx, "BTC", now.Add(time.Minute))
	if e != nil {
		t.Fatal(e)
	}
	for _, g := range result.(map[string]any)["groups"].([]map[string]any) {
		if g["touchRate"] != nil || g["ready"] != false {
			t.Fatal("premature empirical rate")
		}
	}
}
func TestLiquidationModelCoverageCountsQuietGaps(t *testing.T) {
	h, e := Open(Config{Root: t.TempDir(), Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	ctx := context.Background()
	now := time.Now().UTC()
	from := now.Add(-2 * time.Hour)
	for i := 0; i < 3; i++ {
		at := from.Add(time.Duration(i) * 15 * time.Minute)
		if e = h.Store.liquidationPut(ctx, "history", fmt.Sprintf("map.btc..futures/%d", i), "BTC", at, map[string]string{"test": "coverage"}); e != nil {
			t.Fatal(e)
		}
	}
	v, e := h.liquidationModelCoverage(ctx, from, now)
	if e != nil || v > .55 || v < .53 {
		t.Fatal("quiet gap not counted", v, e)
	}
}

func TestLiquidationMatchedRatesUseOnlyCompletePairs(t *testing.T) {
	h, e := Open(Config{Root: t.TempDir(), Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Hour)
	from := now.Add(-15 * 24 * time.Hour)
	// Complete model coverage permits the public report to show empirical rates.
	tx, e := h.Store.research.BeginTx(ctx, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback()
	b, _ := liquidationJSON(map[string]string{"test": "complete model coverage"})
	for i := 0; i < 15*48; i++ {
		at := from.Add(time.Duration(i) * 30 * time.Minute)
		if _, e = tx.ExecContext(ctx, "INSERT INTO lz_records(kind,id,asset,at,payload) VALUES('history',?,'BTC',?,?)", fmt.Sprintf("map.btc..futures/%d", i), at.UnixNano(), b); e != nil {
			t.Fatal(e)
		}
	}
	if e = tx.Commit(); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 33; i++ {
		hit, controlHit := i < 18 || i >= 30, i < 6
		ev := LiquidationStudyEvent{ID: fmt.Sprint(i), Side: "short", Rule: LiquidationRules, Selected: from.Add(time.Duration(i) * 10 * time.Hour), Control: &LiquidationZone{Side: "short"}, Outcomes: map[string]LiquidationOutcome{}, ControlOutcomes: map[string]LiquidationOutcome{}}
		for _, key := range []string{"1", "4"} {
			ev.Outcomes[key] = LiquidationOutcome{State: "complete", Hit: &hit, Coverage: 1}
			ev.ControlOutcomes[key] = LiquidationOutcome{State: "complete", Hit: &controlHit, Coverage: 1}
			if i == 32 {
				ev.ControlOutcomes[key] = LiquidationOutcome{State: "incomplete"}
			}
		}
		if i == 30 || i == 31 {
			ev.Control = nil
		}
		if e = h.Store.liquidationPut(ctx, "event", ev.ID, "BTC", ev.Selected, ev); e != nil {
			t.Fatal(e)
		}
	}
	report, e := h.liquidationStudyView(ctx, "BTC", now)
	if e != nil {
		t.Fatal(e)
	}
	for _, g := range report.(map[string]any)["groups"].([]map[string]any) {
		if g["side"] != "short" {
			if g["ready"] != false || g["touchRate"] != nil {
				t.Fatal("empty direction acquired a rate", g)
			}
			continue
		}
		if g["ready"] != true || g["matched"] != 30 || g["unmatched"] != 2 || g["matchedTouchRate"] != .6 || g["controlTouchRate"] != .2 || g["touchRate"] != float64(21)/33 {
			t.Fatal("paired rates use different samples or lost hits", g)
		}
		if g["matchedConfidenceInterval"] != liquidationWilson(18, 30) || g["controlConfidenceInterval"] != liquidationWilson(6, 30) {
			t.Fatal("paired confidence interval uses the wrong denominator", g)
		}
	}
}
