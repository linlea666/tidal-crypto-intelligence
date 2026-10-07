package datahub

import (
	"context"
	"net/url"
	"testing"
	"time"
)

func TestRadarStorageFailureNeverReturnsAnEmptySuccess(t *testing.T) {
	h, _ := radarTestHub(t)
	saved := h.Store.radar
	h.Store.radar = nil
	defer func() { h.Store.radar = saved }()
	for _, path := range []string{"hl-radar/events", "hl-radar/wallet", "hl-radar/settings", "hl-radar/study"} {
		if _, err := h.radarRead(context.Background(), path, url.Values{}); err == nil {
			t.Fatalf("%s masked unavailable storage as valid data", path)
		}
	}
}

func TestRadarStudyFreezesAtMillionAndSurvivesCancellation(t *testing.T) {
	h, now := radarTestHub(t)
	ctx := context.Background()
	w := radarTestWallet(1, now)
	f := radarTestFill(now, "0", "3", "B", 1)
	if e := h.radarApply(ctx, w, []radarFill{f}, radarTestAccount(now, "3"), now); e != nil {
		t.Fatal(e)
	}
	var n int
	h.Store.radar.db.QueryRow("SELECT count(*) FROM records WHERE kind='study'").Scan(&n)
	if n != 0 {
		t.Fatal("sub-threshold position frozen as million observation")
	}
	later := now.Add(time.Minute)
	radarTestFX(t, h, later)
	add := radarTestFill(later, "3", "7", "B", 2)
	a := radarTestAccount(later, "10")
	a.Positions[0].Position.Value = "1100000"
	a.Margin.Total = "1100000"
	pd, _ := h.Dataset(ID("price", "BTC", "Binance", "spot"))
	if _, e := h.Store.Ingest(pd, Observation{Dataset: pd.ID, Source: pd.Source, ObservedAt: &later, FetchedAt: later, Quality: "valid", Payload: Payload{Price: &Price{"109500", "USDT"}}}); e != nil {
		t.Fatal(e)
	}
	if e := h.radarApply(ctx, w, []radarFill{add}, a, later); e != nil {
		t.Fatal(e)
	}
	ev := radarTestEvents(t, h)[0]
	var tr radarTrial
	if e := radarLoad(ctx, h.Store.radar.db, "study", ev.ID, &tr); e != nil {
		t.Fatal(e)
	}
	if !tr.At.Equal(later) || tr.Price != "109500" || tr.ReferenceSource != "Binance spot / USD" {
		t.Fatalf("wrong freeze %+v", tr)
	}
	a.Positions[0].Position.Value = "900000"
	if e := h.radarApply(ctx, w, nil, a, later.Add(time.Second)); e != nil {
		t.Fatal(e)
	}
	var after radarTrial
	radarLoad(ctx, h.Store.radar.db, "study", ev.ID, &after)
	if tr.Price != after.Price || !tr.At.Equal(after.At) {
		t.Fatal("later facts rewrote frozen observation")
	}
	cancelled, stop := context.WithCancel(ctx)
	stop()
	if h.radarStudy(cancelled, later.Add(26*time.Hour)) == nil {
		t.Fatal("cancelled study succeeded")
	}
	if e := h.radarStudy(ctx, later.Add(26*time.Hour)); e != nil {
		t.Fatal(e)
	}
	radarLoad(ctx, h.Store.radar.db, "study", ev.ID, &after)
	if len(after.Outcomes) != 4 {
		t.Fatal("due windows not recovered")
	}
	for _, o := range after.Outcomes {
		if o.State != "incomplete" || o.Return != nil {
			t.Fatal("missing market became zero return")
		}
	}
	h.Store.radar.db.QueryRow("SELECT count(*) FROM records WHERE kind='study_pending'").Scan(&n)
	if n != 0 {
		t.Fatal("completed job retained")
	}
}
func TestRadarStudyGroupsAndBaselines(t *testing.T) {
	h, now := radarTestHub(t)
	ctx := context.Background()
	for i := 1; i <= 3; i++ {
		if e := h.radarApply(ctx, radarTestWallet(i, now), []radarFill{radarTestFill(now, "0", "10", "B", int64(i))}, radarTestAccount(now, "10"), now); e != nil {
			t.Fatal(e)
		}
	}
	if e := h.radarGroups(ctx, now); e != nil {
		t.Fatal(e)
	}
	v, e := h.radarStudyView(ctx)
	if e != nil {
		t.Fatal(e)
	}
	m := v.(map[string]any)
	if m["independentEvents"].(int) != 1 {
		t.Fatal("synchronized group counted multiple times")
	}
	for _, metric := range m["metrics"].([]radarStudyMetric) {
		if metric.Events != 1 || metric.Pending != 1 {
			t.Fatalf("wrong comparison %+v", metric)
		}
	}
}
func TestRadarMalformedFinancialValuesAndStaleEvidence(t *testing.T) {
	h, now := radarTestHub(t)
	ctx := context.Background()
	w := radarTestWallet(1, now)
	f := radarTestFill(now, "0", "10", "B", 1)
	a := radarTestAccount(now, "10")
	invalid := "garbage"
	a.Positions[0].Position.Liquidation = &invalid
	if h.radarApply(ctx, w, []radarFill{f}, a, now) == nil {
		t.Fatal("invalid liquidation accepted")
	}
	if radarOutboxCount(t, h) != 0 {
		t.Fatal("invalid facts committed")
	}
	a = radarTestAccount(now.Add(-91*time.Second), "10")
	a.Margin.Equity = "0"
	if e := h.radarApply(ctx, w, []radarFill{f}, a, now); e != nil {
		t.Fatal(e)
	}
	if radarOutboxCount(t, h) != 0 {
		t.Fatal("stale position sent")
	}
	a = radarTestAccount(now, "10")
	a.Margin.Equity = "-1"
	if e := h.radarApply(ctx, w, nil, a, now); e != nil {
		t.Fatal(e)
	}
	if radarTestEvents(t, h)[0].EffectiveLeverage != nil {
		t.Fatal("negative equity produced leverage")
	}
	if _, ok := radarMoney(dec("100000000000000000000000")); ok {
		t.Fatal("overflow accepted")
	}
}

func TestRadarForwardReturnsUseCompleteSameMarketBars(t *testing.T) {
	h, now := radarTestHub(t)
	ctx := context.Background()
	at := now.Truncate(time.Hour)
	fx, _ := h.Dataset("fx.usd.kraken")
	if _, e := h.Store.Ingest(fx, Observation{Dataset: fx.ID, Source: fx.Source, ObservedAt: &at, FetchedAt: now, Resolution: 3600, Quality: "valid", Payload: Payload{Rates: []Rate{{"USDT", "1.01"}}}}); e != nil {
		t.Fatal(e)
	}
	d, _ := h.Dataset(ID("candles", "BTC", "Binance", "spot"))
	for i, c := range []Candle{{Open: 100, High: 110, Low: 90, Close: 105, Volume: 1}, {Open: 105, High: 108, Low: 98, Close: 104, Volume: 1}} {
		ts := at.Add(time.Duration(i+1) * 5 * time.Minute)
		if _, e := h.Store.Ingest(d, Observation{Dataset: d.ID, Source: d.Source, ObservedAt: &ts, FetchedAt: now, Resolution: 300, Quality: "valid", Payload: Payload{Candle: &c}}); e != nil {
			t.Fatal(e)
		}
	}
	for _, side := range []string{"long", "short"} {
		tr := radarTrial{Asset: "BTC", Side: side, At: at, Price: "101"}
		o := h.radarOutcome(ctx, tr, 15, at.Add(15*time.Minute))
		want := "4"
		if side == "short" {
			want = "-4"
		}
		if o.State != "complete" || o.Return == nil || *o.Return != want || o.MFE == nil || *o.MFE != "10" || o.MAE == nil || *o.MAE != "-10" || o.Observed != 2 || o.Expected != 2 {
			t.Fatalf("%s invalid outcome %+v", side, o)
		}
		tr.Price = ""
		if o = h.radarOutcome(ctx, tr, 15, at.Add(15*time.Minute)); o.State != "incomplete" || o.Return != nil {
			t.Fatal("missing discovery price backfilled")
		}
	}
}

func TestRadarPartialBarsCannotBecomeCompleteEvidence(t *testing.T) {
	h, now := radarTestHub(t)
	at := now.Truncate(time.Hour)
	d, _ := h.Dataset(ID("candles", "ETH", "Binance", "spot"))
	for i := 1; i <= 2; i++ {
		ts := at.Add(time.Duration(i) * 5 * time.Minute)
		quality := "valid"
		if i == 2 {
			quality = "partial"
		}
		if _, e := h.Store.Ingest(d, Observation{Dataset: d.ID, Source: d.Source, ObservedAt: &ts, FetchedAt: now, Resolution: 300, Quality: quality, Payload: Payload{Candle: &Candle{Open: 100, High: 110, Low: 90, Close: 105, Volume: 1}}}); e != nil {
			t.Fatal(e)
		}
	}
	o := h.radarOutcome(context.Background(), radarTrial{Asset: "ETH", Side: "long", At: at, Price: "100"}, 15, at.Add(15*time.Minute))
	if o.State != "incomplete" || o.Observed != 1 || o.Return != nil {
		t.Fatal("partial bar advertised complete", o)
	}
}
