package datahub

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/debug"
	"strings"
	"testing"
	"time"
)

func shortFixture(end time.Time) (map[int64]FlowBar, ShortBaseline) {
	bs := map[int64]FlowBar{}
	for t := end.Add(-4 * time.Hour); t.Before(end); t = t.Add(5 * time.Minute) {
		bs[t.Unix()] = FlowBar{At: t, Buy: 200000000, Sell: 100000000}
	}
	b := ShortBaseline{From: end.Truncate(time.Hour).Add(-31 * 24 * time.Hour), To: end.Truncate(time.Hour).Add(-time.Hour), AsOf: end.Add(-time.Minute), Valid: true, Coverage: 1, Dates: 29, Windows: map[string]ShortThreshold{}}
	for _, m := range []int{5, 10, 15, 60, 240} {
		b.Windows[fmt.Sprint(m)] = ShortThreshold{BuyP95: flowPtr(50000000.0), SellP95: flowPtr(50000000.0), MedianVolume: flowPtr(100000000.0), Samples: 8000}
	}
	return bs, b
}
func shortTestHub(t *testing.T, now time.Time) *Hub {
	t.Helper()
	h, e := Open(Config{Root: t.TempDir(), Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { h.Store.Close() })
	if e = h.Store.shortPut(context.Background(), "origin", ShortFlowRules, now, now.Add(-48*time.Hour)); e != nil {
		t.Fatal(e)
	}
	return h
}
func shortSnapshot(end time.Time) ShortObservation {
	bs, b := shortFixture(end)
	return buildShortObservation(bs, nil, end, end.Add(time.Minute), b)
}

func TestShortObservationDoesNotQueueBehindFormalResearch(t *testing.T) {
	end := time.Now().UTC().Truncate(5 * time.Minute)
	h := shortTestHub(t, end)
	ctx := context.Background()
	d, _ := h.Dataset(ID("flow", "BTC", "", "spot"))
	for at := end.Add(-4 * time.Hour); at.Before(end); at = at.Add(5 * time.Minute) {
		o := Observation{Dataset: d.ID, Source: d.Source, ObservedAt: flowPtr(at), FetchedAt: at.Add(5 * time.Minute), Resolution: 300, Quality: "valid", Payload: Payload{Flow: &Flow{"2000000", "1000000"}}}
		if _, e := h.Store.Ingest(d, o); e != nil {
			t.Fatal(e)
		}
	}
	_, baseline := shortFixture(end)
	if e := h.Store.shortPut(ctx, "baseline", ShortFlowRules, end, baseline); e != nil {
		t.Fatal(e)
	}
	// Formal research can occupy its sole connection while decoding a long
	// history. It holds no SQLite write lock, so a short observation must not
	// spend its entire computation budget waiting in that Go connection pool.
	busy, e := h.Store.research.Conn(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer busy.Close()
	step, cancel := context.WithTimeout(ctx, 1800*time.Millisecond)
	defer cancel()
	if e = h.shortObservationStep(step, end.Add(time.Minute)); e != nil {
		t.Fatal(e)
	}
	var current ShortObservation
	if !h.Store.LoadState("short-flow/current", &current) || !current.Fresh || current.Windows["5"].Net == nil {
		t.Fatal("independent flow was not published")
	}
	if e = h.shortStudyStep(step, end.Add(time.Minute)); e != nil {
		t.Fatal(e)
	}
	if e = h.shortBaselineStep(step, end.Add(time.Minute)); e != nil {
		t.Fatal(e)
	}
	if h.Store.research.Stats().MaxOpenConnections != 1 || h.Store.shortDB().Stats().MaxOpenConnections != 1 {
		t.Fatal("unbounded connection pool")
	}
	var cache, pages int
	if e = h.Store.shortDB().QueryRow("PRAGMA cache_size").Scan(&cache); e != nil || cache != -256 {
		t.Fatal("short cache cap", cache, e)
	}
	if e = h.Store.shortDB().QueryRow("PRAGMA max_page_count").Scan(&pages); e != nil || pages != 126976 {
		t.Fatal("research page cap", pages, e)
	}
}
func activeShort(s ShortObservation, m int, side string) bool {
	for _, h := range s.Hints {
		if h.Minutes == m && h.Direction == side {
			return h.Active
		}
	}
	return false
}

func TestShortBaselineDoesNotQueueBehindSharedStateReader(t *testing.T) {
	end := time.Now().UTC().Truncate(time.Hour)
	h := shortTestHub(t, end)
	if e := h.Store.SaveState("short-flow/current", ShortObservation{Through: end}); e != nil {
		t.Fatal(e)
	}
	busy, e := h.Store.db.Conn(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer busy.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if e = h.shortBaselineStep(ctx, end.Add(time.Minute)); e != nil {
		t.Fatal("baseline still waits for unrelated hub state pool", e)
	}
}

func TestShortWindowsMissingZeroContinuityAndAge(t *testing.T) {
	end := testTime("2026-10-01T10:30:00Z")
	bs, b := shortFixture(end)
	s := buildShortObservation(bs, nil, end, end.Add(time.Minute), b)
	if !activeShort(s, 5, "buy") || !activeShort(s, 10, "buy") || activeShort(s, 5, "sell") {
		t.Fatal("short direction gates")
	}
	if s.Prices["5"].Return != nil || s.ATR != nil {
		t.Fatal("invented delayed price")
	}
	for _, m := range []int{5, 10, 15, 60, 240} {
		v := flowWindow(bs, end, m, 0)
		if *s.Windows[fmt.Sprint(m)].Net != *v.Net {
			t.Fatal("changed window sums")
		}
	}
	if len(s.Segments) != 6 {
		t.Fatal("independent interval count")
	}
	bs[end.Add(-10*time.Minute).Unix()] = FlowBar{At: end.Add(-10 * time.Minute), Buy: 1, Sell: 2}
	s = buildShortObservation(bs, nil, end, end.Add(time.Minute), b)
	if !activeShort(s, 5, "buy") || activeShort(s, 10, "buy") {
		t.Fatal("rolling positive net is not two independent same-side bars")
	}
	delete(bs, end.Add(-10*time.Minute).Unix())
	s = buildShortObservation(bs, nil, end, end, b)
	if s.Windows["10"].Net != nil || s.Windows["10"].Coverage != .5 || !activeShort(s, 5, "buy") {
		t.Fatal("missing minute was zero-filled or unrelated window blocked")
	}
	bs[end.Add(-5*time.Minute).Unix()] = FlowBar{At: end.Add(-5 * time.Minute)}
	s = buildShortObservation(bs, nil, end, end, b)
	if s.Windows["5"].Net == nil || *s.Windows["5"].Net != 0 || s.Windows["5"].BuyShare != nil || activeShort(s, 5, "buy") {
		t.Fatal("zero / missing / empty volume conflated")
	}
	s = shortSnapshot(end)
	s.age(end.Add(5*time.Minute + time.Nanosecond))
	if s.Fresh || activeShort(s, 5, "buy") {
		t.Fatal("old hints remain active")
	}
	bs, b = shortFixture(end)
	b.To = b.To.Add(time.Hour)
	s = buildShortObservation(bs, nil, end, end, b)
	if activeShort(s, 5, "buy") || s.Windows["5"].Net == nil {
		t.Fatal("baseline cutoff or raw amount fallback")
	}
}

func TestShortBaselineIndependentDistributionsNoFuture(t *testing.T) {
	to := testTime("2026-10-01T00:00:00Z")
	from := to.Add(-30 * 24 * time.Hour)
	bars := map[int64]FlowBar{}
	for at := from; at.Before(to); at = at.Add(5 * time.Minute) {
		v := FlowBar{At: at, Buy: 100, Sell: 70}
		if at.Minute()%15 != 10 {
			v.Buy, v.Sell = 10, 20
		}
		bars[at.Unix()] = v
	}
	b := shortBaseline(bars, from, to, to.Add(time.Hour))
	if !b.Valid || b.Dates != 30 || b.Coverage != 1 {
		t.Fatal(b)
	}
	if *b.Windows["5"].BuyP95 != 30 || *b.Windows["10"].BuyP95 != 20 || *b.Windows["10"].SellP95 != 20 {
		t.Fatal("thresholds scaled or directions contaminated")
	}
	if *b.Windows["5"].MedianVolume == *b.Windows["15"].MedianVolume/3 {
		t.Fatal("fixture must distinguish independently computed medians")
	}
	bars[to.Unix()] = FlowBar{At: to, Buy: 999999999999}
	after := shortBaseline(bars, from, to, to.Add(time.Hour))
	if !reflect.DeepEqual(b, after) {
		t.Fatal("future bar leaked")
	}
	for at := from; at.Before(from.Add(2 * 24 * time.Hour)); at = at.Add(5 * time.Minute) {
		delete(bars, at.Unix())
	}
	if shortBaseline(bars, from, to, to).Valid {
		t.Fatal("coverage gate bypass")
	}
}

func TestShortFormingBarsAreNotClosedByWallClock(t *testing.T) {
	d, _ := FindDataset(ID("flow", "BTC", "", "spot"))
	at := time.Now().UTC().Truncate(time.Minute)
	raw := []byte(fmt.Sprintf(`{"code":"0","data":[{"time":%d,"aggregated_buy_volume_usd":"10","aggregated_sell_volume_usd":"2"},{"time":%d,"aggregated_buy_volume_usd":"9","aggregated_sell_volume_usd":"1"}]}`, at.Add(-time.Minute).UnixMilli(), at.UnixMilli()))
	rows, e := Normalize(d, raw, at.Add(30*time.Second))
	if e != nil || len(rows) != 1 || !rows[0].Time().Equal(at.Add(-time.Minute)) {
		t.Fatalf("forming flow admitted: %v %v", rows, e)
	}
	rows, e = Normalize(d, raw, at.Add(time.Minute))
	if e != nil || len(rows) != 2 {
		t.Fatal("closed boundary rejected", e)
	}
	h := shortTestHub(t, at)
	end := at.Truncate(5 * time.Minute)
	for tm := end.Add(-10 * time.Minute); tm.Before(end); tm = tm.Add(time.Minute) {
		o := Observation{Dataset: d.ID, Source: d.Source, ObservedAt: flowPtr(tm), FetchedAt: tm.Add(time.Minute), Resolution: 60, Quality: "valid", Payload: Payload{Flow: &Flow{"100", "30"}}}
		if tm.Equal(end.Add(-time.Minute)) {
			o.FetchedAt = tm.Add(30 * time.Second)
		}
		if _, e = h.Store.Ingest(d, o); e != nil {
			t.Fatal(e)
		}
	}
	bs, _, through, e := h.shortInput(context.Background(), end.Add(2*time.Minute))
	if e != nil {
		t.Fatal(e)
	}
	if _, ok := bs[end.Add(-5*time.Minute).Unix()]; ok || !through.Equal(end.Add(-5*time.Minute)) {
		t.Fatal("legacy unclosed fact became full later")
	}
}

func TestShortFirstVisibilityDedupeGroupingAndRestart(t *testing.T) {
	end := time.Now().UTC().Truncate(5 * time.Minute)
	h := shortTestHub(t, end)
	ctx := context.Background()
	s := shortSnapshot(end)
	s.Hints[2].Active = false
	s.Hints[3].Active = false
	if e := h.recordShortObservation(ctx, s, s.At); e != nil {
		t.Fatal(e)
	}
	var raw []byte
	if e := h.Store.research.QueryRow("SELECT payload FROM sf_records WHERE kind='episode'").Scan(&raw); e != nil {
		t.Fatal(e)
	}
	var first ShortEpisode
	json.Unmarshal(raw, &first)
	s.Windows["5"] = FlowWindow{Net: flowPtr(int64(9999999))}
	s.At = s.At.Add(time.Minute)
	if e := h.recordShortObservation(ctx, s, s.At); e != nil {
		t.Fatal(e)
	}
	var same ShortEpisode
	if e := h.Store.shortLoad(ctx, "episode", first.ID, &same); e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(first, same) {
		t.Fatal("revision rewrote frozen first view")
	}
	s = shortSnapshot(end.Add(5 * time.Minute))
	if e := h.recordShortObservation(ctx, s, s.At); e != nil {
		t.Fatal(e)
	}
	if e := h.Store.shortLoad(ctx, "episode", first.ID, &same); e != nil {
		t.Fatal(e)
	}
	if len(same.Trials) != 2 || !same.Trials["5"].At.Equal(first.Trials["5"].At) || !same.Trials["10"].At.Equal(s.At) {
		t.Fatal("overlapping windows duplicated episode or lost first time")
	}
	root := h.Store.root
	h.Store.Close()
	h2, e := Open(Config{Root: root, Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	defer h2.Store.Close()
	if e = h2.recordShortObservation(ctx, s, s.At); e != nil {
		t.Fatal(e)
	}
	var count int
	h2.Store.research.QueryRow("SELECT count(*) FROM sf_records WHERE kind='episode'").Scan(&count)
	if count != 1 {
		t.Fatal("restart reset event anchor")
	}
	later := shortSnapshot(end.Add(4*time.Hour + 5*time.Minute))
	if e = h2.recordShortObservation(ctx, later, later.At); e != nil {
		t.Fatal(e)
	}
	h2.Store.research.QueryRow("SELECT count(*) FROM sf_records WHERE kind='episode'").Scan(&count)
	if count != 2 {
		t.Fatal("new four hour episode missing")
	}
}

func TestShortIndependentPriceDelayCapacityAndReadOnlyViews(t *testing.T) {
	end := time.Now().UTC().Truncate(5 * time.Minute)
	h := shortTestHub(t, end)
	ctx := context.Background()
	bs, base := shortFixture(end)
	fd, _ := h.Dataset(ID("flow", "BTC", "", "spot"))
	for _, b := range bs {
		at := b.At
		_, e := h.Store.Ingest(fd, Observation{Dataset: fd.ID, Source: fd.Source, ObservedAt: &at, FetchedAt: end, Resolution: 300, Quality: "valid", Payload: Payload{Flow: &Flow{fmt.Sprint(b.Buy / 100), fmt.Sprint(b.Sell / 100)}}})
		if e != nil {
			t.Fatal(e)
		}
	}
	h.Store.shortPut(ctx, "baseline", ShortFlowRules, end, base)
	formal := map[string]any{"unchanged": "formal engine sentinel"}
	h.Store.SaveState("signals/current/BTC", formal)
	if e := h.shortObservationStep(ctx, end.Add(time.Minute)); e != nil {
		t.Fatal(e)
	}
	var s ShortObservation
	h.Store.LoadState("short-flow/current", &s)
	if !s.Fresh || s.Windows["5"].Net == nil || s.Prices["5"].Return != nil {
		t.Fatal("price delay blocked flow or invented price")
	}
	var f map[string]any
	h.Store.LoadState("signals/current/BTC", &f)
	if !reflect.DeepEqual(f, formal) {
		t.Fatal("formal state modified")
	}
	var before int
	h.Store.research.QueryRow("SELECT count(*) FROM sf_records").Scan(&before)
	for i := 0; i < 3; i++ {
		for _, path := range []string{"signals", "studies"} {
			if _, e := h.Read(ctx, path, url.Values{"asset": {"BTC"}}); e != nil {
				t.Fatal(e)
			}
		}
	}
	var after int
	h.Store.research.QueryRow("SELECT count(*) FROM sf_records").Scan(&after)
	if before != after {
		t.Fatal("GET mutated observations")
	}
	if v := h.shortStudyView("ETH"); v != nil {
		t.Fatal("ETH enabled")
	}
	if _, e := h.Store.research.Exec("UPDATE sf_budget SET used=?", shortFlowBudget); e != nil {
		t.Fatal(e)
	}
	s = shortSnapshot(end.Add(5 * time.Minute))
	e := h.recordShortObservation(ctx, s, s.At)
	if e == nil {
		t.Fatal("sub-budget not enforced")
	}
	h.Store.shortGap(s.At, e)
	view := h.shortObservationView(s.At).(ShortObservation)
	if !view.ResearchPaused || view.Windows["5"].Net == nil {
		t.Fatal("capacity failure hidden or old market removed")
	}
	var notices int
	h.Store.research.QueryRow("SELECT count(*) FROM notices").Scan(&notices)
	if notices != 0 {
		t.Fatal("short flow sent mail")
	}
	backup := filepath.Join(t.TempDir(), "research.sqlite")
	if e = BackupFile(ctx, filepath.Join(h.Store.root, "research.sqlite"), backup); e != nil {
		t.Fatal(e)
	}
	db, e := database(backup)
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	var n int
	db.QueryRow("SELECT count(*) FROM sf_records").Scan(&n)
	if n != before {
		t.Fatal("online backup omitted new tables")
	}
}

func TestShortOutcomesFreezeMissingAndCorrections(t *testing.T) {
	at := time.Now().UTC().Truncate(5 * time.Minute)
	h := shortTestHub(t, at)
	ctx := context.Background()
	tr := newShortTrial("test", "short-5", "buy", at.Add(time.Minute), at, nil)
	d, _ := h.Dataset(ID("candles", "BTC", "Binance", "spot"))
	for i := 0; i < 48; i++ {
		if i == 4 {
			continue
		}
		tm := tr.Start.Add(time.Duration(i) * 5 * time.Minute)
		_, e := h.Store.Ingest(d, Observation{Dataset: d.ID, Source: d.Source, ObservedAt: &tm, FetchedAt: tm.Add(6 * time.Minute), Resolution: 300, Quality: "valid", Payload: Payload{Candle: &Candle{Open: 100, High: 103, Low: 98, Close: 101, Volume: 1}}})
		if e != nil {
			t.Fatal(e)
		}
	}
	if e := h.advanceShortTrial(ctx, tr, tr.Start.Add(5*time.Hour)); e != nil {
		t.Fatal(e)
	}
	if !tr.Done || tr.Outcomes[0].State != "complete" || mathAbs(*tr.Outcomes[0].Return-1) > .0001 || tr.Outcomes[1].State != "incomplete" || tr.Outcomes[3].Return != nil {
		t.Fatal("partial windows became a result", tr.Outcomes)
	}
	frozen, _ := json.Marshal(tr)
	tm := tr.Start.Add(20 * time.Minute)
	h.Store.Ingest(d, Observation{Dataset: d.ID, Source: d.Source, ObservedAt: &tm, FetchedAt: tm.Add(8 * time.Hour), Resolution: 300, Quality: "valid", Payload: Payload{Candle: &Candle{100, 150, 90, 140, 1}}})
	h.advanceShortTrial(ctx, tr, tm.Add(9*time.Hour))
	after, _ := json.Marshal(tr)
	if string(frozen) != string(after) {
		t.Fatal("future backfill repaired frozen outcome")
	}
}
func mathAbs(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}

func TestShortBaselineCheckpointAndHourlyReuse(t *testing.T) {
	end := time.Now().UTC().Truncate(time.Hour)
	h := shortTestHub(t, end)
	ctx := context.Background()
	to := end.Add(-time.Hour)
	from := to.Add(-30 * 24 * time.Hour)
	w := shortBaselineWork{From: from, To: to, Cursor: to.Add(-time.Hour), AsOf: end, Version: h.Store.datasetRangeVersion(ctx, ID("flow", "BTC", "", "spot"), from, to), Bars: []shortStoredBar{}}
	for at := from; at.Before(w.Cursor); at = at.Add(5 * time.Minute) {
		w.Bars = append(w.Bars, storeShortBar(FlowBar{At: at, Buy: 20, Sell: 10}))
	}
	if e := h.Store.shortPut(ctx, "work", ShortFlowRules, end, w); e != nil {
		t.Fatal(e)
	}
	h.Store.SaveState("short-flow/current", ShortObservation{Through: end})
	if e := h.shortBaselineStep(ctx, end); e != nil {
		t.Fatal(e)
	}
	var b ShortBaseline
	h.Store.shortLoad(ctx, "baseline", ShortFlowRules, &b)
	if !b.Valid || b.Coverage >= 1 {
		t.Fatal("checkpoint lost valid history or filled final gap")
	}
	h.Store.SaveState("short-flow/current", ShortObservation{Through: end.Add(time.Hour)})
	if e := h.shortBaselineStep(ctx, end.Add(time.Hour)); e != nil {
		t.Fatal(e)
	}
	h.Store.shortLoad(ctx, "work-v2", ShortFlowRules, &w)
	if !w.Cursor.Equal(to.Add(time.Hour)) || len(w.Bars) < 8000 {
		t.Fatal("hourly advance rebuilt or dropped history")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if e := h.shortBaselineStep(cancelled, end.Add(2*time.Hour)); e == nil || !strings.Contains(e.Error(), "canceled") {
		t.Fatal("canceled batch ignored", e)
	}
}

func TestShortReportDoesNotExposePrematureRates(t *testing.T) {
	end := time.Now().UTC().Truncate(5 * time.Minute)
	h := shortTestHub(t, end)
	ctx := context.Background()
	origin := end.Add(-48 * time.Hour)
	for tm := origin.Add(5 * time.Minute); !tm.After(end); tm = tm.Add(5 * time.Minute) {
		if e := h.Store.shortPut(ctx, "coverage", fmt.Sprint(tm.Unix()), tm, ShortCoverage{Through: tm, Seen: tm.Add(time.Minute), Valid: true}); e != nil {
			t.Fatal(e)
		}
	}
	pe := PriceEpisode{ID: "short-test-buy", Direction: "buy", Start: end.Add(-8 * time.Hour), Detected: end.Add(-8*time.Hour + 15*time.Minute), DataThrough: end.Add(-8*time.Hour + 10*time.Minute)}
	if e := h.insertPriceEvent(ctx, "BTC", pe); e != nil {
		t.Fatal(e)
	}
	tr := newShortTrial("trial", "short-5", "buy", pe.Start.Add(-30*time.Minute), pe.Start.Add(-35*time.Minute), nil)
	ev := ShortEpisode{ID: "event", Direction: "buy", At: tr.At, Trials: map[string]*ShortTrial{"5": tr}}
	h.Store.shortPut(ctx, "episode", ev.ID, ev.At, ev)
	if e := h.buildShortReport(ctx, origin, end); e != nil {
		t.Fatal(e)
	}
	r := h.shortStudyView("BTC").(ShortStudyReport)
	g := r.Groups[0]
	if r.Coverage != 1 || g.Early != 1 || g.PriceEvents != 1 || g.Ready || g.EarlyRate != nil || g.Interval != nil {
		t.Fatal("forward match or sample gate", g)
	}
	if len(shortWilson(0, 0)) != 0 || len(shortWilson(1, 30)) != 2 {
		t.Fatal("interval edge")
	}
}

// Conservative no-GC allocation bound for the largest retained checkpoint plus
// full baseline computation. Streaming SQL scratch and shared SQLite caches are
// exercised separately by the constrained Linux concurrent replay.
func TestShortWorkingSetBounds(t *testing.T) {
	end := time.Now().UTC().Truncate(time.Hour)
	work := shortBaselineWork{From: end.Add(-30 * 24 * time.Hour), To: end, Cursor: end, AsOf: end, Bars: []shortStoredBar{}}
	for at := work.From; at.Before(end); at = at.Add(5 * time.Minute) {
		work.Bars = append(work.Bars, storeShortBar(FlowBar{At: at, Buy: 9999999999999, Sell: 5555555555555}))
	}
	raw, err := json.Marshal(work)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) > shortFlowRowLimit {
		t.Fatal("checkpoint no longer fits row budget", len(raw))
	}
	runtime.GC()
	old := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(old)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	restored := shortBaselineWork{Bars: make([]shortStoredBar, 0, 8640)}
	if err = json.Unmarshal(raw, &restored); err != nil {
		t.Fatal(err)
	}
	bars := make(map[int64]FlowBar, len(restored.Bars))
	for _, v := range restored.Bars {
		bars[v[0]] = v.flow()
	}
	baseline := shortBaseline(bars, restored.From, restored.To, end)
	output, err := json.Marshal(restored)
	if err != nil {
		t.Fatal(err)
	}
	runtime.ReadMemStats(&after)
	used := after.TotalAlloc - before.TotalAlloc + uint64(len(raw))
	runtime.KeepAlive(output)
	runtime.KeepAlive(baseline)
	runtime.KeepAlive(restored)
	runtime.KeepAlive(bars)
	t.Logf("bounded checkpoint + baseline allocations: %.2f MiB", float64(used)/(1<<20))
	if used > shortFlowWorkLimit {
		t.Fatal("8 MiB new working-set budget exceeded", used)
	}
}

func TestShortPausedResearchAndFollowingFreeze(t *testing.T) {
	end := time.Now().UTC().Truncate(5 * time.Minute)
	h := shortTestHub(t, end)
	s := shortSnapshot(end)
	s.ResearchPaused = true
	if e := h.recordShortObservation(context.Background(), s, s.At); e != nil {
		t.Fatal(e)
	}
	var count int
	h.Store.research.QueryRow("SELECT count(*) FROM sf_records WHERE kind='episode'").Scan(&count)
	if count != 0 {
		t.Fatal("paused study enrolled new event")
	}
	s.Following = map[string]bool{"buy": true}
	tr := newShortTrial("x", "short-5", "buy", s.At, s.Through, &s)
	if !tr.Following {
		t.Fatal("already-following price state not frozen")
	}
}

func TestShortHourGapDoesNotEraseFiveMinuteHint(t *testing.T) {
	end := time.Now().UTC().Truncate(5 * time.Minute)
	h := shortTestHub(t, end)
	bs, base := shortFixture(end)
	delete(bs, end.Add(-10*time.Minute).Unix())
	s := buildShortObservation(bs, nil, end, end.Add(time.Minute), base)
	if !activeShort(s, 5, "buy") || activeShort(s, 10, "buy") {
		t.Fatal("fixture")
	}
	if e := h.recordShortObservation(context.Background(), s, s.At); e != nil {
		t.Fatal(e)
	}
	var raw []byte
	if e := h.Store.research.QueryRow("SELECT payload FROM sf_records WHERE kind='episode'").Scan(&raw); e != nil {
		t.Fatal(e)
	}
	var ev ShortEpisode
	json.Unmarshal(raw, &ev)
	if ev.Trials["5"] == nil || ev.Trials["10"] != nil {
		t.Fatal("independent five minute hint lost")
	}
	var cov ShortCoverage
	if e := h.Store.shortLoad(context.Background(), "coverage", fmt.Sprint(end.Unix()), &cov); e != nil {
		t.Fatal(e)
	}
	if cov.Valid {
		t.Fatal("common comparison accepted missing longer window")
	}
}

func TestShortHourlyRolloverPreparesBeforeFirstObservation(t *testing.T) {
	end := time.Now().UTC().Truncate(time.Hour)
	h := shortTestHub(t, end)
	ctx := context.Background()
	bars, _ := shortFixture(end)
	fd, _ := h.Dataset(ID("flow", "BTC", "", "spot"))
	for _, bar := range bars {
		at := bar.At
		if _, e := h.Store.Ingest(fd, Observation{Dataset: fd.ID, Source: fd.Source, ObservedAt: &at, FetchedAt: end, Resolution: 300, Quality: "valid", Payload: Payload{Flow: &Flow{Buy: fmt.Sprint(bar.Buy / 100), Sell: fmt.Sprint(bar.Sell / 100)}}}); e != nil {
			t.Fatal(e)
		}
	}
	oldTo := end.Add(-2 * time.Hour)
	from := oldTo.Add(-30 * 24 * time.Hour)
	work := shortBaselineWork{From: from, To: oldTo, Cursor: oldTo, AsOf: end.Add(-10 * time.Minute), Version: h.Store.datasetRangeVersion(ctx, fd.ID, from, oldTo)}
	baselineBars := make(map[int64]FlowBar, 8640)
	for at := from; at.Before(oldTo); at = at.Add(5 * time.Minute) {
		b := FlowBar{At: at, Buy: 200000000, Sell: 100000000}
		work.Bars = append(work.Bars, storeShortBar(b))
		baselineBars[at.Unix()] = b
	}
	base := shortBaseline(baselineBars, from, oldTo, end.Add(-10*time.Minute))
	if e := h.Store.shortPut(ctx, "work", ShortFlowRules, end, work); e != nil {
		t.Fatal(e)
	}
	if e := h.Store.shortPut(ctx, "baseline", ShortFlowRules, end, base); e != nil {
		t.Fatal(e)
	}
	step, cancel := context.WithTimeout(ctx, 1800*time.Millisecond)
	defer cancel()
	if e := h.shortBaselineStep(step, end.Add(-30*time.Second)); e != nil {
		t.Fatal(e)
	}
	if e := h.shortObservationStep(step, end.Add(time.Minute)); e != nil {
		t.Fatal(e)
	}
	s := h.shortObservationView(end.Add(time.Minute)).(ShortObservation)
	if !s.Baseline.Valid || !s.Baseline.To.Equal(end.Add(-time.Hour)) {
		t.Fatal("hourly baseline stale at first enrollment", s.Baseline)
	}
	var cov ShortCoverage
	if e := h.Store.shortLoad(ctx, "coverage", fmt.Sprint(end.Unix()), &cov); e != nil {
		t.Fatal(e)
	}
	if !cov.Valid {
		t.Fatal("phase ordering created a routine hourly coverage gap")
	}
}

func TestShortFinalCandleGraceRemainsPending(t *testing.T) {
	at := time.Now().UTC().Truncate(5 * time.Minute)
	h := shortTestHub(t, at)
	ctx := context.Background()
	tr := newShortTrial("grace", "short-5", "buy", at, at, nil)
	if e := h.advanceShortTrial(ctx, tr, tr.Start.Add(4*time.Hour+10*time.Minute)); e != nil {
		t.Fatal(e)
	}
	if tr.Done || tr.Outcomes[3].State != "pending" {
		t.Fatal("missing final candle prematurely completed")
	}
	if e := h.advanceShortTrial(ctx, tr, tr.Start.Add(4*time.Hour+20*time.Minute)); e != nil {
		t.Fatal(e)
	}
	if !tr.Done || tr.Outcomes[3].State != "incomplete" {
		t.Fatal("mature gap not finalized")
	}
}

func TestShortV2QuantileWorkingSetBound(t *testing.T) {
	end := time.Now().UTC().Truncate(time.Hour)
	h := shortTestHub(t, end)
	ctx := context.Background()
	to := end.Add(-time.Hour)
	from := to.Add(-30 * 24 * time.Hour)
	version, e := h.Store.shortRangeVersion(ctx, ID("flow", "BTC", "", "spot"), from, to)
	if e != nil {
		t.Fatal(e)
	}
	work := shortBaselineV2{shortBaselineWork: shortBaselineWork{From: from, To: to, Cursor: to, AsOf: end, Version: version, Bars: make([]shortStoredBar, 0, 8640)}, Phase: "quantiles"}
	for at := from; at.Before(to); at = at.Add(5 * time.Minute) {
		work.Bars = append(work.Bars, storeShortBar(FlowBar{At: at, Buy: 9999999999999, Sell: 5555555555555}))
	}
	if e = h.Store.shortPut(ctx, "work-v2", ShortFlowRules, end, work); e != nil {
		t.Fatal(e)
	}
	runtime.GC()
	old := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(old)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	if e = h.shortBaselineAt(ctx, end, end); e != nil {
		t.Fatal(e)
	}
	runtime.ReadMemStats(&after)
	used := after.TotalAlloc - before.TotalAlloc
	t.Logf("v2 checkpoint load + five quantiles + atomic publish: %.2f MiB", float64(used)/(1<<20))
	if used > shortFlowWorkLimit {
		t.Fatal("8 MiB budget exceeded", used)
	}
}
