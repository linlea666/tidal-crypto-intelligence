package datahub

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func costTestFrame(date string) CostFrame {
	a, b := make([]string, 45), make([]string, 45)
	sumA, sumB := int64(0), int64(0)
	for i := range a {
		x, y := int64(10+i), int64(70-i)
		if i >= 11 && i <= 13 {
			x += 500
		}
		if i >= 21 && i <= 23 {
			y += 700
		}
		a[i] = fmt.Sprint(x)
		b[i] = fmt.Sprint(y)
		sumA += x
		sumB += y
	}
	f := CostFrame{Date: date, Price: "85000", Method: onchainMethod, STH: CostCohort{"65000", "1000", a, fmt.Sprint(sumA)}, LTH: CostCohort{"65000", "1000", b, fmt.Sprint(sumB)}}
	f.Revision = costFrameRevision(f)
	return f
}
func costTestHub(t *testing.T) *Hub {
	t.Helper()
	h, e := Open(Config{Root: t.TempDir(), Offline: true, Mail: &MailConfig{To: "observer@example.invalid"}})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { h.Store.Close() })
	return h
}
func costSeed(t *testing.T, h *Hub, at time.Time, f CostFrame, initial bool) {
	t.Helper()
	e := h.Store.onchain.ingest(context.Background(), costBundle{Frames: []CostFrame{f}, Prices: []CostPrice{{Date: f.Date, Value: f.Price}}}, at, initial)
	if e != nil {
		t.Fatal(e)
	}
}
func costSeedFeed(t *testing.T, h *Hub, now time.Time) {
	t.Helper()
	e := h.Store.onchain.save(context.Background(), "feed", CostFeed{LastCheck: &now, LastDate: costDate(now.AddDate(0, 0, -1))})
	if e != nil {
		t.Fatal(e)
	}
}
func TestCostBoundsAndExactBuckets(t *testing.T) {
	c := CostCohort{"0", "100", []string{"10", "20", "30"}, "60"}
	for _, tc := range []struct{ lo, hi, lower, upper string }{{"100", "200", "20", "20"}, {"50", "250", "20", "60"}, {"300", "400", "0", "0"}, {"0", "100", "10", "10"}} {
		b := costInterval(c, dec(tc.lo), dec(tc.hi))
		if b.Lower != tc.lower || b.Upper != tc.upper {
			t.Fatal(tc, b)
		}
	}
	f := costTestFrame("2026-10-03")
	bins, step, e := costBins(f, "2000")
	if e != nil {
		t.Fatal(e)
	}
	if step != "1000" {
		t.Fatal("unaligned source origin must fall back", step)
	}
	sum := dec("0")
	for _, b := range bins {
		sum = sum.Add(dec(b.Total))
	}
	if !sum.Equal(dec(f.STH.Total).Add(dec(f.LTH.Total))) {
		t.Fatal("nonconserving aggregation")
	}
	f.STH.Start = "64000"
	f.LTH.Start = "64000"
	_, step, e = costBins(f, "2000")
	if e != nil || step != "2000" {
		t.Fatal(step, e)
	}
	c.Total = "59"
	if validateCostCohort(c) == nil {
		t.Fatal("bad sum")
	}
	c.Total = "60"
	c.Values = nil
	if validateCostCohort(c) == nil {
		t.Fatal("missing treated as zero")
	}
}
func TestCostFixedWindowAndDenominator(t *testing.T) {
	f := costTestFrame("2026-10-03")
	before := costConcentration(f, "5")
	g := f
	g.Price = "77000"
	after := costConcentration(g, "5")
	if before == after {
		t.Fatal("fixture requires moving window effect")
	}
	a, b := costIntervalSupply(f, dec("76000"), dec("79000")), costIntervalSupply(g, dec("76000"), dec("79000"))
	if a != b {
		t.Fatal("price altered stock")
	}
	if d := costDifference(a, b); d.Total.Lower != "0" || d.Total.Upper != "0" {
		t.Fatal(d)
	}
	for _, z := range costZones(f) {
		if !dec(z.High).Sub(dec(z.Low)).Equal(dec("3000")) {
			t.Fatal(z)
		}
	}
}
func TestCostVolatilityRequiresContinuousDailyPrices(t *testing.T) {
	prices := map[string]string{}
	end := testTime("2026-10-03T00:00:00Z")
	for i := 0; i < 22; i++ {
		prices[costDate(end.AddDate(0, 0, -i))] = "100"
	}
	if v := costVolatility("2026-10-03", prices); v == nil || *v != 0 {
		t.Fatal(v)
	}
	delete(prices, "2026-09-20")
	if costVolatility("2026-10-03", prices) != nil {
		t.Fatal("gap interpolated")
	}
}
func TestCostWeeklyBaselineDoesNotCountDailyDuplicates(t *testing.T) {
	f := costTestFrame("2026-10-03")
	summaries := []costSummary{}
	at, _ := costDay(f.Date)
	for i := 1; i <= 365; i++ {
		d := at.AddDate(0, 0, -i)
		summaries = append(summaries, costSummary{Date: costDate(d), Method: onchainMethod, Concentration: map[string]CostBounds{"5": {Lower: "1", Upper: "1"}}})
	}
	m := costMetricsFromSummaries(f, summaries, nil)
	if m.BaselineSamples != 52 || m.ConcentrationRank == nil {
		t.Fatal(m.BaselineSamples)
	}
	m = costMetricsFromSummaries(f, summaries[:280], nil)
	if m.ConcentrationRank != nil {
		t.Fatal("40 weeks insufficient")
	}
}
func TestCostRevisionAvailabilityAndTransaction(t *testing.T) {
	h := costTestHub(t)
	ctx := context.Background()
	now := testTime("2026-10-04T08:00:00Z")
	f := costTestFrame("2026-10-03")
	costSeed(t, h, now, f, true)
	costSeed(t, h, now.Add(time.Hour), f, false)
	got, e := h.Store.onchain.frame(ctx, f.Date, now.Add(2*time.Hour))
	if e != nil || got == nil || !got.FirstSeen.Equal(now) || got.Origin != "imported" {
		t.Fatal(got, e)
	}
	f.Price = "86000"
	costSeed(t, h, now.Add(2*time.Hour), f, false)
	old, _ := h.Store.onchain.frame(ctx, f.Date, now.Add(time.Hour))
	if old.Price != "85000" {
		t.Fatal("revised history backdated")
	}
	latest, _ := h.Store.onchain.frame(ctx, f.Date, now.Add(3*time.Hour))
	if latest.Price != "86000" {
		t.Fatal(latest)
	}
	_, e = h.Store.onchain.db.Exec("CREATE TRIGGER fail_frame BEFORE INSERT ON frames_v2 BEGIN SELECT RAISE(ABORT,'injected'); END")
	if e != nil {
		t.Fatal(e)
	}
	f.Date = "2026-10-04"
	e = h.Store.onchain.ingest(ctx, costBundle{Frames: []CostFrame{f}, Prices: []CostPrice{{Date: f.Date, Value: "87000"}}}, now.Add(24*time.Hour), false)
	if e == nil {
		t.Fatal("expected atomic failure")
	}
	p, _ := h.Store.onchain.prices(ctx, now.Add(48*time.Hour))
	if _, ok := p[f.Date]; ok {
		t.Fatal("price partially committed")
	}
}
func TestCostCycleFrozenNoLookaheadGapAndInvalidation(t *testing.T) {
	now := testTime("2026-10-04T08:00:00Z")
	f := costTestFrame("2026-10-03")
	m := CostMetrics{Zones: []CostZone{{Side: "above", Low: "86000", High: "89000", Supply: "2000"}}}
	s := costState{}
	costAdvance(&s, f, m, now, true)
	if s.From != "2026-10-05" || s.Through != "2026-10-11" {
		t.Fatal(s)
	}
	f.Date = "2026-10-04"
	f.Price = "90000"
	if ev := costAdvance(&s, f, m, now.AddDate(0, 0, 1), false); len(ev) != 0 {
		t.Fatal("partial day triggered", ev)
	}
	f.Date = "2026-10-05"
	ev := costAdvance(&s, f, m, now.AddDate(0, 0, 2), false)
	if len(ev) != 1 || ev[0].Kind != "pending" {
		t.Fatal(ev)
	}
	m.Zones[0].High = "99000"
	f.Date = "2026-10-06"
	ev = costAdvance(&s, f, m, now.AddDate(0, 0, 3), false)
	if len(ev) != 1 || ev[0].Kind != "confirmed" || ev[0].Zone.High != "89000" {
		t.Fatal("moved frozen boundary", ev)
	}
	f.Date = "2026-10-07"
	f.Price = "88500"
	ev = costAdvance(&s, f, m, now.AddDate(0, 0, 4), false)
	if len(ev) != 1 || ev[0].Kind != "invalidated" {
		t.Fatal(ev)
	}
	if len(costAdvance(&s, f, m, now.AddDate(0, 0, 4), false)) != 0 {
		t.Fatal("duplicate transition")
	}
	s = costState{}
	f.Date = "2026-10-03"
	costAdvance(&s, f, m, now, true)
	f.Price = "110000"
	f.Date = "2026-10-05"
	costAdvance(&s, f, m, now.AddDate(0, 0, 2), false)
	f.Date = "2026-10-07"
	ev = costAdvance(&s, f, m, now.AddDate(0, 0, 4), false)
	for _, e := range ev {
		if e.Kind == "confirmed" {
			t.Fatal("gap counted as second day")
		}
	}
}
func TestCostCompressionInitializationAndRearm(t *testing.T) {
	yes := 10.
	m := CostMetrics{ConcentrationRank: &CostBounds{"90", "90"}, VolatilityRank: &yes}
	s := costState{}
	now := testTime("2026-10-04T08:00:00Z")
	f := costTestFrame("2026-10-03")
	costAdvance(&s, f, m, now, true)
	for i := 1; i <= 3; i++ {
		f.Date = costDate(testTime("2026-10-03T00:00:00Z").AddDate(0, 0, i))
		for _, e := range costAdvance(&s, f, m, now.AddDate(0, 0, i), false) {
			if e.Kind == "concentrated" {
				t.Fatal("initial state replayed")
			}
		}
	}
	m.ConcentrationRank = &CostBounds{"10", "10"}
	for i := 4; i <= 5; i++ {
		f.Date = costDate(testTime("2026-10-03T00:00:00Z").AddDate(0, 0, i))
		costAdvance(&s, f, m, now.AddDate(0, 0, i), false)
	}
	if s.Compression {
		t.Fatal("not rearmed")
	}
	m.ConcentrationRank = &CostBounds{"90", "90"}
	count := 0
	for i := 6; i <= 7; i++ {
		f.Date = costDate(testTime("2026-10-03T00:00:00Z").AddDate(0, 0, i))
		for _, e := range costAdvance(&s, f, m, now.AddDate(0, 0, i), false) {
			if e.Kind == "concentrated" {
				count++
			}
		}
	}
	if count != 1 {
		t.Fatal(count)
	}
}
func TestCostFreshnessGraceAnd304Clock(t *testing.T) {
	now := testTime("2026-10-04T05:59:00Z")
	f := CostFeed{LastDate: "2026-10-02", LastCheck: &now}
	if s, _ := costFeedStatus(f, now); s != "fresh" {
		t.Fatal(s)
	}
	if s, _ := costFeedStatus(f, now.Add(time.Minute)); s != "delayed" {
		t.Fatal(s)
	}
	f.LastDate = "2026-10-03"
	if s, _ := costFeedStatus(f, now.Add(4*time.Hour)); s != "unreachable" {
		t.Fatal(s)
	}
	f.LastCheck = flowPtr(now.Add(4 * time.Hour))
	f.LastDate = "2026-10-02"
	if s, _ := costFeedStatus(f, *f.LastCheck); s != "delayed" {
		t.Fatal("304 advanced source date")
	}
}
func TestCostSnapshotImportNoHistoricalAlert(t *testing.T) {
	h := costTestHub(t)
	ctx := context.Background()
	now := testTime("2026-10-04T08:00:00Z")
	f := costTestFrame("2026-10-03")
	costSeed(t, h, now, f, true)
	costSeedFeed(t, h, now)
	if e := h.evaluateCostDay(ctx, now, true); e != nil {
		t.Fatal(e)
	}
	if e := h.evaluateCostDay(ctx, now.Add(time.Hour), false); e != nil {
		t.Fatal(e)
	}
	var n int
	h.Store.onchain.db.QueryRow("SELECT count(*) FROM events").Scan(&n)
	if n != 1 {
		t.Fatal(n)
	}
	h.Store.onchain.db.QueryRow("SELECT count(*) FROM outbox").Scan(&n)
	if n != 0 {
		t.Fatal("initial email")
	}
}
func TestCostMailOwnershipSharedLimitAndAtMostOnce(t *testing.T) {
	h := costTestHub(t)
	h.offline = false
	ctx := context.Background()
	now := time.Now().UTC()
	h.boot = now.Add(-time.Hour)
	costSeedFeed(t, h, now)
	h.Store.onchain.save(ctx, "settings", CostSettings{true})
	var sends atomic.Int32
	h.mailSend = func(context.Context, MailConfig, string, string) error {
		sends.Add(1)
		return errors.New("uncertain transport")
	}
	event := CostEvent{ID: "new-cost", Kind: "confirmed", Date: costDate(now.AddDate(0, 0, -1)), DetectedAt: now, Rules: OnchainRules, Price: "85000"}
	costPrepareNotice(t, h, &event, now)
	tx, e := h.Store.onchain.db.BeginTx(ctx, nil)
	if e != nil {
		t.Fatal(e)
	}
	if e = costInsertEvent(ctx, tx, event, true); e != nil {
		t.Fatal(e)
	}
	tx.Commit()
	// Legacy process must not read, suppress, decode or send our payload.
	raw, _ := json.Marshal(event)
	_, e = h.Store.research.Exec("INSERT INTO notices(id,signal_id,kind,created,status,payload) VALUES(?,?,?,?,?,?)", "cost-new-cost", event.ID, "onchain-cost:confirmed", now.Unix(), "pending", raw)
	if e != nil {
		t.Fatal(e)
	}
	if e = h.processNotices(ctx, now); e != nil {
		t.Fatal(e)
	}
	var status string
	h.Store.research.QueryRow("SELECT status FROM notices WHERE id='cost-new-cost'").Scan(&status)
	if status != "pending" || sends.Load() != 0 {
		t.Fatal(status, sends.Load())
	}
	_ = h.processCostNotices(ctx, now)
	_ = h.processCostNotices(ctx, now.Add(time.Minute))
	if sends.Load() != 1 {
		t.Fatal("uncertain mail retried", sends.Load())
	}
	h.Store.research.QueryRow("SELECT status FROM notices WHERE id='cost-new-cost'").Scan(&status)
	if status != "delivery_unknown" {
		t.Fatal(status)
	}
	for i := 0; i < 5; i++ {
		_, e = h.Store.research.Exec("INSERT INTO mail_batches VALUES(?,?)", fmt.Sprint("prior", i), now.UnixNano())
		if e != nil {
			t.Fatal(e)
		}
	}
	attempts, e := h.mailAttempts(ctx, now.Add(time.Second))
	if e != nil || attempts != 6 {
		t.Fatal(attempts, e)
	}
}
func TestCostForwardOutcomeUsesAfterDetectionAndFreezesMissing(t *testing.T) {
	now := testTime("2026-10-04T13:00:00Z")
	event := CostEvent{ID: "e", Kind: "confirmed", DetectedAt: now}
	prices := map[string]string{"2026-10-03": "1", "2026-10-04": "100"}
	for i := 1; i <= 7; i++ {
		prices[costDate(now.AddDate(0, 0, i))] = fmt.Sprint(100 + i)
	}
	r := costOutcome(event, 7, prices, now.AddDate(0, 0, 8))
	if r.Status != "complete" || math.Abs(*r.Return-7) > 1e-8 || r.From != "2026-10-05" {
		t.Fatal(r)
	}
	delete(prices, "2026-10-09")
	r = costOutcome(event, 7, prices, now.AddDate(0, 0, 8))
	if r.Status != "missing" || r.Return != nil {
		t.Fatal(r)
	}
	h := costTestHub(t)
	ctx := context.Background()
	tx, _ := h.Store.onchain.db.BeginTx(ctx, nil)
	costInsertEvent(ctx, tx, event, false)
	tx.Commit()
	if e := h.processCostResearch(ctx, now.AddDate(0, 0, 8)); e != nil {
		t.Fatal(e)
	}
	var raw []byte
	h.Store.onchain.db.QueryRow("SELECT payload FROM results WHERE event_id='e' AND horizon=7").Scan(&raw)
	if !bytes.Contains(raw, []byte(`"missing"`)) {
		t.Fatal(string(raw))
	}
}
func TestCostLocalReadsAndBackupRestore(t *testing.T) {
	h := costTestHub(t)
	now := time.Now().UTC()
	f := costTestFrame(costDate(now.AddDate(0, 0, -1)))
	costSeed(t, h, now, f, true)
	costSeedFeed(t, h, now)
	ctx := context.Background()
	h.Scheduler.fetch = func(context.Context, Dataset) ([]byte, error) { t.Fatal("GET spent quota"); return nil, nil }
	for i := 0; i < 100; i++ {
		b, e := h.Read(ctx, "onchain-cost", url.Values{})
		if e != nil || !bytes.Contains(b, []byte(f.Date)) {
			t.Fatal(e)
		}
	}
	for _, q := range []url.Values{{"asset": {"ETH"}}, {"step": {"0"}}, {"low": {"10"}, "high": {"1"}}, {"date": {"oops"}}} {
		if _, e := h.Read(ctx, "onchain-cost", q); e == nil {
			t.Fatal("bad parameters", q)
		}
	}
	backup := filepath.Join(t.TempDir(), "onchain.sqlite")
	if e := BackupFile(ctx, h.Store.onchain.path, backup); e != nil {
		t.Fatal(e)
	}
	db, e := database(backup)
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	s := costStore{db: db, path: backup}
	got, e := s.frame(ctx, f.Date, now)
	if e != nil || got == nil || got.Price != f.Price {
		t.Fatal(got, e)
	}
}
func costTestSource(t *testing.T, date string) []byte {
	t.Helper()
	day, _ := costDay(date)
	f := costTestFrame(date)
	p := func(c CostCohort) *costPacked {
		x, _ := json.Marshal(map[string]any{"start": json.Number(c.Start), "step": json.Number(c.Step), "values": func() []json.Number {
			out := []json.Number{}
			for _, v := range c.Values {
				out = append(out, json.Number(v))
			}
			return out
		}()})
		return &costPacked{T0: day.UnixMilli(), N: 1, V: []json.Number{json.Number(c.Total)}, X: map[string]json.RawMessage{"0": x}}
	}
	desc, _ := json.Marshal(onchainSourceDescription)
	charts := []costSourceChart{{ID: 1056, Description: desc, Series: []costSourceSeries{{Key: "sth_supply", PD: p(f.STH)}, {Key: "lth_supply", PD: p(f.LTH)}}}, {ID: 118, Series: []costSourceSeries{{Key: "price", PD: &costPacked{T0: day.UnixMilli(), N: 1, V: []json.Number{"85000"}}}}}}
	b, e := json.Marshal(charts)
	if e != nil {
		t.Fatal(e)
	}
	return b
}
func TestCostSourceContractAndBoundedHTTP(t *testing.T) {
	now := testTime("2026-10-04T08:00:00Z")
	b := costTestSource(t, "2026-10-03")
	got, e := parseCostBundle(bytes.NewReader(b), false, now)
	if e != nil || len(got.Frames) != 1 {
		t.Fatal(e)
	}
	for _, bad := range [][]byte{[]byte(`{}`), append(append([]byte{}, b...), []byte(`{}`)...), bytes.ReplaceAll(b, []byte(`"n":1`), []byte(`"n":2`))} {
		if _, e = parseCostBundle(bytes.NewReader(bad), false, now); e == nil {
			t.Fatal("malformed accepted")
		}
	}
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Header.Get("Cookie") != "" || r.Header.Get("Authorization") != "" {
			t.Error("private credentials")
		}
		if r.Header.Get("If-None-Match") == `"same"` {
			w.WriteHeader(304)
			return
		}
		w.Header().Set("ETag", `"same"`)
		w.Header().Set("Content-Encoding", "gzip")
		z := gzip.NewWriter(w)
		z.Write(b)
		z.Close()
	}))
	defer srv.Close()
	out, unchanged, e := fetchCostBundle(context.Background(), srv.Client(), srv.URL, "", false, now)
	if e != nil || unchanged || len(out.Frames) != 1 {
		t.Fatal(e)
	}
	_, unchanged, e = fetchCostBundle(context.Background(), srv.Client(), srv.URL, out.ETag, false, now)
	if e != nil || !unchanged || calls != 2 {
		t.Fatal(e, calls)
	}
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "7200")
		w.WriteHeader(429)
	}))
	defer srv2.Close()
	_, _, e = fetchCostBundle(context.Background(), srv2.Client(), srv2.URL, "", false, now)
	var he *costHTTPError
	if !errors.As(e, &he) || he.Retry != 2*time.Hour {
		t.Fatal(e)
	}
}
func TestCostRealPublicContract(t *testing.T) {
	path := os.Getenv("TIDAL_COST_CONTRACT")
	if path == "" {
		t.Skip("opt-in captured public contract; never committed")
	}
	raw, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	manifestRaw, e := os.ReadFile("testdata/onchain-contract-manifest.json")
	if e != nil {
		t.Fatal(e)
	}
	var manifest struct {
		SHA256 string `json:"sha256"`
	}
	if e = json.Unmarshal(manifestRaw, &manifest); e != nil {
		t.Fatal(e)
	}
	hash := sha256.Sum256(raw)
	if hex.EncodeToString(hash[:]) != manifest.SHA256 {
		t.Fatal("captured contract differs from the versioned provenance fixture")
	}
	f, e := os.Open(path)
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	b, e := parseCostBundle(f, true, time.Now().UTC())
	if e != nil {
		t.Fatal(e)
	}
	var found *CostFrame
	for i := range b.Frames {
		if b.Frames[i].Date == "2026-10-03" {
			found = &b.Frames[i]
		}
	}
	if found == nil {
		t.Fatal("known observation absent")
	}
	if found.Price != "84746.87" {
		t.Fatal(found.Price)
	}
	if x := costIntervalSupply(*found, dec("62000"), dec("65000")); x.STH.Lower != "1075881" || x.STH.Upper != x.STH.Lower {
		t.Fatal(x)
	}
	if x := costIntervalSupply(*found, dec("84000"), dec("87000")); x.LTH.Lower != "1007752" {
		t.Fatal(x)
	}
	if x := costIntervalSupply(*found, dec("83000"), dec("85000")); x.Total.Lower != "1514998" {
		t.Fatal(x)
	}
	c := costConcentration(*found, "5")
	if math.Abs(dec(c.Lower).InexactFloat64()-12.631393) > 1e-5 {
		t.Fatal(c)
	}
	for _, f := range b.Frames {
		if f.Date == "2026-08-01" {
			t.Fatal("missing date invented")
		}
	}
	if len(b.Prices) < 5900 {
		t.Fatal("full daily series missing")
	}
	t.Logf("public contract: %d cost snapshots, %d daily prices, concentration %s", len(b.Frames), len(b.Prices), c.Lower)
}

func TestCostSourceWorkingSet(t *testing.T) {
	path := os.Getenv("TIDAL_COST_CONTRACT")
	if path == "" {
		t.Skip("opt-in public-source allocation measurement")
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	var peak atomic.Uint64
	done, stopped := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(stopped)
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-done:
				return
			case <-tick.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				for old := peak.Load(); m.HeapAlloc > old && !peak.CompareAndSwap(old, m.HeapAlloc); old = peak.Load() {
				}
			}
		}
	}()
	b, err := parseCostBundle(f, true, time.Now().UTC())
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	close(done)
	<-stopped
	runtime.KeepAlive(b)
	if err != nil {
		t.Fatal(err)
	}
	additional := max(peak.Load(), after.HeapAlloc) - before.HeapAlloc
	t.Logf("onchain source peak additional active heap %.2f MiB; retained %.2f MiB", float64(additional)/(1<<20), float64(after.HeapAlloc-before.HeapAlloc)/(1<<20))
	if additional > 32<<20 {
		t.Fatalf("onchain 32 MiB additional working-set target exceeded")
	}
}

// Real public snapshot only; opt-in output is restricted to an ignored QA root.
func TestCostBrowserFixture(t *testing.T) {
	root := os.Getenv("TIDAL_COST_QA_ROOT")
	if root == "" {
		t.Skip("opt-in isolated browser fixture")
	}
	if !filepath.IsAbs(root) || !strings.Contains(root, "/tmp/onchain-qa/") {
		t.Fatal("isolated QA root required")
	}
	f, e := os.Open(os.Getenv("TIDAL_COST_CONTRACT"))
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	now := time.Now().UTC()
	bundle, e := parseCostBundle(f, true, now)
	if e != nil {
		t.Fatal(e)
	}
	h, e := Open(Config{Root: root, Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	ctx := context.Background()
	if e = h.Store.onchain.ingest(ctx, bundle, now, true); e != nil {
		t.Fatal(e)
	}
	latest, e := h.Store.onchain.frame(ctx, "", now)
	if e != nil {
		t.Fatal(e)
	}
	if e = h.Store.onchain.save(ctx, "feed", CostFeed{LastCheck: &now, LastDate: latest.Date}); e != nil {
		t.Fatal(e)
	}
	if e = h.evaluateCostDay(ctx, now, true); e != nil {
		t.Fatal(e)
	}
	t.Log("real public snapshot imported to isolated browser root")
}

func TestCostVersionGzipAndCollectorRecovery(t *testing.T) {
	ctx := context.Background()
	now := testTime("2026-10-04T08:00:00Z")
	body := costTestSource(t, "2026-10-03")
	version, hits, fail := "one", 0, 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fail != 0 {
			w.WriteHeader(fail)
			return
		}
		w.Header().Set("Content-Encoding", "gzip")
		z := gzip.NewWriter(w)
		defer z.Close()
		if r.URL.Path == "/version" {
			fmt.Fprintf(z, `{"version":%q}`, version)
			return
		}
		hits++
		w.Header().Set("ETag", `"initial"`)
		z.Write(body)
	}))
	defer srv.Close()
	h := costTestHub(t)
	if _, e := h.pollOnchain(ctx, srv.Client(), srv.URL, now); e != nil {
		t.Fatal(e)
	}
	f, e := h.Store.onchain.frame(ctx, "", now)
	if e != nil || f == nil || f.Origin != "imported" {
		t.Fatal(f, e)
	}
	if _, e := h.pollOnchain(ctx, srv.Client(), srv.URL, now.Add(time.Hour)); e != nil {
		t.Fatal(e)
	}
	// Test bundle only contains one close, so the first next-day recovery attempt is
	// allowed; duplicate hours on the same UTC date must not repeatedly full-fetch.
	expectedHits := hits
	if _, e := h.pollOnchain(ctx, srv.Client(), srv.URL, now.Add(2*time.Hour)); e != nil {
		t.Fatal(e)
	}
	if hits != expectedHits {
		t.Fatal("same-version request without a recovery need", hits)
	}
	fail = 403
	if _, e = h.pollOnchain(ctx, srv.Client(), srv.URL, now.Add(3*time.Hour)); e == nil {
		t.Fatal("auth change ignored")
	}
	feed, _ := h.costFeed(ctx)
	if !feed.Disabled {
		t.Fatal("automatic authentication retries remain enabled")
	}
	fail = 0
	version = "two"
	if _, e = h.pollOnchain(ctx, srv.Client(), srv.URL, now.Add(4*time.Hour)); e != nil || hits != expectedHits {
		t.Fatal("disabled source retried", e, hits)
	}
	last, _ := h.Store.onchain.frame(ctx, "", now.Add(4*time.Hour))
	if last == nil || last.Revision != f.Revision {
		t.Fatal("last valid frame lost")
	}
}

func TestCostUncertainRankDoesNotRearm(t *testing.T) {
	rank := 10.
	m := CostMetrics{ConcentrationRank: &CostBounds{"79", "81"}, VolatilityRank: &rank}
	if costCompressed(m) != nil {
		t.Fatal("uncertain boundary classified as false")
	}
	s := costState{LastDate: "2026-10-01", Compression: true, Clear: 1}
	f := costTestFrame("2026-10-02")
	costAdvance(&s, f, m, testTime("2026-10-03T08:00:00Z"), false)
	if !s.Compression || s.Clear != 0 {
		t.Fatal("uncertain day rearmed condition", s)
	}
}

func TestCostCapacityIsolationAndRollbackQuiesce(t *testing.T) {
	h := costTestHub(t)
	ctx := context.Background()
	now := time.Now().UTC()
	costSeed(t, h, now, costTestFrame(costDate(now.AddDate(0, 0, -1))), true)
	st := h.Store.onchain
	original := st.path
	pressure := filepath.Join(t.TempDir(), "capacity.sqlite")
	f, e := os.Create(pressure)
	if e != nil {
		t.Fatal(e)
	}
	if e = f.Truncate(onchainBudget * 95 / 100); e != nil {
		t.Fatal(e)
	}
	f.Close()
	st.path = pressure
	if e = st.ingest(ctx, costBundle{Frames: []CostFrame{costTestFrame(costDate(now.AddDate(0, 0, -2)))}}, now, false); e == nil {
		t.Fatal("capacity must pause writes")
	}
	if _, e = st.frame(ctx, "", now); e != nil {
		t.Fatal("capacity blocked reads", e)
	}
	if _, e = h.SetCostSettings(ctx, CostSettings{EmailEnabled: false}, now); e != nil {
		t.Fatal("cannot disable mail at capacity", e)
	}
	st.path = original
	event := CostEvent{ID: "rollback", Kind: "confirmed", Date: costDate(now), DetectedAt: now}
	tx, _ := st.db.BeginTx(ctx, nil)
	if e = costInsertEvent(ctx, tx, event, true); e != nil {
		t.Fatal(e)
	}
	if e = tx.Commit(); e != nil {
		t.Fatal(e)
	}
	_, e = h.Store.research.Exec("INSERT INTO notices(id,kind,status,created) VALUES('cost-rollback','onchain-cost:confirmed','pending',?),('old-keep','flow:confirmed','pending',?)", now.Unix(), now.Unix())
	if e != nil {
		t.Fatal(e)
	}
	if e = QuiesceOnchain(ctx, h.Store.root); e != nil {
		t.Fatal(e)
	}
	if e = QuiesceOnchain(ctx, h.Store.root); e != nil {
		t.Fatal("quiesce not idempotent", e)
	}
	var status string
	st.db.QueryRow("SELECT status FROM outbox WHERE id='rollback'").Scan(&status)
	if status != "suppressed_rollback" {
		t.Fatal(status)
	}
	h.Store.research.QueryRow("SELECT status FROM notices WHERE id='cost-rollback'").Scan(&status)
	if status != "suppressed_rollback" {
		t.Fatal(status)
	}
	h.Store.research.QueryRow("SELECT status FROM notices WHERE id='old-keep'").Scan(&status)
	if status != "pending" {
		t.Fatal("old notice changed", status)
	}
	var n int
	st.db.QueryRow("SELECT count(*) FROM events").Scan(&n)
	if n != 1 {
		t.Fatal("audit removed")
	}
}

func TestCostResearchMatureResultsNotStarved(t *testing.T) {
	h := costTestHub(t)
	ctx := context.Background()
	now := testTime("2026-10-04T08:00:00Z")
	tx, _ := h.Store.onchain.db.BeginTx(ctx, nil)
	// Many earlier 60-day observations are still waiting. A newer seven-day
	// observation is already due and must be finalized in this bounded pass.
	for i := 0; i < 70; i++ {
		e := CostEvent{ID: fmt.Sprint("old", i), Kind: "confirmed", DetectedAt: now.AddDate(0, 0, -40).Add(time.Duration(i) * time.Second)}
		if err := costInsertEvent(ctx, tx, e, false); err != nil {
			t.Fatal(err)
		}
		for _, n := range []int{7, 14, 30} {
			tx.Exec("INSERT INTO results VALUES(?,?,?)", e.ID, n, `{}`)
		}
	}
	e := CostEvent{ID: "new-due", Kind: "confirmed", DetectedAt: now.AddDate(0, 0, -9)}
	if err := costInsertEvent(ctx, tx, e, false); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := h.processCostResearch(ctx, now); err != nil {
		t.Fatal(err)
	}
	var b []byte
	if err := h.Store.onchain.db.QueryRow("SELECT payload FROM results WHERE event_id='new-due' AND horizon=7").Scan(&b); err != nil {
		t.Fatal("mature task starved", err)
	}
	var result CostResult
	json.Unmarshal(b, &result)
	if result.Status != "missing" {
		t.Fatal(result)
	}
}

func TestCostZoneBoundedExtremePriceAndOffsetGrid(t *testing.T) {
	f := costTestFrame("2026-10-03")
	f.Price = "1000000000000"
	started := time.Now()
	if zones := costZones(f); len(zones) != 0 {
		t.Fatal(zones)
	}
	if time.Since(started) > time.Second {
		t.Fatal("zone work followed price span instead of bucket count")
	}
	f.Price = "85000"
	f.STH.Start = "65500"
	f.LTH.Start = "65500"
	bins, actual, e := costBins(f, "1000")
	if e != nil || actual != "1000" || bins[0].Low != "65500" {
		t.Fatal("native origin lost", bins, actual, e)
	}
	if zones := costZones(f); len(zones) != 0 {
		t.Fatal("shifted native grid generated fixed-grid candidates", zones)
	}
}

func TestCostEvidenceCoverageUnitsAndUnknownFunding(t *testing.T) {
	h := costTestHub(t)
	now := time.Now().UTC()
	day := now.Truncate(24*time.Hour).AddDate(0, 0, -1)
	flow, _ := h.Dataset(ID("flow", "BTC", "", "spot"))
	for i := 0; i < 288; i++ {
		at := day.Add(time.Duration(i) * 5 * time.Minute)
		o := Observation{Dataset: flow.ID, Source: flow.Source, ObservedAt: &at, FetchedAt: now, Resolution: 300, Quality: "valid", Payload: Payload{Flow: &Flow{Buy: "123.45", Sell: "20.01"}}}
		if _, e := h.Store.Ingest(flow, o); e != nil {
			t.Fatal(e)
		}
	}
	fd, _ := h.Dataset(ID("funding", "ALL", "", "futures"))
	at := day.Add(16 * time.Hour)
	if _, e := h.Store.Ingest(fd, Observation{Dataset: fd.ID, Source: fd.Source, ObservedAt: &at, FetchedAt: now, Quality: "valid", Payload: Payload{Funding: []Funding{{Asset: "BTC", Venue: "Example", RatePercent: "0.01"}}}}); e != nil {
		t.Fatal(e)
	}
	evid := h.costEvidence(context.Background(), costDate(day), time.Now().Add(time.Minute))
	for _, e := range evid {
		switch e.Kind {
		case "flow":
			if e.Coverage == nil || *e.Coverage != 1 || e.Status != "partial" {
				t.Fatal("temporal coverage became claimed common-source coverage", e)
			}
			values := e.Data.(map[string]any)
			if values["buyUsd"] != "35553.6" || values["sellUsd"] != "5762.88" || values["netUsd"] != "29790.72" {
				t.Fatal("BTC/USD/cents conflated", e)
			}
		case "funding":
			if e.Status != "partial" {
				t.Fatal("unknown funding interval/type accepted", e)
			}
		case "etf":
			if e.Status != "missing" || e.Data != nil {
				t.Fatal("absent report filled with zero", e)
			}
		}
	}
}

func TestCostSameSecondRestartAndDurableNoticeResult(t *testing.T) {
	h := costTestHub(t)
	h.offline = false
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second).Add(900 * time.Millisecond)
	h.boot = now.Add(-800 * time.Millisecond)
	costSeedFeed(t, h, now)
	if e := h.Store.onchain.save(ctx, "settings", CostSettings{EmailEnabled: true}); e != nil {
		t.Fatal(e)
	}
	sends := 0
	h.mailSend = func(context.Context, MailConfig, string, string) error { sends++; return nil }
	event := CostEvent{ID: "new-same-second", Kind: "confirmed", DetectedAt: now, Date: costDate(now.AddDate(0, 0, -1)), Rules: OnchainRules}
	costPrepareNotice(t, h, &event, now)
	tx, _ := h.Store.onchain.db.BeginTx(ctx, nil)
	if e := costInsertEvent(ctx, tx, event, true); e != nil {
		t.Fatal(e)
	}
	tx.Commit()
	if e := h.processCostNotices(ctx, now); e != nil {
		t.Fatal(e)
	}
	if sends != 1 {
		t.Fatal("fresh post-boot event suppressed", sends)
	}
	var status string
	h.Store.onchain.db.QueryRow("SELECT status FROM outbox WHERE id=?", event.ID).Scan(&status)
	if status != "sent" {
		t.Fatal("delivery outcome not archived", status)
	}
	if _, e := h.Store.research.Exec("DELETE FROM notices WHERE id=?", "cost-"+event.ID); e != nil {
		t.Fatal(e)
	}
	body, e := h.costReadLocal(ctx, "onchain-cost/events", url.Values{}, now)
	if e != nil || !bytes.Contains(body, []byte(`"noticeStatus":"sent"`)) {
		t.Fatal("shared cleanup erased audit outcome", string(body), e)
	}
}

func TestCostHeatmapZeroSupplyIsNotMissingDate(t *testing.T) {
	h := costTestHub(t)
	now := time.Now().UTC()
	ctx := context.Background()
	for _, days := range []int{3, 1} {
		f := costTestFrame(costDate(now.AddDate(0, 0, -days)))
		for _, c := range []*CostCohort{&f.STH, &f.LTH} {
			c.Total = dec(c.Total).Sub(dec(c.Values[20])).String()
			c.Values[20] = "0"
		}
		costSeed(t, h, now, f, true)
	}
	raw, e := h.costReadLocal(ctx, "onchain-cost/history", url.Values{"mode": {"heatmap"}}, now)
	if e != nil {
		t.Fatal(e)
	}
	var out struct {
		Cells [][3]string `json:"cells"`
	}
	if e = json.Unmarshal(raw, &out); e != nil {
		t.Fatal(e)
	}
	zeros := 0
	for _, cell := range out.Cells {
		if cell[0] == costDate(now.AddDate(0, 0, -2)) {
			t.Fatal("missing day invented", cell)
		}
		if cell[1] == "85000" {
			if cell[2] != "0" {
				t.Fatal(cell)
			}
			zeros++
		}
	}
	if zeros != 2 {
		t.Fatal("observed zero disappeared into missing", zeros)
	}
}

func TestCostStatisticsSeparateRulesAndMinimumSamples(t *testing.T) {
	h := costTestHub(t)
	ctx := context.Background()
	now := time.Now().UTC()
	tx, _ := h.Store.onchain.db.BeginTx(ctx, nil)
	defer tx.Rollback()
	for _, v := range []struct {
		rules string
		n     int
	}{{"old-rule", 30}, {OnchainRules, 29}} {
		for i := 0; i < v.n; i++ {
			at := now.AddDate(-2, 0, i*10)
			event := CostEvent{ID: fmt.Sprintf("%s-%d", v.rules, i), Kind: "confirmed", Rules: v.rules, DetectedAt: at}
			if e := costInsertEvent(ctx, tx, event, false); e != nil {
				t.Fatal(e)
			}
			r := costOutcome(event, 7, nil, now)
			r.Status = "complete"
			value := 1.
			r.Return = &value
			b, _ := json.Marshal(r)
			if _, e := tx.Exec("INSERT INTO results VALUES(?,?,?)", event.ID, 7, b); e != nil {
				t.Fatal(e)
			}
		}
	}
	if e := tx.Commit(); e != nil {
		t.Fatal(e)
	}
	view, e := h.costLegacyResearchView(ctx, now, "forward", 0, 10)
	if e != nil {
		t.Fatal(e)
	}
	groups := view.(map[string]any)["groups"].([]costResearchGroup)
	checked := 0
	for _, g := range groups {
		if g.Horizon != 7 {
			continue
		}
		checked++
		if g.Rules == OnchainRules && (g.RiseRate != nil || g.Independent != 29) {
			t.Fatal("rule mixed or statistics released early", g)
		}
		if g.Rules == "old-rule" && (g.RiseRate == nil || *g.RiseRate != 1 || len(g.Interval) != 2) {
			t.Fatal("eligible statistics absent", g)
		}
	}
	if checked != 2 {
		t.Fatal(groups)
	}
}
