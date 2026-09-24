package datahub

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func pointer(s string) *string { return &s }
func testTime(s string) time.Time {
	t, e := time.Parse(time.RFC3339, s)
	if e != nil {
		panic(e)
	}
	return t
}
func TestWalletCoverageAliasesAndIrregularIntervals(t *testing.T) {
	d, _ := FindDataset(ID("balance-history", "BTC", "", "chain"))
	now := testTime("2026-09-25T12:00:00Z")
	raw := []byte(`{"code":"0","data":{"time_list":[1790208000000,1790294400000],"data_map":{"Gate":[100,90],"Gate.io":[100,90],"KuCoin":[1000,null],"Binance":[100,120]}}}`)
	rows, e := Normalize(d, raw, now)
	if e != nil {
		t.Fatal(e)
	}
	if len(rows[0].Payload.Balances) != 3 {
		t.Fatal("alias double-counted")
	}
	c := comparableBalances(rows[0], rows[1])
	if c.Delta == nil || *c.Delta != "10" || len(c.Excluded) != 1 || len(c.Venues) != 2 {
		t.Fatalf("coverage flipped direction %+v", c)
	}
	at := rows[1].Time().Add(time.Hour)
	rows[1].ObservedAt = &at
	c = comparableBalances(rows[0], rows[1])
	if c.Hours != 25 {
		t.Fatal("irregular interval hidden")
	}
	bad := []byte(`{"code":"0","data":{"time_list":[1790208000000],"data_map":{"Gate":[100],"Gate.io":[90]}}}`)
	if _, e := Normalize(d, bad, now); e == nil {
		t.Fatal("conflicting alias accepted")
	}
	a := Observation{ObservedAt: &now, Payload: Payload{Balances: []Balance{{Venue: "A", Value: nil}}}}
	later := now.Add(time.Hour)
	b := Observation{ObservedAt: &later, Payload: Payload{Balances: []Balance{{Venue: "A", Value: pointer("3")}}}}
	if comparableBalances(a, b).Delta != nil {
		t.Fatal("null was zero-filled")
	}
}
func TestETFReportingAndReconciliation(t *testing.T) {
	now := testTime("2026-09-24T12:00:00Z")
	r := ETFRecord{Date: "2026-09-24", USD: pointer("0"), Funds: map[string]*string{"A": pointer("0")}, Reconciled: true}
	if etfState(r, now) != "unfinished" {
		t.Fatal("forming zero accepted")
	}
	if etfState(r, now.Add(24*time.Hour)) != "zero_unconfirmed" {
		t.Fatal("placeholder zero accepted")
	}
	r.Funds = map[string]*string{"A": pointer("5"), "B": pointer("-5")}
	if etfState(r, now.Add(24*time.Hour)) != "reported" {
		t.Fatal("real zero rejected")
	}
	r.Date = "2026-09-19"
	if etfState(r, now) != "non_trading_day" {
		t.Fatal("weekend treated as flow")
	}
	r.Date = "2026-09-07"
	if etfState(r, now) != "non_trading_day" {
		t.Fatal("holiday treated as flow")
	}
	for _, asset := range Assets() {
		d, _ := FindDataset(ID("etf", asset, "", "fund"))
		raw := []byte(`{"code":"0","data":[{"timestamp":1790121600000,"flow_usd":10,"etf_flows":[{"etf_ticker":"A","flow_usd":6},{"etf_ticker":"B","flow_usd":4}]}]}`)
		rows, e := Normalize(d, raw, now)
		if e != nil {
			t.Fatal(e)
		}
		if !rows[0].Payload.ETF.Reconciled {
			t.Fatal("sum mismatch")
		}
	}
	if nextETF(testTime("2026-09-24T00:29:00Z")) != testTime("2026-09-24T00:30:00Z") || nextETF(testTime("2026-09-24T00:30:00Z")) != testTime("2026-09-25T00:30:00Z") {
		t.Fatal("ETF cadence drift")
	}
}
func TestFactVintagesDoNotLeakAndSurviveBackup(t *testing.T) {
	w := testStore(t)
	d, _ := FindDataset(ID("flow", "BTC", "", "spot"))
	at := time.Now().UTC().Truncate(time.Hour)
	one := at.Add(time.Minute)
	two := one.Add(time.Minute)
	o := Observation{Dataset: d.ID, Source: d.Source, ObservedAt: &at, FetchedAt: one, Resolution: 60, Quality: "valid", Payload: Payload{Flow: &Flow{"10", "5"}}}
	if _, e := w.Ingest(d, o); e != nil {
		t.Fatal(e)
	}
	o.FetchedAt = two
	if _, e := w.Ingest(d, o); e != nil {
		t.Fatal(e)
	}
	o.Payload.Flow = &Flow{"30", "5"}
	if _, e := w.Ingest(d, o); e != nil {
		t.Fatal(e)
	}
	check := func(asOf time.Time, want string) {
		t.Helper()
		got := ""
		if e := w.FactsAsOf(context.Background(), d.ID, at, at.Add(time.Hour), asOf, func(o Observation) error { got = o.Payload.Flow.Buy; return nil }); e != nil {
			t.Fatal(e)
		}
		if got != want {
			t.Fatalf("asOf %v got %s want %s", asOf, got, want)
		}
	}
	check(at, "")
	check(one, "10")
	check(two, "30")
	p := filepath.Join(t.TempDir(), "research.sqlite")
	if e := BackupFile(context.Background(), filepath.Join(w.root, "research.sqlite"), p); e != nil {
		t.Fatal(e)
	}
	db, e := database(p)
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	var n int
	if e = db.QueryRow("SELECT count(*) FROM facts").Scan(&n); e != nil || n != 2 {
		t.Fatalf("backup missing revisions %d %v", n, e)
	}
}
func TestSharedQueriesAndStudyDedup(t *testing.T) {
	h, e := Open(Config{Root: t.TempDir(), Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	for i := 0; i < 100; i++ {
		for _, path := range []string{"signals", "wallet-trends", "etf", "studies"} {
			if _, e = h.Read(context.Background(), path, url.Values{"asset": {"ETH"}}); e != nil {
				t.Fatal(e)
			}
		}
	}
	if h.Scheduler.quota.Calls != 0 {
		t.Fatal("GET spent quota")
	}
	a, e := h.CreateStudy(StudyRequest{Asset: "BTC"})
	if e != nil {
		t.Fatal(e)
	}
	b, e := h.CreateStudy(StudyRequest{Asset: "BTC"})
	if e != nil {
		t.Fatal(e)
	}
	if a.ID != b.ID {
		t.Fatal("study duplicated")
	}
	if h.Scheduler.quota.Calls != 0 {
		t.Fatal("queue bypassed scheduler")
	}
	if len(a.Jobs) != 4 {
		t.Fatal("expected shared flow/oi/premium tasks", a)
	}
}
func TestSignalGatesSymmetryAndConfirmation(t *testing.T) {
	end := testTime("2026-09-24T12:00:00Z")
	b := SignalBaseline{Valid: true, P95: 100, P05: -100, P90: 100, P10: -100, Median15: 100, Median60: 100}
	bars := map[int64]FlowBar{}
	for i := 1; i <= 36; i++ {
		at := end.Add(-time.Duration(i) * 5 * time.Minute)
		bars[at.Unix()] = FlowBar{at, 100, 10}
	}
	pattern, complete := signalCondition(bars, end, b, "buy")
	if pattern != "burst" || !complete {
		t.Fatal("buy condition failed")
	}
	if pattern, _ := signalCondition(bars, end, b, "sell"); pattern != "" {
		t.Fatal("symmetric side false positive")
	}
	for k, v := range bars {
		v.Buy, v.Sell = v.Sell, v.Buy
		bars[k] = v
	}
	if p, _ := signalCondition(bars, end, b, "sell"); p != "burst" {
		t.Fatal("sell asymmetry")
	}
	delete(bars, end.Add(-time.Hour).Unix())
	if p, complete := signalCondition(bars, end, b, "sell"); p != "" || complete {
		t.Fatal("gap allowed")
	}
	sig := Signal{DataThrough: end, Direction: "buy", FrozenHigh: 110, FrozenLow: 90, Expires: end.Add(4 * time.Hour)}
	candles := map[int64]Candle{}
	for i := -1; i < 2; i++ {
		at := end.Add(time.Duration(i) * 5 * time.Minute)
		candles[at.Unix()] = Candle{Open: 110, High: 112, Low: 110, Close: 111}
		bars[at.Unix()] = FlowBar{at, 100, 10}
	}
	if confirms(sig, bars, candles, end.Add(5*time.Minute)) {
		t.Fatal("one candle confirmation")
	}
	if !confirms(sig, bars, candles, end.Add(10*time.Minute)) {
		t.Fatal("two candles failed")
	}
	candles[end.Unix()] = Candle{Close: 100}
	if confirms(sig, bars, candles, end.Add(10*time.Minute)) {
		t.Fatal("moving reference/false confirmation")
	}
}
func TestBaselineCoverageAndNoDuplicatedResolution(t *testing.T) {
	to := testTime("2026-09-24T00:00:00Z")
	from := to.Add(-30 * 24 * time.Hour)
	rows := []Observation{}
	for at := from; at.Before(to); at = at.Add(5 * time.Minute) {
		v := at
		rows = append(rows, Observation{ObservedAt: &v, Resolution: 300, Quality: "valid", Payload: Payload{Flow: &Flow{"100", "90"}}})
	}
	b := buildSignalBaseline(rows, from, to, to)
	if !b.Valid || b.Coverage != 1 || b.Dates != 30 {
		t.Fatalf("valid baseline rejected %+v", b)
	}
	b = buildSignalBaseline(rows[500:], from, to, to)
	if b.Valid {
		t.Fatal("<95% accepted")
	}
	at := to
	one := Observation{ObservedAt: &at, Resolution: 300, Quality: "valid", Payload: Payload{Flow: &Flow{"50", "25"}}}
	dupes := []Observation{one}
	for i := 0; i < 5; i++ {
		t := at.Add(time.Duration(i) * time.Minute)
		dupes = append(dupes, Observation{ObservedAt: &t, Resolution: 60, Quality: "valid", Payload: Payload{Flow: &Flow{"10", "5"}}})
	}
	m := flowBars(dupes, 300)
	if m[at.Unix()].Buy != 5000 {
		t.Fatal("multiple granularities double counted")
	}
}
func TestBarrierAmbiguityAndMissingNotLoss(t *testing.T) {
	at := testTime("2026-09-24T00:00:00Z")
	atr := 10.0
	c := map[int64]Candle{at.Unix(): {High: 125, Low: 85, Close: 100}}
	if outcomeAt(c, at, "buy", 100, &atr).Barrier != "ambiguous" {
		t.Fatal("same bar chose winning side")
	}
	delete(c, at.Unix())
	if outcomeAt(c, at, "buy", 100, &atr).Barrier != "incomplete" {
		t.Fatal("missing treated as loss")
	}
}
func TestObserverContractFailuresAndQuota(t *testing.T) {
	w := testStore(t)
	d, _ := FindDataset(ID("balance-list", "ETH", "", "chain"))
	calls := 0
	s := NewScheduler(w, []Dataset{d}, func(context.Context, Dataset) ([]byte, error) {
		calls++
		return []byte(`{"code":"0","data":[{"exchange_name":"Example","total_balance":"bad"}]}`), nil
	}, true, time.Now())
	s.mu.Lock()
	s.jobs[d.ID].Next = time.Time{}
	s.mu.Unlock()
	if !s.Step(context.Background(), time.Now()) {
		t.Fatal("probe not scheduled")
	}
	s.wg.Wait()
	if calls != 1 || !s.jobs[d.ID].Disabled || s.jobs[d.ID].ContractStatus != "failed" {
		t.Fatal("failed contract enabled")
	}
	if _, ok := w.Latest(d.ID); ok {
		t.Fatal("invalid observation ingested")
	}
	if s.quota.Calls != 1 {
		t.Fatal("failed probe didn't consume quota")
	}
}
func TestNoticeDedupAndNoRestartBacklog(t *testing.T) {
	h, e := Open(Config{Root: t.TempDir(), Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	now := time.Now().UTC()
	sig := Signal{ID: "one", Asset: "BTC", At: now, Direction: "buy"}
	for i := 0; i < 3; i++ {
		if e = h.queueNotice(sig, "anomaly", now); e != nil {
			t.Fatal(e)
		}
	}
	var count int
	var status string
	_ = h.Store.research.QueryRow("SELECT count(*),status FROM notices").Scan(&count, &status)
	if count != 1 || status != "unconfigured" {
		t.Fatalf("notice dedup %d %s", count, status)
	}
	h.mail = &MailConfig{}
	past := h.boot.Add(-time.Minute)
	_, e = h.Store.research.Exec("INSERT INTO notices(id,created,status,payload) VALUES('old',?,'pending','{}')", past.Unix())
	if e != nil {
		t.Fatal(e)
	}
	if e = h.processNotices(context.Background(), now); e != nil {
		t.Fatal(e)
	}
	_ = h.Store.research.QueryRow("SELECT status FROM notices WHERE id='old'").Scan(&status)
	if status != "suppressed_restart" {
		t.Fatal("restart replays mail")
	}
}
func BenchmarkSignalBaseline(b *testing.B) {
	to := time.Now().UTC().Truncate(24 * time.Hour)
	from := to.Add(-30 * 24 * time.Hour)
	rows := []Observation{}
	for at := from; at.Before(to); at = at.Add(time.Minute) {
		v := at
		rows = append(rows, Observation{ObservedAt: &v, Resolution: 60, Quality: "valid", Payload: Payload{Flow: &Flow{fmt.Sprint(100 + at.Minute()), "90"}}})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = buildSignalBaseline(rows, from, to, to)
	}
}
func TestStudyResponseJSON(t *testing.T) {
	r := StudyResult{Events: []StudyEvent{}, Missing: []string{"not complete"}}
	if _, e := json.Marshal(r); e != nil {
		t.Fatal(e)
	}
}

func TestConcurrentStudyCreationAndShortRange(t *testing.T) {
	h, e := Open(Config{Root: t.TempDir(), Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	var wg sync.WaitGroup
	ids := make(chan string, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, e := h.CreateStudy(StudyRequest{Asset: "ETH"})
			if e != nil {
				t.Error(e)
				return
			}
			ids <- s.ID
		}()
	}
	wg.Wait()
	close(ids)
	unique := map[string]bool{}
	for id := range ids {
		unique[id] = true
	}
	if len(unique) != 1 {
		t.Fatal("concurrent jobs were duplicated")
	}
	now := time.Now().UTC()
	r, e := h.evaluateStudy(context.Background(), Study{Asset: "ETH", From: now.Add(-time.Hour), To: now}, now)
	if e != nil || r.BaselineDays == 30 || len(r.Missing) == 0 {
		t.Fatal("short study passed")
	}
}

func TestResearchFullDoesNotStopLiveAndShadowNotRewritten(t *testing.T) {
	h, e := Open(Config{Root: t.TempDir(), Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	w := h.Store
	now := time.Now().UTC().Truncate(time.Minute)
	d, _ := FindDataset(ID("flow", "BTC", "", "spot"))
	w.mu.Lock()
	w.status.ResearchBytes = ResearchBudget
	w.mu.Unlock()
	o := Observation{Dataset: d.ID, Source: d.Source, ObservedAt: &now, FetchedAt: now, Resolution: 60, Quality: "valid", Payload: Payload{Flow: &Flow{"25", "10"}}}
	if _, e = w.Ingest(d, o); e != nil {
		t.Fatal(e)
	}
	if _, ok := w.Latest(d.ID); !ok {
		t.Fatal("live snapshot blocked by research cap")
	}
	var gap map[string]any
	if !w.LoadState("research/gap", &gap) {
		t.Fatal("unrecorded gap")
	}
	w.mu.Lock()
	w.status.ResearchPaused = false
	w.mu.Unlock()
	if e = h.recordShadow(context.Background(), "BTC", now, now, false, false); e != nil {
		t.Fatal(e)
	}
	if e = h.recordShadow(context.Background(), "BTC", now.Add(time.Second), now, true, true); e != nil {
		t.Fatal(e)
	}
	s, e := w.shadowSamples(context.Background(), "BTC", now.Add(-time.Hour), now.Add(time.Hour))
	if e != nil || len(s) != 1 || s[0].Eligible {
		t.Fatal("backfill rewrote live availability")
	}
}

func TestSignalAtomicLifecycleRestartAndExpiry(t *testing.T) {
	h, e := Open(Config{Root: t.TempDir(), Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	ctx := context.Background()
	end := time.Now().UTC().Truncate(5 * time.Minute)
	now := end.Add(30 * time.Second)
	h.boot = end.Add(-time.Hour)
	base := SignalBaseline{At: now, Valid: true, Coverage: 1, Dates: 30, P95: 100, P05: -100, P90: 100, P10: -100, Median15: 1, Median60: 1}
	fd, _ := h.Dataset(ID("flow", "BTC", "", "spot"))
	cd, _ := h.Dataset(ID("candles", "BTC", "Binance", "spot"))
	feed := func(from, to time.Time, px float64) {
		t.Helper()
		for at := from; at.Before(to); at = at.Add(time.Minute) {
			v := at
			o := Observation{Dataset: fd.ID, Source: fd.Source, ObservedAt: &v, FetchedAt: to.Add(30 * time.Second), Resolution: 60, Quality: "valid", Payload: Payload{Flow: &Flow{"100", "10"}}}
			if _, e = h.Store.Ingest(fd, o); e != nil {
				t.Fatal(e)
			}
			if at.Unix()%300 == 0 {
				o.Dataset = cd.ID
				o.Source = cd.Source
				o.Resolution = 300
				o.Payload = Payload{Candle: &Candle{Open: px, High: px + 1, Low: px - 1, Close: px, Volume: 100}}
				if _, e = h.Store.Ingest(cd, o); e != nil {
					t.Fatal(e)
				}
			}
		}
	}
	feed(end.Add(-4*time.Hour), end, 100)
	_ = h.Store.SaveState("signals/baseline/BTC", base)
	state := signalState{Last: end.Add(-5 * time.Minute), Active: map[string]string{}, Clear: map[string]*time.Time{}}
	if e = h.commitSignals(ctx, "BTC", state, nil, nil, now); e != nil {
		t.Fatal(e)
	}
	if e = h.processSignals(ctx, now); e != nil {
		t.Fatal(e)
	}
	var count int
	_ = h.Store.research.QueryRow("SELECT count(*) FROM notices").Scan(&count)
	if count != 1 {
		t.Fatalf("anomaly notices %d", count)
	}
	feed(end, end.Add(10*time.Minute), 110)
	if e = h.processSignals(ctx, now.Add(10*time.Minute)); e != nil {
		t.Fatal(e)
	}
	if e = h.processSignals(ctx, now.Add(10*time.Minute+time.Second)); e != nil {
		t.Fatal(e)
	}
	_ = h.Store.research.QueryRow("SELECT count(*) FROM notices").Scan(&count)
	if count != 2 {
		t.Fatalf("confirmation not deduped: %d", count)
	}
	var sig Signal
	id := fmt.Sprintf("BTC-buy-%d", end.Unix())
	if e = h.Store.document(ctx, "signal", id, &sig); e != nil || sig.State != "confirmed" || sig.FrozenHigh != 101 {
		t.Fatalf("incorrect freeze/state %+v %v", sig, e)
	}
	h.boot = now.Add(11 * time.Minute)
	feed(end.Add(10*time.Minute), end.Add(15*time.Minute), 111)
	if e = h.processSignals(ctx, now.Add(15*time.Minute)); e != nil {
		t.Fatal(e)
	}
	_ = h.Store.research.QueryRow("SELECT count(*) FROM notices").Scan(&count)
	if count != 2 {
		t.Fatal("restart replayed notice")
	}
	// An interrupted transaction must not persist a signal without its cursor.
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if e = h.commitSignals(cancelled, "BTC", state, []Signal{{ID: "bad", Asset: "BTC", At: now}}, map[string]string{"bad": "anomaly"}, now); e == nil {
		t.Fatal("cancelled commit succeeded")
	}
	if e = h.Store.document(ctx, "signal", "bad", &sig); e == nil {
		t.Fatal("partial signal survived rollback")
	}
	sig = Signal{ID: "expires", Asset: "BTC", Direction: "sell", State: "anomaly", At: now, Expires: now.Add(4 * time.Hour)}
	state = signalState{Last: end, Active: map[string]string{"sell": sig.ID}, Clear: map[string]*time.Time{}}
	if e = h.commitSignals(ctx, "BTC", state, []Signal{sig}, nil, now); e != nil {
		t.Fatal(e)
	}
	if e = h.processSignals(ctx, now.Add(4*time.Hour)); e != nil {
		t.Fatal(e)
	}
	if e = h.Store.document(ctx, "signal", "expires", &sig); e != nil || sig.State != "expired" {
		t.Fatal("stale data prevented expiry", sig.State, e)
	}
}

func TestForwardObservationRequiresFullFourteenDaysAndCountsGaps(t *testing.T) {
	h, e := Open(Config{Root: t.TempDir(), Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	start := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	if e = h.Store.SaveState("forward/origin/BTC", start); e != nil {
		t.Fatal(e)
	}
	ctx := context.Background()
	tx, _ := h.Store.research.BeginTx(ctx, nil)
	for i := 0; i < 14*288; i++ {
		at := start.Add(time.Duration(i) * 5 * time.Minute)
		b, _ := json.Marshal(ShadowSample{At: at, Through: at, Eligible: true, BaselineValid: true})
		if _, e = tx.Exec("INSERT INTO shadow VALUES(?,?,?)", "BTC", at.Unix(), b); e != nil {
			t.Fatal(e)
		}
	}
	if e = tx.Commit(); e != nil {
		t.Fatal(e)
	}
	read := func(now time.Time) ForwardReport {
		t.Helper()
		if e := h.buildForwardReport(ctx, "BTC", now); e != nil {
			t.Fatal(e)
		}
		var r ForwardReport
		if e := h.Store.document(ctx, "forward-report", "BTC", &r); e != nil {
			t.Fatal(e)
		}
		return r
	}
	end := start.Add(14 * 24 * time.Hour)
	if read(end.Add(-time.Second)).Ready {
		t.Fatal("partial fourteen days passed")
	}
	r := read(end)
	if !r.Ready || r.Expected != 4032 || r.Coverage != 1 {
		t.Fatalf("complete observation rejected: %+v", r)
	}
	if _, e = h.Store.research.Exec("DELETE FROM shadow WHERE asset=? AND ts<?", "BTC", start.Add(24*time.Hour).Unix()); e != nil {
		t.Fatal(e)
	}
	r = read(end)
	if r.Ready || r.Expected != 4032 || r.Coverage >= .95 {
		t.Fatal("leading gap shortened denominator")
	}
}

func TestMailRollingCapBeforeDelivery(t *testing.T) {
	h, e := Open(Config{Root: t.TempDir(), Offline: true, Mail: &MailConfig{}})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	now := time.Now().UTC()
	for i := 0; i < 6; i++ {
		_, e = h.Store.research.Exec("INSERT INTO notices(id,created,status,attempted,payload) VALUES(?,?,'sent',?,'{}')", fmt.Sprint(i), now.Unix(), now.Add(-time.Duration(i+1)*time.Minute).Unix())
		if e != nil {
			t.Fatal(e)
		}
	}
	if e = h.queueNotice(Signal{ID: "limited", Asset: "BTC", At: now}, "anomaly", now); e != nil {
		t.Fatal(e)
	}
	if e = h.processNotices(context.Background(), now); e != nil {
		t.Fatal("attempted SMTP beyond cap", e)
	}
	var s string
	_ = h.Store.research.QueryRow("SELECT status FROM notices WHERE id='limited/anomaly'").Scan(&s)
	if s != "pending" {
		t.Fatal(s)
	}
}

func BenchmarkStudyWindows(b *testing.B) {
	end := time.Now().UTC().Truncate(time.Hour)
	from := end.Add(-90 * 24 * time.Hour)
	bars := map[int64]FlowBar{}
	for at := from; at.Before(end); at = at.Add(5 * time.Minute) {
		bars[at.Unix()] = FlowBar{at, 100 + int64(at.Minute()), 90}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for at := from.Add(30 * 24 * time.Hour); at.Before(end); at = at.Add(time.Hour) {
			_ = baselineFromBars(bars, at.Add(-30*24*time.Hour), at, end)
		}
	}
}
