package datahub

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"reflect"
	"testing"
	"time"
)

func TestOrphanLifecycleConfirmsWithoutRearmAndExpiresWithoutData(t *testing.T) {
	h, e := Open(Config{Root: t.TempDir(), Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	ctx := context.Background()
	end := time.Now().UTC().Truncate(5 * time.Minute)
	now := end.Add(30 * time.Second)
	h.boot = end.Add(-time.Hour)
	fd, _ := h.Dataset(ID("flow", "BTC", "", "spot"))
	cd, _ := h.Dataset(ID("candles", "BTC", "Binance", "spot"))
	for at := end.Add(-15 * time.Minute); at.Before(end); at = at.Add(time.Minute) {
		o := Observation{Dataset: fd.ID, Source: fd.Source, ObservedAt: &at, FetchedAt: now, Resolution: 60, Quality: "valid", Payload: Payload{Flow: &Flow{"100", "10"}}}
		if _, e = h.Store.Ingest(fd, o); e != nil {
			t.Fatal(e)
		}
		if at.Unix()%300 == 0 {
			o.Dataset = cd.ID
			o.Source = cd.Source
			o.Resolution = 300
			o.Payload = Payload{Candle: &Candle{Open: 110, High: 111, Low: 109, Close: 110}}
			if _, e = h.Store.Ingest(cd, o); e != nil {
				t.Fatal(e)
			}
		}
	}
	sig := Signal{ID: "orphan", Asset: "BTC", Rules: SignalRules, Direction: "buy", At: end.Add(-40 * time.Minute), DataThrough: end.Add(-40 * time.Minute), Expires: end.Add(3 * time.Hour), State: "weakened", FrozenHigh: 101}
	state := signalState{Last: end.Add(-5 * time.Minute), Active: map[string]string{}, Clear: map[string]*time.Time{}}
	if e = h.commitSignals(ctx, "BTC", state, []Signal{sig}, nil, now); e != nil {
		t.Fatal(e)
	}
	if e = h.processSignals(ctx, now); e != nil {
		t.Fatal(e)
	}
	var got Signal
	if e = h.Store.document(ctx, "signal", sig.ID, &got); e != nil {
		t.Fatal(e)
	}
	if got.State != "confirmed" || got.ConfirmedAt == nil || !got.At.Equal(sig.At) || got.ConfirmedThrough == nil || !got.ConfirmedThrough.Equal(end) {
		t.Fatalf("lost untracked confirmation: %+v", got)
	}
	expired := sig
	expired.ID = "overdue"
	expired.At = end.Add(-9 * time.Hour)
	expired.Expires = end.Add(-5 * time.Hour)
	if e = h.Store.saveDocument("signal", expired.ID, "BTC", expired.At, expired); e != nil {
		t.Fatal(e)
	}
	if e = h.processSignals(ctx, now.Add(time.Second)); e != nil {
		t.Fatal(e)
	}
	if e = h.Store.document(ctx, "signal", expired.ID, &got); e != nil {
		t.Fatal(e)
	}
	if got.State != "expired" || got.Repair == nil || got.ConfirmedAt != nil || !got.At.Equal(expired.At) {
		t.Fatalf("incorrect retrospective repair: %+v", got)
	}
	var n int
	_ = h.Store.research.QueryRow("SELECT count(*) FROM notices WHERE signal_id='overdue'").Scan(&n)
	if n != 0 {
		t.Fatal("historical repair mailed")
	}
}

func TestEpisodeOneToOneStableAndPending(t *testing.T) {
	start := testTime("2026-09-20T01:00:00Z")
	episodes := []PriceEpisode{{ID: "first", Direction: "buy", Start: start, Detected: start.Add(10 * time.Minute)}, {ID: "second", Direction: "buy", Start: start.Add(2 * time.Hour), Detected: start.Add(2*time.Hour + 10*time.Minute)}}
	signals := []Signal{{ID: "one", Direction: "buy", At: start.Add(5 * time.Minute)}, {ID: "too-new", Direction: "buy", At: start.Add(8 * time.Hour)}}
	r := matchEpisodes(episodes, signals, start.Add(9*time.Hour))
	if r.Following != 1 || r.Early != 0 || r.Missed != 1 || r.Pending != 1 || r.Unmatched != 0 {
		t.Fatalf("double credit or premature false positive: %+v", r)
	}
	if r.Matches[0].SignalID != "one" || r.Matches[1].SignalID != "" {
		t.Fatal(r.Matches)
	}
	reversed := []Signal{signals[1], signals[0]}
	if !reflect.DeepEqual(r, matchEpisodes(episodes, reversed, start.Add(9*time.Hour))) {
		t.Fatal("matching depends on input order")
	}
}

func TestClosedScalarsDoNotLeakHourlyCloseOrFuture(t *testing.T) {
	h, e := Open(Config{Root: t.TempDir(), Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	end := time.Now().UTC().Truncate(time.Hour)
	now := end.Add(time.Hour)
	for _, id := range []string{ID("oi-history", "BTC", "", "futures"), ID("premium", "BTC", "Coinbase", "spot")} {
		d, _ := h.Dataset(id)
		for _, res := range []int{300, 3600} {
			p := Payload{OI: []Interest{{USD: fmt.Sprint(res)}}}
			if d.Kind == "premium" {
				p = Payload{Premium: &Premium{USD: fmt.Sprint(res)}}
			}
			o := Observation{Dataset: id, Source: d.Source, ObservedAt: &end, FetchedAt: now, Resolution: res, Quality: "valid", Payload: p}
			if _, e = h.Store.Ingest(d, o); e != nil {
				t.Fatal(e)
			}
		}
		m, e := h.closedScalars(context.Background(), id, end, end.Add(5*time.Minute), now)
		if e != nil {
			t.Fatal(e)
		}
		if len(m) != 1 || m[end.Add(5*time.Minute).Unix()] != 300 {
			t.Fatalf("hour close overwrote five-minute close: %v", m)
		}
		m, e = h.closedScalars(context.Background(), id, end, end.Add(4*time.Minute), now)
		if e != nil || len(m) != 0 {
			t.Fatalf("unfinished interval used: %v %v", m, e)
		}
	}
}

func candidateFixture(end time.Time) (map[int64]FlowBar, map[int64]Candle, SignalBaseline) {
	bars := map[int64]FlowBar{}
	candles := map[int64]Candle{}
	for at := end.Add(-17 * time.Hour); at.Before(end); at = at.Add(5 * time.Minute) {
		bars[at.Unix()] = FlowBar{at, 750_000_000, 250_000_000}
		candles[at.Unix()] = Candle{Open: 100, High: 102, Low: 98, Close: 100}
	}
	p95 := 4_000_000_000.0
	return bars, candles, SignalBaseline{Valid: true, P90: 2_000_000_000, P95Hour: &p95, Median60: 6_000_000_000}
}
func TestCandidateFixedFeaturesMissingAndOverextension(t *testing.T) {
	end := testTime("2026-09-20T12:00:00Z")
	bars, c, base := candidateFixture(end)
	f, ok := candidateFeatures(bars, c, end, base)
	if !ok || f.Net1H != 6_000_000_000 || f.Net4H != 24_000_000_000 || f.PositiveQuarters != 4 || f.VolumeRatio != 2 || f.Stage != "range" || candidateLevel(f, base, 5_000_000_000) != "strong" {
		t.Fatalf("bad shared features: %+v", f)
	}
	if candidateLevel(f, base, 8_000_000_000) != "observe" {
		t.Fatal("sensitivity threshold not applied")
	}
	last := c[end.Add(-5*time.Minute).Unix()]
	last.Close = 110
	last.High = 111
	c[end.Add(-5*time.Minute).Unix()] = last
	f, ok = candidateFeatures(bars, c, end, base)
	if !ok || !f.Extended || candidateLevel(f, base, 5_000_000_000) != "observe" {
		t.Fatal("extended move promoted")
	}
	delete(bars, end.Add(-time.Hour).Unix())
	if _, ok = candidateFeatures(bars, c, end, base); ok {
		t.Fatal("missing interval treated as zero")
	}
}
func TestCandidateUpgradeFrozenGroupedAndValidationGated(t *testing.T) {
	h, e := Open(Config{Root: t.TempDir(), Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	end := time.Now().UTC().Truncate(5 * time.Minute)
	bars, c, base := candidateFixture(end)
	ctx := context.Background()
	for k, b := range bars {
		b.Buy = 650_000_000
		b.Sell = 350_000_000
		bars[k] = b
	}
	state := signalState{Active: map[string]string{}, Clear: map[string]*time.Time{}}
	updates := []Signal{}
	if e = h.processCandidate(ctx, "BTC", &state, bars, c, base, end, end.Add(time.Second), &updates); e != nil {
		t.Fatal(e)
	}
	if len(updates) != 1 || updates[0].Level != "observe" {
		t.Fatal(updates)
	}
	first := updates[0]
	if e = h.commitSignals(ctx, "BTC", state, updates, map[string]string{first.ID: "strong"}, end); e != nil {
		t.Fatal(e)
	}
	for k, b := range bars {
		b.Buy = 750_000_000
		b.Sell = 250_000_000
		bars[k] = b
	}
	updates = nil
	if e = h.processCandidate(ctx, "BTC", &state, bars, c, base, end, end.Add(time.Minute), &updates); e != nil {
		t.Fatal(e)
	}
	if len(updates) != 1 || updates[0].ID != first.ID || updates[0].Upgrade == nil || updates[0].Features.Net1H != first.Features.Net1H || updates[0].Upgrade.Features.Net1H != 6_000_000_000 {
		t.Fatal("upgrade rewrote original facts", updates)
	}
	if e = h.commitSignals(ctx, "BTC", state, updates, map[string]string{first.ID: "strong"}, end); e != nil {
		t.Fatal(e)
	}
	var n int
	_ = h.Store.research.QueryRow("SELECT count(*) FROM notices").Scan(&n)
	if n != 0 {
		t.Fatal("unvalidated candidate mailed")
	}
	updates = nil
	if e = h.processCandidate(ctx, "BTC", &state, bars, c, base, end, end.Add(2*time.Minute), &updates); e != nil || len(updates) != 0 {
		t.Fatal("duplicate upgrade", e)
	}
}

func TestStudySubrangesBoundedAndUnavailableDoesNotBlock(t *testing.T) {
	now := time.Now().UTC()
	s := Study{Asset: "BTC", Pipeline: studyPipeline, Created: now, From: now.Add(-89 * 24 * time.Hour).Truncate(time.Hour), To: now.Truncate(time.Hour)}
	reqs := studyRequests(s, now)
	seen := map[string]int{}
	for _, r := range reqs {
		if r.To.Sub(*r.From) > time.Duration(1000*r.Resolution)*time.Second {
			t.Fatal("unbounded failure domain", r)
		}
		if r.Purpose == "research" {
			seen[r.Dataset]++
		}
	}
	if len(seen) != 4 {
		t.Fatal(seen)
	}
	for _, n := range seen {
		if n < 20 {
			t.Fatal("long interval not split")
		}
	}
	h, e := Open(Config{Root: t.TempDir(), Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	h.queueStudy(&s, now)
	before := s.QueueCursor
	h.Scheduler.mu.Lock()
	for _, id := range s.Jobs {
		if j := h.Scheduler.jobs[id]; j != nil {
			j.Disabled = true
			j.ErrorKind = "empty_window"
		}
	}
	h.Scheduler.mu.Unlock()
	h.queueStudy(&s, now)
	if s.QueueCursor <= before {
		t.Fatal("disabled child blocked later ranges")
	}
	coverage := h.studyCoverage(s)
	unavailable, untried := false, false
	for _, v := range coverage {
		unavailable = unavailable || v["state"] == "unavailable"
		untried = untried || v["state"] == "untried"
	}
	if !unavailable || !untried {
		t.Fatal("lost independent coverage statuses")
	}
}
func TestCandidateMailNeedsHumanReviewAndFreshForwardEvidence(t *testing.T) {
	h, e := Open(Config{Root: t.TempDir(), Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	ctx := context.Background()
	now := time.Now().UTC()
	h.boot = now.Add(-time.Minute)
	origin := now.Add(-15 * 24 * time.Hour)
	h.mail = &MailConfig{DashboardURL: "https://example.com"}
	report := ForwardReport{Evaluation: EvaluationVersion, CandidateReady: true, CandidateOrigin: &origin, To: now}
	if e = h.Store.saveDocument("forward-report", "BTC", "BTC", now, report); e != nil {
		t.Fatal(e)
	}
	if h.candidateMailAllowed(ctx, now) {
		t.Fatal("sample size auto-enabled mail")
	}
	reviewed := Study{ID: "reviewed-study", Asset: "BTC", Pipeline: studyPipeline, From: now.Add(-90 * 24 * time.Hour), To: now, InputVersion: "facts-1", Result: &StudyResult{CoreCalculated: true, Evaluation: EvaluationVersion, FlowCoverage: 1, CandleCoverage: 1, BaselineDays: 30, DevelopmentDays: 30, HoldoutDays: 30, CandidateComparison: &CandidateComparison{Rules: CandidateRules, Evaluation: EvaluationVersion}}}
	reviewed.ValidationID, e = h.freezeStudyValidation(ctx, reviewed, now)
	if e != nil {
		t.Fatal(e)
	}
	if e = h.Store.saveDocument("study", reviewed.ID, "BTC", now, reviewed); e != nil {
		t.Fatal(e)
	}
	h.mail.CandidateApproval = &CandidateMailApproval{StudyID: reviewed.ID, ValidationID: reviewed.ValidationID, Rules: CandidateRules, Evaluation: EvaluationVersion, ReviewedAt: now, ReceiptVerifiedAt: now, EqualBudgetReviewed: true, PerformanceAccepted: true}
	if !h.candidateMailAllowed(ctx, now) {
		t.Fatal("reviewed gate not usable")
	}
	// Retention can make the mutable study incomplete without changing the
	// completed evidence actually reviewed. A new completed revision cannot.
	reviewed.Result.CoreCalculated = false
	if e = h.Store.saveDocument("study", reviewed.ID, "BTC", now, reviewed); e != nil || !h.candidateMailAllowed(ctx, now) {
		t.Fatal("retention invalidated frozen reviewed evidence", e)
	}
	approvedID := reviewed.ValidationID
	reviewed.ValidationID = "new-completed-revision"
	if e = h.Store.saveDocument("study", reviewed.ID, "BTC", now, reviewed); e != nil || h.candidateMailAllowed(ctx, now) {
		t.Fatal("approval silently followed a new revision", e)
	}
	reviewed.ValidationID = approvedID
	if e = h.Store.saveDocument("study", reviewed.ID, "BTC", now, reviewed); e != nil {
		t.Fatal(e)
	}
	if h.candidateMailAllowed(ctx, now.Add(3*time.Hour)) {
		t.Fatal("stale report authorized mail")
	}
	s := Signal{ID: "new", Asset: "BTC", Rules: CandidateRules, Level: "strong", At: now, DataThrough: now, Expires: now.Add(4 * time.Hour)}
	if e = h.queueNotice(s, "strong", now); e != nil {
		t.Fatal(e)
	}
	if e = h.queueNotice(s, "strong", now); e != nil {
		t.Fatal(e)
	}
	var n int
	_ = h.Store.research.QueryRow("SELECT count(*) FROM notices").Scan(&n)
	if n != 1 {
		t.Fatal("upgrade notice duplicated")
	}
	// Inspect suppression without invoking SMTP or network.
	if e = h.processNotices(ctx, now.Add(5*time.Hour)); e != nil {
		t.Fatal(e)
	}
	var status string
	_ = h.Store.research.QueryRow("SELECT status FROM notices WHERE signal_id='new'").Scan(&status)
	if status != "suppressed_expired_or_validation" {
		t.Fatal(status)
	}
}
func TestSignalsAdditiveFilterAndHistoricalCases(t *testing.T) {
	h, e := Open(Config{Root: t.TempDir(), Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	now := time.Now()
	ctx := context.Background()
	for _, rule := range []string{SignalRules, CandidateRules} {
		s := Signal{ID: rule, Asset: "BTC", At: now, Rules: rule}
		if e = h.Store.saveDocument("signal", s.ID, "BTC", now, s); e != nil {
			t.Fatal(e)
		}
	}
	v, e := h.SignalsView(ctx, "BTC", "", CandidateRules)
	if e != nil {
		t.Fatal(e)
	}
	items := v.(map[string]any)["items"].([]json.RawMessage)
	if len(items) != 1 || decodeSignal(items[0]).Rules != CandidateRules {
		t.Fatal(v)
	}
	if !selectedCase(testTime("2026-08-19T00:00:00Z")) || selectedCase(testTime("2026-09-10T00:00:00Z")) {
		t.Fatal("case leakage")
	}
}

func TestCaseReconciliationPreservesBothInputs(t *testing.T) {
	h, e := Open(Config{Root: t.TempDir(), Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	end := testTime("2026-08-19T12:00:00Z")
	start := end.Add(-time.Hour)
	now := end.Add(5 * time.Hour)
	d, _ := h.Dataset(ID("flow", "BTC", "", "spot"))
	o := Observation{Dataset: d.ID, Source: d.Source, ObservedAt: &start, FetchedAt: now, Resolution: 3600, Quality: "valid", Payload: Payload{Flow: &Flow{"0", "0"}}}
	if _, e = h.Store.Ingest(d, o); e != nil {
		t.Fatal(e)
	}
	bars := map[int64]FlowBar{}
	for at := start; at.Before(end); at = at.Add(5 * time.Minute) {
		bars[at.Unix()] = FlowBar{at, 20000, 10000}
	}
	cases := h.studyCases(context.Background(), Study{Asset: "BTC", From: end.Add(-7 * 24 * time.Hour), To: end.Add(7 * 24 * time.Hour)}, bars, map[int64]Candle{}, now)
	found := false
	for _, row := range cases[0]["hourly"].([]map[string]any) {
		if row["end"].(time.Time).Equal(end) {
			r := row["reconciliation"].(map[string]any)
			if r["nativeBuyCents"] != int64(0) || r["fineBuyCents"] != int64(240000) || r["conflict"] != true || *row["netCents"].(*int64) != 120000 {
				t.Fatal(r)
			}
			found = true
		}
	}
	if !found {
		t.Fatal("case hour absent")
	}
}

func TestCandidateHistoricalCommonUniverseAndDelays(t *testing.T) {
	end := testTime("2026-07-01T00:00:00Z")
	from := end.Add(-90 * 24 * time.Hour)
	bars := map[int64]FlowBar{}
	c := map[int64]Candle{}
	for at := from; at.Before(end); at = at.Add(5 * time.Minute) {
		bars[at.Unix()] = FlowBar{at, 750_000_000, 250_000_000}
		c[at.Unix()] = Candle{Open: 100, High: 102, Low: 98, Close: 100}
	}
	// A fixed, infrequent burst passes the 1.5x volume gate without tuning history.
	for at := end.Add(-3 * 24 * time.Hour); at.Before(end.Add(-3*24*time.Hour + time.Hour)); at = at.Add(5 * time.Minute) {
		bars[at.Unix()] = FlowBar{at, 2_000_000_000, 300_000_000}
	}
	oi := map[int64]float64{}
	for at := from; at.Before(end); at = at.Add(5 * time.Minute) {
		oi[at.Add(5*time.Minute).Unix()] = 1000
	}
	r, e := evaluateCandidates(context.Background(), bars, c, from, end, CandidateContextSeries{bars, oi})
	if e != nil {
		t.Fatal(e)
	}
	if r.CommonWindows == 0 || len(r.Development) != 21 || len(r.Holdout) != 21 {
		t.Fatalf("incomplete comparisons: %+v", r)
	}
	if r.AuxiliaryWindows == 0 || len(r.Auxiliary) != 4 || len(r.EqualBudget) != 3 {
		t.Fatal("context or equal-budget comparison missing")
	}
	for _, trial := range r.EqualBudget {
		if trial.Signals != r.EqualBudget[0].Signals {
			t.Fatal("unequal reminder budget")
		}
	}
	if r.Auxiliary[0].Signals == 0 || r.Auxiliary[2].Signals != 0 || r.Auxiliary[3].Signals != 0 {
		t.Fatal("flat OI treated as missing or directional", r.Auxiliary)
	}
	strong := -1
	for _, v := range r.Holdout {
		if v.Rules == "strong-5000万" {
			if strong < 0 {
				strong = v.Signals
			}
			if v.Signals != strong {
				t.Fatal("delay changed the eligible population")
			}
			if v.Signals == 0 || v.Return4HMedian == nil || v.MFE4HMedian == nil || v.MAE4HMedian == nil {
				t.Fatalf("outcome missing: %+v", v)
			}
		}
	}
}

func TestPriceLedgerAnchorSurvivesReportWindowAndRestart(t *testing.T) {
	h, e := Open(Config{Root: t.TempDir(), Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	ctx := context.Background()
	end := time.Now().UTC().Truncate(5 * time.Minute)
	if e = h.Store.SaveState("forward/event-ledger/BTC", EvaluationVersion); e != nil {
		t.Fatal(e)
	}
	bars, c, base := candidateFixture(end)
	_, _ = bars, base
	for _, t0 := range []time.Time{end.Add(-10 * time.Minute), end.Add(-5 * time.Minute)} {
		c[t0.Unix()] = Candle{Open: 104, High: 106, Low: 103, Close: 105}
	}
	if e = h.priceEventLedger(ctx, "BTC", end.Add(time.Minute), c, end, true); e != nil {
		t.Fatal(e)
	}
	first, e := h.loadPriceEvents(ctx, "BTC", end.Add(-time.Hour), end.Add(time.Hour))
	if e != nil || len(first) != 1 {
		t.Fatalf("event missing %v %v", first, e)
	}
	// A replay of the same market bars after restart cannot move discovery or anchor.
	h.boot = end.Add(2 * time.Minute)
	if e = h.priceEventLedger(ctx, "BTC", end.Add(3*time.Minute), c, end, true); e != nil {
		t.Fatal(e)
	}
	after, e := h.loadPriceEvents(ctx, "BTC", end.Add(-30*time.Minute), end.Add(time.Hour))
	if e != nil || !reflect.DeepEqual(first, after) {
		t.Fatal("ledger anchor changed", first, after, e)
	}
}

func TestNoticeSameSecondRestartNeverSendsBacklog(t *testing.T) {
	h, e := Open(Config{Root: t.TempDir(), Offline: true, Mail: &MailConfig{}})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	now := time.Now().UTC().Truncate(time.Second)
	h.boot = now.Add(500 * time.Millisecond)
	sig := Signal{ID: "restart-boundary", Asset: "BTC", Rules: SignalRules, At: now.Add(100 * time.Millisecond), DataThrough: now, Expires: now.Add(4 * time.Hour)}
	if e = h.queueNotice(sig, "anomaly", sig.At); e != nil {
		t.Fatal(e)
	}
	if e = h.processNotices(context.Background(), now.Add(time.Second)); e != nil {
		t.Fatal("attempted SMTP for a restart backlog", e)
	}
	var state string
	_ = h.Store.research.QueryRow("SELECT status FROM notices WHERE signal_id=?", sig.ID).Scan(&state)
	if state != "suppressed_restart" {
		t.Fatal(state)
	}
}

func TestStudyValidationImmutableAcrossRetentionAndRevisions(t *testing.T) {
	h, e := Open(Config{Root: t.TempDir(), Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	ctx := context.Background()
	now := time.Now().UTC()
	s := Study{ID: "frozen", Asset: "BTC", InputVersion: "one", Pipeline: studyPipeline, From: now.Add(-90 * 24 * time.Hour), To: now, Result: &StudyResult{CoreCalculated: true, Evaluation: EvaluationVersion, FlowCoverage: 1, CandleCoverage: 1, BaselineDays: 30, DevelopmentDays: 30, HoldoutDays: 30, CandidateComparison: &CandidateComparison{Rules: CandidateRules, Evaluation: EvaluationVersion, CommonWindows: 123}}}
	id, e := h.freezeStudyValidation(ctx, s, now)
	if e != nil {
		t.Fatal(e)
	}
	s.Result.CandidateComparison.CommonWindows = 999
	same, e := h.freezeStudyValidation(ctx, s, now.Add(time.Hour))
	if e != nil || id != same {
		t.Fatal("same input version duplicated snapshot", e)
	}
	var snapshot StudyValidation
	if e = h.Store.document(ctx, "study-validation", id, &snapshot); e != nil || snapshot.Comparison.CommonWindows != 123 || !snapshot.CalculatedAt.Equal(now) {
		t.Fatal("snapshot overwritten", snapshot, e)
	}
	if raw, e := h.Read(ctx, "study-validations/"+id, url.Values{"asset": {"BTC"}}); e != nil || json.Unmarshal(raw, &snapshot) != nil || snapshot.InputVersion != "one" {
		t.Fatal("review evidence not readable through the local API", e)
	}
	if _, e := h.Read(ctx, "study-validations/"+id, url.Values{"asset": {"ETH"}}); e == nil {
		t.Fatal("BTC evidence returned as ETH")
	}
	s.InputVersion = "two"
	revised, e := h.freezeStudyValidation(ctx, s, now.Add(time.Hour))
	if e != nil || revised == id {
		t.Fatal("revision reused reviewed identity")
	}
	s.Result.CoreCalculated = false
	if _, e = h.freezeStudyValidation(ctx, s, now); e == nil {
		t.Fatal("incomplete study gained validation")
	}
	if e = h.Store.document(ctx, "study-validation", id, &snapshot); e != nil {
		t.Fatal("retention status removed original evidence", e)
	}
}
