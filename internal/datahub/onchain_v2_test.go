package datahub

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func costPrepareNotice(t *testing.T, h *Hub, event *CostEvent, now time.Time) {
	t.Helper()
	ctx := context.Background()
	event.CaseID = "case-" + event.ID
	event.Price = "85000"
	event.Direction = "up"
	event.PriceSource = onchainSource
	c := CostCase{ID: event.CaseID, Rules: OnchainRules, State: event.Kind, Boundary: "84000", Direction: "up", LastDate: event.Date, PriceSource: onchainSource, Quality: "available"}
	b, _ := json.Marshal(c)
	if _, e := h.Store.onchain.db.Exec("INSERT INTO cases VALUES(?,?,?)", c.ID, now.UnixNano(), b); e != nil {
		t.Fatal(e)
	}
	if e := h.Store.onchain.ingest(ctx, costBundle{Prices: []CostPrice{{Date: event.Date, Value: event.Price}}}, now, false); e != nil {
		t.Fatal(e)
	}
}
func v2Case() CostCase {
	s := costState{}
	costNewCases(&s, []CostZone{{Side: "above", Low: "84000", High: "87000", Supply: "10"}}, "onchain", onchainMethod, "frozen-revision", testTime("2026-10-04T06:00:00Z"))
	return s.Cases[0]
}
func v2Price(day, value string) *CostPrice {
	seen := costClose(day).Add(time.Hour)
	return &CostPrice{Date: day, Value: value, FirstSeen: seen, ValidatedAt: &seen}
}
func v2Step(c *CostCase, day, value string) []CostEvent {
	p := v2Price(day, value)
	return costAdvanceCase(c, p, day, p.FirstSeen)
}
func TestCostV2LifecycleFutureCloseAndDayEight(t *testing.T) {
	c := v2Case()
	if c.From != "2026-10-04" || c.Through != "2026-10-10" {
		t.Fatal(c)
	}
	if e := v2Step(&c, "2026-10-03", "88000"); len(e) > 0 {
		t.Fatal("pre-freeze close used")
	}
	for i := 4; i <= 9; i++ {
		v2Step(&c, fmt.Sprintf("2026-10-%02d", i), "85000")
	}
	e := v2Step(&c, "2026-10-10", "88000")
	if len(e) != 1 || e[0].Kind != "pending" {
		t.Fatal(e)
	}
	e = v2Step(&c, "2026-10-11", "89000")
	if len(e) != 1 || e[0].Kind != "confirmed" || c.TrackThrough != "2026-12-10" {
		t.Fatal(c, e)
	}
	if len(v2Step(&c, "2026-10-11", "89000")) != 0 {
		t.Fatal("duplicate confirmation")
	}
	e = v2Step(&c, "2026-10-14", "87000")
	if len(e) != 1 || e[0].Kind != "invalidated" {
		t.Fatal("after-cycle/equality invalidation lost", c, e)
	}
	// Strictly future at midnight: the close happening at freeze is ineligible.
	c = v2Case()
	c.FrozenAt = testTime("2026-10-05T00:00:00Z")
	if len(v2Step(&c, "2026-10-04", "89000")) != 0 {
		t.Fatal("same instant accepted")
	}
}
func TestCostV2MissingAndExpirationAreSeparate(t *testing.T) {
	c := v2Case()
	v2Step(&c, "2026-10-04", "88000")
	costAdvanceCase(&c, nil, "2026-10-05", costClose("2026-10-05"))
	e := v2Step(&c, "2026-10-06", "88000")
	if c.Count != 1 || len(e) != 1 || e[0].Kind != "pending" {
		t.Fatal(c, e)
	}
	v2Step(&c, "2026-10-07", "88000")
	costAdvanceCase(&c, nil, "2026-10-08", costClose("2026-10-08"))
	if c.State != "confirmed" || c.Quality != "unknown" {
		t.Fatal(c)
	}
	e = costAdvanceCase(&c, nil, "2026-12-08", costClose("2026-12-08"))
	if len(e) != 1 || e[0].Kind != "tracking_expired" {
		t.Fatal("time expiry depends on price", c, e)
	}
	c = v2Case()
	e = v2Step(&c, "2026-10-11", "88000")
	if len(e) != 1 || e[0].Kind != "discovery_expired" {
		t.Fatal("new day-eight entry accepted", e)
	}
}
func TestCostV2InsideWindowAndCycleDedup(t *testing.T) {
	f := costTestFrame("2026-10-03")
	zones := costZones(f)
	found := false
	for _, z := range zones {
		if z.Side == "inside" {
			found = true
			if dec(z.Low).GreaterThan(dec(f.Price)) || !dec(z.High).GreaterThan(dec(f.Price)) {
				t.Fatal(z)
			}
		}
	}
	if !found {
		t.Fatal("inside omitted")
	}
	s := costState{}
	now := testTime("2026-10-04T06:00:00Z")
	costNewCases(&s, zones, "onchain", f.Method, f.Revision, now)
	n := len(s.Cases)
	s.Cases[0].State = "confirmed"
	costNewCases(&s, zones, "onchain", f.Method, "revised", now.AddDate(0, 0, 7))
	if len(s.Cases) != n || s.Cases[0].State != "confirmed" {
		t.Fatal("cycle replaced active cases")
	}
}
func TestCostV2CostContractFailureKeepsPriceAndDailyReconciliation(t *testing.T) {
	now := testTime("2026-10-04T08:00:00Z")
	b := costTestSource(t, "2026-10-03")
	b = bytes.Replace(b, []byte("155 most recent"), []byte("unverified method"), 1)
	out, e := parseCostBundle(bytes.NewReader(b), false, now)
	if e != nil || len(out.Frames) != 0 || len(out.Prices) != 1 || out.CostError == "" {
		t.Fatal("price thrown away with cost", out, e)
	}
	h := costTestHub(t)
	ctx := context.Background()
	f := costTestFrame("2026-10-03")
	costSeed(t, h, now, f, true)
	f.Price = "85001"
	costSeed(t, h, now.Add(time.Hour), f, false)
	var distributions, frames int
	h.Store.onchain.db.QueryRow("SELECT count(*) FROM distributions").Scan(&distributions)
	h.Store.onchain.db.QueryRow("SELECT count(*) FROM frames_v2").Scan(&frames)
	if distributions != 1 || frames != 2 {
		t.Fatal("price duplicated distribution", distributions, frames)
	}
	old, _ := h.Store.onchain.frame(ctx, f.Date, now)
	latest, _ := h.Store.onchain.frame(ctx, f.Date, now.Add(time.Hour))
	if old.Price != "85000" || latest.Price != "85001" || latest.STH.Total != f.STH.Total || len(latest.STH.Values) == 0 {
		t.Fatal(old, latest)
	}
	p, _ := h.Store.onchain.price(ctx, f.Date, now.Add(time.Hour))
	if p.ValidatedAt == nil || p.IntervalEnd == nil || p.Role != "daily_close" {
		t.Fatal(p)
	}
}
func TestCostV2PricesTrackWhenStructureFails(t *testing.T) {
	h := costTestHub(t)
	ctx := context.Background()
	now := testTime("2026-10-05T06:00:00Z")
	c := v2Case()
	c.State = "confirmed"
	c.LastDate = "2026-10-03"
	c.ConfirmedDate = "2026-10-03"
	c.TrackThrough = "2026-12-02"
	s := costState{Rules: OnchainRules, Cases: []CostCase{c}, EnabledAt: now.Add(-time.Hour)}
	if e := h.Store.onchain.save(ctx, "observation", s); e != nil {
		t.Fatal(e)
	}
	if e := h.Store.onchain.ingest(ctx, costBundle{Prices: []CostPrice{{Date: "2026-10-04", Value: "86000"}}}, now, false); e != nil {
		t.Fatal(e)
	}
	if e := h.evaluateCostDay(ctx, now, false); e != nil {
		t.Fatal(e)
	}
	got, e := h.Store.onchain.caseByID(ctx, c.ID)
	if e != nil || got.State != "invalidated" {
		t.Fatal(got, e)
	}
}
func TestCostV2AttentionDedupAndFX(t *testing.T) {
	c := v2Case()
	at := testTime("2026-10-04T08:00:00Z")
	point := func(value string) *CostEvent {
		p := costReference{Value: value, CloseAt: at, FirstSeen: at.Add(time.Minute)}
		e := costAttention(&c, p, p.FirstSeen)
		at = at.Add(4 * time.Hour)
		return e
	}
	if e := point("86900"); e == nil || e.Kind != "attention_near" {
		t.Fatal(e)
	}
	if point("86900") != nil {
		t.Fatal("repeated near")
	}
	if e := point("88000"); e == nil || e.Kind != "attention_outside" {
		t.Fatal(e)
	}
	point("85000")
	point("85000")
	if point("88000") == nil {
		t.Fatal("not rearmed")
	}
	c.State = "confirmed"
	if e := point("86000"); e == nil || e.Kind != "attention_returned" || c.State != "confirmed" {
		t.Fatal(e)
	}
	h := costTestHub(t)
	ctx := context.Background()
	end := testTime("2026-10-04T12:00:00Z")
	h.boot = end.Add(-time.Hour)
	h.captureCostFX(ctx, []Rate{{Quote: "USDT", USD: "0.99"}}, end.Add(-31*time.Second))
	var n int
	h.Store.onchain.db.QueryRow("SELECT count(*) FROM fx_boundary").Scan(&n)
	if n != 0 {
		t.Fatal("stale FX saved")
	}
	h.captureCostFX(ctx, []Rate{{Quote: "USDT", USD: "0.99"}}, end.Add(-5*time.Second))
	d, _ := h.Dataset(ID("candles", "BTC", "Binance", "spot"))
	start := end.Add(-5 * time.Minute)
	_, e := h.Store.Ingest(d, Observation{Dataset: d.ID, Source: "binance", ObservedAt: &start, FetchedAt: end.Add(time.Minute), Resolution: 300, Quality: "valid", Payload: Payload{Candle: &Candle{Open: 85000, High: 86000, Low: 84000, Close: 85000, Volume: 1}}})
	if e != nil {
		t.Fatal(e)
	}
	p, e := h.costFourHourPrice(ctx, end, end.Add(2*time.Minute))
	if e != nil || p == nil || p.Value != "84150" {
		t.Fatal(p, e)
	}
	if p, e = h.costFourHourPrice(ctx, end, end.Add(16*time.Minute)); e != nil || p != nil {
		t.Fatal("late replay", p, e)
	}
}
func TestCostV2NotificationRestartAndSuperseded(t *testing.T) {
	h := costTestHub(t)
	h.offline = false
	ctx := context.Background()
	now := time.Now().UTC()
	h.boot = now
	h.Store.onchain.save(ctx, "settings", CostSettings{true})
	count := 0
	h.mailSend = func(context.Context, MailConfig, string, string) error { count++; return nil }
	ev := CostEvent{ID: "survives-restart", Kind: "confirmed", Date: costDate(now.AddDate(0, 0, -1)), DetectedAt: now.Add(-time.Minute), Rules: OnchainRules}
	costPrepareNotice(t, h, &ev, now)
	tx, _ := h.Store.onchain.db.BeginTx(ctx, nil)
	costInsertEvent(ctx, tx, ev, true)
	tx.Commit()
	if e := h.processCostNotices(ctx, now); e != nil || count != 1 {
		t.Fatal("unsent current notice lost on ordinary restart", count, e)
	}
	ev.ID = "invalidated-before-send"
	ev.DetectedAt = now
	costPrepareNotice(t, h, &ev, now)
	c, _ := h.Store.onchain.caseByID(ctx, ev.CaseID)
	c.State = "invalidated"
	raw, _ := json.Marshal(c)
	h.Store.onchain.db.Exec("UPDATE cases SET payload=? WHERE id=?", raw, c.ID)
	tx, _ = h.Store.onchain.db.BeginTx(ctx, nil)
	costInsertEvent(ctx, tx, ev, true)
	tx.Commit()
	if e := h.processCostNotices(ctx, now); e != nil || count != 1 {
		t.Fatal("superseded notice sent", count, e)
	}
}
func TestCostV2DailyAndResearchOutcome(t *testing.T) {
	now := testTime("2026-10-04T08:00:00Z")
	s := costState{Rules: OnchainRules}
	trials, d := costDailyResearch(&s, nil, nil, nil, nil, "2026-10-03", now, false)
	if len(trials) != 0 || d.Structure != "missing" || d.Price != "missing" {
		t.Fatal(d)
	}
	at := testTime("2026-01-01T12:00:00Z")
	v := 20.
	price := "100"
	trial := costTrial{ID: "down", DetectedAt: at, Direction: "down", PreVolatility: &v, DiscoveryPrice: &price}
	prices := map[string]string{}
	for i := 0; i <= 14; i++ {
		prices[costDayAdd("2026-01-01", i)] = fmt.Sprint(100 - i)
	}
	r := costTrialOutcome(trial, 14, prices, at.AddDate(0, 0, 16), nil)
	if r.Status != "complete" || r.DirectionalReturn == nil || *r.DirectionalReturn <= 0 || r.AnchorDate != "2026-01-01" || r.From != "2026-01-02" || r.VolatilityRatio == nil {
		t.Fatal(r)
	}
	delete(prices, "2026-01-08")
	r = costTrialOutcome(trial, 14, prices, at.AddDate(0, 0, 16), nil)
	if r.Status != "missing" {
		t.Fatal("missing path fabricated", r)
	}
}
func TestCostV2ResearchReservesMissingOverlap(t *testing.T) {
	h := costTestHub(t)
	ctx := context.Background()
	now := time.Now().UTC()
	first := now.AddDate(0, 0, -100)
	for i := 0; i < 2; i++ {
		trial := costTrial{ID: fmt.Sprint(i), Rules: OnchainRules, Group: "onchain", Direction: "up", EpisodeID: fmt.Sprint(i), DetectedAt: first.AddDate(0, 0, i), Date: costDate(first.AddDate(0, 0, i))}
		raw, _ := json.Marshal(trial)
		h.Store.onchain.db.Exec("INSERT INTO trials VALUES(?,?,?,?)", trial.ID, trial.DetectedAt.UnixNano(), trial.Group, raw)
		r := costTrialOutcome(trial, 14, nil, now, nil)
		if i == 1 {
			r.Status = "complete"
			v := 1.
			r.DirectionalReturn = &v
		}
		raw, _ = json.Marshal(r)
		h.Store.onchain.db.Exec("INSERT INTO trial_results VALUES(?,?,?)", trial.ID, 14, raw)
	}
	view, e := h.costResearchView(ctx, now, "forward", 0, 10)
	if e != nil {
		t.Fatal(e)
	}
	for _, g := range view.(map[string]any)["groups"].([]costValidationGroup) {
		if g.Horizon == 14 && (g.Independent != 0 || g.Missing != 1 || g.Overlapping != 1 || g.Rate != nil) {
			t.Fatal("missing first trial replaced by later winner", g)
		}
	}
}

func TestCostV2RestorePreservesSettingAndSuppressesOldQueue(t *testing.T) {
	h := costTestHub(t)
	ctx := context.Background()
	now := time.Now().UTC()
	h.Store.onchain.save(ctx, "settings", CostSettings{true})
	ev := CostEvent{ID: "restore", Rules: OnchainRules, Kind: "confirmed", DetectedAt: now, Date: costDate(now.AddDate(0, 0, -1))}
	costPrepareNotice(t, h, &ev, now)
	tx, _ := h.Store.onchain.db.BeginTx(ctx, nil)
	costInsertEvent(ctx, tx, ev, true)
	tx.Commit()
	raw, _ := json.Marshal(ev)
	h.Store.research.Exec("INSERT INTO notices(id,kind,status,payload) VALUES(?,?,?,?)", "cost-restore", "onchain-cost:confirmed", "pending", raw)
	if e := RestoreOnchainBoundary(ctx, h.Store.root); e != nil {
		t.Fatal(e)
	}
	settings, e := h.CostSettings(ctx)
	if e != nil || !settings.EmailEnabled {
		t.Fatal("restore changed preference", settings, e)
	}
	var status string
	h.Store.research.QueryRow("SELECT status FROM notices WHERE id='cost-restore'").Scan(&status)
	if status != "suppressed_restore" {
		t.Fatal(status)
	}
	h.Store.onchain.db.QueryRow("SELECT status FROM outbox WHERE id='restore'").Scan(&status)
	if status != "suppressed_restore" {
		t.Fatal(status)
	}
	if e = RestoreOnchainBoundary(ctx, h.Store.root); e != nil {
		t.Fatal("restore is not idempotent", e)
	}
}

func TestCostV2PublicationGraceDoesNotEraseConsecutiveClose(t *testing.T) {
	c := v2Case()
	v2Step(&c, "2026-10-04", "88000")
	costAdvanceCase(&c, nil, "2026-10-05", testTime("2026-10-06T00:01:00Z"))
	if c.Count != 1 || c.Quality != "unknown" {
		t.Fatal("normal publication wait erased yesterday", c)
	}
	e := v2Step(&c, "2026-10-05", "89000")
	if len(e) != 1 || e[0].Kind != "confirmed" {
		t.Fatal(c, e)
	}
	c = v2Case()
	v2Step(&c, "2026-10-04", "88000")
	costAdvanceCase(&c, nil, "2026-10-05", testTime("2026-10-06T06:01:00Z"))
	if c.Count != 0 {
		t.Fatal("real missing day did not interrupt")
	}
}
func TestCostV2SQLiteFloorAndUnavailableHealth(t *testing.T) {
	for _, v := range []string{"3.51.3", "3.52.0", "3.50.7", "3.44.6"} {
		if !costSQLiteSafe(v) {
			t.Fatal(v)
		}
	}
	for _, v := range []string{"3.51.2", "3.50.6", "garbage"} {
		if costSQLiteSafe(v) {
			t.Fatal(v)
		}
	}
	h := costTestHub(t)
	original := h.Store.onchain
	h.Store.onchain = &costStore{}
	defer func() { h.Store.onchain = original }()
	if h.onchainHealth().(map[string]any)["status"] != "unavailable" {
		t.Fatal("unavailable module did not isolate")
	}
}

func TestCostV2StatisticsThirtyAndRuleIsolation(t *testing.T) {
	h := costTestHub(t)
	ctx := context.Background()
	now := time.Now().UTC()
	start := now.AddDate(-8, 0, 0)
	for _, spec := range []struct {
		rule string
		n    int
	}{{"prior", 30}, {OnchainRules, 29}} {
		for i := 0; i < spec.n; i++ {
			trial := costTrial{ID: fmt.Sprintf("%s-%d", spec.rule, i), Rules: spec.rule, Group: "onchain", Direction: "down", EpisodeID: fmt.Sprint(i), DetectedAt: start.AddDate(0, 0, i*70)}
			raw, _ := json.Marshal(trial)
			h.Store.onchain.db.Exec("INSERT INTO trials VALUES(?,?,?,?)", trial.ID, trial.DetectedAt.UnixNano(), trial.Group, raw)
			r := costTrialOutcome(trial, 14, nil, now, nil)
			r.Status = "complete"
			v := 2.
			r.DirectionalReturn = &v
			raw, _ = json.Marshal(r)
			h.Store.onchain.db.Exec("INSERT INTO trial_results VALUES(?,?,?)", trial.ID, 14, raw)
		}
	}
	view, e := h.costResearchView(ctx, now, "forward", 0, 10)
	if e != nil {
		t.Fatal(e)
	}
	checked := 0
	for _, g := range view.(map[string]any)["groups"].([]costValidationGroup) {
		if g.Horizon != 14 {
			continue
		}
		checked++
		if g.Rules == OnchainRules && (g.Rate != nil || g.Independent != 29) {
			t.Fatal(g)
		}
		if g.Rules == "prior" && (g.Rate == nil || *g.Rate != 1 || g.Median == nil || *g.Median != 2) {
			t.Fatal(g)
		}
	}
	if checked != 2 {
		t.Fatal(checked)
	}
}

func TestCostV2LegacyFrameRemainsReadableAcrossRollback(t *testing.T) {
	h := costTestHub(t)
	s := h.Store.onchain
	ctx := context.Background()
	now := testTime("2026-10-04T08:00:00Z")
	f := costTestFrame("2026-10-03")
	f.Revision, f.FirstSeen, f.Origin = costFrameRevision(f), now, "imported"
	raw, _ := costPack(f)
	summary, _ := json.Marshal(costSummary{f.Date, f.Revision, now, f.Origin, f.Method, map[string]CostBounds{"5": costConcentration(f, "5")}, f.Price})
	if _, e := s.db.Exec("INSERT INTO frames VALUES(?,?,?,?,?,?,?)", f.Date, f.Revision, now.UnixNano(), f.Origin, f.Method, raw, summary); e != nil {
		t.Fatal(e)
	}
	costSeed(t, h, now.Add(time.Hour), f, false)
	f.Price = "85001"
	costSeed(t, h, now.Add(2*time.Hour), f, false)
	var n int
	if e := s.db.QueryRow("SELECT count(*) FROM frames").Scan(&n); e != nil || n != 1 {
		t.Fatal(n, e)
	}
	var legacy CostFrame
	if e := s.db.QueryRow("SELECT payload FROM frames").Scan(&raw); e != nil {
		t.Fatal(e)
	}
	if e := costUnpack(raw, &legacy); e != nil || len(legacy.STH.Values) == 0 || legacy.Price != "85000" {
		t.Fatal("old reader lost raw values", e, legacy)
	}
	latest, e := s.frame(ctx, "", now.Add(3*time.Hour))
	if e != nil || latest.Price != "85001" || len(latest.STH.Values) == 0 {
		t.Fatal(latest, e)
	}
	sm, e := s.summaries(ctx, now.Add(3*time.Hour))
	if e != nil || len(sm) != 1 || sm[0].Price != "85001" {
		t.Fatal(sm, e)
	}
	s.save(ctx, "observation", costState{Rules: OnchainRules})
	s.save(ctx, "feed", CostFeed{LastDate: f.Date})
	if e = QuiesceOnchain(ctx, h.Store.root); e != nil {
		t.Fatal(e)
	}
	s.db.QueryRow("SELECT count(*) FROM state WHERE key IN ('feed','observation')").Scan(&n)
	if n != 0 {
		t.Fatal("incompatible runtime state retained")
	}
	s.db.QueryRow("SELECT count(*) FROM state WHERE key LIKE 'rollback-%'").Scan(&n)
	if n != 2 {
		t.Fatal("rollback lost runtime audit", n)
	}
	latest, e = s.frame(ctx, "", now.Add(3*time.Hour))
	if e != nil || latest.Price != "85001" {
		t.Fatal("v2 audit deleted", e)
	}
}

func TestCostV2DailyFullChecksExistingPriceOnce(t *testing.T) {
	ctx := context.Background()
	h := costTestHub(t)
	now := testTime("2026-10-04T05:00:00Z")
	f := costTestFrame("2026-10-03")
	b := costBundle{Frames: []CostFrame{f}}
	for i := 0; i < 23; i++ {
		b.Prices = append(b.Prices, CostPrice{Date: costDayAdd(f.Date, -i), Value: "85000"})
	}
	if e := h.Store.onchain.ingest(ctx, b, now, true); e != nil {
		t.Fatal(e)
	}
	prior := now.AddDate(0, 0, -1)
	h.Store.onchain.save(ctx, "feed", CostFeed{Version: "same", LastFull: &prior, LastCheck: &now, LastDate: f.Date, LastPriceDate: f.Date})
	hits := 0
	body := bytes.ReplaceAll(costTestSource(t, f.Date), []byte("85000"), []byte("85001"))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/version" {
			fmt.Fprint(w, `{"version":"same"}`)
			return
		}
		if r.URL.Query().Get("resolution") != "full_v2" {
			t.Error("reconcile must fetch full")
		}
		hits++
		w.Write(body)
	}))
	defer srv.Close()
	for _, at := range []time.Time{now, now.Add(time.Hour), now.Add(2 * time.Hour)} {
		if _, e := h.pollOnchain(ctx, srv.Client(), srv.URL, at); e != nil {
			t.Fatal(e)
		}
	}
	if hits != 1 {
		t.Fatal("existing dates skipped reconciliation or duplicated daily budget", hits)
	}
	p, e := h.Store.onchain.price(ctx, f.Date, now.Add(2*time.Hour))
	if e != nil || p == nil || p.Value != "85001" {
		t.Fatal(p, e)
	}
	old, e := h.Store.onchain.price(ctx, f.Date, now)
	if e != nil || old.Value != "85000" {
		t.Fatal("revision rewrote first availability", old, e)
	}
}

func TestCostV2SettingsBoundarySurvivesCrossDatabaseInterruption(t *testing.T) {
	h := costTestHub(t)
	ctx := context.Background()
	now := time.Now().UTC()
	h.Store.onchain.save(ctx, "settings", CostSettings{true})
	ev := CostEvent{ID: "old-on-enable", Kind: "confirmed", Rules: OnchainRules, Date: costDate(now.AddDate(0, 0, -1)), DetectedAt: now.Add(-time.Minute)}
	costPrepareNotice(t, h, &ev, now)
	// The settings transaction committed, but the shared queue update did not.
	h.Store.onchain.save(ctx, "settings-boundary", map[string]any{"at": now})
	status, e := h.costNoticeEligibility(ctx, ev, CostSettings{true}, now)
	if e != nil || status != "suppressed_setting" {
		t.Fatal(status, e)
	}
}

// Real SMTP protocol on loopback only. Production config still requires TLS;
// the test bypasses config loading and never contacts an external mail server.
func TestCostV2IsolatedSMTPAcceptedAndUnknown(t *testing.T) {
	for _, mode := range []string{"accepted", "unknown"} {
		t.Run(mode, func(t *testing.T) {
			listener, e := net.Listen("tcp", "127.0.0.1:0")
			if e != nil {
				t.Fatal(e)
			}
			defer listener.Close()
			received := make(chan string, 1)
			go func() {
				conn, e := listener.Accept()
				if e != nil {
					received <- "accept failed"
					return
				}
				defer conn.Close()
				conn.SetDeadline(time.Now().Add(5 * time.Second))
				r := bufio.NewReader(conn)
				w := bufio.NewWriter(conn)
				reply := func(s string) { fmt.Fprint(w, s+"\r\n"); w.Flush() }
				reply("220 localhost isolated test")
				for {
					line, e := r.ReadString('\n')
					if e != nil {
						return
					}
					line = strings.TrimSpace(line)
					switch {
					case strings.HasPrefix(line, "EHLO"):
						reply("250-localhost\r\n250 AUTH PLAIN")
					case strings.HasPrefix(line, "AUTH"):
						reply("235 2.7.0 accepted")
					case strings.HasPrefix(line, "MAIL"), strings.HasPrefix(line, "RCPT"):
						reply("250 accepted")
					case line == "DATA":
						reply("354 end with dot")
						var message strings.Builder
						for {
							data, e := r.ReadString('\n')
							if e != nil {
								return
							}
							if data == ".\r\n" {
								break
							}
							message.WriteString(data)
						}
						received <- message.String()
						if mode == "unknown" {
							return
						}
						reply("250 2.0.0 accepted")
					case line == "QUIT":
						reply("221 bye")
						return
					default:
						reply("500 unexpected test command")
					}
				}
			}()
			h := costTestHub(t)
			h.offline = false
			h.mailSend = sendMail
			h.mail = &MailConfig{Host: "127.0.0.1", Port: listener.Addr().(*net.TCPAddr).Port, TLS: "test-loopback", Username: "isolated", Password: "isolated", From: "sender@example.invalid", To: "recipient@example.invalid"}
			now := time.Now().UTC()
			h.boot = now.Add(-time.Hour)
			ctx := context.Background()
			h.Store.onchain.save(ctx, "settings", CostSettings{true})
			event := CostEvent{ID: "smtp-" + mode, Kind: "confirmed", Date: costDate(now.AddDate(0, 0, -1)), DetectedAt: now, Rules: OnchainRules}
			costPrepareNotice(t, h, &event, now)
			tx, _ := h.Store.onchain.db.BeginTx(ctx, nil)
			costInsertEvent(ctx, tx, event, true)
			if e = tx.Commit(); e != nil {
				t.Fatal(e)
			}
			if e = h.processCostNotices(ctx, now); (mode == "accepted" && e != nil) || (mode == "unknown" && e == nil) {
				t.Fatal("unexpected SMTP result", mode, e)
			}
			select {
			case body := <-received:
				if !strings.Contains(body, "SMTP") && !strings.Contains(body, event.ID) {
					t.Fatal("missing frozen event body", body)
				}
			case <-time.After(6 * time.Second):
				t.Fatal("no SMTP DATA")
			}
			var status string
			if e = h.Store.research.QueryRow("SELECT status FROM notices WHERE id=?", "cost-"+event.ID).Scan(&status); e != nil {
				t.Fatal(e)
			}
			if mode == "accepted" && status != "sent" || mode == "unknown" && status != "delivery_unknown" {
				t.Fatal(mode, status)
			}
			if e = h.processCostNotices(ctx, now.Add(time.Minute)); e != nil {
				t.Fatal(e)
			}
			var attempts int
			h.Store.research.QueryRow("SELECT count(*) FROM mail_batches").Scan(&attempts)
			if attempts != 1 {
				t.Fatal("attempt repeated", attempts)
			}
		})
	}
}

func TestCostV2DiscoveryClockExpiresWithoutPrice(t *testing.T) {
	c := v2Case()
	e := costAdvanceCase(&c, nil, "2026-10-11", costClose("2026-10-11"))
	if len(e) != 1 || e[0].Kind != "discovery_expired" || e[0].FirstSeen != nil {
		t.Fatal(c, e)
	}
}
