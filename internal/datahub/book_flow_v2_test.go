package datahub

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestBookCurrentHealthExpiresWithoutInventingFailureHistory(t *testing.T) {
	now := time.Now().UTC()
	at := now.Add(-4 * time.Minute)
	c := minuteContract{Healthy: flowPtr(true), LastSuccessAt: &at, VerifiedAt: &at, ActivatedAt: &at}
	projected := currentMinuteContract(c, now)
	if projected.Healthy == nil || *projected.Healthy || projected.Failure == "" || projected.LastFailureAt != nil || !*c.Healthy {
		t.Fatal(c, projected)
	}
	if currentMinuteContract(minuteContract{}, now).Healthy != nil {
		t.Fatal("legacy unknown became zero/false")
	}
}

// Replay 38353087979: tick 06:02:07.518 read a book fetched at
// 06:02:07.522. A concurrent arrival is not a loss of the still-fresh
// previously observed book, and cannot be used before its acquisition time.
func TestBookFlowArrivalAfterTickDoesNotBreakKnownPath(t *testing.T) {
	h, err := Open(Config{Root: t.TempDir(), Offline: true, BookFlowMode: "run"})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Store.Close()
	ctx := context.Background()
	start := time.Now().UTC().Truncate(time.Minute).Add(-20*time.Minute + 7522*time.Millisecond)
	h.boot = start.Add(-time.Hour)
	seedBookFlowBaseline(t, h, start)
	for i := 0; i < 7; i++ {
		at := start.Add(time.Duration(i) * time.Minute)
		fx, _ := h.Dataset("fx.usd.kraken")
		if _, err = h.Store.Ingest(fx, Observation{Dataset: fx.ID, Source: fx.Source, ObservedAt: &at, FetchedAt: at, Quality: "valid", Payload: Payload{Rates: []Rate{{Quote: "USDT", USD: "1"}}}}); err != nil {
			t.Fatal(err)
		}
		bookFlowResourceInput(t, h, at)
		if i == 5 {
			if err = h.bookFlowStep(ctx, at.Add(-4*time.Millisecond)); err != nil {
				t.Fatal(err)
			}
			var s bookFlowState
			if err = h.Store.bookFlowLoad(ctx, "state", BookFlowRules, &s); err != nil {
				t.Fatal(err)
			}
			for _, e := range s.Blocked {
				if e.Ended || !e.CompletePath || hasBookUpdate(e, "data_gap") {
					t.Fatal("concurrent arrival destroyed known path", e.Venue)
				}
				if s.Current[e.Venue].At.Equal(at.Truncate(time.Minute)) || hasBookUpdate(e, "absorption") {
					t.Fatal("future arrival used early", e.Venue)
				}
			}
		}
		if err = h.bookFlowStep(ctx, at.Add(10*time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	events, err := paperRows[bookEvent](ctx, h.Store.shortDB(), "SELECT payload FROM book_flow WHERE kind='event'")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatal("missing venue", len(events))
	}
	for _, e := range events {
		if !e.CompletePath || !hasBookUpdate(e, "absorption") {
			t.Fatal("valid path failed to advance", e.Venue)
		}
		for _, u := range e.Updates {
			if u.Kind == "absorption" && (u.At.Before(start.Add(5*time.Minute)) || u.Evidence == nil || len(u.Evidence.BeforeLevels) == 0 || len(u.Evidence.AfterLevels) == 0) {
				t.Fatal("unproven absorption", e.Venue)
			}
		}
	}
	// A future latest record must not prolong an expired previous snapshot.
	later := start.Add(10 * time.Minute)
	bookFlowResourceInput(t, h, later.Add(4*time.Millisecond))
	if err = h.bookFlowStep(ctx, later); err != nil {
		t.Fatal(err)
	}
	events, err = paperRows[bookEvent](ctx, h.Store.shortDB(), "SELECT payload FROM book_flow WHERE kind='event'")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range events {
		if !e.Ended || e.CompletePath || !hasBookUpdate(e, "data_gap") {
			t.Fatal("expired evidence prolonged", e.Venue)
		}
	}
}

func TestResearchWritersCoordinateAndCancel(t *testing.T) {
	w := testStore(t)
	ctx := context.Background()
	tx, err := w.research.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO sf_records VALUES('test','rollback',0,'{}')"); err != nil {
		t.Fatal(err)
	}
	short, stop := context.WithTimeout(ctx, 35*time.Millisecond)
	start := time.Now()
	err = w.bookFlowSave(short, "test", "blocked", start, true, false)
	stop()
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 200*time.Millisecond {
		t.Fatal("uncancellable writer", err, time.Since(start))
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	var n int
	w.research.QueryRow("SELECT count(*) FROM sf_records WHERE id='rollback'").Scan(&n)
	if n != 0 {
		t.Fatal("partial rollback")
	}
	errs := make(chan error, 8)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 25; j++ {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				var e error
				if i%2 == 0 {
					e = w.bookFlowSave(ctx, "test", fmt.Sprintf("%d/%d", i, j), time.Now(), true, false)
				} else {
					_, e = w.research.ExecContext(ctx, "INSERT INTO sf_records VALUES('test',?,0,'{}')", fmt.Sprintf("%d/%d", i, j))
				}
				cancel()
				if e != nil {
					errs <- e
					return
				}
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}
	if w.research.writer != w.shortResearch.writer {
		t.Fatal("separate gates")
	}
}
func TestBookContractRecoversWithinBudgetAndKeepsHistory(t *testing.T) {
	w := testStore(t)
	s := NewScheduler(w, bookFlowRegistry("collect"), nil, false, time.Now())
	d := s.jobs[minuteFootID("Binance")].Dataset
	now := time.Now().UTC().Truncate(time.Minute).Add(10 * time.Second)
	if _, err := s.processMinuteContract(context.Background(), d, nil, now, errors.New("source unavailable")); err != nil {
		t.Fatal(err)
	}
	rows := []Observation{}
	for i := 5; i > 0; i-- {
		rows = append(rows, footFixture("Binance", now.Truncate(time.Minute).Add(-time.Duration(i)*time.Minute), now, "80000", "220000"))
	}
	other, err := database(filepath.Join(w.Root(), "research.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	tx, err := other.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec("UPDATE book_flow_budget SET used=used WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { time.Sleep(80 * time.Millisecond); tx.Rollback(); close(done) }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := s.processMinuteContract(ctx, d, rows, now.Add(time.Second), nil)
	<-done
	if err != nil || c.Healthy == nil || !*c.Healthy || c.Failure != "" || c.LastFailure != "source unavailable" || c.LastFailureAt == nil || c.LastSuccessAt == nil || c.RecoveredAt == nil {
		t.Fatal(c, err)
	}
}
func seedBookV2Inputs(t *testing.T, h *Hub, now time.Time) {
	t.Helper()
	fd, _ := h.Dataset("fx.usd.kraken")
	if _, err := h.Store.Ingest(fd, Observation{Dataset: fd.ID, Source: fd.Source, ObservedAt: &now, FetchedAt: now, Quality: "valid", Payload: Payload{Rates: []Rate{{Quote: "USDT", USD: "1"}}}}); err != nil {
		t.Fatal(err)
	}
	for _, venue := range []string{"Binance", "OKX"} {
		d, _ := h.Dataset(ID("book", "BTC", venue, "spot"))
		o := bookFixture(now.Truncate(time.Minute), "30")
		o.Dataset = d.ID
		o.FetchedAt = now
		if _, err := h.Store.Ingest(d, o); err != nil {
			t.Fatal(err)
		}
		md, _ := h.Dataset(minuteFootID(venue))
		foot := footFixture(venue, now.Truncate(time.Minute).Add(-time.Minute), now, "80000", "220000")
		if _, err := h.Store.Ingest(md, foot); err != nil {
			t.Fatal(err)
		}
		c := minuteContract{Venue: venue, Healthy: flowPtr(true), VerifiedAt: &now, ActivatedAt: &now, LastSuccessAt: &now}
		if err := h.Store.bookFlowSave(context.Background(), "contract", md.ID, now, c, false); err != nil {
			t.Fatal(err)
		}
	}
}
func TestBookCollectionNaturalMinutesFreezeAndRunOrigin(t *testing.T) {
	h, err := Open(Config{Root: t.TempDir(), Offline: true, BookFlowMode: "collect"})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Store.Close()
	ctx := context.Background()
	start := time.Now().UTC().Truncate(time.Minute).Add(10 * time.Second)
	h.boot = start.Add(-time.Hour)
	for i := 0; i < 65; i++ {
		now := start.Add(time.Duration(i) * time.Minute)
		if i != 20 {
			seedBookV2Inputs(t, h, now)
		}
		if err = h.bookFlowCycle(ctx, now); err != nil {
			t.Fatal(i, err)
		}
	}
	now := start.Add(64 * time.Minute)
	a, err := h.bookFlowAcceptance(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	if a["observedMinutes"] != 60 || a["timelyMinutes"] != 58 || !a["ready"].(bool) {
		t.Fatal(a)
	}
	var origin time.Time
	if err = h.Store.bookFlowLoad(ctx, "origin", BookFlowRules, &origin); err != sql.ErrNoRows {
		t.Fatal("collect enrolled", err)
	}
	var before bookCollection
	h.Store.bookFlowLoad(ctx, "collection", BookFlowCollection, &before)
	// Re-fetching a missed minute now cannot change its frozen deadline result.
	md, _ := h.Dataset(minuteFootID("Binance"))
	late := footFixture("Binance", start.Truncate(time.Minute).Add(19*time.Minute), now, "80000", "220000")
	h.Store.Ingest(md, late)
	if err = h.collectBookFlow(ctx, now); err != nil {
		t.Fatal(err)
	}
	var after bookCollection
	h.Store.bookFlowLoad(ctx, "collection", BookFlowCollection, &after)
	if before.Closed[start.Truncate(time.Minute).Add(19*time.Minute).Unix()] != after.Closed[start.Truncate(time.Minute).Add(19*time.Minute).Unix()] {
		t.Fatal("late repair")
	}
	h.bookFlowMode = "run"
	if err = h.bookFlowCycle(ctx, now); err != nil {
		t.Fatal(err)
	}
	if err = h.Store.bookFlowLoad(ctx, "origin", BookFlowRules, &origin); err != nil || !origin.Equal(now) {
		t.Fatal("origin backdated", origin, err)
	}
}
func bookNoticeTestHub(t *testing.T) (*Hub, time.Time) {
	t.Helper()
	h, err := Open(Config{Root: t.TempDir(), Offline: true, BookFlowMode: "run"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Store.Close() })
	now := time.Now().UTC()
	h.boot = now.Add(-time.Hour)
	h.offline = false
	h.mail = &MailConfig{To: "test@example.invalid"}
	seedBookV2Inputs(t, h, now)
	if err = h.Store.bookFlowSave(context.Background(), "receipt", BookFlowMailTemplate+"/verified", now, radarReceiptRecord{now, h.radarRecipient()}, false); err != nil {
		t.Fatal(err)
	}
	if _, err = h.SetBookFlowSettings(context.Background(), true, now.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	return h, now
}
func queueBookTestNotice(t *testing.T, h *Hub, now time.Time, venue, suffix, stage string) bookEvent {
	t.Helper()
	p := buildBookPoint(bookFixture(now.Add(-time.Minute), "30"), flowPtr("1"), now.Add(-time.Minute))
	z := *zoneIn(p, bookZone{Side: "buy", Low: "80300"})
	z.IncreaseUSD = flowPtr("1607000")
	z.Multiple = flowPtr("3")
	e := newBookEvent(venue, p, z, now.Add(-30*time.Second))
	e.ID += "/" + suffix
	e.Expires = now.Add(20 * time.Minute)
	u := bookUpdate{Kind: stage, At: now, SourceAt: now.Add(-time.Second)}
	appendBookUpdate(&e, stage, now, u.SourceAt, nil)
	if err := h.Store.bookFlowSave(context.Background(), "event", e.ID, now, e, false); err != nil {
		t.Fatal(err)
	}
	s, err := h.bookSettings(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	tx, err := h.Store.shortDB().BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = queueBookNotice(context.Background(), tx, s, e, u, now); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return e
}
func TestBookMailMergeDedupLimitsAndSeparateClock(t *testing.T) {
	h, now := bookNoticeTestHub(t)
	calls := 0
	h.mailSend = func(_ context.Context, _ MailConfig, title, body string) error {
		calls++
		if !strings.Contains(title, "效果验证中") || !strings.Contains(body, "Binance") || calls == 1 && !strings.Contains(body, "OKX") {
			t.Fatal(title, body)
		}
		return nil
	}
	e := queueBookTestNotice(t, h, now, "Binance", "one", "persistent")
	queueBookTestNotice(t, h, now, "OKX", "two", "persistent")
	ctx := context.Background()
	if err := h.processBookNotices(ctx, now); err != nil {
		t.Fatal(err)
	}
	if err := h.processBookNotices(ctx, now); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatal("not merged/deduped", calls)
	}
	var trial ShortTrial
	if err := h.Store.bookFlowLoad(ctx, "trial", e.ID+"/mail-persistent", &trial); err != nil || trial.At.Before(now) || !trial.Start.Equal(trial.At.Truncate(5*time.Minute).Add(5*time.Minute)) {
		t.Fatal(trial, err)
	}
	queueBookTestNotice(t, h, now, "Binance", "three", "persistent")
	if err := h.processBookNotices(ctx, now); err != nil {
		t.Fatal(err)
	}
	four := queueBookTestNotice(t, h, now, "Binance", "four", "persistent")
	if err := h.processBookNotices(ctx, now); err != nil {
		t.Fatal(err)
	}
	var status string
	h.Store.research.QueryRow("SELECT status FROM notices WHERE id=?", four.ID+"/mail/persistent").Scan(&status)
	if calls != 2 || status != "suppressed_rate_limit" {
		t.Fatal(calls, status)
	}
}
func TestBookMailSuppressesRestartStaleGapAndPriority(t *testing.T) {
	for _, reason := range []string{"restart", "stale", "gap", "priority", "unknown"} {
		t.Run(reason, func(t *testing.T) {
			h, now := bookNoticeTestHub(t)
			calls := 0
			h.mailSend = func(context.Context, MailConfig, string, string) error {
				calls++
				return errors.New("unknown submission")
			}
			e := queueBookTestNotice(t, h, now, "Binance", reason, "persistent")
			want := ""
			switch reason {
			case "restart":
				h.boot = now.Add(time.Second)
				want = "suppressed_restart"
			case "stale":
				now = now.Add(4 * time.Minute)
				want = "suppressed_stale"
			case "gap":
				e.CompletePath = false
				h.Store.bookFlowSave(context.Background(), "event", e.ID, now, e, false)
				want = "suppressed_evidence"
			case "priority":
				h.Store.research.Exec("INSERT INTO notices(id,kind,status,payload) VALUES('older','anomaly','pending','{}')")
				want = "suppressed_existing_priority"
			case "unknown":
				want = "delivery_unknown"
			}
			h.processBookNotices(context.Background(), now)
			h.processBookNotices(context.Background(), now.Add(time.Second))
			var status string
			h.Store.research.QueryRow("SELECT status FROM notices WHERE id=?", e.ID+"/mail/persistent").Scan(&status)
			if status != want || reason == "unknown" && calls != 1 || reason != "unknown" && calls != 0 {
				t.Fatal(status, calls)
			}
		})
	}
}
func TestBookLatePublicationNeverEnrolls(t *testing.T) {
	h, err := Open(Config{Root: t.TempDir(), Offline: true, BookFlowMode: "run"})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Store.Close()
	now := time.Now().UTC()
	p := buildBookPoint(bookFixture(now, "30"), flowPtr("1"), now)
	e := newBookEvent("Binance", p, *zoneIn(p, bookZone{Side: "buy", Low: "80300"}), now)
	if err = h.commitBookFlow(context.Background(), bookFlowState{Origin: now, Parameters: bookFlowParameters()}, map[string]bookEvent{e.ID: e}); err != nil {
		t.Fatal(err)
	}
	if err = h.publishBookFlow(context.Background(), now.Add(10*time.Second)); err == nil {
		t.Fatal("late publication accepted")
	}
	if err = h.publishBookFlow(context.Background(), now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	var n int
	h.Store.shortDB().QueryRow("SELECT count(*) FROM book_flow WHERE kind IN ('clock','trial')").Scan(&n)
	if n != 0 {
		t.Fatal("re-enrolled", n)
	}
}
func TestBookIntervalUsesCompleteEvidenceCohort(t *testing.T) {
	h, err := Open(Config{Root: t.TempDir(), Offline: true, BookFlowMode: "run"})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Store.Close()
	now := time.Now().UTC()
	origin := now.Add(-40 * 24 * time.Hour)
	ctx := context.Background()
	for i := 0; i < 102; i++ {
		at := origin.Add(time.Duration(i) * 5 * time.Hour)
		id := fmt.Sprintf("%s/sample/%d", BookFlowRules, i)
		e := bookEvent{ID: id, CompletePath: i < 100}
		h.Store.bookFlowSave(ctx, "event", id, at, e, false)
		side := "buy"
		if i%2 == 1 {
			side = "sell"
		}
		value := 1.0
		if i >= 100 {
			value = -100
		}
		trial := newShortTrial(id+"/persistent", BookFlowRules+"/persistent", side, at, at, nil)
		trial.Outcomes = []ShortOutcome{{Minutes: 240, State: "complete", Return: &value}}
		trial.Done = true
		h.Store.bookFlowSave(ctx, "trial", trial.ID, at, trial, false)
	}
	r, err := h.bookFlowReport(ctx, bookFlowState{Origin: origin, Days: map[string]bookFlowDay{"all": {Samples: 100, Timely: 100}}}, now)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(r)
	var result struct {
		Groups []struct {
			Stage    string
			Eligible bool
			Mean     string `json:"meanPercent"`
			Quality  struct {
				N    int    `json:"independent4h"`
				Mean string `json:"meanPercent"`
			} `json:"completeEvidence"`
			Interval struct {
				Low  string `json:"lowPercent"`
				High string `json:"highPercent"`
			} `json:"interval95"`
		}
	}
	if err = json.Unmarshal(b, &result); err != nil {
		t.Fatal(err)
	}
	for _, g := range result.Groups {
		if g.Stage == "persistent" && (!g.Eligible || g.Quality.N != 100 || g.Quality.Mean != "1" || g.Interval.Low != "1" || g.Interval.High != "1" || g.Mean == "1") {
			t.Fatal(string(b))
		}
	}
}

func TestBookFlowViewDoesNotQueueBehindFormalResearch(t *testing.T) {
	h, err := Open(Config{Root: t.TempDir(), Offline: true, BookFlowMode: "collect"})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Store.Close()
	now := time.Now().UTC()
	if _, err = h.Store.shortDB().Exec("INSERT INTO notices(id,signal_id,kind,created,status,payload) VALUES('book-test','event-test','book-flow:persistent',?,'disabled',?)", now.Unix(), []byte(`{"templateVersion":"book-flow-two-stage-v1"}`)); err != nil {
		t.Fatal(err)
	}
	// A formal history reader can retain its connection while decoding. The
	// complete book view, including mail audit, must use the bounded short pool.
	busy, err := h.Store.research.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	v, err := h.bookFlowView(ctx, url.Values{"asset": {"BTC"}}, now)
	if err != nil {
		t.Fatal("book view waited for formal history", err)
	}
	notices := v.(map[string]any)["notifications"].([]any)
	if len(notices) != 1 || notices[0].(map[string]any)["id"] != "book-test" {
		t.Fatal("audit hidden instead of read", notices)
	}
	if h.Store.research.Stats().MaxOpenConnections != 1 || h.Store.shortDB().Stats().MaxOpenConnections != 1 {
		t.Fatal("connection budget expanded")
	}
}
