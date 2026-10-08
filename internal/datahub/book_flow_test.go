package datahub

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"testing"
	"time"
)

func bookFixture(at time.Time, qty string) Observation {
	b := &Book{Bids: []Level{{"79000", "1"}, {"80350", qty}, {"80550", "1"}, {"80600", "1"}}, Asks: []Level{{"80601", "1"}, {"80850", "10"}, {"81200", "1"}, {"82000", "1"}}}
	o := Observation{Dataset: ID("book", "BTC", "Binance", "spot"), Source: "coinglass", ObservedAt: &at, FetchedAt: at.Add(5 * time.Second), Resolution: 60, Quality: "valid", Payload: Payload{Book: b}}
	o.Revision = digest(o)
	return o
}
func bookPrior(at time.Time, qty string) []bookPoint {
	rows := []bookPoint{}
	for i := 30; i > 0; i-- {
		o := bookFixture(at.Add(-time.Duration(i)*time.Minute), qty)
		rows = append(rows, buildBookPoint(o, flowPtr("1"), o.FetchedAt))
	}
	return rows
}
func TestBookFlowFixedRegionGrowthAndUnknown(t *testing.T) {
	at := time.Now().UTC().Truncate(time.Minute)
	prior := bookPrior(at, "10")
	p := buildBookPoint(bookFixture(at, "30"), flowPtr("1"), at.Add(10*time.Second))
	z := rawBookZone(bookFixture(at, "30").Payload.Book.Bids, "buy", dec("80300"))
	v := evaluateBookZone(z, p, prior, at.Add(-time.Hour))
	if !v.Enhanced || *v.Multiple != "3" || *v.IncreaseUSD != "1607000" {
		t.Fatal(v)
	}
	// Moving reference price and FX cannot manufacture native quantity growth.
	unchanged := buildBookPoint(bookFixture(at, "10"), flowPtr("2"), at.Add(10*time.Second))
	unchanged.Reference = flowPtr("80400")
	u := evaluateBookZone(*zoneIn(unchanged, z), unchanged, prior, at.Add(-time.Hour))
	if u.Enhanced || *u.IncreaseUSD != "0" {
		t.Fatal("unchanged wall reported as growth", u)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*bookPoint, *bookZone, *[]bookPoint)
	}{
		{"fx", func(p *bookPoint, z *bookZone, h *[]bookPoint) { p.FX = nil }},
		{"coverage", func(p *bookPoint, z *bookZone, h *[]bookPoint) { *h = (*h)[:28] }},
		{"missing", func(p *bookPoint, z *bookZone, h *[]bookPoint) { z.Quantity = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pp, zz, hh := p, z, append([]bookPoint(nil), prior...)
			tc.mutate(&pp, &zz, &hh)
			if evaluateBookZone(zz, pp, hh, at.Add(-time.Hour)).Enhanced {
				t.Fatal("unknown became qualified")
			}
		})
	}
	if rawBookZone([]Level{{"80350", "100"}}, "buy", dec("80300")).Quantity != nil {
		t.Fatal("truncated returned range treated complete")
	}
}
func footFixture(venue string, at, fetched time.Time, buy, sell string) Observation {
	f := Foot{Low: "80300", High: "80500", BuyBase: dec(buy).Div(dec("80400")).String(), SellBase: dec(sell).Div(dec("80400")).String(), BuyQuote: buy, SellQuote: sell, BuyUSDT: buy, SellUSDT: sell, BuyCount: 10, SellCount: 20}
	o := Observation{Dataset: minuteFootID(venue), Source: "coinglass", ObservedAt: &at, FetchedAt: fetched, Resolution: 60, Quality: "valid", Payload: Payload{Foot: []Foot{f}}}
	o.Revision = digest(o)
	return o
}
func TestBookFlowAbsorptionNegativeDeltaAndBoundaries(t *testing.T) {
	at := time.Now().UTC().Truncate(time.Minute)
	p := buildBookPoint(bookFixture(at.Add(-6*time.Minute), "30"), flowPtr("1"), at.Add(-6*time.Minute+10*time.Second))
	z := *zoneIn(p, bookZone{Side: "buy", Low: "80300"})
	e := newBookEvent("Binance", p, z, at.Add(-6*time.Minute+10*time.Second))
	rows := []Observation{}
	for i := 0; i < 5; i++ {
		rows = append(rows, footFixture("Binance", at.Add(time.Duration(i-5)*time.Minute), at.Add(5*time.Second), "80000", "220000"))
	}
	points := []bookPoint{buildBookPoint(bookFixture(at.Add(-5*time.Minute), "30"), flowPtr("1"), at.Add(-5*time.Minute+5*time.Second)), buildBookPoint(bookFixture(at, "25"), flowPtr("1"), at.Add(5*time.Second))}
	v := evaluateBookFoot(e, rows, points, flowPtr("1"), at.Add(10*time.Second))
	if v == nil || !v.Absorption || v.Following {
		t.Fatal("selling into retained bids is absorption", v)
	}
	for i := range rows {
		rows[i].Payload.Foot[0].BuyQuote, rows[i].Payload.Foot[0].SellQuote = "220000", "80000"
	}
	v = evaluateBookFoot(e, rows, points, flowPtr("1"), at.Add(10*time.Second))
	if v == nil || v.Absorption || !v.Following {
		t.Fatal(v)
	}
	rows[0].Dataset = minuteFootID("OKX")
	if evaluateBookFoot(e, rows, points, flowPtr("1"), at.Add(10*time.Second)) != nil {
		t.Fatal("cross-venue proof")
	}
	rows[0].Dataset = minuteFootID("Binance")
	rows[0].Payload.Foot = append(rows[0].Payload.Foot, Foot{Low: "80290", High: "80310", SellQuote: "10000000"})
	v = evaluateBookFoot(e, rows, points, flowPtr("1"), at.Add(10*time.Second))
	if v == nil || v.Following {
		t.Fatal("boundary uncertainty guessed away")
	}
	if evaluateBookFoot(e, rows[:4], points, flowPtr("1"), at.Add(10*time.Second)) != nil {
		t.Fatal("partial five minutes")
	}
	if evaluateBookFoot(e, rows, points, flowPtr("1"), at.Add(4*time.Minute)) != nil {
		t.Fatal("late footprint qualified")
	}
}
func TestBookFlowProfileBudgetAndStrictFreshness(t *testing.T) {
	reg := bookFlowRegistry("run")
	cost := 0.0
	for _, d := range reg {
		if d.Source != "coinglass" || d.Disabled {
			continue
		}
		if coreFiveFoot(d) {
			continue
		}
		cost += 60 / float64(d.Refresh)
		if coreBook(d) && d.Refresh != 60 {
			t.Fatal(d)
		}
		if minuteFoot(d) && (d.Resolution != 60 || d.Refresh != 60 || d.Params["limit"] != "10") {
			t.Fatal(d)
		}
		if d.Kind == "whales" && (d.Refresh != 900 || d.TTL != 480) {
			t.Fatal("slower polling rejuvenated old whale", d)
		}
	}
	if math.Abs(cost-9.77361111111111) > .00001 {
		t.Fatal("quota planning drift", cost)
	}
	q := Quota{}
	at := time.Now()
	for i := 0; i < 12; i++ {
		n := at.Add(time.Duration(i) * 5100 * time.Millisecond)
		if !q.available(n) {
			t.Fatal(i)
		}
		q.Starts = append(q.Starts, n)
	}
	if q.available(at.Add(59 * time.Second)) {
		t.Fatal("rolling quota exceeded")
	}
}
func TestMinuteFootContractForwardCutoverAndNoDuplicate(t *testing.T) {
	w := testStore(t)
	reg := bookFlowRegistry("collect")
	s := NewScheduler(w, reg, nil, false, time.Now())
	d := Dataset{}
	for _, v := range reg {
		if v.ID == minuteFootID("Binance") {
			d = v
		}
	}
	at := time.Now().UTC().Truncate(5 * time.Minute)
	var c minuteContract
	for n := 0; n < 3; n++ {
		fetched := at.Add(time.Duration(n)*time.Minute + 10*time.Second)
		rows := []Observation{}
		for i := 0; i < 10; i++ {
			rows = append(rows, footFixture("Binance", fetched.Truncate(time.Minute).Add(-time.Duration(10-i)*time.Minute), fetched, "80000", "220000"))
		}
		var e error
		c, e = s.processMinuteContract(context.Background(), d, rows, fetched, nil)
		if e != nil {
			t.Fatal(e)
		}
		if n < 2 && c.VerifiedAt != nil {
			t.Fatal("one reply prematurely verified")
		}
		dup, e := s.processMinuteContract(context.Background(), d, rows, fetched.Add(time.Second), nil)
		if e != nil {
			t.Fatal(e)
		}
		if dup.Consecutive != c.Consecutive {
			t.Fatal("duplicate response counted")
		}
	}
	if c.VerifiedAt == nil || c.Cutover == nil || !c.Cutover.After(*c.VerifiedAt) {
		t.Fatal(c)
	}
	rows := []Observation{}
	fetched := c.Cutover.Add(5*time.Minute + 10*time.Second)
	for i := 0; i < 5; i++ {
		rows = append(rows, footFixture("Binance", c.Cutover.Add(time.Duration(i)*time.Minute), fetched, "80000", "220000"))
	}
	c, err := s.processMinuteContract(context.Background(), d, rows, fetched, nil)
	if err != nil {
		t.Fatal(err)
	}
	if c.ActivatedAt == nil {
		t.Fatal("complete aggregate not activated")
	}
	legacy, _ := FindDataset(ID("footprint", "BTC", "Binance", "spot"))
	latest, ok := w.Latest(legacy.ID)
	if !ok || latest.Source != "derived/coinglass-minute-v1" || latest.Payload.Foot[0].BuyQuote != "400000" || latest.Samples != 5 {
		t.Fatal(latest)
	}
	bad := rows[0]
	bad.Payload.Foot = append([]Foot(nil), bad.Payload.Foot...)
	bad.Payload.Foot[0].BuyQuote = "999999999999"
	if validMinuteFoot(bad) {
		t.Fatal("unit/amount contract violated")
	}
}
func TestBookFlowAtomicStateForwardOnlyAndGET(t *testing.T) {
	h, err := Open(Config{Root: t.TempDir(), Offline: true, BookFlowMode: "run"})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Store.Close()
	at := time.Now().UTC().Truncate(time.Minute)
	h.boot = at.Add(-time.Hour)
	for n := 0; n < 33; n++ {
		source := at.Add(time.Duration(n) * time.Minute)
		now := source.Add(10 * time.Second)
		fxd, _ := h.Dataset("fx.usd.kraken")
		fx := Observation{Dataset: fxd.ID, Source: fxd.Source, ObservedAt: &now, FetchedAt: now, Quality: "valid", Payload: Payload{Rates: []Rate{{Quote: "USDT", USD: "1"}}}}
		if _, err = h.Store.Ingest(fxd, fx); err != nil {
			t.Fatal(err)
		}
		for _, venue := range []string{"Binance", "OKX"} {
			d, _ := h.Dataset(ID("book", "BTC", venue, "spot"))
			qty := "10"
			if n >= 31 {
				qty = "30"
			}
			o := bookFixture(source, qty)
			o.Dataset = d.ID
			if _, err = h.Store.Ingest(d, o); err != nil {
				t.Fatal(err)
			}
		}
		if err = h.bookFlowStep(context.Background(), now); err != nil {
			t.Fatal(n, err)
		}
	}
	var count int
	if err = h.Store.shortDB().QueryRow("SELECT count(*) FROM book_flow WHERE kind='first'").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatal("overlap/repeated event", count)
	}
	var state bookFlowState
	if err = h.Store.bookFlowLoad(context.Background(), "state", BookFlowRules, &state); err != nil {
		t.Fatal(err)
	}
	if len(state.Blocked) != 2 || !hasBookUpdate(state.Blocked[0], "persistent") {
		t.Fatal(state.Blocked)
	}
	before := h.Scheduler.State()["calls"]
	for _, path := range []string{"book-flow", "alert-audit"} {
		q := url.Values{}
		if path == "alert-audit" {
			q.Set("layer", "book")
		}
		_, err = h.Read(context.Background(), path, q)
		if err != nil {
			t.Fatal(path, err)
		}
	}
	if h.Scheduler.State()["calls"] != before {
		t.Fatal("GET fetched upstream")
	}
	var notices int
	h.Store.research.QueryRow("SELECT count(*) FROM notices").Scan(&notices)
	if notices != 0 {
		t.Fatal("new mail")
	}
	// Atomic rollback: a full sub-budget cannot leave a first event without its
	// state/observation, nor mutate the previous immutable first evidence.
	var first []byte
	h.Store.shortDB().QueryRow("SELECT payload FROM book_flow WHERE kind='first' ORDER BY id LIMIT 1").Scan(&first)
	if _, err = h.Store.shortDB().Exec("UPDATE book_flow_budget SET used=?", bookFlowBudget); err != nil {
		t.Fatal(err)
	}
	e := state.Blocked[0]
	e.ID += "/unregistered"
	e.Updates = nil
	if err = h.commitBookFlow(context.Background(), state, map[string]bookEvent{e.ID: e}); err == nil {
		t.Fatal("budget ignored")
	}
	h.Store.shortDB().QueryRow("SELECT count(*) FROM book_flow WHERE kind='first'").Scan(&count)
	if count != 2 {
		t.Fatal("partial commit")
	}
	var old bookEvent
	if err = json.Unmarshal(first, &old); err != nil || old.ID == e.ID {
		t.Fatal(err)
	}
}
func TestBookFlowDailyIntervalDeterministic(t *testing.T) {
	at := time.Now().UTC().Add(-31 * 24 * time.Hour)
	d := map[string][]float64{}
	for i := 0; i < 31; i++ {
		d[at.Add(time.Duration(i)*24*time.Hour).Format("2006-01-02")] = []float64{float64(i%5) - 2}
	}
	a, _ := json.Marshal(bookDailyInterval(d, at, at.Add(31*24*time.Hour)))
	b, _ := json.Marshal(bookDailyInterval(d, at, at.Add(31*24*time.Hour)))
	if string(a) != string(b) {
		t.Fatal(fmt.Sprint(string(a), string(b)))
	}
}

func TestBookFlowPublicationClockAndRecovery(t *testing.T) {
	h, err := Open(Config{Root: t.TempDir(), Offline: true, BookFlowMode: "run"})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Store.Close()
	at := time.Now().UTC().Truncate(5 * time.Minute).Add(-time.Second)
	p := buildBookPoint(bookFixture(at, "30"), flowPtr("1"), at)
	z := *zoneIn(p, bookZone{Side: "buy", Low: "80300"})
	e := newBookEvent("Binance", p, z, at)
	s := bookFlowState{Origin: at, Parameters: bookFlowParameters()}
	if err = h.commitBookFlow(context.Background(), s, map[string]bookEvent{e.ID: e}); err != nil {
		t.Fatal(err)
	}
	var n int
	h.Store.shortDB().QueryRow("SELECT count(*) FROM book_flow WHERE kind='trial'").Scan(&n)
	if n != 0 {
		t.Fatal("enrolled before committed evidence was read")
	}
	visible := at.Add(2 * time.Second)
	if err = h.publishBookFlow(context.Background(), visible); err != nil {
		t.Fatal(err)
	}
	if err = h.publishBookFlow(context.Background(), visible.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	var tr ShortTrial
	if err = h.Store.bookFlowLoad(context.Background(), "trial", e.ID+"/enhanced", &tr); err != nil {
		t.Fatal(err)
	}
	if !tr.At.Equal(visible) || !tr.Start.Equal(visible.Truncate(5*time.Minute).Add(5*time.Minute)) {
		t.Fatal("outcome backdated across five-minute boundary", tr)
	}
	var clock time.Time
	h.Store.bookFlowLoad(context.Background(), "clock", e.ID+"/enhanced", &clock)
	if !clock.Equal(visible) {
		t.Fatal("duplicate publication changed origin")
	}
	h.Store.shortDB().QueryRow("SELECT count(*) FROM book_flow WHERE kind='trial'").Scan(&n)
	if n != 1 {
		t.Fatal(n)
	}
}

func TestMinuteProbeTerminatedWithoutDisablingLegacy(t *testing.T) {
	w := testStore(t)
	reg := bookFlowRegistry("collect")
	s := NewScheduler(w, reg, func(context.Context, Dataset) ([]byte, error) { return []byte(`{"code":"0","data":[]}`), nil }, true, time.Now())
	j := s.jobs[minuteFootID("Binance")]
	for i := 0; i < 10; i++ {
		s.mu.Lock()
		s.inflight++
		j.InFlight = true
		s.mu.Unlock()
		s.run(context.Background(), *j)
	}
	if !j.Disabled || j.ContractStatus != "failed" {
		t.Fatal("unbounded probe", j)
	}
	if s.jobs[ID("footprint", "BTC", "Binance", "spot")].Disabled {
		t.Fatal("failed probe removed old source")
	}
}

func TestMinuteFootRealParserPathAndEvidence(t *testing.T) {
	h, err := Open(Config{Root: t.TempDir(), Offline: true, BookFlowMode: "run"})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Store.Close()
	now := time.Now().UTC().Truncate(time.Minute).Add(-10*time.Minute + 10*time.Second)
	h.boot = now.Add(-time.Hour)
	seedBookFlowBaseline(t, h, now.Add(-time.Minute))
	for i := 0; i < 9; i++ {
		at := now.Add(time.Duration(i) * time.Minute)
		d, _ := h.Dataset("fx.usd.kraken")
		_, err = h.Store.Ingest(d, Observation{Dataset: d.ID, Source: d.Source, ObservedAt: &at, FetchedAt: at, Quality: "valid", Payload: Payload{Rates: []Rate{{Quote: "USDT", USD: "1"}}}})
		if err != nil {
			t.Fatal(err)
		}
		bookFlowResourceInput(t, h, at)
		if err = h.bookFlowStep(context.Background(), at); err != nil {
			t.Fatal(err)
		}
	}
	events, err := paperRows[bookEvent](context.Background(), h.Store.shortDB(), "SELECT payload FROM book_flow WHERE kind='event'")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatal("actual parser/worker path", len(events))
	}
	for _, e := range events {
		if !hasBookUpdate(e, "absorption") {
			t.Fatal("missing absorption", e.ID)
		}
		for _, u := range e.Updates {
			if u.Kind == "absorption" && (len(u.Evidence.BeforeLevels) == 0 || len(u.Evidence.AfterLevels) == 0 || u.Evidence.FX == nil) {
				t.Fatal("unfrozen raw depth or FX")
			}
		}
	}
	// A retry of one source minute cannot increase the calendar-minute denominator.
	var a, b bookFlowState
	h.Store.bookFlowLoad(context.Background(), "state", BookFlowRules, &a)
	if err = h.bookFlowStep(context.Background(), now.Add(8*time.Minute+time.Second)); err != nil {
		t.Fatal(err)
	}
	h.Store.bookFlowLoad(context.Background(), "state", BookFlowRules, &b)
	aa, _ := json.Marshal(a.Days)
	bb, _ := json.Marshal(b.Days)
	if string(aa) != string(bb) {
		t.Fatal("duplicate inflated coverage")
	}
	// Restarted worker ends its old active path, preserving initial evidence.
	h.boot = now.Add(8*time.Minute + 2*time.Second)
	if err = h.bookFlowStep(context.Background(), now.Add(8*time.Minute+3*time.Second)); err != nil {
		t.Fatal(err)
	}
	events, err = paperRows[bookEvent](context.Background(), h.Store.shortDB(), "SELECT payload FROM book_flow WHERE kind='event'")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range events {
		if e.CompletePath || !hasBookUpdate(e, "data_gap") {
			t.Fatal("restart silently joined path", e.ID)
		}
	}
}

func TestBookFlowSellAbsorptionSymmetry(t *testing.T) {
	at := time.Now().UTC().Truncate(time.Minute)
	p := buildBookPoint(bookFixture(at.Add(-6*time.Minute), "30"), flowPtr("1"), at.Add(-6*time.Minute))
	z := bookZone{Side: "sell", Low: "80300", High: "80500", Quantity: flowPtr("30")}
	e := newBookEvent("OKX", p, z, at.Add(-6*time.Minute))
	points := []bookPoint{{At: at.Add(-5 * time.Minute), Fetched: at.Add(-5 * time.Minute), Known: at.Add(-5 * time.Minute), Zones: []bookZone{z}}, {At: at, Fetched: at, Known: at, Zones: []bookZone{z}}}
	rows := []Observation{}
	for i := 5; i > 0; i-- {
		rows = append(rows, footFixture("OKX", at.Add(-time.Duration(i)*time.Minute), at, "240000", "80000"))
	}
	v := evaluateBookFoot(e, rows, points, flowPtr("1"), at.Add(time.Second))
	if v == nil || !v.Absorption || v.Following {
		t.Fatal("buy aggressors into retained asks", v)
	}
	points[1].Zones = []bookZone{{Side: "sell", Low: "80300", High: "80500", Quantity: flowPtr("20")}}
	v = evaluateBookFoot(e, rows, points, flowPtr("1"), at.Add(time.Second))
	if v == nil || v.Absorption {
		t.Fatal("depth retention below threshold", v)
	}
}

// Full registry scheduling replay, using the real selection and quota gate.
// Latencies/failures are explicit stress inputs, not claimed as a measured SLA.
func TestBookFlowFullRegistrySchedulingBounds(t *testing.T) {
	w := testStore(t)
	start := time.Now().UTC().Truncate(time.Minute)
	s := NewScheduler(w, bookFlowRegistry("collect"), nil, true, start)
	for _, j := range s.jobs {
		if coreFiveFoot(j.Dataset) {
			j.Disabled = true
		}
	}
	type flight struct {
		j    *Job
		end  time.Time
		fail bool
	}
	flights := []flight{}
	counts := map[string]int{}
	maxQueue := map[string]time.Duration{}
	requests := 0
	for now := start; now.Before(start.Add(2 * time.Hour)); now = now.Add(250 * time.Millisecond) {
		active := []flight{}
		for _, f := range flights {
			if f.end.After(now) {
				active = append(active, f)
				continue
			}
			f.j.InFlight = false
			f.j.LastSuccess = &now
			if f.fail {
				s.quota.Cooldown = now.Add(time.Minute)
				f.j.Next = now.Add(time.Minute)
			} else {
				for !f.j.Next.After(now) {
					f.j.Next = f.j.Next.Add(time.Duration(f.j.Dataset.Refresh) * time.Second)
				}
			}
		}
		flights = active
		if len(flights) >= 2 || !s.quota.available(now) {
			continue
		}
		j := s.selectJobLocked(now)
		if j == nil {
			continue
		}
		wait := now.Sub(j.Next)
		if wait > maxQueue[j.ID] {
			maxQueue[j.ID] = wait
		}
		counts[j.ID]++
		requests++
		j.InFlight = true
		s.quota.Starts = append(s.quota.Starts, now)
		delay := []time.Duration{time.Second, 2 * time.Second, 5 * time.Second, 12 * time.Second, 30 * time.Second}[requests%5]
		flights = append(flights, flight{j, now.Add(delay), requests == 65 || requests == 251})
		if len(s.quota.Starts) > 12 || len(flights) > 2 {
			t.Fatal("quota/concurrency bypass")
		}
	}
	for _, j := range s.jobs {
		if j.Disabled || j.Dataset.Source != "coinglass" {
			continue
		}
		if j.Mode == "live" && j.Dataset.Refresh <= 1800 && counts[j.ID] == 0 {
			t.Fatal("starved live source", j.ID)
		}
		if coreBook(j.Dataset) || minuteFoot(j.Dataset) {
			if counts[j.ID] < 100 || maxQueue[j.ID] > 3*time.Minute {
				t.Fatal("core cadence starved", j.ID, counts[j.ID], maxQueue[j.ID])
			}
		}
	}
	for _, market := range []string{"spot", "futures"} {
		id := ID("flow", "BTC", "", market)
		if counts[id] < 15 {
			t.Fatal("formal dependency starved", id, counts[id])
		}
	}
	t.Logf("2h full registry bounded replay: %d starts; preserved 12/min, 5.1s spacing and two inflight", requests)
}
