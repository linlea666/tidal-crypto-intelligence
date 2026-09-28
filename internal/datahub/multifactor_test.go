package datahub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func multifactorFixture(end time.Time, side string) (map[int64]FlowBar, map[int64]Candle, SignalBaseline) {
	b, c, base := candidateFixture(end)
	base.Median15 = 1.5e9
	base.Directional = &DirectionalBaseline{Buy: DirectionThreshold{FastP95: flowPtr(1e9), HourP90: flowPtr(3e9), HourP95: flowPtr(5e9)}, Sell: DirectionThreshold{FastP95: flowPtr(1e9), HourP90: flowPtr(3e9), HourP95: flowPtr(5e9)}}
	if side == "sell" {
		for k, v := range b {
			v.Buy, v.Sell = v.Sell, v.Buy
			b[k] = v
		}
	}
	return b, c, base
}
func TestMultifactorCoreTwoDirectionsAndMissingDegrades(t *testing.T) {
	end := testTime("2026-09-28T08:00:00Z")
	for _, side := range []string{"buy", "sell"} {
		bars, c, base := multifactorFixture(end, side)
		s := buildFlowSnapshot(bars, c, end, end.Add(time.Minute), base, true)
		s.explain(side)
		for _, a := range s.Assessments {
			if a.Direction == side {
				if !a.Fast || !a.Sustained || !a.Large || a.Supported {
					t.Fatalf("core/missing error %+v", a)
				}
			} else if a.pattern() != "" {
				t.Fatal("opposite side passed")
			}
		}
		if len(s.Evidence) != 6 || s.Evidence[3].State != "missing" {
			t.Fatal(s.Evidence)
		}
		if s.Spot["60"].Net == nil || *s.Spot["60"].Net != 6e9*sideSign(side) {
			t.Fatal(s.Spot)
		}
		delete(bars, end.Add(-10*time.Minute).Unix())
		s = buildFlowSnapshot(bars, c, end, end, base, true)
		if s.Spot["15"].Net != nil || s.Assessments[0].pattern() != "" || s.Assessments[1].pattern() != "" {
			t.Fatal("gap treated as zero")
		}
	}
}
func TestMultifactorSeparateTailsExactMoneyAndNoVolume(t *testing.T) {
	d := directionalBaseline([]float64{1, 2, 3, -100, -200, -300}, []float64{10, 20, 30, -1000, -2000, -3000})
	if *d.Sell.FastP95 != 290 {
		t.Fatal(d)
	}
	if math.Abs(*d.Buy.FastP95-2.9) > 1e-8 || *d.Sell.HourP90 != 2800 {
		t.Fatal("directions not independent", d)
	}
	end := testTime("2026-09-28T08:00:00Z")
	bars, c, base := multifactorFixture(end, "buy")
	base.Median15 = 1e9
	for i, n := range []int64{3e8, 3e8, 4e8} {
		at := end.Add(-time.Duration(i+1) * 5 * time.Minute)
		bars[at.Unix()] = FlowBar{At: at, Buy: n + 1e8, Sell: 1e8}
	}
	s := buildFlowSnapshot(bars, c, end, end, base, true)
	if !s.Assessments[0].Fast {
		t.Fatal("boundary rejected")
	}
	at := end.Add(-5 * time.Minute)
	b := bars[at.Unix()]
	b.Buy--
	bars[at.Unix()] = b
	s = buildFlowSnapshot(bars, c, end, end, base, true)
	if s.Assessments[0].Fast {
		t.Fatal("one cent below accepted")
	}
	base.Median15, base.Median60 = 1e15, 1e15
	s = buildFlowSnapshot(bars, c, end, end, base, true)
	if s.Assessments[0].pattern() != "" {
		t.Fatal("net sign without volume signaled")
	}
	for k, b := range bars {
		b.Buy, b.Sell = 0, 0
		bars[k] = b
	}
	s = buildFlowSnapshot(bars, c, end, end, base, true)
	if s.Spot["15"].BuyShare != nil || s.Spot["15"].Net == nil || *s.Spot["15"].Net != 0 || s.Assessments[1].pattern() != "" {
		t.Fatal("zero turnover misclassified")
	}
}
func TestMultifactorReversalNotBlockedAndPerpConflict(t *testing.T) {
	end := testTime("2026-09-28T08:00:00Z")
	bars, c, base := multifactorFixture(end, "buy")
	for at := end.Add(-4 * time.Hour); at.Before(end.Add(-time.Hour)); at = at.Add(5 * time.Minute) {
		bars[at.Unix()] = FlowBar{at, 1e8, 9e8}
	}
	s := buildFlowSnapshot(bars, c, end, end, base, true)
	perp := flowWindow(bars, end, 60, base.Median60)
	perp.Net = flowPtr(int64(-6e9))
	perp.BuyShare = flowPtr(25.0)
	perp.SellShare = flowPtr(75.0)
	s.Context.Futures = map[string]FlowWindow{"60": perp}
	s.explain("buy")
	if !s.Assessments[0].Fast || s.Assessments[0].Supported || !strings.Contains(s.Headline, "较长窗口仍有卖压") || s.Evidence[1].State != "conflict" {
		t.Fatal(s.Headline, s.Assessments, s.Evidence)
	}
}
func TestMultifactorOIUnitsFundingCycleAndLiquidationGap(t *testing.T) {
	end := testTime("2026-09-28T08:00:00Z")
	series := contextSeries{Coin: map[int64]float64{end.Unix(): 100, end.Add(-time.Hour).Unix(): 100}, USD: map[int64]float64{end.Unix(): 110, end.Add(-time.Hour).Unix(): 100}, Liquidations: map[int64]FlowBar{}}
	base := contextBaseline{OIAbsP75: flowPtr(1.0), Funding: map[string]fundingBaseline{}}
	old := Funding{Asset: "BTC", Venue: "Binance", Margin: "stablecoin", RatePercent: "0.02", Hours: flowPtr(8.0), RateKind: "predicted"}
	cur := old
	cur.Hours = flowPtr(4.0)
	cur.RatePercent = "0.03"
	base.Funding[fundingKey(cur)] = fundingBaseline{P95: flowPtr(.02), P05: flowPtr(-.02), Valid: true}
	series.Funding = []Observation{{FetchedAt: end.Add(-10 * time.Minute), Payload: Payload{Funding: []Funding{old}}}, {FetchedAt: end, Payload: Payload{Funding: []Funding{cur}}}}
	c := buildDerivativeContext(series, base, end, end)
	if *c.OI.Coin1H != 0 || math.Abs(*c.OI.USD1H-10) > 1e-8 || c.OI.Regime != "stable" {
		t.Fatal(c.OI)
	}
	if c.Funding[0].Change != nil {
		t.Fatal("period switch compared")
	}
	cur.RateKind = "unknown"
	series.Funding[1].Payload.Funding[0] = cur
	c = buildDerivativeContext(series, base, end, end)
	if c.Funding[0].Comparable || fundingKey(cur) != "" {
		t.Fatal("unknown funding classified")
	}
	for at := end.Add(-time.Hour); at.Before(end); at = at.Add(5 * time.Minute) {
		series.Liquidations[at.Unix()] = FlowBar{At: at, Buy: 10000, Sell: 20000}
	}
	delete(series.Liquidations, end.Add(-30*time.Minute).Unix())
	c = buildDerivativeContext(series, base, end, end)
	if c.Liquidations["60"].Long != nil || c.Liquidations["15"].Long == nil {
		t.Fatal("liquidation gap imputed")
	}
}
func TestMultifactorNativeCoinParserAndAvailability(t *testing.T) {
	h, e := Open(Config{Root: t.TempDir(), Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	end := time.Now().UTC().Truncate(5 * time.Minute)
	d, ok := h.Dataset(ID("oi-coin-history", "BTC", "", "futures"))
	if !ok || d.Params["unit"] != "coin" || d.Refresh != 300 {
		t.Fatal(d)
	}
	if _, ok = h.Dataset(ID("oi-coin-history", "ETH", "", "futures")); ok {
		t.Fatal("expanded ETH scope")
	}
	data := []any{map[string]any{"time": end.Add(-5 * time.Minute).UnixMilli(), "close": "1234"}}
	rows, e := normalizeObservers(d, data, end)
	if e != nil || len(rows) != 1 || rows[0].Payload.OI[0].USD != "" || rows[0].Payload.OI[0].Base != "1234" {
		t.Fatal(rows, e)
	}
	rows[0].FetchedAt = end.Add(time.Minute)
	if _, e = h.Store.Ingest(d, rows[0]); e != nil {
		t.Fatal(e)
	}
	s, e := h.contextSeries(context.Background(), "BTC", end.Add(-time.Hour), end, end)
	if e != nil || len(s.Coin) != 0 {
		t.Fatal("future availability leak", s.Coin, e)
	}
	s, e = h.contextSeries(context.Background(), "BTC", end.Add(-time.Hour), end, end.Add(time.Minute))
	if e != nil || s.Coin[end.Unix()] != 1234 || len(s.USD) != 0 {
		t.Fatal(s, e)
	}
}
func TestMultifactorConfirmReclaimAndObservationBoundary(t *testing.T) {
	end := testTime("2026-09-28T08:00:00Z")
	bars, c, _ := multifactorFixture(end, "sell")
	s := Signal{Direction: "sell", DataThrough: end.Add(-time.Hour), Expires: end.Add(3 * time.Hour), FrozenLow: 101}
	if !confirms(s, bars, c, end) {
		t.Fatal("two closes not confirmed")
	}
	s.ConfirmedAt = &end
	s.ConfirmedThrough = &end
	p := priceProgress(s, bars, c, end, end, true)
	if p.Status != "holding" {
		t.Fatal(p)
	}
	for _, at := range []time.Time{end.Add(-10 * time.Minute), end.Add(-5 * time.Minute)} {
		c[at.Unix()] = Candle{Open: 102, High: 103, Low: 101, Close: 102}
	}
	p = priceProgress(s, bars, c, end, end, true)
	if p.Status != "reclaimed" || !s.ConfirmedAt.Equal(end) {
		t.Fatal(p)
	}
	p = priceProgress(s, bars, c, end, end.Add(5*time.Hour), false)
	if p.Status != "ended_with_gap" {
		t.Fatal(p)
	}
	for at := end; at.Before(end.Add(5 * time.Hour)); at = at.Add(5 * time.Minute) {
		bars[at.Unix()] = FlowBar{at, 1, 2}
		c[at.Unix()] = Candle{Open: 100, High: 100, Low: 100, Close: 100}
	}
	p = priceProgress(s, bars, c, end.Add(5*time.Hour), end.Add(5*time.Hour), true)
	if !p.DataThrough.Equal(end.Add(4*time.Hour)) || p.Status != "completed_holding" {
		t.Fatal("followup used beyond horizon", p)
	}
}
func TestMultifactorCutoverTwoStageAndLegacyTail(t *testing.T) {
	h, e := Open(Config{Root: t.TempDir(), Offline: true, Mail: &MailConfig{}})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	h.boot = now.Add(-time.Hour)
	h.Store.SaveState("signals/multifactor-cutover", now)
	old := Signal{ID: "BTC-old", Asset: "BTC", Rules: SignalRules, At: now.Add(-time.Hour), DataThrough: now, Expires: now.Add(time.Hour)}
	if e = h.queueNotice(old, "anomaly", old.At); e != nil {
		t.Fatal(e)
	}
	h.Store.research.Exec("UPDATE notices SET status='sent',attempted=? WHERE signal_id=?", now.Add(-time.Hour).Unix(), old.ID)
	newer := old
	newer.ID = "BTC-new"
	newer.Rules = MultifactorRules
	newer.At = now
	legacyNew := old
	legacyNew.ID = "BTC-control"
	legacyNew.At = now.Add(time.Second)
	state := signalState{}
	if e = h.commitSignals(ctx, "BTC", state, []Signal{old, newer, legacyNew}, map[string]string{old.ID: "confirmed", newer.ID: "anomaly", legacyNew.ID: "anomaly"}, now); e != nil {
		t.Fatal(e)
	}
	if e = h.commitSignals(ctx, "BTC", state, []Signal{newer}, map[string]string{newer.ID: "strong"}, now); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 2; i++ {
		if e = h.commitSignals(ctx, "BTC", state, []Signal{newer}, map[string]string{newer.ID: "confirmed"}, now); e != nil {
			t.Fatal(e)
		}
	}
	var n int
	h.Store.research.QueryRow("SELECT count(*) FROM notices").Scan(&n)
	if n != 4 {
		t.Fatal("cutover/two-stage duplicated", n)
	}
	m, e := h.signalMailResults(ctx, []string{old.ID, newer.ID})
	if e != nil || len(m) != 4 || m[0].Completed != nil {
		t.Fatal("fabricated legacy completion", m, e)
	}
}
func TestMultifactorMailOutcomesSharedLedgerAndStaleError(t *testing.T) {
	h, e := Open(Config{Root: t.TempDir(), Offline: true, Mail: &MailConfig{}})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	h.offline = false
	ctx := context.Background()
	now := time.Now().UTC()
	h.boot = now.Add(-time.Hour)
	h.Store.SaveState("mail/error", map[string]any{"at": now.Add(-time.Hour), "error": "old connection failure"})
	for i, sendErr := range []error{mailRejected("SMTP连接失败"), errors.New("SMTP发送结果不确定"), nil} {
		s := Signal{ID: fmt.Sprint(i), Asset: "BTC", Rules: MultifactorRules, At: now, DataThrough: now, Expires: now.Add(time.Hour)}
		if e = h.queueNotice(s, "anomaly", now); e != nil {
			t.Fatal(e)
		}
		h.mailSend = func(context.Context, MailConfig, string, string) error { return sendErr }
		_, err := h.deliverNoticeBatch(ctx, now.Add(time.Duration(i)*time.Second), []string{s.ID + "/anomaly"}, "test", "test")
		if !errors.Is(err, sendErr) {
			t.Fatal(err)
		}
	}
	rows, e := h.signalMailResults(ctx, []string{"0", "1", "2"})
	if e != nil || len(rows) != 3 {
		t.Fatal(rows, e)
	}
	for i, want := range []string{"failed_before_submission", "delivery_unknown", "sent"} {
		if rows[i].Status != want || rows[i].Completed == nil {
			t.Fatal(rows)
		}
	}
	status := h.mailStatus().(map[string]any)
	if status["lastError"] != nil || status["historicalError"] == nil || status["latestStatus"] != "sent" {
		t.Fatal(status)
	}
	h.Store.SaveState("mail/error", map[string]any{"at": now.Add(time.Minute), "error": "new persistence failure"})
	if h.mailStatus().(map[string]any)["lastError"] == nil {
		t.Fatal("a previous success hid a newer worker error")
	}
	if n, e := h.mailAttempts(ctx, now.Add(time.Minute)); e != nil || n != 3 {
		t.Fatal(n, e)
	}
}

func TestMultifactorSupportRequiresDistinctFundingVenuesAndNoExtension(t *testing.T) {
	end := testTime("2026-09-28T08:00:00Z")
	for _, side := range []string{"buy", "sell"} {
		bars, c, base := multifactorFixture(end, side)
		s := buildFlowSnapshot(bars, c, end, end, base, true)
		s.Price.Return1H, s.Price.DisplacementATR = flowPtr(float64(sideSign(side))), flowPtr(float64(sideSign(side)))
		s.Context.Futures = map[string]FlowWindow{"60": s.Spot["60"]}
		s.Context.OI = OIContext{Coin1H: flowPtr(2.0), AbsP75: flowPtr(1.0), Regime: "expanding"}
		f := FundingPoint{Funding: Funding{Venue: "Binance", RatePercent: "0.01", RateKind: "predicted", Hours: flowPtr(8.0)}, Comparable: true, P95: flowPtr(.02), P05: flowPtr(-.02)}
		s.Context.Funding = []FundingPoint{f, f}
		s.explain(side)
		if flowLevel(&s, side) == "supported" {
			t.Fatal("two margin categories counted as two venues")
		}
		s.Context.Funding[1].Venue = "OKX"
		s.explain(side)
		if flowLevel(&s, side) != "supported" {
			t.Fatal("complete, non-conflicting evidence failed", s.Evidence)
		}
		s.Price.DisplacementATR = flowPtr(1.51 * float64(sideSign(side)))
		s.explain(side)
		if flowLevel(&s, side) == "supported" {
			t.Fatal("large displacement incorrectly upgraded")
		}
		s.Price.DisplacementATR = flowPtr(float64(sideSign(side)))
		for i := range s.Context.Funding {
			s.Context.Funding[i].RatePercent = fmt.Sprint(.03 * float64(sideSign(side)))
		}
		s.explain(side)
		if flowLevel(&s, side) == "supported" || s.Evidence[3].State != "conflict" {
			t.Fatal("funding crowding did not constrain upgrade")
		}
	}
}

func TestMultifactorSustainedWithZeroLastQuarterAndGapBreaksRearm(t *testing.T) {
	h, err := Open(Config{Root: t.TempDir(), Offline: true})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Store.Close()
	end := testTime("2026-09-28T08:00:00Z")
	bars, c, base := multifactorFixture(end, "sell")
	for i := 1; i <= 3; i++ {
		at := end.Add(-time.Duration(i) * 5 * time.Minute)
		bars[at.Unix()] = FlowBar{At: at}
	}
	s := buildFlowSnapshot(bars, c, end, end, base, true)
	if !s.Assessments[1].Sustained || s.Assessments[1].Fast {
		t.Fatal("fixture", s.Assessments)
	}
	state := signalState{Active: map[string]string{}, Clear: map[string]*time.Time{}}
	updates := []Signal{}
	notices := map[string]string{}
	step := func(at time.Time) {
		t.Helper()
		if err := h.processMultifactor(context.Background(), "BTC", &state, s, c, true, at, &updates, notices); err != nil {
			t.Fatal(err)
		}
	}
	step(end)
	if len(updates) != 1 || updates[0].Progress == nil || updates[0].Progress.Status != "waiting" || updates[0].BuyShare != nil || updates[0].Multifactor.Spot["15"].BuyShare != nil {
		t.Fatal("zero turnover manufactured a share", updates)
	}
	if err := h.Store.saveDocument("signal", updates[0].ID, "BTC", end, updates[0]); err != nil {
		t.Fatal(err)
	}
	for i := range s.Assessments {
		s.Assessments[i].Fast, s.Assessments[i].Sustained = false, false
	}
	step(end.Add(5 * time.Minute))
	s.Fresh = false
	step(end.Add(30 * time.Minute))
	s.Fresh = true
	step(end.Add(35 * time.Minute))
	step(end.Add(60 * time.Minute))
	if state.Active["multifactor-sell"] == "" {
		t.Fatal("gap counted toward 30 observed minutes")
	}
	step(end.Add(65 * time.Minute))
	if state.Active["multifactor-sell"] != "" {
		t.Fatal("complete clear interval did not rearm")
	}
}
func TestMultifactorTrialsUseFutureOpenAndExcludeCases(t *testing.T) {
	at := testTime("2026-09-10T08:00:00Z")
	c := map[int64]Candle{}
	for t := at.Add(-5 * time.Minute); t.Before(at.Add(25 * time.Hour)); t = t.Add(5 * time.Minute) {
		c[t.Unix()] = Candle{Open: 200, High: 202, Low: 198, Close: 200}
	}
	c[at.Add(-5*time.Minute).Unix()] = Candle{Open: 100, High: 200, Low: 100, Close: 100}
	s := Signal{ID: "one", At: at, Direction: "buy", ATR: flowPtr(10.0)}
	r := multifactorTrial(MultifactorRules, "buy", []Signal{s}, nil, c, 0, at.Add(48*time.Hour))
	if r.Return4HMedian == nil || *r.Return4HMedian != 0 {
		t.Fatal("credited pre-notice return", r)
	}
	if !multifactorCase(testTime("2026-09-28T00:00:00Z")) || multifactorCase(at) {
		t.Fatal("case exclusion")
	}
	b, _ := json.Marshal(r)
	if strings.Contains(string(b), "NaN") {
		t.Fatal(string(b))
	}
}

func TestMultifactorCheckpointAndFrozenSnapshotBackup(t *testing.T) {
	h, err := Open(Config{Root: t.TempDir(), Offline: true})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Store.Close()
	ctx := context.Background()
	now := testTime("2026-09-28T08:00:00Z")
	study := Study{ID: "checkpoint-mf", Asset: "BTC", From: now.Add(-34 * 24 * time.Hour), To: now, Created: now, Pipeline: studyPipeline}
	bars, candles := map[int64]FlowBar{}, map[int64]Candle{}
	r, err := h.evaluateMultifactor(ctx, study, bars, candles, now)
	if err != nil || r.State != "calculating" {
		t.Fatal(r, err)
	}
	var first multiCheckpoint
	if err := h.Store.document(ctx, "multifactor-study-progress", study.ID, &first); err != nil {
		t.Fatal(err)
	}
	r, err = h.evaluateMultifactor(ctx, study, bars, candles, now)
	if err != nil || r.State != "association_calculated" {
		t.Fatal(r, err)
	}
	var second multiCheckpoint
	h.Store.document(ctx, "multifactor-study-progress", study.ID, &second)
	if !second.Cursor.After(first.Cursor) || len(second.Ticks) != 0 {
		t.Fatal("gaps became usable observations")
	}
	for _, g := range r.Groups {
		if g.Windows != 0 || len(g.Trials) != 0 {
			t.Fatal("missing history fabricated a comparison")
		}
	}
	b, c, base := multifactorFixture(now, "buy")
	snap := buildFlowSnapshot(b, c, now, now, base, true)
	snap.explain("buy")
	s := Signal{ID: "frozen-mf", Asset: "BTC", At: now, Rules: MultifactorRules, Multifactor: &snap}
	if err := h.Store.saveDocument("signal", s.ID, "BTC", now, s); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Store.research.Exec("INSERT INTO mail_results VALUES('batch',?,'sent','')", now.UnixNano()); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "research.sqlite")
	if err := BackupFile(ctx, filepath.Join(h.Store.Root(), "research.sqlite"), path); err != nil {
		t.Fatal(err)
	}
	db, err := database(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var payload []byte
	if err := db.QueryRow("SELECT payload FROM documents WHERE kind='signal' AND id=?", s.ID).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	var restored Signal
	if json.Unmarshal(payload, &restored) != nil || restored.Multifactor == nil || *restored.Multifactor.Spot["60"].Net != 6e9 {
		t.Fatal("frozen fact lost in backup")
	}
	var n int
	if err := db.QueryRow("SELECT count(*) FROM mail_results WHERE status='sent'").Scan(&n); err != nil || n != 1 {
		t.Fatal("mail ledger backup", n, err)
	}
}

func TestMultifactorFullTraceFitsDocumentCapAndRecovers(t *testing.T) {
	start := testTime("2026-07-01T00:00:00Z")
	p := multiCheckpoint{Version: "fixed-input", Cursor: start, Active: map[string]bool{"core/buy": true}, Clear: map[string]*time.Time{}}
	for i := 0; i < 60*24*12; i++ {
		p.Ticks = append(p.Ticks, multiResearchTick{start.Add(time.Duration(i) * 5 * time.Minute), 31})
	}
	for i := 0; i < 5000; i++ {
		p.Signals = append(p.Signals, multiResearchSignal{ID: fmt.Sprint(i), Rules: MultifactorRules, Direction: "sell", At: start.Add(time.Duration(i) * 20 * time.Minute), ATR: flowPtr(100.0), Following: i%2 == 0, Mask: 31})
	}
	raw, _ := json.Marshal(multiCheckpointRecords{p.Ticks, p.Signals})
	if len(raw) <= 1<<20 {
		t.Fatal("fixture does not exercise the original document limit", len(raw))
	}
	encoded, err := json.Marshal(p)
	if err != nil || len(encoded) > 1<<20 {
		t.Fatal("checkpoint still exceeds unchanged limit", len(encoded), err)
	}
	var restored multiCheckpoint
	if err = json.Unmarshal(encoded, &restored); err != nil {
		t.Fatal(err)
	}
	if restored.Version != p.Version || !restored.Active["core/buy"] || len(restored.Ticks) != len(p.Ticks) || len(restored.Signals) != len(p.Signals) || !restored.Signals[22].asSignal().Multifactor.Price.Following["sell"] {
		t.Fatal("checkpoint lost decision or matching evidence")
	}
	broken, _ := json.Marshal(multiCheckpointWire{Records: []byte("not-gzip")})
	if json.Unmarshal(broken, &restored) == nil {
		t.Fatal("corrupt checkpoint accepted")
	}
	t.Logf("complete trace JSON %d bytes; stored checkpoint %d bytes", len(raw), len(encoded))
}
