package datahub

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPaperReadonlySnapshotAPIAndSourcePublication(t *testing.T) {
	h, err := Open(Config{Root: t.TempDir(), Offline: true, PaperMode: "collect"})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Store.Close()
	ctx := context.Background()
	now := time.Now().UTC()
	sig := Signal{ID: "formal-first-publication", Asset: "BTC", Direction: "buy", Rules: MultifactorRules, At: now, Level: "initial"}
	if err = h.commitSignals(ctx, "BTC", signalState{}, []Signal{sig}, map[string]string{sig.ID: "anomaly"}, now); err != nil {
		t.Fatal(err)
	}
	sig.Level = "later-confirmed"
	if err = h.commitSignals(ctx, "BTC", signalState{}, []Signal{sig}, map[string]string{sig.ID: "confirmed"}, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err = h.commitSignals(ctx, "BTC", signalState{}, []Signal{sig}, map[string]string{sig.ID: "anomaly"}, now.Add(2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	pubs, _, _, err := h.Store.paperPublications(ctx, 0)
	if err != nil || len(pubs) != 1 || pubs[0].Signal.Level != "initial" || !pubs[0].At.Equal(now.Truncate(time.Millisecond)) {
		t.Fatalf("publication not immutable: %+v %v", pubs, err)
	}
	before := h.Store.paper.snapshot()
	for _, path := range []string{"paper", "paper/trades"} {
		raw, err := h.Read(ctx, path, url.Values{})
		if err != nil {
			t.Fatal(path, err)
		}
		var body map[string]any
		if err = json.Unmarshal(raw, &body); err != nil {
			t.Fatal(err)
		}
	}
	if after := h.Store.paper.snapshot(); after.Cursor != before.Cursor || after.Origin != nil {
		t.Fatal("read started experiment or consumed signals")
	}
	if _, err = h.Read(ctx, "paper", url.Values{"asset": {"ETH"}}); err == nil {
		t.Fatal("ETH paper should be disabled")
	}
	if h.Scheduler.State()["calls"].(int64) != 0 {
		t.Fatal("paper reads used CoinGlass quota")
	}
}

func TestPaperPublicWireCaseSensitiveFields(t *testing.T) {
	p, now := paperFixture(t)
	ctx := context.Background()
	candles := map[int64]Candle{}
	// Exact Binance event/clock and price/estimated-price keys must coexist.
	mark := []byte(fmt.Sprintf(`{"stream":"btcusdt@markPrice@1s","data":{"e":"markPriceUpdate","E":%d,"s":"BTCUSDT","st":1,"p":"10000","P":"0","i":"10000","r":"0.0001","T":%d}}`, now.UnixMilli(), now.Add(time.Hour).UnixMilli()))
	if err := p.streamMessage(ctx, paperMessage{At: now, Raw: mark}, candles, false); err != nil {
		t.Fatal("valid p rejected because P or E overwrote a different wire field:", err)
	}
	paperSignal(t, p, 1, now, "buy")
	at := now.Add(time.Second)
	quote := []byte(fmt.Sprintf(`{"e":"bookTicker","u":2,"E":%d,"T":%d,"s":"BTCUSDT","st":1,"b":"9999","B":"1","a":"10000","A":"1"}`, at.UnixMilli(), at.UnixMilli()))
	if err := p.streamMessage(ctx, paperMessage{At: at, Raw: quote}, candles, false); err != nil {
		t.Fatal("valid e/E public quote rejected:", err)
	}
	for _, a := range p.snapshot().Accounts {
		if a.Position == nil {
			t.Fatal("valid public wire quote did not execute the delayed paper entry")
		}
		paperAssertDecimal(t, a.Position.Entry, "10002")
	}
	if p.quote.EventAt.UnixMilli() != at.UnixMilli() {
		t.Fatal("lost the uppercase exchange event clock")
	}
}

func TestPaperKlinePublicWireCaseSensitiveFields(t *testing.T) {
	p, now := paperFixture(t)
	candles := map[int64]Candle{}
	start := now.Truncate(5 * time.Minute).Add(-5 * time.Minute)
	for _, closed := range []bool{false, true} {
		// Include the entire documented kline shape: L is a trade ID, l is
		// the low price. Check both key orders to forbid overwrite by folding.
		for _, low := range []string{`"l":"9990","L":12345`, `"L":12345,"l":"9990"`} {
			raw := []byte(fmt.Sprintf(`{"stream":"btcusdt@kline_5m","data":{"e":"kline","E":%d,"s":"BTCUSDT","st":1,"k":{"t":%d,"T":%d,"s":"BTCUSDT","i":"5m","f":12000,%s,"o":"10000","c":"10005","h":"10010","v":"100","n":346,"x":%t,"q":"1000000","V":"40","Q":"400000","B":"0"}}}`, now.UnixMilli(), start.UnixMilli(), now.UnixMilli()-1, low, closed))
			if err := p.streamMessage(context.Background(), paperMessage{At: now, Raw: raw}, candles, false); err != nil {
				t.Fatal("public kline rejected:", err)
			}
			if !closed && len(candles) != 0 {
				t.Fatal("incomplete kline entered ATR inputs")
			}
		}
	}
	if len(candles) != 1 || candles[start.Unix()].Low != 9990 {
		t.Fatal("kline low was overwritten by trade ID", candles)
	}
}

func TestPaperFundingPollingFreshnessAndAdmission(t *testing.T) {
	p, now := paperFixture(t)
	s := p.snapshot()
	// A normal response is already older than the two-minute history lag
	// when it arrives. The next poll is thirty seconds later.
	s.FundingThrough = now.Add(-150 * time.Second)
	s.ExpectedFunding = []int64{now.Add(time.Hour).UnixMilli()}
	if !paperFundingKnown(s, now, true) {
		t.Fatal("healthy funding poll can never become current")
	}
	if paperFundingKnown(s, now, false) {
		t.Fatal("poll tolerance incorrectly finalized a closed trade")
	}
	if paperFundingKnown(s, now.Add(31*time.Second), true) {
		t.Fatal("funding poll stale beyond bounded tolerance accepted")
	}
	s.ExpectedFunding = append(s.ExpectedFunding, now.UnixMilli())
	if paperFundingKnown(s, now, true) {
		t.Fatal("poll tolerance hid an overdue settlement")
	}
	s.ExpectedFunding = s.ExpectedFunding[:1]
	if err := p.commit(context.Background(), s, paperBatch{}); err != nil {
		t.Fatal(err)
	}
	paperSignal(t, p, 1, now, "buy")
	paperTick(t, p, 2, now.Add(time.Second), "9999", "10000", "1")
	for _, a := range p.snapshot().Accounts {
		if a.Position == nil {
			t.Fatal("normal funding polling blocked entry", a)
		}
	}
}
func TestPaperCapacityAndRecoveryMismatch(t *testing.T) {
	p, now := paperFixture(t)
	ctx := context.Background()
	paperSignal(t, p, 1, now, "buy")
	paperTick(t, p, 2, now.Add(time.Second), "9999", "10000", "1")
	var pages int
	if err := p.db.QueryRow("PRAGMA page_count").Scan(&pages); err != nil {
		t.Fatal(err)
	}
	if _, err := p.db.Exec(fmt.Sprintf("PRAGMA max_page_count=%d", pages)); err != nil {
		t.Fatal(err)
	}
	err := p.commit(ctx, p.snapshot(), paperBatch{Events: []paperEvent{{At: now, Kind: "test_full", Reason: strings.Repeat("x", 1<<20)}}})
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "full") {
		t.Fatal("expected SQLITE_FULL", err)
	}
	p.failure(err, now.Add(2*time.Second))
	if !p.snapshot().Gap || p.snapshot().Accounts[0].Position == nil {
		t.Fatal("disk full lost position or left entries enabled")
	}
	if _, err = p.db.Exec("PRAGMA max_page_count=30720"); err != nil {
		t.Fatal(err)
	}
	paperTick(t, p, 3, now.Add(3*time.Second), "9999", "10000", "1")
	if err = p.verifyRecovery(); err != nil {
		t.Fatal(err)
	}
	s := p.snapshot()
	s.Accounts[0].Fees = s.Accounts[0].Fees.Add(pd("1"))
	if err = p.commit(ctx, s, paperBatch{}); err != nil {
		t.Fatal(err)
	}
	if err = p.verifyRecovery(); err == nil {
		t.Fatal("corrupt restored cash accepted")
	}
}
func TestPaperTimeExitShortFundingAndDeterministicLedger(t *testing.T) {
	run := func() []byte {
		p, now := paperFixture(t)
		ctx := context.Background()
		paperSignal(t, p, 1, now, "sell")
		paperTick(t, p, 2, now.Add(time.Second), "9999", "10000", "1")
		if err := p.settle(ctx, []paperFunding{{At: now.Add(time.Second), Acquired: now.Add(time.Minute), Rate: pd(".001"), Mark: pd("10000")}}, now.Add(time.Minute)); err != nil {
			t.Fatal(err)
		}
		paperAssertDecimal(t, p.snapshot().Accounts[0].Funding, "1")
		// Advance while proving a continuous path in test-only observations.
		for i := 2; i <= 4*3600+1; i += 5 {
			paperTick(t, p, int64(i+1), now.Add(time.Duration(i)*time.Second), "9999", "10000", "1")
		}
		paperTick(t, p, 20000, now.Add(4*time.Hour+time.Second), "9999", "10000", "1")
		if p.snapshot().Accounts[1].Position.ExitReason != "time_limit" {
			t.Fatal("missing four-hour trigger")
		}
		paperTick(t, p, 20001, now.Add(4*time.Hour+2*time.Second), "9999", "10000", "1")
		if p.snapshot().Accounts[0].Position == nil || p.snapshot().Accounts[1].Position != nil {
			t.Fatal("time exit leaked to opposite-only group")
		}
		fills, err := paperRows[paperFill](ctx, p.db, "SELECT payload FROM paper_fills ORDER BY id")
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(fills)
		return raw
	}
	if a, b := string(run()), string(run()); a != b {
		t.Fatal("same frozen input produced different fills")
	}
}

// Opt-in public connectivity check. No fixtures, accounts, credentials or
// order endpoints: it only runs the dedicated collector in collect mode.
func TestPaperPublicConnectivity(t *testing.T) {
	if os.Getenv("TIDAL_PAPER_PUBLIC_ACCEPTANCE") != "1" {
		t.Skip("opt-in public perpetual feed acceptance")
	}
	h, err := Open(Config{Root: t.TempDir(), PaperMode: "collect"})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Store.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	h.paperWorker(ctx)
	p := h.Store.paper
	p.mu.RLock()
	feed := p.feed
	p.mu.RUnlock()
	s := p.snapshot()
	if s.Origin != nil {
		t.Fatal("collection enabled accounts")
	}
	raw, _ := json.Marshal(map[string]any{"at": time.Now().UTC(), "feed": feed, "state": s})
	if dir := os.Getenv("TIDAL_RESOURCE_OUTPUT"); dir != "" {
		if err = os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(dir, "paper-public.json"), raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if feed.Quote == nil || feed.ATR == nil || feed.MarkAt == nil || !feed.Instrument.valid(time.Now()) {
		t.Fatalf("public collector incomplete: quote=%t ATR=%t mark=%t instrument=%s failure=%s", feed.Quote != nil, feed.ATR != nil, feed.MarkAt != nil, feed.Instrument.Status, s.LastFailure)
	}
	t.Logf("public feeds verified; quote=%d ATR=%s paperBytes=%d", feed.Quote.ID, feed.ATR, feed.Bytes)
}

func TestPaperCommonEntriesAwaitBothClosures(t *testing.T) {
	p, now := paperFixture(t)
	paperSignal(t, p, 1, now, "buy")
	paperTick(t, p, 2, now.Add(time.Second), "9999", "10000", "1")
	paperTick(t, p, 3, now.Add(2*time.Second), "10300", "10301", "1")
	paperTick(t, p, 4, now.Add(3*time.Second), "10300", "10301", "1")
	v, err := p.summary(context.Background(), now.Add(4*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	accounts := v.(map[string]any)["accounts"].([]any)
	for _, a := range accounts {
		if a.(map[string]any)["commonEntries"].(paperStats).Closed != 0 {
			t.Fatal("one-sided completed pair entered comparison")
		}
	}
	if accounts[1].(map[string]any)["all"].(paperStats).Complete != 1 {
		t.Fatal("independent risk account result lost")
	}
}
func TestPaperEquityClockAndIntraMinuteGap(t *testing.T) {
	p, now := paperFixture(t)
	ctx := context.Background()
	at := now.Add(300 * time.Millisecond)
	if err := p.heartbeat(ctx, at, false); err != nil {
		t.Fatal(err)
	}
	if err := p.discontinuity(ctx, now.Add(time.Second), "test_gap"); err != nil {
		t.Fatal(err)
	}
	for i := 2; i <= 62; i++ {
		at = now.Add(time.Duration(i) * time.Second)
		paperTick(t, p, int64(i+1), at, "9999", "10000", "1")
		if err := p.heartbeat(ctx, at, false); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := paperRows[paperEquity](ctx, p.db, "SELECT payload FROM paper_equity WHERE group_id='risk' ORDER BY at")
	if err != nil || len(rows) != 2 {
		t.Fatalf("%v %d", err, len(rows))
	}
	if !rows[0].At.Equal(now.Add(300 * time.Millisecond)) {
		t.Fatal("equity value backdated to minute start")
	}
	if rows[1].BeforeFunding != nil {
		t.Fatal("intra-minute gap was bridged after recovery")
	}
}
func TestPaperFundingSubmillisecondExitBoundary(t *testing.T) {
	p, now := paperFixture(t)
	paperSignal(t, p, 1, now, "buy")
	paperTick(t, p, 2, now.Add(time.Second), "9999", "10000", "1")
	paperSignal(t, p, 2, now.Add(2*time.Second), "sell")
	paperTick(t, p, 3, now.Add(3*time.Second+500*time.Microsecond), "9999", "10000", "1")
	if err := p.settle(context.Background(), []paperFunding{{At: now.Add(3 * time.Second), Acquired: now.Add(time.Minute), Rate: pd(".001"), Mark: pd("10000")}}, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	for _, a := range p.snapshot().Accounts {
		paperAssertDecimal(t, a.Funding, "-.99")
	}
}

func TestPaperFundingLateRecordRemainsInFetchWindow(t *testing.T) {
	p, now := paperFixture(t)
	s := p.snapshot()
	s.FundingThrough = now.Add(-2 * time.Minute)
	due := now.Add(-48 * time.Hour)
	s.ExpectedFunding = []int64{due.UnixMilli()}
	from, through := paperFundingWindow(s, now)
	if !from.Equal(due.Add(-time.Second)) || through.Sub(from) != 24*time.Hour {
		t.Fatal("unsettled old record fell behind bounded fetch window", from, through)
	}
	if err := p.commit(context.Background(), s, paperBatch{}); err != nil {
		t.Fatal(err)
	}
	if err := p.settle(context.Background(), []paperFunding{{At: due, Acquired: now, Mark: pd("10000"), Rate: pd(".001")}}, through); err != nil {
		t.Fatal(err)
	}
	from, through = paperFundingWindow(p.snapshot(), now)
	if !from.Equal(s.FundingThrough.Add(-time.Hour)) || !through.Equal(now.Add(-2*time.Minute)) {
		t.Fatal("resolved late record prevented normal overlap refresh", from, through)
	}
}

func TestPaperAnnouncedFundingClockDoesNotMoveActualSettlement(t *testing.T) {
	p, now := paperFixture(t)
	ctx := context.Background()
	paperSignal(t, p, 1, now, "buy")
	paperTick(t, p, 2, now.Add(time.Second), "9999", "10000", "1")
	paperSignal(t, p, 2, now.Add(2*time.Second), "sell")
	paperTick(t, p, 3, now.Add(3*time.Second), "9999", "10000", "1")
	s := p.snapshot()
	due := now.Add(3 * time.Second)
	s.ExpectedFunding = []int64{due.UnixMilli()}
	if err := p.commit(ctx, s, paperBatch{}); err != nil {
		t.Fatal(err)
	}
	if err := p.settle(ctx, []paperFunding{{At: due.Add(time.Millisecond), Acquired: now.Add(time.Minute), Rate: pd(".001"), Mark: pd("10000")}}, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	s = p.snapshot()
	if !paperFundingKnown(s, due.Add(time.Second), false) {
		t.Fatal("official millisecond timing difference left funding pending")
	}
	for _, a := range s.Accounts {
		paperAssertDecimal(t, a.Funding, "0") // Closed before actual settlement.
	}
	if paperSettlementObserved(s, due.Add(2*time.Second).UnixMilli()) {
		t.Fatal("unrelated announcement matched a settlement")
	}
}

func TestPaperHaltedContractWaitsToClose(t *testing.T) {
	p, now := paperFixture(t)
	paperSignal(t, p, 1, now, "buy")
	paperTick(t, p, 2, now.Add(time.Second), "9999", "10000", "1")
	paperSignal(t, p, 2, now.Add(2*time.Second), "sell")
	p.instrument.Status = "HALT"
	paperTick(t, p, 3, now.Add(3*time.Second), "9999", "10000", "1")
	for _, a := range p.snapshot().Accounts {
		if a.Position == nil || a.Position.ExitReason != "opposite_signal" || !a.Position.Remaining.Equal(a.Position.Quantity) {
			t.Fatal("halted contract fabricated an executable close")
		}
	}
	p.instrument.Status = "TRADING"
	paperTick(t, p, 4, now.Add(4*time.Second), "9999", "10000", "1")
	for _, a := range p.snapshot().Accounts {
		if a.Position != nil {
			t.Fatal("valid restored contract could not finish pending close")
		}
	}
}

func TestPaperRestartCannotReusePartiallyConsumedQuote(t *testing.T) {
	p, now := paperFixture(t)
	ctx := context.Background()
	paperSignal(t, p, 1, now, "buy")
	paperTick(t, p, 2, now.Add(time.Second), "9999", "10000", "1")
	paperSignal(t, p, 2, now.Add(2*time.Second), "sell")
	paperTick(t, p, 3, now.Add(3*time.Second), "9999", "10000", ".04")
	dest := filepath.Join(t.TempDir(), "paper.sqlite")
	if err := BackupFile(ctx, p.path, dest); err != nil {
		t.Fatal(err)
	}
	r, err := openPaper(filepath.Dir(dest), "run")
	if err != nil {
		t.Fatal(err)
	}
	defer r.db.Close()
	defer r.readDB.Close()
	r.instrument = p.instrument
	if err = r.discontinuity(ctx, now.Add(3500*time.Millisecond), "restart_gap"); err != nil {
		t.Fatal(err)
	}
	paperTick(t, r, 3, now.Add(5*time.Second), "9999", "10000", ".04")
	for _, a := range r.snapshot().Accounts {
		paperAssertDecimal(t, a.Position.Remaining, ".059")
	}
	paperTick(t, r, 4, now.Add(6*time.Second), "9999", "10000", ".04")
	for _, a := range r.snapshot().Accounts {
		paperAssertDecimal(t, a.Position.Remaining, ".019")
	}
	if err = r.verifyRecovery(); err != nil {
		t.Fatal(err)
	}
}
