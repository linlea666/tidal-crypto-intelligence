package datahub

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

func pd(v string) decimal.Decimal { return decimal.RequireFromString(v) }
func paperFixture(t *testing.T) (*paperStore, time.Time) {
	t.Helper()
	p, err := openPaper(t.TempDir(), "run")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.readDB.Close(); p.db.Close() })
	now := time.Date(2026, 10, 1, 10, 30, 0, 0, time.UTC)
	origin := now.Add(-time.Hour)
	s := p.snapshot()
	s.Generation = "test-experiment"
	s.Origin = &origin
	s.Gap = false
	s.GoodSince = now.Add(-time.Minute)
	s.Pause = ""
	s.Source = "test-source"
	s.LastTick = now.Add(-time.Second)
	s.FundingThrough = now.Add(24 * time.Hour)
	if err = p.commit(context.Background(), s, paperBatch{}); err != nil {
		t.Fatal(err)
	}
	p.instrument = paperInstrument{At: now.Add(-time.Minute), Status: "TRADING", Step: pd("0.001"), Minimum: pd("0.001"), Maximum: pd("1000"), MinNotional: pd("5")}
	p.atr = &paperIntent{ATR: pd("100"), ATRThrough: now.Truncate(time.Hour)}
	p.markAt = now
	p.quote = &paperQuote{ID: 1, At: now, EventAt: now, Bid: pd("9999"), Ask: pd("10000"), BidQty: pd("1"), AskQty: pd("1")}
	p.lastQuoteID = 1
	return p, now
}
func paperSignal(t *testing.T, p *paperStore, seq int64, at time.Time, side string) {
	t.Helper()
	sig := Signal{ID: side + "-" + at.Format(time.RFC3339Nano), Asset: "BTC", Direction: side, Rules: MultifactorRules, At: at, Level: "anomaly"}
	if err := p.consume(context.Background(), []paperPublication{{Seq: seq, At: at, Signal: sig}}, at, false); err != nil {
		t.Fatal(err)
	}
}
func paperTick(t *testing.T, p *paperStore, id int64, at time.Time, bid, ask, size string) {
	t.Helper()
	p.markAt = at
	q := paperQuote{ID: id, At: at, EventAt: at, Bid: pd(bid), Ask: pd(ask), BidQty: pd(size), AskQty: pd(size)}
	if err := p.onQuote(context.Background(), q, false); err != nil {
		t.Fatal(err)
	}
}
func paperAssertDecimal(t *testing.T, got decimal.Decimal, want string) {
	t.Helper()
	if !got.Equal(pd(want)) {
		t.Fatalf("got %s want %s", got, want)
	}
}
func TestPaperDelayedEntryPartialExitAndReversal(t *testing.T) {
	p, now := paperFixture(t)
	ctx := context.Background()
	paperSignal(t, p, 1, now, "buy")
	paperTick(t, p, 2, now.Add(999*time.Millisecond), "9999", "10000", "1")
	if p.snapshot().Accounts[0].Position != nil {
		t.Fatal("filled before one-second latency")
	}
	paperTick(t, p, 3, now.Add(time.Second), "9999", "10000", "1")
	for _, a := range p.snapshot().Accounts {
		if a.Position == nil {
			t.Fatal("not filled")
		}
		paperAssertDecimal(t, a.Position.Entry, "10002")
		paperAssertDecimal(t, a.Position.Quantity, "0.099")
		paperAssertDecimal(t, a.Fees, "0.495099")
	}
	// Re-reading the committed publication must not add size or intake rows.
	paperSignal(t, p, 1, now, "buy")
	paperSignal(t, p, 2, now.Add(2*time.Second), "sell")
	paperTick(t, p, 4, now.Add(3*time.Second), "10100", "10101", "0.04")
	paperAssertDecimal(t, p.snapshot().Accounts[0].Position.Remaining, "0.059")
	paperTick(t, p, 4, now.Add(3500*time.Millisecond), "10100", "10101", "0.04")
	paperAssertDecimal(t, p.snapshot().Accounts[0].Position.Remaining, "0.059")
	paperTick(t, p, 5, now.Add(4*time.Second), "10100", "10101", "0.04")
	paperTick(t, p, 6, now.Add(5*time.Second), "10100", "10101", "0.04")
	for _, a := range p.snapshot().Accounts {
		if a.Position != nil {
			t.Fatal("must finish close before reverse")
		}
		if a.Pending == nil || a.Pending.After != now.Add(6*time.Second) {
			t.Fatal("reverse latency missing")
		}
		paperAssertDecimal(t, a.Gross, "9.50202")
		paperAssertDecimal(t, a.Fees, "0.99494901")
	}
	paperTick(t, p, 7, now.Add(5500*time.Millisecond), "10100", "10101", "1")
	if p.snapshot().Accounts[0].Position != nil {
		t.Fatal("reverse filled too early")
	}
	paperTick(t, p, 8, now.Add(6*time.Second), "10100", "10101", "1")
	for _, a := range p.snapshot().Accounts {
		if a.Position == nil || a.Position.Signal.Direction != "sell" {
			t.Fatal("reverse not opened")
		}
	}
	trades, err := paperRows[paperTrade](ctx, p.db, "SELECT payload FROM paper_trades WHERE exited IS NOT NULL")
	if err != nil || len(trades) != 2 {
		t.Fatalf("%d %v", len(trades), err)
	}
	for _, tr := range trades {
		if tr.ExitReason != "opposite_signal" || tr.FillCount != 4 {
			t.Fatalf("wrong exit %+v", tr)
		}
	}
}
func TestPaperATRStopTimeAndNoReentry(t *testing.T) {
	for _, side := range []string{"buy", "sell"} {
		t.Run(side, func(t *testing.T) {
			p, now := paperFixture(t)
			paperSignal(t, p, 1, now, side)
			paperTick(t, p, 2, now.Add(time.Second), "9999", "10000", "1")
			bid, ask := "9800", "9801"
			if side == "sell" {
				bid, ask = "10200", "10201"
			}
			paperTick(t, p, 3, now.Add(2*time.Second), bid, ask, "1")
			s := p.snapshot()
			if s.Accounts[0].Position.ExitReason != "" || s.Accounts[1].Position.ExitReason != "stop_loss" {
				t.Fatal("exit groups diverged incorrectly")
			}
			paperTick(t, p, 4, now.Add(3*time.Second), bid, ask, "1")
			if p.snapshot().Accounts[1].Position != nil {
				t.Fatal("stop not filled")
			}
			paperTick(t, p, 5, now.Add(4*time.Second), "9999", "10000", "1")
			if p.snapshot().Accounts[1].Position != nil {
				t.Fatal("reused original event")
			}
			paperSignal(t, p, 2, now.Add(5*time.Second), side)
			paperTick(t, p, 6, now.Add(6*time.Second), "9999", "10000", "1")
			if p.snapshot().Accounts[1].Position == nil {
				t.Fatal("new event not eligible")
			}
		})
	}
}
func TestPaperSkipRulesAndUnknownFunding(t *testing.T) {
	cases := []struct {
		name   string
		change func(*paperStore)
		size   string
	}{{"atr", func(p *paperStore) { p.atr = nil }, "1"}, {"instrument", func(p *paperStore) { p.instrument.Status = "HALT" }, "1"}, {"recovery", func(p *paperStore) { s := p.snapshot(); s.Gap = true; p.commit(context.Background(), s, paperBatch{}) }, "1"}, {"size", func(p *paperStore) {}, "0.001"}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, now := paperFixture(t)
			tc.change(p)
			paperSignal(t, p, 1, now, "buy")
			paperTick(t, p, 2, now.Add(time.Second), "9999", "10000", tc.size)
			for _, a := range p.snapshot().Accounts {
				if a.Position != nil {
					t.Fatal("ineligible entry filled")
				}
			}
		})
	}
	p, now := paperFixture(t)
	s := p.snapshot()
	s.FundingThrough = now.Add(-time.Hour)
	if paperFundingKnown(s, now, false) {
		t.Fatal("unknown funding became zero")
	}
	s.FundingThrough = now
	s.ExpectedFunding = []int64{now.UnixMilli()}
	if paperFundingKnown(s, now, false) {
		t.Fatal("missing expected settlement counted known")
	}
}
func TestPaperFundingBoundariesAndIdempotence(t *testing.T) {
	p, now := paperFixture(t)
	paperSignal(t, p, 1, now, "buy")
	paperTick(t, p, 2, now.Add(time.Second), "9999", "10000", "1")
	paperSignal(t, p, 2, now.Add(2*time.Second), "sell")
	paperTick(t, p, 3, now.Add(3*time.Second), "10100", "10101", "0.04")
	paperTick(t, p, 4, now.Add(4*time.Second), "10100", "10101", "1")
	records := []paperFunding{{At: now.Add(time.Second), Acquired: now.Add(time.Hour), Rate: pd("0.001"), Mark: pd("10000")}, {At: now.Add(3 * time.Second), Acquired: now.Add(time.Hour), Rate: pd("-0.002"), Mark: pd("10000")}, {At: now.Add(4 * time.Second), Acquired: now.Add(time.Hour), Rate: pd("0.009"), Mark: pd("10000")}}
	if err := p.settle(context.Background(), records, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	// 0.099 * -10 at entry; 0.059 * +20 after partial exit; no charge
	// at final close. Repeating the fetch cannot apply either charge twice.
	for _, a := range p.snapshot().Accounts {
		paperAssertDecimal(t, a.Funding, "0.19")
	}
	if err := p.settle(context.Background(), records, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	for _, a := range p.snapshot().Accounts {
		paperAssertDecimal(t, a.Funding, "0.19")
	}
	rows, err := paperRows[paperFundingEntry](context.Background(), p.db, "SELECT payload FROM paper_funding")
	if err != nil || len(rows) != 4 {
		t.Fatalf("funding rows %d %v", len(rows), err)
	}
	records[0].Rate = pd("0.2")
	if err = p.settle(context.Background(), records, now.Add(time.Hour)); err == nil {
		t.Fatal("rewrote immutable funding")
	}
	if s := p.snapshot(); s.FundingConflict == nil || paperFundingKnown(s, now.Add(4*time.Second), false) || !paperFundingKnown(s, now, false) {
		t.Fatal("funding revision did not conservatively invalidate affected results")
	}
}
func TestPaperRestartBackupAtomicFailureAndGap(t *testing.T) {
	p, now := paperFixture(t)
	ctx := context.Background()
	paperSignal(t, p, 1, now, "buy")
	before := p.snapshot()
	_, err := p.db.Exec(`CREATE TRIGGER reject_paper_fill BEFORE INSERT ON paper_fills BEGIN SELECT RAISE(ABORT,'test disk failure'); END;`)
	if err != nil {
		t.Fatal(err)
	}
	q := *p.quote
	q.ID = 2
	q.At = now.Add(time.Second)
	q.EventAt = q.At
	if err = p.onQuote(ctx, q, false); err == nil {
		t.Fatal("expected transaction failure")
	}
	if !reflect.DeepEqual(before, p.snapshot()) {
		t.Fatal("failed ledger transaction changed cash/cursor")
	}
	if _, err = p.db.Exec("DROP TRIGGER reject_paper_fill"); err != nil {
		t.Fatal(err)
	}
	paperTick(t, p, 3, now.Add(2*time.Second), "9999", "10000", "1")
	dest := filepath.Join(t.TempDir(), "paper.sqlite")
	if err = BackupFile(ctx, p.path, dest); err != nil {
		t.Fatal(err)
	}
	manifest, err := os.ReadFile(dest + ".manifest.json")
	if err != nil || !strings.Contains(string(manifest), "test-experiment") {
		t.Fatal("missing backup cursor/generation", err)
	}
	restored, err := openPaper(filepath.Dir(dest), "run")
	if err != nil {
		t.Fatal(err)
	}
	defer restored.db.Close()
	defer restored.readDB.Close()
	if err = restored.discontinuity(ctx, now.Add(time.Minute), "restart_gap"); err != nil {
		t.Fatal(err)
	}
	restored.instrument = p.instrument
	paperTick(t, restored, 4, now.Add(time.Minute+time.Second), "9999", "10000", "1")
	for _, a := range restored.snapshot().Accounts {
		if a.Position != nil {
			t.Fatal("restart did not close existing position")
		}
	}
	rows, err := paperRows[paperTrade](ctx, restored.db, "SELECT payload FROM paper_trades")
	if err != nil {
		t.Fatal(err)
	}
	for _, tr := range rows {
		if tr.ExitReason != "data_gap" || len(tr.Quality) != 1 {
			t.Fatal("missing recovery evidence")
		}
	}
	if err = restored.heartbeat(ctx, now.Add(time.Minute+2*time.Second), false); err != nil {
		t.Fatal(err)
	}
	if !restored.snapshot().Gap {
		t.Fatal("resumed before 30s")
	}
}
func TestPaperStatisticsAndEquityGaps(t *testing.T) {
	p, now := paperFixture(t)
	s := p.snapshot()
	origin := now.Add(-31 * 24 * time.Hour)
	s.Origin = &origin
	s.ObservedSeconds = 100
	s.CoveredSeconds = 99
	trades := []paperTrade{}
	for i := 0; i < 100; i++ {
		at := origin.Add(time.Duration(i/4) * 24 * time.Hour)
		end := at.Add(time.Hour)
		side := "buy"
		gross := "2"
		if i%2 == 1 {
			side = "sell"
			gross = "-1"
		}
		trades = append(trades, paperTrade{Signal: Signal{Direction: side}, Entered: at, Exited: &end, Entry: pd("10000"), Quantity: pd(".1"), Gross: pd(gross), Fees: pd(".1")})
	}
	r := paperStatistics(trades, s, now, false)
	if !r.Eligible || r.Complete != 100 || r.ConfidenceLow == nil {
		t.Fatalf("wrong gate %+v", r)
	}
	paperAssertDecimal(t, *r.Expectancy, "0.4")
	paperAssertDecimal(t, *r.WinRate, "50")
	x, y, z := pd("10000"), pd("9900"), pd("9000")
	b := paperBatch{Equity: []paperEquity{{Group: "risk", At: now.Add(-4 * time.Minute), BeforeFunding: &x}, {Group: "risk", At: now.Add(-3 * time.Minute), BeforeFunding: &y}, {Group: "risk", At: now.Add(-2 * time.Minute)}, {Group: "risk", At: now.Add(-time.Minute), BeforeFunding: &z}}}
	if err := p.commit(context.Background(), s, b); err != nil {
		t.Fatal(err)
	}
	curve, err := p.equityCurve(context.Background(), s, "risk", now)
	if err != nil {
		t.Fatal(err)
	}
	paperAssertDecimal(t, *curve.MaximumDrawdown, "100")
	if curve.GapSamples != 1 || curve.Points[2].Value != nil {
		t.Fatal("chart bridged missing data")
	}
}
func TestPaperDecimalAndCandleValidation(t *testing.T) {
	for _, v := range []string{"NaN", "1e999", "-1", "0", "1e-100"} {
		if _, err := paperDecimal(v, true); err == nil {
			t.Fatal("accepted", v)
		}
	}
	if !paperPublicURL(paperREST+"/fapi/v1/exchangeInfo") || !paperPublicURL(paperBookWS) || !paperPublicURL(paperMarketWS) || paperPublicURL("https://example.com/order") || paperPublicURL(paperREST+"/fapi/v1/order") {
		t.Fatal("unexpected public URL boundary")
	}
	var row []json.RawMessage
	_ = json.Unmarshal([]byte(`[0,"10","9","8","10","0",299999]`), &row)
	if _, _, err := paperCandle(row, time.Unix(301, 0)); err == nil {
		t.Fatal("accepted invalid OHLC")
	}
}
