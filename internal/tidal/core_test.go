package tidal

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"
)

func testEngine() (*Engine, time.Time) {
	e := NewEngine()
	e.BootAt = time.Now().Add(-time.Hour)
	now := time.Now().UTC()
	e.SetRate("USDT", "0.998", now)
	e.SetRate("USDC", "1.002", now)
	return e, now
}
func testBook(t *testing.T, e *Engine, v, q string, bids, asks [][]string) *Book {
	t.Helper()
	for _, b := range e.Books {
		if b.Instrument.Venue == v && b.Instrument.Asset == "BTC" && b.Instrument.Quote == q {
			if err := b.Snapshot(bids, asks, 10); err != nil {
				t.Fatal(err)
			}
			return b
		}
	}
	t.Fatal("book not found")
	return nil
}
func zoneAmount(f Frame, side string) int64 {
	var n int64
	for _, z := range f.Zones {
		if z.Side == side {
			n += z.USDCents
		}
	}
	return n
}
func TestSpotUniverseHas24UniquePairs(t *testing.T) {
	m := map[string]bool{}
	for _, i := range Instruments() {
		if m[i.Key()] {
			t.Fatal("duplicate", i)
		}
		m[i.Key()] = true
		if i.Venue == "okx" && i.Quote == "USDC" {
			t.Fatal("legacy unified pair duplicated")
		}
	}
	if len(m) != 24 {
		t.Fatal(len(m))
	}
}
func TestSequenceGapDuplicateAndDepth(t *testing.T) {
	e, _ := testEngine()
	b := testBook(t, e, "kraken", "USD", [][]string{{"100", "2"}, {"99", "1"}}, [][]string{{"101", "1"}, {"102", "1"}})
	if err := b.Delta([][]string{{"100", "0"}, {"98", "3"}}, nil, 11, 10, true, 2); err != nil {
		t.Fatal(err)
	}
	if b.LowBid != 98 {
		t.Fatal("depth boundary not refreshed")
	}
	if err := b.Delta([][]string{{"99", "99"}}, nil, 11, 10, true, 2); err != nil {
		t.Fatal("duplicate rejected", err)
	}
	if b.Bids[99].Q != 1 {
		t.Fatal("duplicate applied")
	}
	if b.Delta(nil, nil, 13, 12, true, 2) == nil || b.Valid {
		t.Fatal("sequence hole accepted")
	}
	if b.Delta(nil, nil, 14, 13, true, 2) == nil {
		t.Fatal("invalid book resumed without snapshot")
	}
}
func TestDeepOverlapFXAndStaleness(t *testing.T) {
	e, now := testEngine()
	b := testBook(t, e, "okx", "USDT", [][]string{{"100", "2"}, {"99", "1"}}, [][]string{{"100.1", "1"}, {"102", "1"}})
	b.SetDeep([][]string{{"100", "999"}, {"99", "999"}, {"98", "3"}}, [][]string{{"100.1", "999"}, {"102", "999"}, {"103", "2"}})
	e.Tick(now.Add(time.Millisecond * 20))
	f := e.Frame("BTC")
	want := cents((100*2 + 99 + 98*3) * .998)
	if got := zoneAmount(f, "bid"); got != want {
		t.Fatalf("deep overlap or FX: got %d want %d", got, want)
	}
	e.Tick(now.Add(31 * time.Second))
	if zoneAmount(e.Frame("BTC"), "bid") != 0 {
		t.Fatal("stale FX counted")
	}
	b.Invalidate("test gap")
	e.SetRate("USDT", "1", time.Now())
	e.Tick(time.Now())
	if zoneAmount(e.Frame("BTC"), "bid") != 0 {
		t.Fatal("invalid depth counted")
	}
}
func TestTradeDirectionDedupeUnitsAndHistoricalFX(t *testing.T) {
	e, now := testEngine()
	c := NewCollector(e)
	i := Instrument{"coinbase", "BTC", "USD", "BTC-USD", "spot"}
	c.coinbaseTrade(i, map[string]any{"side": "buy", "trade_id": "7", "price": "100.25", "size": "2", "time": now.Format(time.RFC3339Nano)})
	c.coinbaseTrade(i, map[string]any{"side": "buy", "trade_id": "7", "price": "100.25", "size": "2", "time": now.Format(time.RFC3339Nano)})
	fs := e.Flows("BTC", "spot", now.Truncate(time.Minute))
	if len(fs) != 1 || fs[0].BuyCents != 0 || fs[0].SellCents != 20050 || fs[0].Trades != 1 {
		t.Fatalf("maker side/dedupe wrong: %+v", fs)
	}
	swap := Instrument{"okx", "BTC", "USDT", "BTC-USDT-SWAP", "perp"}
	e.SetRate("USDT", "1.1", now.Add(2*time.Second))
	e.Trade(swap, "1", "buy", "100", "20", now, .01)
	fs = e.Flows("BTC", "perp", now.Truncate(time.Minute))
	if fs[0].BuyCents != 1996 || math.Abs(fs[0].BaseQty-.2) > 1e-9 {
		t.Fatalf("unit/event FX wrong: %+v", fs[0])
	}
	e.Trade(swap, "2", "invalid", "100", "20", now, .01)
	if e.Flows("BTC", "perp", now.Truncate(time.Minute))[0].Trades != 1 {
		t.Fatal("invalid side was accepted")
	}
}
func TestMissingFXCanRetryAndNeverAssumeDollar(t *testing.T) {
	e := NewEngine()
	e.BootAt = time.Now().Add(-time.Hour)
	now := time.Now()
	i := Instrument{"binance", "BTC", "USDT", "BTCUSDT", "spot"}
	e.Trade(i, "1", "buy", "100", "1", now, 1)
	e.SetRate("USDT", "0.9", now)
	e.Trade(i, "1", "buy", "100", "1", now, 1)
	fs := e.Flows("BTC", "spot", now.Truncate(time.Minute))
	if len(fs) != 1 || fs[0].BuyCents != 9000 || !fs[0].Partial {
		t.Fatalf("retry/FX gap: %+v", fs)
	}
}
func TestKrakenOfficialChecksumPreservesDecimals(t *testing.T) { // Official Kraken book-checksum-v2 example.
	b := newBook(Instrument{Venue: "kraken"})
	bids := [][]string{{"45283.5", "0.10000000"}, {"45283.4", "1.54582015"}, {"45282.1", "0.10000000"}, {"45281.0", "0.10000000"}, {"45280.3", "1.54592586"}, {"45279.0", "0.07990000"}, {"45277.6", "0.03310103"}, {"45277.5", "0.30000000"}, {"45277.3", "1.54602737"}, {"45276.6", "0.15445238"}}
	asks := [][]string{{"45285.2", "0.00100000"}, {"45286.4", "1.54571953"}, {"45286.6", "1.54571109"}, {"45289.6", "1.54560911"}, {"45290.2", "0.15890660"}, {"45291.8", "1.54553491"}, {"45294.7", "0.04454749"}, {"45296.1", "0.35380000"}, {"45297.5", "0.09945542"}, {"45299.5", "0.18772827"}}
	if err := b.Snapshot(bids, asks, 0); err != nil {
		t.Fatal(err)
	}
	if got := KrakenCRC(b); got != 3310070434 {
		t.Fatal(got)
	}
}
func TestWhaleNullStaleCrossAndCurrentNotional(t *testing.T) {
	e, now := testEngine()
	w := NewWhales(NewCollector(e))
	a := "0x" + strings.Repeat("1", 40)
	w.Add(a, 1, true)
	state := map[string]any{"time": now.UnixMilli(), "assetPositions": []any{map[string]any{"position": map[string]any{"coin": "BTC", "szi": "-2", "positionValue": "200", "entryPx": "110", "leverage": map[string]any{"value": 20, "type": "cross"}, "liquidationPx": nil, "unrealizedPnl": "20"}}}}
	w.parse(a, state)
	w.core()
	ws := e.Whales("BTC")
	if len(ws) != 1 || ws[0].Side != "short" || ws[0].Liquidation != nil || ws[0].Distance != nil || ws[0].USDCents != 20040 || ws[0].Margin != "cross" {
		t.Fatalf("bad whale %+v", ws)
	}
	bs := WhaleBuckets(ws, 25, now)
	if len(bs) != 1 || bs[0].Kind != "entry" {
		t.Fatal("null liquidation fabricated", bs)
	}
	ws[0].At = now.Add(-91 * time.Second)
	if len(WhaleBuckets(ws, 25, now)) != 0 {
		t.Fatal("stale positions counted")
	}
}
func sampleFrame(at time.Time, n int64) Frame {
	return Frame{Asset: "BTC", At: at, Price: 100, Step: 25, Zones: []Zone{{Price: 75, Step: 25, Side: "bid", USDCents: n, Sources: map[string]int64{"okx": n}}}, Coverage: []Coverage{{Valid: true, BidLow: 50, Bid: 100, Ask: 101, AskHigh: 150}}, Candle: Candle{at.Unix(), 100, 105, 95, 102}}
}
func TestStorageRollupRecoveryRetentionAndWhalePartition(t *testing.T) {
	s, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	at := time.Now().UTC().Add(-31 * 24 * time.Hour).Truncate(24 * time.Hour)
	for j, n := range []int64{100, 300} {
		tm := at.Add(time.Duration(j) * time.Minute)
		if err = s.Write("1m", "frame", "BTC", tm, sampleFrame(tm, n)); err != nil {
			t.Fatal(err)
		}
		s.Write("1m", "flow:spot", "BTC", tm, []Flow{{Venue: "okx", Asset: "BTC", Market: "spot", Minute: tm.Unix(), BuyCents: n, PriceBins: map[int64][2]int64{3: {n, 0}}}})
		s.Write("1m", "liquidations", "BTC", tm, []Liquidation{{Venue: "okx", At: tm, USDCents: n}})
	}
	if err = s.Cleanup(time.Now()); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(s.Root, "history", "1m", at.Format("2006-01-02")+".sqlite")
	if _, err = os.Stat(path); err != nil {
		t.Fatal("unrolled partition deleted")
	}
	if err = s.Rollup(at); err != nil {
		t.Fatal(err)
	}
	rs, _ := s.Query("15m", "frame", "BTC", at, at.Add(time.Minute), 10)
	var f Frame
	json.Unmarshal(rs[0].Data, &f)
	if f.Zones[0].USDCents != 200 {
		t.Fatal("orderbook snapshots were summed", f.Zones)
	}
	rs, _ = s.Query("15m", "flow:spot", "BTC", at, at.Add(time.Minute), 10)
	var fs []Flow
	json.Unmarshal(rs[0].Data, &fs)
	if fs[0].BuyCents != 400 {
		t.Fatal("trades were averaged")
	}
	rs, _ = s.Query("15m", "liquidations", "BTC", at, at.Add(time.Minute), 10)
	var ls []Liquidation
	json.Unmarshal(rs[0].Data, &ls)
	if len(ls) != 2 {
		t.Fatal("liquidations lost")
	}
	s.Write("5m", "whales", "BTC", time.Now(), []Whale{})
	if _, err = os.Stat(filepath.Join(s.Root, "history", "whale-5m", time.Now().UTC().Format("2006-01-02")+".sqlite")); err != nil {
		t.Fatal(err)
	}
	s.Put("rollupThrough", at.Add(24*time.Hour).Unix())
	if err = s.Cleanup(time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("expired summarized partition retained")
	}
	s.mu.Lock()
	s.Paused = true
	s.mu.Unlock()
	s.Write("5s", "frame", "BTC", time.Now(), sampleFrame(time.Now(), 1))
	rs, _ = s.Query("5s", "frame", "BTC", time.Now().Add(-time.Hour), time.Now().Add(time.Hour), 10)
	if len(rs) != 0 {
		t.Fatal("fine history ignored disk pause")
	}
}
func TestHTTPAuthOriginAndAnnotationDirection(t *testing.T) {
	e, _ := testEngine()
	s, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	wh := NewWhales(NewCollector(e))
	hash, _ := bcrypt.GenerateFromPassword([]byte("test-password-long"), bcrypt.MinCost)
	api := NewServer(e, s, wh, string(hash), t.TempDir(), "test", true)
	h := api.Handler()
	do := func(method, path, body, origin string, cookie *http.Cookie) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		if origin != "" {
			r.Header.Set("Origin", origin)
		}
		if cookie != nil {
			r.AddCookie(cookie)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, r)
		return rec
	}
	if do("GET", "/api/v1/levels", "", "", nil).Code != 401 {
		t.Fatal("unauthenticated API allowed")
	}
	if do("POST", "/api/v1/login", `{"password":"test-password-long"}`, "https://evil.invalid", nil).Code != 403 {
		t.Fatal("CSRF accepted")
	}
	r := do("POST", "/api/v1/login", `{"password":"test-password-long"}`, "", nil)
	if r.Code != 200 {
		t.Fatal(r.Body.String())
	}
	cookies := r.Result().Cookies()
	if len(cookies) != 1 || !cookies[0].Secure || !cookies[0].HttpOnly {
		t.Fatal("insecure session cookie")
	}
	cookie := cookies[0]
	if do("GET", "/api/v1/levels", "", "", cookie).Code != 200 {
		t.Fatal("cold empty frame failed")
	}
	if do("POST", "/api/v1/annotations", `{"asset":"BTC","side":"long","entry":100,"stop":110,"target":120}`, "", cookie).Code != 400 {
		t.Fatal("reversed stop accepted")
	}
	if do("POST", "/api/v1/annotations", `{"asset":"BTC","side":"long","entry":100,"stop":90,"target":120}`, "", cookie).Code != 200 {
		t.Fatal("annotation rejected")
	}
	do("POST", "/api/v1/logout", "", "", cookie)
	if do("GET", "/api/v1/health", "", "", cookie).Code != 401 {
		t.Fatal("logout did not revoke")
	}
}
func TestConcurrentUpdatesQueriesAndBoundedCandidates(t *testing.T) {
	e, now := testEngine()
	b := testBook(t, e, "coinbase", "USD", [][]string{{"100", "1"}}, [][]string{{"100.1", "1"}})
	w := NewWhales(NewCollector(e))
	for j := 0; j < 1100; j++ {
		w.Add(fmt.Sprintf("0x%040x", j+1), float64(j), false)
	}
	if len(w.Candidates) != 1000 {
		t.Fatal(len(w.Candidates))
	}
	var wg sync.WaitGroup
	for j := 0; j < 3; j++ {
		wg.Add(1)
		go func(j int) {
			defer wg.Done()
			for n := 0; n < 100; n++ {
				switch j {
				case 0:
					b.Delta([][]string{{"100", fmt.Sprint(n + 1)}}, nil, 0, 0, false, 0)
				case 1:
					e.Tick(now)
				case 2:
					e.Frame("BTC")
					e.Whales("BTC")
					e.Health()
				}
			}
		}(j)
	}
	wg.Wait()
}

func TestRestartPreservesObservedLiquidations(t *testing.T) {
	s, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	at := time.Now().UTC().Truncate(time.Minute).Add(-time.Minute)
	want := Liquidation{Venue: "bybit", Asset: "BTC", Side: "long", Price: 84000, USDCents: 4200000, At: at.Add(3 * time.Second), PriceType: "bankruptcy"}
	if err := s.Write("1m", "liquidations", "BTC", at, []Liquidation{want}); err != nil {
		t.Fatal(err)
	}
	e, _ := testEngine()
	if err := s.Initialize(e, NewWhales(NewCollector(e))); err != nil {
		t.Fatal(err)
	}
	got := e.Liquidations("BTC")
	if len(got) != 1 || got[0].USDCents != want.USDCents || !got[0].At.Equal(want.At) {
		t.Fatalf("restart lost event: %+v", got)
	}
}
func TestHistoryGapIsNotZeroAndReadIsStreaming(t *testing.T) {
	s, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	now := time.Now().UTC().Truncate(time.Minute)
	for _, d := range []time.Duration{-5 * time.Minute, -time.Minute} {
		at := now.Add(d)
		s.Write("1m", "frame", "BTC", at, sampleFrame(at, 100))
	}
	n := 0
	if err = s.Visit(context.Background(), "1m", "frame", "BTC", now.Add(-time.Hour), now, 0, func(r Record) error { n++; return nil }); err != nil || n != 2 {
		t.Fatal(err, n)
	}
	e, _ := testEngine()
	api := NewServer(e, s, NewWhales(NewCollector(e)), "", t.TempDir(), "test", false)
	r := httptest.NewRequest("GET", "/api/v1/history?hours=4&price=75&step=25", nil)
	rec := httptest.NewRecorder()
	api.history(rec, r, "BTC")
	if !bytes.Contains(rec.Body.Bytes(), []byte(`"usdCents":null`)) {
		t.Fatal("missing sample was connected as a wall", rec.Body.String())
	}
}

func TestDiskBudgetPressureAndWhaleCap(t *testing.T) {
	s, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	// Account for images/logs/backups exactly as the host reporter does, without allocating 20GB.
	raw, _ := json.Marshal(map[string]int64{"bytes": 21 << 30})
	os.WriteFile(filepath.Join(s.Root, "external-usage.json"), raw, 0600)
	if err = s.Cleanup(time.Now()); err != nil {
		t.Fatal(err)
	}
	if !s.Report().Paused {
		t.Fatal("budget overflow did not pause fine history")
	}
	s.Write("5s", "frame", "BTC", time.Now(), sampleFrame(time.Now(), 1))
	rs, _ := s.Query("5s", "frame", "BTC", time.Now().Add(-time.Hour), time.Now().Add(time.Hour), 10)
	if len(rs) > 0 {
		t.Fatal("fine write continued under pressure")
	}
	if err = s.Write("15m", "frame", "BTC", time.Now(), sampleFrame(time.Now(), 1)); err != nil {
		t.Fatal("coarse data unavailable", err)
	}
	s.mu.Lock()
	s.WhaleBytes = 2 << 30
	s.Paused = false
	s.mu.Unlock()
	s.Write("1m", "whale-buckets", "BTC", time.Now(), []WhaleBucket{})
	rs, _ = s.Query("1m", "whale-buckets", "BTC", time.Now().Add(-time.Hour), time.Now().Add(time.Hour), 10)
	if len(rs) > 0 {
		t.Fatal("whale sub-budget was ignored")
	}
}
func TestWithdrawalEvidenceExcludesFXAndDisconnect(t *testing.T) {
	e, now := testEngine()
	b := testBook(t, e, "okx", "USDT", [][]string{{"100", "100"}}, [][]string{{"100.1", "1"}})
	e.Tick(now)
	b.Delta([][]string{{"100", "1"}}, nil, 11, 10, true, 400)
	e.SetRate("USDT", "1", now.Add(time.Second))
	e.Tick(now.Add(time.Second))
	for _, z := range e.Frame("BTC").Zones {
		if strings.Contains(z.Evidence, "撤走") {
			t.Fatal("FX move classified as cancel")
		}
	}
	b.Invalidate("gap")
	e.Tick(now.Add(2 * time.Second))
	b.Snapshot([][]string{{"100", "50"}}, [][]string{{"100.1", "1"}}, 20)
	e.Tick(now.Add(8 * time.Second))
	for _, z := range e.Frame("BTC").Zones {
		if strings.Contains(z.Evidence, "撤走") || z.Seconds != 0 {
			t.Fatal("reconnect retained continuity", z)
		}
	}
}
func BenchmarkBurstBookAggregation(b *testing.B) {
	e, _ := testEngine()
	for _, book := range e.Books {
		bs, as := [][]string{}, [][]string{}
		mid := 84000.0
		if book.Instrument.Asset == "ETH" {
			mid = 2700
		}
		for j := 0; j < 5000; j++ {
			bs = append(bs, []string{fmt.Sprintf("%.5f", mid-float64(j+1)*mid/100000), "0.02"})
			as = append(as, []string{fmt.Sprintf("%.5f", mid+float64(j+1)*mid/100000), "0.03"})
		}
		book.Snapshot(bs, as, 1)
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		e.Tick(time.Now())
	}
}
