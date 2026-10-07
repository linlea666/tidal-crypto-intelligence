package datahub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func radarTestHub(t *testing.T) (*Hub, time.Time) {
	t.Helper()
	h, e := Open(Config{Root: t.TempDir(), Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { h.Store.Close() })
	now := time.Now().UTC().Truncate(time.Millisecond)
	h.boot = now.Add(-time.Hour)
	radarTestFX(t, h, now)
	return h, now
}
func radarTestFX(t *testing.T, h *Hub, now time.Time) {
	t.Helper()
	d, _ := h.Dataset("fx.usd.kraken")
	_, e := h.Store.Ingest(d, Observation{Dataset: d.ID, Source: "kraken", ObservedAt: &now, FetchedAt: now, TimeBasis: "source", Resolution: 60, Quality: "valid", Payload: Payload{Rates: []Rate{{"USDC", "1"}, {"USDT", "1"}}}})
	if e != nil {
		t.Fatal(e)
	}
}
func radarTestWallet(i int, now time.Time) RadarWallet {
	return RadarWallet{Address: fmt.Sprintf("0x%040x", i), FirstSeen: now, Earliest: &now, HistoryFrom: now.Add(-30 * 24 * time.Hour), HistoryThrough: now, HistoryComplete: true, LedgerComplete: true, Age: "recent", Ledger: []radarLedger{}}
}
func radarTestFill(now time.Time, start, size, side string, tid int64) radarFill {
	return radarFill{Coin: "BTC", Px: "100000", Sz: size, Start: start, Side: side, Time: now.UnixMilli(), TID: tid, Hash: fmt.Sprint(tid)}
}
func radarTestAccount(now time.Time, size string) radarAccount {
	var a radarAccount
	raw := fmt.Sprintf(`{"time":%d,"marginSummary":{"accountValue":"100000","totalNtlPos":"%s"},"assetPositions":[{"position":{"coin":"BTC","szi":"%s","positionValue":"%s","entryPx":"100000","liquidationPx":null,"leverage":{"value":40,"type":"cross"}}}]}`, now.UnixMilli(), dec(size).Abs().Mul(dec("100000")).String(), size, dec(size).Abs().Mul(dec("100000")).String())
	_ = json.Unmarshal([]byte(raw), &a)
	return a
}
func radarTestEvents(t *testing.T, h *Hub) []RadarEvent {
	t.Helper()
	v, e := h.radarEventsView(context.Background(), url.Values{"limit": {"50"}})
	if e != nil {
		t.Fatal(e)
	}
	return v.(map[string]any)["events"].([]RadarEvent)
}
func radarOutboxCount(t *testing.T, h *Hub) int {
	t.Helper()
	var n int
	if e := h.Store.radar.db.QueryRow("SELECT count(*) FROM outbox").Scan(&n); e != nil {
		t.Fatal(e)
	}
	return n
}
func TestRadarPositionTransitions(t *testing.T) {
	for _, c := range []struct{ start, size, side, after, opened string }{{"10", "3", "A", "7", "0"}, {"10", "10", "A", "0", "0"}, {"10", "15", "A", "-5", "5"}, {"-10", "4", "B", "-6", "0"}, {"0", "2", "A", "-2", "2"}, {"-4", "2", "A", "-6", "2"}} {
		_, a, o, e := radarTransition(radarTestFill(time.Now(), c.start, c.size, c.side, 1))
		if e != nil || a.String() != c.after || o.String() != c.opened {
			t.Fatalf("%+v %s %s %v", c, a, o, e)
		}
	}
}
func TestRadarOpeningDedupeReductionAndFlip(t *testing.T) {
	h, now := radarTestHub(t)
	w := radarTestWallet(1, now)
	f := radarTestFill(now, "0", "10", "B", 1)
	apply := func(fs []radarFill, size string, at time.Time) {
		t.Helper()
		if e := h.radarApply(context.Background(), w, fs, radarTestAccount(at, size), at); e != nil {
			t.Fatal(e)
		}
	}
	apply([]radarFill{f}, "10", now)
	events := radarTestEvents(t, h)
	if len(events) != 1 || !events[0].InitialRecorded || events[0].Liquidation != nil || events[0].ConfiguredLeverage != 40 || *events[0].EffectiveLeverage != "10" {
		t.Fatalf("bad event %+v", events)
	}
	apply([]radarFill{f}, "10", now)
	if radarOutboxCount(t, h) != 1 {
		t.Fatal("duplicate opening")
	}
	close := radarTestFill(now.Add(time.Second), "10", "10", "A", 2)
	apply([]radarFill{f, close}, "0", now.Add(time.Second))
	if radarOutboxCount(t, h) != 1 || radarTestEvents(t, h)[0].Closed == nil {
		t.Fatal("closing sell became opening short")
	}
	reopen := radarTestFill(now.Add(2*time.Second), "0", "10", "A", 3)
	apply([]radarFill{reopen}, "-10", now.Add(2*time.Second))
	if radarOutboxCount(t, h) != 2 {
		t.Fatal("new short not sent")
	}
	flip := radarTestFill(now.Add(3*time.Second), "-10", "20", "B", 4)
	apply([]radarFill{flip}, "10", now.Add(3*time.Second))
	if radarOutboxCount(t, h) != 3 {
		t.Fatal("flip not split")
	}
}
func TestRadarOldSnapshotMissingFXAndThresholds(t *testing.T) {
	for _, tc := range []struct {
		name, size string
		old, fx    bool
		want       int
	}{{"boundary", "9.9999", false, true, 0}, {"million", "10", false, true, 1}, {"snapshot", "10", true, true, 0}, {"missing_fx", "10", false, false, 0}} {
		t.Run(tc.name, func(t *testing.T) {
			h, now := radarTestHub(t)
			if !tc.fx {
				h.Store.mu.Lock()
				delete(h.Store.latest, "fx.usd.kraken")
				h.Store.mu.Unlock()
			}
			f := radarTestFill(now, "0", tc.size, "B", 1)
			if tc.old {
				h.boot = now.Add(time.Second)
			}
			if e := h.radarApply(context.Background(), radarTestWallet(1, now), []radarFill{f}, radarTestAccount(now, tc.size), now); e != nil {
				t.Fatal(e)
			}
			if n := radarOutboxCount(t, h); n != tc.want {
				t.Fatalf("outbox %d", n)
			}
		})
	}
}
func TestRadarHistoryUnknownEstablishedAndDepositSemantics(t *testing.T) {
	for _, kind := range []string{"deposit", "internalTransfer", "accountClassTransfer"} {
		t.Run(kind, func(t *testing.T) {
			h, now := radarTestHub(t)
			w := radarTestWallet(1, now)
			var l radarLedger
			if e := json.Unmarshal([]byte(fmt.Sprintf(`{"time":%d,"hash":"x","delta":{"type":"%s","usdc":100000}}`, now.Add(-time.Minute).UnixMilli(), kind)), &l); e != nil {
				t.Fatal(e)
			}
			w.Ledger = []radarLedger{l}
			f := radarTestFill(now, "0", "10", "A", 1)
			if e := h.radarApply(context.Background(), w, []radarFill{f}, radarTestAccount(now, "-10"), now); e != nil {
				t.Fatal(e)
			}
			want := 1
			if kind == "deposit" {
				want = 2
			}
			if radarOutboxCount(t, h) != want {
				t.Fatal("incorrect deposit/upgrade", kind)
			}
		})
	}
	h, now := radarTestHub(t)
	w := radarTestWallet(1, now)
	w.HistoryComplete = false
	w.Earliest = nil
	f := radarTestFill(now, "0", "10", "B", 1)
	if e := h.radarApply(context.Background(), w, []radarFill{f}, radarTestAccount(now, "10"), now); e != nil {
		t.Fatal(e)
	}
	if radarTestEvents(t, h)[0].Age != "unknown" || radarOutboxCount(t, h) != 1 {
		t.Fatal("unknown age lost fact alert")
	}
	w = radarTestWallet(2, now)
	old := now.Add(-8 * 24 * time.Hour)
	w.Earliest = &old
	if e := h.radarApply(context.Background(), w, []radarFill{f}, radarTestAccount(now, "10"), now); e != nil {
		t.Fatal(e)
	}
	if radarOutboxCount(t, h) != 1 {
		t.Fatal("established wallet treated new")
	}
}
func TestRadarTransactionConflictAndSyncGroups(t *testing.T) {
	h, now := radarTestHub(t)
	for i := 1; i <= 3; i++ {
		w := radarTestWallet(i, now)
		f := radarTestFill(now, "0", "10", "A", int64(i))
		if e := h.radarApply(context.Background(), w, []radarFill{f}, radarTestAccount(now, "-10"), now); e != nil {
			t.Fatal(e)
		}
	}
	if e := h.radarGroups(context.Background(), now); e != nil {
		t.Fatal(e)
	}
	if radarOutboxCount(t, h) != 4 {
		t.Fatal("group should produce one upgrade")
	}
	if e := h.radarGroups(context.Background(), now); e != nil {
		t.Fatal(e)
	}
	if radarOutboxCount(t, h) != 4 {
		t.Fatal("duplicate group upgrade")
	}
	f := radarTestFill(now, "0", "11", "A", 1)
	e := h.radarApply(context.Background(), radarTestWallet(1, now), []radarFill{f}, radarTestAccount(now, "-11"), now)
	if e == nil {
		t.Fatal("conflicting duplicate accepted")
	}
	if len(radarTestEvents(t, h)) != 3 {
		t.Fatal("transaction not atomic")
	}
}
func TestRadarMailPoolsFailureRestartAndRouting(t *testing.T) {
	h, now := radarTestHub(t)
	h.offline = false
	h.mail = &MailConfig{RadarReceiptVerifiedAt: &now}
	h.mailSend = func(context.Context, MailConfig, string, string) error { return errors.New("unknown submission") }
	for _, topic := range []string{"hl-radar", "existing"} {
		for i := 0; i < 6; i++ {
			id := fmt.Sprintf("%s-%d", topic, i)
			_, e := h.Store.research.Exec("INSERT INTO notices(id,kind,status,payload) VALUES(?,?,'pending','{}')", id, topic)
			if e != nil {
				t.Fatal(e)
			}
			sent, e := h.deliverTopicNoticeBatch(context.Background(), now, []string{id}, "test", "test", topic)
			if !sent || e == nil {
				t.Fatal("attempt not counted", topic, e)
			}
		}
		ok, e := h.mailBudgetAllowed(context.Background(), now, topic)
		if e != nil || ok {
			t.Fatal("pool cap missing")
		}
	}
	total, e := h.totalMailAttempts(context.Background(), now)
	if e != nil || total != 12 {
		t.Fatal(total, e)
	}
	old, _ := h.mailAttempts(context.Background(), now)
	if old != 6 {
		t.Fatal("old pool changed")
	}
	_, e = h.Store.research.Exec("INSERT INTO notices(id,kind,created,status,payload) VALUES('hl-protected','hl-radar:opening',?,'pending','{}')", now.Add(-time.Minute).Unix())
	if e != nil {
		t.Fatal(e)
	}
	if e = h.processNotices(context.Background(), now); e != nil {
		t.Fatal(e)
	}
	var status string
	_ = h.Store.research.QueryRow("SELECT status FROM notices WHERE id='hl-protected'").Scan(&status)
	if status != "pending" {
		t.Fatal("legacy worker consumed radar")
	}
}
func TestRadarOutboxRestartBackupAndLocalReads(t *testing.T) {
	h, now := radarTestHub(t)
	w := radarTestWallet(1, now)
	f := radarTestFill(now, "0", "10", "B", 1)
	if e := h.radarApply(context.Background(), w, []radarFill{f}, radarTestAccount(now, "10"), now); e != nil {
		t.Fatal(e)
	}
	h.boot = now.Add(time.Second)
	if e := h.radarProcessNotices(context.Background(), now.Add(2*time.Second)); e != nil {
		t.Fatal(e)
	}
	var state string
	_ = h.Store.research.QueryRow("SELECT status FROM notices WHERE kind LIKE 'hl-radar:%'").Scan(&state)
	if state != "suppressed_restart" {
		t.Fatal(state)
	}
	if e := BackupFile(context.Background(), h.Store.radar.path, filepath.Join(t.TempDir(), "radar.sqlite")); e != nil {
		t.Fatal(e)
	}
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))
	defer srv.Close()
	h.radar.url = srv.URL
	for i := 0; i < 100; i++ {
		for _, p := range []string{"hl-radar/events", "hl-radar/status", "hl-radar/settings", "hl-radar/wallet"} {
			_, e := h.Read(context.Background(), p, url.Values{"address": {w.Address}})
			if e != nil {
				t.Fatal(e)
			}
		}
	}
	if calls.Load() != 0 {
		t.Fatal("GET called upstream")
	}
}
func TestRadarBoundsAndNoFXDiscovery(t *testing.T) {
	h, now := radarTestHub(t)
	trade := radarTrade{Coin: "BTC", Px: "100000", Sz: "3", Time: now.UnixMilli(), TID: 1, Users: []string{radarTestWallet(1, now).Address, radarTestWallet(2, now).Address}}
	if e := h.radarDiscover(trade, now); e != nil {
		t.Fatal(e)
	}
	if len(h.radar.queue) != 2 {
		t.Fatal("no discovery")
	}
	_ = h.radarDiscover(trade, now)
	if len(h.radar.queue) != 2 {
		t.Fatal("replayed trade counted")
	}
	h.radar.weights = []radarWeight{{At: time.Now(), N: 600}}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if h.radar.reserve(ctx, 2) == nil {
		t.Fatal("weight exceeded")
	}
}
func TestRadarSourceContracts(t *testing.T) {
	h, now := radarTestHub(t)
	w := radarTestWallet(1, now)
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		switch body["type"] {
		case "userFillsByTime":
			json.NewEncoder(rw).Encode([]radarFill{radarTestFill(now, "0", "10", "B", 1)})
		case "userNonFundingLedgerUpdates":
			rw.Write([]byte("[]"))
		case "userRole":
			rw.Write([]byte(`{"role":"subAccount","data":{"master":"0x0000000000000000000000000000000000000009"}}`))
		case "clearinghouseState":
			json.NewEncoder(rw).Encode(radarTestAccount(time.Now(), "10"))
		default:
			t.Error("unexpected info")
		}
	}))
	defer server.Close()
	h.radar.url = server.URL
	if e := h.radarFetch(context.Background(), w.Address, now); e != nil {
		t.Fatal(e)
	}
	if radarOutboxCount(t, h) != 1 {
		t.Fatal("source path failed")
	}
	var got RadarWallet
	_ = radarLoad(context.Background(), h.Store.radar.db, "wallet", w.Address, &got)
	if got.Role != "subAccount" || got.Master == "" {
		t.Fatal("lost account relationship")
	}
}
