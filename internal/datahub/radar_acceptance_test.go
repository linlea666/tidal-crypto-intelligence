package datahub

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/coder/websocket"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRadarBudgetRollbackExpiryAndActivationBoundary(t *testing.T) {
	h, now := radarTestHub(t)
	w := radarTestWallet(1, now)
	f := radarTestFill(now, "0", "10", "B", 1)
	ctx := context.Background()
	if _, e := h.Store.radar.db.Exec(`CREATE TRIGGER fail_radar_outbox BEFORE INSERT ON outbox BEGIN SELECT RAISE(ABORT,'injected storage failure'); END;`); e != nil {
		t.Fatal(e)
	}
	if h.radarApply(ctx, w, []radarFill{f}, radarTestAccount(now, "10"), now) == nil {
		t.Fatal("write fault ignored")
	}
	var n int
	_ = h.Store.radar.db.QueryRow("SELECT count(*) FROM events").Scan(&n)
	if n != 0 {
		t.Fatal("event survived failed outbox")
	}
	_, _ = h.Store.radar.db.Exec("DROP TRIGGER fail_radar_outbox")
	if e := h.radarApply(ctx, w, []radarFill{f}, radarTestAccount(now, "10"), now); e != nil {
		t.Fatal(e)
	}
	h.mail = &MailConfig{RadarReceiptVerifiedAt: &now}
	h.offline = false
	calls := 0
	h.mailSend = func(context.Context, MailConfig, string, string) error { calls++; return nil }
	if _, e := h.SetRadarSettings(ctx, RadarSettings{true}, now.Add(time.Second)); e != nil {
		t.Fatal(e)
	}
	if e := h.radarProcessNotices(ctx, now.Add(2*time.Second)); e != nil {
		t.Fatal(e)
	}
	if calls != 0 {
		t.Fatal("enabling replayed pre-enable event")
	}
	w = radarTestWallet(2, now)
	if e := h.radarApply(ctx, w, []radarFill{radarTestFill(now.Add(3*time.Second), "0", "10", "B", 2)}, radarTestAccount(now.Add(3*time.Second), "10"), now.Add(3*time.Second)); e != nil {
		t.Fatal(e)
	}
	if e := h.radarProcessNotices(ctx, now.Add(6*time.Minute)); e != nil {
		t.Fatal(e)
	}
	if calls != 0 {
		t.Fatal("expired event sent")
	}
}
func TestRadarLiveReadOnlyContractOptional(t *testing.T) {
	if os.Getenv("TIDAL_RADAR_LIVE_CHECK") != "1" {
		t.Skip("explicit read-only public contract")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	r := newRadarRuntime()
	var trades []radarTrade
	if e := r.query(ctx, map[string]any{"type": "recentTrades", "coin": "BTC"}, 120, &trades); e != nil {
		t.Fatal(e)
	}
	if len(trades) == 0 || len(trades[0].Users) != 2 {
		t.Fatal("missing public addresses")
	}
	a := trades[0].Users[0]
	var state radarAccount
	if e := r.query(ctx, map[string]any{"type": "clearinghouseState", "user": a}, 2, &state); e != nil {
		t.Fatal(e)
	}
	var fills []radarFill
	if e := r.query(ctx, map[string]any{"type": "userFillsByTime", "user": a, "startTime": time.Now().Add(-30 * time.Minute).UnixMilli()}, 120, &fills); e != nil {
		t.Fatal(e)
	}
	if state.Time <= 0 {
		t.Fatal("account snapshot timestamp missing")
	}
	if len(fills) == 0 {
		t.Fatal("no fills available to verify startPosition contract")
	}
	checked := false
	for _, f := range fills {
		if ValidAsset(f.Coin) {
			if _, _, _, e := radarTransition(f); e != nil {
				t.Fatal(e)
			}
			checked = true
			break
		}
	}
	if !checked {
		t.Fatal("no BTC/ETH fill contract observed")
	}
	c, _, e := websocket.Dial(ctx, r.ws, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer c.CloseNow()
	c.SetReadLimit(2 << 20)
	if e = c.Write(ctx, websocket.MessageText, []byte(`{"method":"subscribe","subscription":{"type":"trades","coin":"BTC"}}`)); e != nil {
		t.Fatal(e)
	}
	for {
		_, b, e := c.Read(ctx)
		if e != nil {
			t.Fatal(e)
		}
		var m struct {
			Channel string       `json:"channel"`
			Data    []radarTrade `json:"data"`
		}
		if json.Unmarshal(b, &m) == nil && m.Channel == "trades" && len(m.Data) > 0 {
			if len(m.Data[0].Users) != 2 {
				t.Fatal("WS users missing")
			}
			t.Logf("public REST and WS contract passed: recent=%d fills=%d; no storage or SMTP", len(trades), len(fills))
			break
		}
	}
}
func TestRadarTruncatedAndOversizedSources(t *testing.T) {
	h, now := radarTestHub(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b map[string]any
		_ = json.NewDecoder(r.Body).Decode(&b)
		switch b["type"] {
		case "userFillsByTime":
			v := make([]radarFill, 2000)
			for i := range v {
				v[i] = radarTestFill(now, "0", "10", "B", int64(i))
			}
			json.NewEncoder(w).Encode(v)
		case "userNonFundingLedgerUpdates":
			w.Write([]byte("[]"))
		case "userRole":
			w.Write([]byte(`{"role":"user"}`))
		case "clearinghouseState":
			json.NewEncoder(w).Encode(radarTestAccount(now, "10"))
		default:
			w.Write([]byte(strings.Repeat("x", 2<<20+1)))
		}
	}))
	defer srv.Close()
	h.radar.url = srv.URL
	if e := h.radarFetch(context.Background(), radarTestWallet(1, now).Address, now); e != nil {
		t.Fatal(e)
	}
	var w RadarWallet
	if e := radarLoad(context.Background(), h.Store.radar.db, "wallet", radarTestWallet(1, now).Address, &w); e != nil {
		t.Fatal(e)
	}
	if w.HistoryComplete {
		t.Fatal("same timestamp pagination saturation called complete")
	}
	var raw any
	if e := h.radar.query(context.Background(), map[string]any{"type": "oversize"}, 2, &raw); e == nil {
		t.Fatal("oversize accepted")
	}
}
func TestRadarBrowserFixture(t *testing.T) {
	root := os.Getenv("TIDAL_RADAR_QA_ROOT")
	if root == "" {
		t.Skip("isolated browser fixture")
	}
	if !filepath.IsAbs(root) || !strings.Contains(root, "/tmp/radar-qa/") {
		t.Fatal("requires isolated QA root")
	}
	h, e := Open(Config{Root: root, Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	now := time.Now().UTC().Truncate(time.Millisecond)
	h.boot = now.Add(-time.Hour)
	radarTestFX(t, h, now)
	for i := 1; i <= 28; i++ {
		w := radarTestWallet(i, now)
		f := radarTestFill(now.Add(-time.Duration(i)*time.Second), "0", "10", "A", int64(i))
		if i%2 == 0 {
			w.HistoryComplete = false
		}
		w.HistoryNote = "离线浏览器验收夹具，不代表真实市场或策略效果"
		if e = h.radarApply(context.Background(), w, []radarFill{f}, radarTestAccount(now, "-10"), now); e != nil {
			t.Fatal(e)
		}
	}
	if e = h.radarGroups(context.Background(), now); e != nil {
		t.Fatal(e)
	}
	if e = h.radarExport(context.Background(), now); e != nil {
		t.Fatal(e)
	}
	t.Log(fmt.Sprintf("isolated %d events; no upstream or SMTP", 28))
}
