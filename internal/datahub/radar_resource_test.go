package datahub

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// Production WS, candidate queue, weighted REST and transaction paths together.
// The producer uses elapsed slots so dropped ticker wakes do not reduce offered
// traffic invisibly. All generated data lives exclusively in this test binary.
func radarResourceStart(t *testing.T, h *Hub) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	var requests, offered, frames atomic.Int64
	var mu sync.Mutex
	fills := map[string]radarFill{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/info" {
			requests.Add(1)
			var body struct {
				Type string `json:"type"`
				User string `json:"user"`
			}
			if json.NewDecoder(r.Body).Decode(&body) != nil {
				w.WriteHeader(400)
				return
			}
			mu.Lock()
			f, exists := fills[body.User]
			mu.Unlock()
			switch body.Type {
			case "userFillsByTime":
				if exists {
					json.NewEncoder(w).Encode([]radarFill{f})
				} else {
					w.Write([]byte("[]"))
				}
			case "userNonFundingLedgerUpdates":
				w.Write([]byte("[]"))
			case "userRole":
				w.Write([]byte(`{"role":"user"}`))
			case "clearinghouseState":
				json.NewEncoder(w).Encode(radarTestAccount(time.Now().UTC(), "10"))
			default:
				w.WriteHeader(400)
			}
			return
		}
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		c.SetReadLimit(4096)
		_, raw, err := c.Read(ctx)
		if err != nil {
			return
		}
		var sub struct {
			Subscription struct {
				Type string `json:"type"`
			}
		}
		if json.Unmarshal(raw, &sub) != nil {
			return
		}
		reader := make(chan struct{})
		go func() {
			defer close(reader)
			for {
				if _, _, e := c.Read(ctx); e != nil {
					return
				}
			}
		}()
		defer func() { c.CloseNow(); <-reader }()
		if sub.Subscription.Type == "userFills" {
			select {
			case <-ctx.Done():
			case <-reader:
			}
			return
		}
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		began := time.Now()
		var sent int64
		for {
			select {
			case <-ctx.Done():
				return
			case <-reader:
				return
			case now := <-tick.C:
				target := int64(time.Since(began) / (10 * time.Millisecond)) // 100 public trades/s
				n := target - sent
				if n > 500 {
					offered.Add(n - 500)
					n = 500
				} // excess is explicitly offered/dropped
				batch := make([]radarTrade, 0, n)
				for j := int64(0); j < n; j++ {
					seq := sent + j + 1
					address := fmt.Sprintf("0x%040x", 1+(seq/2000)%32)
					mu.Lock()
					if _, ok := fills[address]; !ok {
						fills[address] = radarTestFill(now.UTC(), "0", "10", "B", seq)
					}
					mu.Unlock()
					batch = append(batch, radarTrade{Coin: "BTC", Px: "100000", Sz: "0.1", Time: now.UnixMilli(), TID: seq, Users: []string{address, "0x0000000000000000000000000000000000000000"}})
				}
				offered.Add(n)
				sent = target
				b, _ := json.Marshal(map[string]any{"channel": "trades", "data": batch})
				send, stop := context.WithTimeout(ctx, time.Second)
				e := c.Write(send, websocket.MessageText, b)
				stop()
				if e != nil {
					return
				}
				frames.Add(int64(len(batch)))
			}
		}
	}))
	h.radar.url = srv.URL + "/info"
	h.radar.ws = "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"
	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		wg.Add(3)
		go func() { defer wg.Done(); h.radarCollector(ctx) }()
		go func() { defer wg.Done(); h.radarBackground(ctx) }()
		go func() {
			defer wg.Done()
			// Production FX refresh runs independently of expensive maintenance.
			// Keep that topology in replay, otherwise stale FX silently disables
			// the radar workload during the very contention we intend to test.
			tick := time.NewTicker(5 * time.Second)
			defer tick.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case at := <-tick.C:
					d, _ := h.Dataset("fx.usd.kraken")
					_, err := h.Store.Ingest(d, Observation{Dataset: d.ID, Source: d.Source, ObservedAt: &at, FetchedAt: at, Quality: "valid", Payload: Payload{Rates: []Rate{{"USDT", "1.0001"}, {"USDC", "1.0000"}}}})
					if err != nil {
						t.Error("concurrent radar FX refresh", err)
						return
					}
				}
			}
		}()
		wg.Wait()
	}()
	return func() {
		cancel()
		<-done
		srv.Close()
		var count int
		if e := h.Store.radar.db.QueryRow("SELECT count(*) FROM events").Scan(&count); e != nil {
			t.Error(e)
		}
		h.radar.mu.Lock()
		health := h.radar.health
		weight := h.radar.weightLocked(time.Now())
		h.radar.mu.Unlock()
		t.Logf("radar wire replay offered=%d delivered=%d REST=%d events=%d gaps=%d rejected=%d weight=%d bytes=%d", offered.Load(), frames.Load(), requests.Load(), count, health.Gaps, health.Dropped, weight, h.Store.radar.size())
		if count == 0 || frames.Load() == 0 || requests.Load() == 0 {
			t.Error("radar replay failed to exercise verified opening path")
		}
		if weight > 600 {
			t.Error("radar exceeded independent REST budget")
		}
	}
}
func TestRadarResourceWireActor(t *testing.T) {
	h, now := radarTestHub(t)
	h.boot = now.Add(-time.Second)
	stop := radarResourceStart(t, h)
	defer stop()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		var n int
		if e := h.Store.radar.db.QueryRow("SELECT count(*) FROM events").Scan(&n); e == nil && n > 0 {
			return
		}
		time.Sleep(30 * time.Millisecond)
	}
	t.Fatal("public wire actor did not verify opening")
}
