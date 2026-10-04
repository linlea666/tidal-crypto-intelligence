package tidal

import (
	"github.com/linlea666/tidal-crypto-intelligence/internal/datahub"
	"golang.org/x/crypto/bcrypt"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestV21ReadOnlyHTTPAndAuth(t *testing.T) {
	e, _ := testEngine()
	st, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	hub, err := datahub.Open(datahub.Config{Root: t.TempDir(), Offline: true})
	if err != nil {
		t.Fatal(err)
	}
	defer hub.Store.Close()
	hash, _ := bcrypt.GenerateFromPassword([]byte("test-only-long-password"), bcrypt.MinCost)
	server := NewServer(e, st, NewWhales(NewCollector(e)), string(hash), t.TempDir(), "test", false)
	server.Hub = hub
	handler := server.Handler()
	login := httptest.NewRecorder()
	handler.ServeHTTP(login, httptest.NewRequest("POST", "/api/v1/login", strings.NewReader(`{"password":"test-only-long-password"}`)))
	if login.Code != 200 {
		t.Fatal("login failed")
	}
	cookie := login.Result().Cookies()[0]
	paths := []string{"activity?asset=BTC&hours=1", "activity?asset=ETH&hours=24", "levels?asset=ETH&step=5", "large-orders?asset=BTC&history=1", "large-orders?asset=ETH", "onchain-cost", "onchain-cost/history", "onchain-cost/events", "onchain-cost/research", "onchain-cost/settings"}
	for i := 0; i < 100; i++ {
		req := httptest.NewRequest("GET", "/api/v2/"+paths[i%len(paths)], nil)
		req.AddCookie(cookie)
		r := httptest.NewRecorder()
		handler.ServeHTTP(r, req)
		if r.Code != 200 {
			t.Fatalf("read %s: %d %s", paths[i%len(paths)], r.Code, r.Body.String())
		}
		if r.Header().Get("Cache-Control") != "private, no-cache" {
			t.Fatal("market data cache not private")
		}
	}
	state := hub.Scheduler.State()
	if state["calls"].(int64) != 0 {
		t.Fatal("100 HTTP queries spent upstream quota")
	}
	r := httptest.NewRecorder()
	handler.ServeHTTP(r, httptest.NewRequest("GET", "/api/v2/activity", nil))
	if r.Code != 401 {
		t.Fatal("activity leaked before auth")
	}
	for _, body := range []string{`{"emailEnabled":false}`, `{}`, `{"emailEnabled":"false"}`, `{"emailEnabled":false,"unknown":true}`, `{"emailEnabled":false}{}`} {
		req := httptest.NewRequest("PUT", "/api/v2/onchain-cost/settings", strings.NewReader(body))
		req.AddCookie(cookie)
		r := httptest.NewRecorder()
		handler.ServeHTTP(r, req)
		want := 400
		if body == `{"emailEnabled":false}` {
			want = 200
		}
		if r.Code != want {
			t.Fatalf("settings body %s returned %d: %s", body, r.Code, r.Body.String())
		}
	}
	for _, method := range []string{"GET", "PUT"} {
		r := httptest.NewRecorder()
		handler.ServeHTTP(r, httptest.NewRequest(method, "/api/v2/onchain-cost/settings", strings.NewReader(`{"emailEnabled":false}`)))
		if r.Code != 401 {
			t.Fatal("onchain settings lack authentication", method, r.Code)
		}
	}
}
