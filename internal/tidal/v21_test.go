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
	paths := []string{"activity?asset=BTC&hours=1", "activity?asset=ETH&hours=24", "levels?asset=ETH&step=5", "large-orders?asset=BTC&history=1", "large-orders?asset=ETH"}
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
}
