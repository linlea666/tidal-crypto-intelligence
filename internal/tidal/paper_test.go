package tidal

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/linlea666/tidal-crypto-intelligence/internal/datahub"
	"golang.org/x/crypto/bcrypt"
)

func TestPaperHTTPReadOnlyAndAuthentication(t *testing.T) {
	engine, _ := testEngine()
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	hub, err := datahub.Open(datahub.Config{Root: t.TempDir(), Offline: true, PaperMode: "collect"})
	if err != nil {
		t.Fatal(err)
	}
	defer hub.Store.Close()
	hash, _ := bcrypt.GenerateFromPassword([]byte("paper-test-password"), bcrypt.MinCost)
	server := NewServer(engine, store, NewWhales(NewCollector(engine)), string(hash), t.TempDir(), "test", false)
	server.Hub = hub
	handler := server.Handler()
	for _, path := range []string{"paper", "paper/trades", "paper/trade?id=missing", "alert-audit"} {
		r := httptest.NewRecorder()
		handler.ServeHTTP(r, httptest.NewRequest("GET", "/api/v2/"+path, nil))
		if r.Code != 401 {
			t.Fatal("paper data leaked before auth", path, r.Code)
		}
	}
	login := httptest.NewRecorder()
	handler.ServeHTTP(login, httptest.NewRequest("POST", "/api/v1/login", strings.NewReader(`{"password":"paper-test-password"}`)))
	if login.Code != 200 {
		t.Fatal("login", login.Code)
	}
	cookie := login.Result().Cookies()[0]
	for _, method := range []string{"GET", "POST", "PUT", "DELETE"} {
		for _, path := range []string{"paper", "paper/trades", "alert-audit"} {
			req := httptest.NewRequest(method, "/api/v2/"+path, nil)
			req.AddCookie(cookie)
			r := httptest.NewRecorder()
			handler.ServeHTTP(r, req)
			expected := 405
			if method == "GET" {
				expected = 200
			}
			if r.Code != expected {
				t.Fatalf("%s %s: %d %s", method, path, r.Code, r.Body.String())
			}
		}
	}
	if hub.Scheduler.State()["calls"].(int64) != 0 {
		t.Fatal("paper request fetched upstream")
	}
}
