package tidal

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/linlea666/tidal-crypto-intelligence/internal/datahub"
	"golang.org/x/crypto/bcrypt"
)

func TestVIXHTTPAuthSettingsAndIsolation(t *testing.T) {
	engine, _ := testEngine()
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	hub, err := datahub.Open(datahub.Config{Root: t.TempDir(), Offline: true})
	if err != nil {
		t.Fatal(err)
	}
	defer hub.Store.Close()
	hash, _ := bcrypt.GenerateFromPassword([]byte("test-vix-only-password"), bcrypt.MinCost)
	s := NewServer(engine, store, NewWhales(NewCollector(engine)), string(hash), t.TempDir(), "test", false)
	s.Hub = hub
	handler := s.Handler()
	do := func(method, path, body string, cookie *http.Cookie, origin string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		if cookie != nil {
			r.AddCookie(cookie)
		}
		if origin != "" {
			r.Header.Set("Origin", origin)
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w
	}
	for _, path := range []string{"vix", "vix/history", "vix/alerts", "vix/settings"} {
		if r := do("GET", "/api/v2/"+path, "", nil, ""); r.Code != 401 {
			t.Fatal("unauthenticated", path, r.Code)
		}
	}
	login := do("POST", "/api/v1/login", `{"password":"test-vix-only-password"}`, nil, "")
	if login.Code != 200 {
		t.Fatal(login.Body.String())
	}
	cookie := login.Result().Cookies()[0]
	for _, path := range []string{"vix", "vix/history", "vix/history?range=1m", "vix/history?range=3m", "vix/history?range=1y", "vix/alerts", "vix/settings"} {
		r := do("GET", "/api/v2/"+path, "", cookie, "")
		if r.Code != 200 || r.Header().Get("Cache-Control") != "private, no-cache" {
			t.Fatal(path, r.Code, r.Body.String())
		}
	}
	for _, body := range []string{`{}`, `{"emailEnabled":null}`, `{"emailEnabled":"yes"}`, `{"emailEnabled":true,"threshold":20}`, `{"emailEnabled":true} {}`} {
		if r := do("PUT", "/api/v2/vix/settings", body, cookie, ""); r.Code != 400 {
			t.Fatal("invalid settings", body, r.Code)
		}
	}
	if r := do("PUT", "/api/v2/vix/settings", `{"emailEnabled":true}`, cookie, "https://evil.invalid"); r.Code != 403 {
		t.Fatal("CSRF", r.Code)
	}
	if r := do("PUT", "/api/v2/vix/settings", `{"emailEnabled":true}`, cookie, ""); r.Code != 200 || !strings.Contains(r.Body.String(), `"emailEnabled":true`) {
		t.Fatal(r.Code, r.Body.String())
	}
	if r := do("POST", "/api/v2/vix/settings", `{"emailEnabled":true}`, cookie, ""); r.Code != 405 {
		t.Fatal("method", r.Code)
	}
	if r := do("GET", "/api/v2/vix/history?range=all", "", cookie, ""); r.Code != 400 {
		t.Fatal("unbounded range")
	}
	if r := do("GET", "/api/v2/signals?asset=ETH", "", cookie, ""); r.Code != 200 || !strings.Contains(r.Body.String(), `"enabled":false`) {
		t.Fatal("ETH research scope changed", r.Code)
	}
	if hub.Scheduler.State()["calls"].(int64) != 0 {
		t.Fatal("HTTP reads requested upstream")
	}
	if r := do("GET", "/readyz", "", nil, ""); r.Code != 503 {
		t.Fatal("VIX settings made crypto ready", r.Code)
	}
}
