package tidal

import (
	"github.com/linlea666/tidal-crypto-intelligence/internal/datahub"
	"golang.org/x/crypto/bcrypt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRadarAuthenticatedReadOnlyAPI(t *testing.T) {
	engine, _ := testEngine()
	store, e := OpenStore(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer store.Close()
	hub, e := datahub.Open(datahub.Config{Root: t.TempDir(), Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	defer hub.Store.Close()
	hash, _ := bcrypt.GenerateFromPassword([]byte("radar-test-password"), bcrypt.MinCost)
	s := NewServer(engine, store, NewWhales(NewCollector(engine)), string(hash), t.TempDir(), "test", false)
	s.Hub = hub
	request := func(method, path, body string, c *http.Cookie, origin string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		if c != nil {
			r.AddCookie(c)
		}
		if origin != "" {
			r.Header.Set("Origin", origin)
		}
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		return w
	}
	for _, path := range []string{"hl-radar/events", "hl-radar/status", "hl-radar/settings", "hl-radar/study"} {
		if w := request("GET", "/api/v2/"+path, "", nil, ""); w.Code != 401 {
			t.Fatal(w.Code)
		}
	}
	login := request("POST", "/api/v1/login", `{"password":"radar-test-password"}`, nil, "")
	cookie := login.Result().Cookies()[0]
	for _, path := range []string{"hl-radar/events", "hl-radar/status", "hl-radar/settings", "hl-radar/study", "hl-radar/wallet?address=0x0000000000000000000000000000000000000001"} {
		if w := request("GET", "/api/v2/"+path, "", cookie, ""); w.Code != 200 || w.Header().Get("Cache-Control") != "private, no-cache" {
			t.Fatal(path, w.Code, w.Body.String())
		}
	}
	for _, body := range []string{`{}`, `{"emailEnabled":null}`, `{"emailEnabled":false,"threshold":1}`, `{"emailEnabled":false} {}`} {
		if w := request("PUT", "/api/v2/hl-radar/settings", body, cookie, ""); w.Code != 400 {
			t.Fatal(w.Code)
		}
	}
	if w := request("PUT", "/api/v2/hl-radar/settings", `{"emailEnabled":false}`, cookie, "https://evil.invalid"); w.Code != 403 {
		t.Fatal("CSRF", w.Code)
	}
	if w := request("PUT", "/api/v2/hl-radar/settings", `{"emailEnabled":true}`, cookie, ""); w.Code != 400 {
		t.Fatal("missing receipt accepted")
	}
	if w := request("PUT", "/api/v2/hl-radar/settings", `{"emailEnabled":false}`, cookie, ""); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	for _, tc := range []struct {
		body   string
		cookie *http.Cookie
		origin string
		status int
	}{{`{"action":"test"}`, nil, "", 401}, {`{"action":"test"}`, cookie, "https://evil.invalid", 403}, {`{"action":"test","unexpected":true}`, cookie, "", 400}, {`{"action":"test"}`, cookie, "", 400}} {
		if w := request("POST", "/api/v2/hl-radar/mail-test", tc.body, tc.cookie, tc.origin); w.Code != tc.status {
			t.Fatal("receipt endpoint", w.Code, w.Body.String())
		}
	}
	for _, path := range []string{"hl-radar/events?asset=SOL", "hl-radar/events?before=invalid", "hl-radar/wallet?address=bad"} {
		if w := request("GET", "/api/v2/"+path, "", cookie, ""); w.Code != 400 {
			t.Fatal(w.Code)
		}
	}
}
