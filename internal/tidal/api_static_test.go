package tidal

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestEntryRevalidatesAcrossReleaseWithoutChangingAssetCaching(t *testing.T) {
	root := t.TempDir()
	for name, body := range map[string]string{"index.html": "release entry", "app-hash.js": "release asset"} {
		path := filepath.Join(root, name)
		if e := os.WriteFile(path, []byte(body), 0600); e != nil {
			t.Fatal(e)
		}
		at := time.Now().Add(-time.Hour)
		if e := os.Chtimes(path, at, at); e != nil {
			t.Fatal(e)
		}
	}
	s := &Server{WebDir: root}
	for _, path := range []string{"/", "/activity/deep-link"} {
		r := httptest.NewRecorder()
		s.static(r, httptest.NewRequest("GET", path, nil))
		if r.Code != 200 || r.Body.String() != "release entry" || r.Header().Get("Cache-Control") != "no-cache" {
			t.Fatalf("entry may remain heuristically fresh after deployment: %s %d %v", path, r.Code, r.Header())
		}
		conditional := httptest.NewRequest("GET", path, nil)
		conditional.Header.Set("If-Modified-Since", r.Header().Get("Last-Modified"))
		revalidated := httptest.NewRecorder()
		s.static(revalidated, conditional)
		if revalidated.Code != http.StatusNotModified || revalidated.Header().Get("Cache-Control") != "no-cache" {
			t.Fatal("conditional revalidation lost its cache policy")
		}
	}
	asset := httptest.NewRecorder()
	s.static(asset, httptest.NewRequest("GET", "/app-hash.js", nil))
	if asset.Code != 200 || asset.Header().Get("Cache-Control") != "" || asset.Body.String() != "release asset" {
		t.Fatal("existing hashed asset behavior changed")
	}
}
