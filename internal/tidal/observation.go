package tidal

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

// A bounded local snapshot lets the host record collector health without a
// reusable login credential or an unauthenticated diagnostics HTTP endpoint.
func (s *Store) writeObservation(e *Engine, w *Whales, now time.Time) error {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	markets := map[string]any{}
	for _, asset := range []string{"BTC", "ETH"} {
		f := e.Frame(asset)
		validBooks, freshWhales := 0, 0
		for _, c := range f.Coverage {
			if c.Valid {
				validBooks++
			}
		}
		ws := e.Whales(asset)
		for _, p := range ws {
			if p.Valid {
				freshWhales++
			}
		}
		markets[asset] = map[string]any{"validBooks": validBooks, "totalBooks": len(f.Coverage), "coverage": f.Coverage, "rates": f.Rates, "derivatives": e.Derivatives(asset), "freshWhales": freshWhales, "observedWhales": len(ws), "frameAt": f.At}
	}
	out := map[string]any{"at": now.UTC(), "startedAt": e.Started, "uptimeSeconds": int64(now.Sub(e.BootAt).Seconds()), "heapBytes": mem.HeapAlloc, "goroutines": runtime.NumGoroutine(), "markets": markets, "feeds": e.Health(), "whales": w.Info(), "storage": s.Report()}
	data, err := json.Marshal(out)
	if err != nil {
		return err
	}
	path := filepath.Join(s.Root, "collector-health.json")
	if err = os.WriteFile(path+".tmp", data, 0600); err != nil {
		return err
	}
	return os.Rename(path+".tmp", path)
}
