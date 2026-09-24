package datahub

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func (h *Hub) fxHistory(ctx context.Context) {
	for ctx.Err() == nil {
		byHour := map[int64][]Rate{}
		for _, quote := range []string{"USDT", "USDC"} {
			v, e := directGet(ctx, "https://api.kraken.com/0/public/OHLC?pair="+quote+"USD&interval=60")
			if e != nil {
				continue
			}
			root := object(v)
			if len(array(root["error"])) > 0 {
				continue
			}
			for name, x := range object(root["result"]) {
				if name == "last" {
					continue
				}
				for _, raw := range array(x) {
					r := array(raw)
					if len(r) < 5 {
						continue
					}
					at := timestamp(r[0])
					if at == nil || at.Add(time.Hour).After(time.Now()) {
						continue
					}
					rate, e := validNumber(r[1], false)
					if e == nil && dec(rate).IsPositive() {
						byHour[at.Unix()] = append(byHour[at.Unix()], Rate{quote, rate})
					}
				}
			}
		}
		d, _ := h.Dataset("fx.usd.kraken")
		for ts, rates := range byHour {
			at := time.Unix(ts, 0).UTC()
			_, _ = h.Store.Ingest(d, Observation{Dataset: d.ID, Source: "kraken", ObservedAt: &at, FetchedAt: time.Now().UTC(), TimeBasis: "source", Resolution: 3600, Quality: "valid", Payload: Payload{Rates: rates}})
		}
		if !sleep(ctx, time.Hour) {
			return
		}
	}
}

// Restricted reports contain aggregate health only, never wallet addresses or keys.
func (h *Hub) writeReport() {
	now := time.Now().UTC()
	markets := map[string]any{}
	for _, a := range Assets() {
		valid, total := 0, 0
		for _, d := range Registry() {
			if d.Kind == "book" && d.Asset == a {
				total++
				if o, ok := h.Store.Latest(d.ID); ok && o.Fresh(d, now) {
					if _, _, fx := h.Rate(d.Quote, now); fx {
						valid++
					}
				}
			}
		}
		d, _ := h.Dataset(ID("whales", "ALL", "Hyperliquid", "futures"))
		o, ok := h.Store.Latest(d.ID)
		fresh, observed := 0, 0
		for _, w := range o.Payload.Whales {
			if w.Asset == a {
				observed++
				if ok && o.Fresh(d, now) && freshWhale(w, now) {
					fresh++
				}
			}
		}
		_, priceAt, priceValid := h.CurrentPrice(a, now)
		markets[a] = map[string]any{"validBooks": valid, "totalBooks": total, "freshWhales": fresh, "observedWhales": observed, "priceAt": priceAt, "priceValid": priceValid}
	}
	quota := h.Scheduler.State()
	delete(quota, "jobs")
	fx, ok := h.Store.Latest("fx.usd.kraken")
	d, _ := h.Dataset("fx.usd.kraken")
	statuses := []map[string]any{}
	for _, c := range h.Catalog() {
		ds := c["dataset"].(Dataset)
		if strings.HasPrefix(ds.ID, "wallet.") {
			continue
		}
		statuses = append(statuses, map[string]any{"dataset": ds.ID, "status": c["status"], "observedAt": c["observedAt"], "fetchedAt": c["fetchedAt"]})
	}
	contracts := []map[string]any{}
	state := h.Scheduler.State()
	if jobs, ok := state["jobs"].([]Job); ok {
		for _, j := range jobs {
			if j.Dataset.Contract && j.Mode == "live" {
				contracts = append(contracts, map[string]any{"dataset": j.Dataset.ID, "status": j.ContractStatus, "disabled": j.Disabled, "failures": j.Failures, "error": j.Error})
			}
		}
	}
	quality := map[string]any{}
	for _, a := range Assets() {
		var q map[string]any
		h.Store.LoadState("signals/quality/"+a, &q)
		quality[a] = q
	}
	var signalError, studyError, gap map[string]any
	h.Store.LoadState("signals/error", &signalError)
	h.Store.LoadState("studies/error", &studyError)
	h.Store.LoadState("research/gap", &gap)
	b, e := json.Marshal(map[string]any{"at": now, "generation": "v2", "markets": markets, "fx": metadata(d, fx, ok), "scheduler": quota, "storage": h.Store.Status(), "datasets": statuses, "legacyCollectorsRunning": false, "contracts": contracts, "signals": quality, "signalError": signalError, "studyError": studyError, "researchGap": gap, "mail": h.mailStatus()})
	if e != nil {
		return
	}
	path := filepath.Join(filepath.Dir(h.Store.Root()), "collector-health.json")
	if os.WriteFile(path+".tmp", b, 0644) == nil {
		_ = os.Rename(path+".tmp", path)
	}
}

func (h *Hub) pruneWallets(now time.Time) {
	h.Scheduler.mu.Lock()
	defer h.Scheduler.mu.Unlock()
	for id, j := range h.Scheduler.jobs {
		if j.Mode == "live" || j.InFlight || (!j.Completed && !j.Disabled) || j.LastAttempt == nil || now.Sub(*j.LastAttempt) < 24*time.Hour {
			continue
		}
		delete(h.Scheduler.jobs, id)
		if j.Dataset.Kind != "wallet" {
			continue
		}
		h.Store.write.Lock()
		_, err := h.Store.db.Exec("DELETE FROM latest WHERE dataset=?", j.Dataset.ID)
		if err == nil {
			h.Store.mu.Lock()
			h.Store.status.HotBytes -= h.Store.sizes[j.Dataset.ID]
			delete(h.Store.latest, j.Dataset.ID)
			delete(h.Store.sizes, j.Dataset.ID)
			h.Store.mu.Unlock()
			h.mu.Lock()
			delete(h.registry, j.Dataset.ID)
			h.mu.Unlock()
		}
		h.Store.write.Unlock()
	}
	_ = h.Scheduler.persistLocked()
}
