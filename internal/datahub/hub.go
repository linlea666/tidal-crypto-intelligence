package datahub

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
)

type Config struct {
	Mail               *MailConfig
	Root, BaseURL, Key string
	Offline            bool
}
type cachedView struct {
	Raw   json.RawMessage
	Until time.Time
	Epoch uint64
}
type viewFlight struct {
	done chan struct{}
	raw  json.RawMessage
	err  error
}
type Hub struct {
	mail       *MailConfig
	Store      *Warehouse
	Scheduler  *Scheduler
	registry   map[string]Dataset
	mu         sync.RWMutex
	viewMu     sync.Mutex
	studyMu    sync.Mutex
	views      map[string]cachedView
	flights    map[string]*viewFlight
	viewBytes  int
	baselineMu sync.RWMutex
	baselines  map[string]Baseline
	wallMu     sync.RWMutex
	walls      wallHistory
	continuity map[string]wallContinuity
	boot       time.Time
	offline    bool
}

func Open(cfg Config) (*Hub, error) {
	w, e := OpenWarehouse(cfg.Root)
	if e != nil {
		return nil, e
	}
	if cfg.BaseURL == "" {
		cfg.BaseURL = "https://proxy.keystore.com.cn/api/v1/proxy/coinglass"
	}
	u, e := url.Parse(cfg.BaseURL)
	if e != nil || u.Scheme != "https" || u.Host == "" {
		w.Close()
		return nil, errors.New("CoinGlass base URL must use HTTPS")
	}
	h := &Hub{Store: w, registry: map[string]Dataset{}, views: map[string]cachedView{}, flights: map[string]*viewFlight{}, baselines: map[string]Baseline{}, boot: time.Now().UTC(), offline: cfg.Offline, mail: cfg.Mail}
	w.LoadState("baselines", &h.baselines)
	w.LoadState("wallHistory", &h.walls)
	w.LoadState("wallContinuity", &h.continuity)
	for _, d := range Registry() {
		h.registry[d.ID] = d
	}
	h.Scheduler = NewScheduler(w, Registry(), NewFetcher(cfg.BaseURL, strings.TrimSpace(cfg.Key)), cfg.Key != "" && !cfg.Offline, h.boot)
	var fingerprint string
	keyHash := fmt.Sprintf("%x", sha256.Sum256([]byte(cfg.Key)))
	if !w.LoadState("keyFingerprint", &fingerprint) || fingerprint != keyHash {
		h.Scheduler.quota.AuthFailed = false
		_ = w.SaveState("keyFingerprint", keyHash)
	}
	var parserVersion string
	if !w.LoadState("parserVersion", &parserVersion) || parserVersion != RulesVersion {
		for _, j := range h.Scheduler.jobs {
			if j.Mode == "live" {
				if j.Failures > 0 {
					j.Next = h.boot
				}
				j.Failures = 0
				j.Error = ""
				j.Disabled = j.Dataset.Disabled
			}
		}
		_ = w.SaveState("parserVersion", RulesVersion)
	}
	for _, j := range h.Scheduler.jobs {
		if _, ok := h.registry[j.Dataset.ID]; !ok {
			h.registry[j.Dataset.ID] = j.Dataset
		}
	}
	return h, nil
}
func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
func (h *Hub) Dataset(id string) (Dataset, bool) {
	h.mu.RLock()
	d, ok := h.registry[id]
	h.mu.RUnlock()
	return d, ok
}
func (h *Hub) Catalog() []map[string]any {
	h.mu.RLock()
	defer h.mu.RUnlock()
	now := time.Now()
	out := []map[string]any{}
	for _, d := range h.registry {
		o, ok := h.Store.Latest(d.ID)
		status := "missing"
		if ok {
			status = o.Status(d, now)
		}
		out = append(out, map[string]any{"dataset": d, "status": status, "observedAt": o.ObservedAt, "fetchedAt": o.FetchedAt, "revision": o.Revision, "history": h.Store.Progress(d.ID)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i]["dataset"].(Dataset).ID < out[j]["dataset"].(Dataset).ID })
	return out
}
func (h *Hub) datasets() map[string]Dataset {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := map[string]Dataset{}
	for k, v := range h.registry {
		out[k] = v
	}
	return out
}
func (h *Hub) Request(req DataRequest) (Job, error) {
	j, e := h.Scheduler.Request(req, time.Now().UTC(), false)
	if e == nil {
		h.mu.Lock()
		if _, exists := h.registry[j.Dataset.ID]; !exists {
			h.registry[j.Dataset.ID] = j.Dataset
		}
		h.mu.Unlock()
	}
	return j, e
}
func (h *Hub) Run(ctx context.Context) {
	var wg sync.WaitGroup
	start := func(f func(context.Context)) { wg.Add(1); go func() { defer wg.Done(); f(ctx) }() }
	start(h.Scheduler.Run)
	start(h.researchWorker)
	if !h.offline {
		start(h.prices)
		start(h.fx)
		start(h.candles)
		start(h.fxHistory)
	}
	start(func(ctx context.Context) {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		h.maintain(ctx)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				h.maintain(ctx)
			}
		}
	})
	<-ctx.Done()
	wg.Wait()
}
func (h *Hub) maintain(ctx context.Context) {
	h.pruneWallets(time.Now().UTC())
	cancelCtx, cancel := context.WithTimeout(ctx, 40*time.Second)
	defer cancel()
	_ = h.Store.Maintain(cancelCtx, h.datasets(), time.Now().UTC())
	liquidityError := ""
	if err := h.SampleLiquidity(cancelCtx, time.Now().UTC()); err != nil {
		liquidityError = err.Error()
	}
	_ = h.Store.SaveState("liquidityError", liquidityError)
	h.SampleWalls(time.Now().UTC())
	h.SampleDistributions(time.Now().UTC())
	h.writeReport()
	if h.Store.Status().Paused {
		return
	}
	var seeded bool
	if !h.Store.LoadState("baselineSeeded", &seeded) || !seeded {
		now := time.Now().UTC().Truncate(time.Hour)
		from := now.Add(-30 * 24 * time.Hour)
		all := true
		for _, d := range Registry() {
			if d.Kind == "book" {
				_, e := h.Scheduler.Request(DataRequest{Dataset: d.ID, From: &from, To: &now, Resolution: 3600}, time.Now(), true)
				if e != nil {
					all = false
				}
			}
		}
		if all {
			_ = h.Store.SaveState("baselineSeeded", true)
		}
	}
	// Baselines are prepared off the read path, at most once per hour.
	var last time.Time
	if !h.Store.LoadState("baselineComputed", &last) || time.Since(last) > time.Hour {
		baselineCtx, baselineCancel := context.WithTimeout(cancelCtx, 12*time.Second)
		err := h.BuildBaselines(baselineCtx)
		baselineCancel()
		message := ""
		if err != nil {
			message = err.Error()
		}
		_ = h.Store.SaveState("baselineBuildError", message)
	}
}
func (h *Hub) Ready() bool {
	now := time.Now()
	fxd, _ := h.Dataset("fx.usd.kraken")
	fx, ok := h.Store.Latest(fxd.ID)
	if !ok || !fx.Fresh(fxd, now) {
		return false
	}
	for _, a := range Assets() {
		d, _ := h.Dataset(ID("price", a, "Binance", "spot"))
		o, ok := h.Store.Latest(d.ID)
		if !ok || !o.Fresh(d, now) {
			return false
		}
		valid := 0
		for _, d := range Registry() {
			if d.Kind == "book" && d.Asset == a {
				if o, ok := h.Store.Latest(d.ID); ok && o.Fresh(d, now) {
					valid++
				}
			}
		}
		if valid < 3 {
			return false
		}
	}
	return true
}
func directGet(ctx context.Context, u string) (any, error) {
	req, e := http.NewRequestWithContext(ctx, "GET", u, nil)
	if e != nil {
		return nil, e
	}
	client := &http.Client{Timeout: 15 * time.Second}
	r, e := client.Do(req)
	if e != nil {
		return nil, errors.New("direct feed connection failed")
	}
	defer r.Body.Close()
	if r.StatusCode != 200 {
		return nil, errors.New("direct feed HTTP failure")
	}
	var v any
	d := json.NewDecoder(io.LimitReader(r.Body, 8<<20))
	d.UseNumber()
	e = d.Decode(&v)
	return v, e
}
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
func (h *Hub) prices(ctx context.Context) {
	delay := time.Second
	for ctx.Err() == nil {
		conn, _, e := websocket.Dial(ctx, "wss://data-stream.binance.vision/stream?streams=btcusdt@aggTrade/ethusdt@aggTrade", nil)
		if e == nil {
			conn.SetReadLimit(65536)
			last := map[string]time.Time{}
			for ctx.Err() == nil {
				readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
				_, b, err := conn.Read(readCtx)
				cancel()
				if err != nil {
					break
				}
				var msg struct {
					Data struct {
						Symbol string `json:"s"`
						Price  string `json:"p"`
						Time   int64  `json:"T"`
					} `json:"data"`
				}
				if json.Unmarshal(b, &msg) != nil {
					continue
				}
				a := strings.TrimSuffix(msg.Data.Symbol, "USDT")
				if !ValidAsset(a) || dec(msg.Data.Price).IsNegative() || dec(msg.Data.Price).IsZero() {
					continue
				}
				now := time.Now().UTC()
				if now.Sub(last[a]) < time.Second {
					continue
				}
				at := time.UnixMilli(msg.Data.Time).UTC()
				d, _ := h.Dataset(ID("price", a, "Binance", "spot"))
				_, _ = h.Store.Ingest(d, Observation{Dataset: d.ID, Source: "binance", ObservedAt: &at, FetchedAt: now, TimeBasis: "source", Quality: "valid", Payload: Payload{Price: &Price{msg.Data.Price, "USDT"}}})
				last[a] = now
				delay = time.Second
			}
			conn.CloseNow()
		}
		if !sleep(ctx, delay) {
			return
		}
		delay = min(30*time.Second, delay*2)
	}
}
func (h *Hub) fx(ctx context.Context) {
	d, _ := h.Dataset("fx.usd.kraken")
	lastSaved := time.Time{}
	for ctx.Err() == nil {
		now := time.Now().UTC()
		v, e := directGet(ctx, "https://api.kraken.com/0/public/Ticker?pair=USDTUSD,USDCUSD")
		if e == nil {
			p := Payload{}
			root := object(v)
			if len(array(root["error"])) == 0 {
				for name, item := range object(root["result"]) {
					m := object(item)
					a, b := array(m["a"]), array(m["b"])
					if len(a) == 0 || len(b) == 0 {
						continue
					}
					quote := "USDT"
					if strings.Contains(name, "USDC") {
						quote = "USDC"
					}
					rate := dec(str(a[0])).Add(dec(str(b[0]))).Div(dec("2"))
					if rate.IsPositive() {
						p.Rates = append(p.Rates, Rate{quote, rate.String()})
					}
				}
			}
			if len(p.Rates) == 2 {
				sort.Slice(p.Rates, func(i, j int) bool { return p.Rates[i].Quote < p.Rates[j].Quote })
				o := Observation{Dataset: d.ID, Source: "kraken", FetchedAt: now, TimeBasis: "retrieval", Quality: "valid", Payload: p}
				if now.Sub(lastSaved) >= time.Minute {
					_, _ = h.Store.Ingest(d, o)
					lastSaved = now
				} else {
					o.Revision = digest(o)
					h.Store.putHot(d.ID, o)
				}
			}
		}
		if !sleep(ctx, 5*time.Second) {
			return
		}
	}
}
func (h *Hub) candles(ctx context.Context) {
	for ctx.Err() == nil {
		for _, a := range Assets() {
			if ctx.Err() != nil {
				return
			}
			d, _ := h.Dataset(ID("candles", a, "Binance", "spot"))
			now := time.Now().UTC()
			limit := "500"
			if previous, ok := h.Store.Latest(d.ID); ok && now.Sub(previous.Time()) < time.Hour {
				limit = "12"
			}
			v, e := directGet(ctx, "https://data-api.binance.vision/api/v3/klines?symbol="+a+"USDT&interval=5m&limit="+limit)
			if e != nil {
				continue
			}
			for _, x := range array(v) {
				r := array(x)
				if len(r) < 7 {
					continue
				}
				at := timestamp(r[0])
				end := timestamp(r[6])
				if at == nil || end == nil || end.After(now) {
					continue
				}
				c := Candle{num(r[1]), num(r[2]), num(r[3]), num(r[4]), num(r[5])}
				if c.Low <= 0 || c.High < c.Low {
					continue
				}
				_, _ = h.Store.Ingest(d, Observation{Dataset: d.ID, Source: "binance", ObservedAt: at, FetchedAt: now, TimeBasis: "source", Resolution: 300, Quality: "valid", Payload: Payload{Candle: &c}})
			}
		}
		if !sleep(ctx, 5*time.Minute) {
			return
		}
	}
}
func (h *Hub) Rate(quote string, now time.Time) (string, *time.Time, bool) {
	if quote == "USD" {
		return "1", nil, true
	}
	d, _ := h.Dataset("fx.usd.kraken")
	o, ok := h.Store.Latest(d.ID)
	if !ok || !o.Fresh(d, now) {
		return "", nil, false
	}
	for _, r := range o.Payload.Rates {
		if r.Quote == quote {
			t := o.Time()
			return r.USD, &t, true
		}
	}
	return "", nil, false
}
func (h *Hub) CurrentPrice(asset string, now time.Time) (float64, *time.Time, bool) {
	d, _ := h.Dataset(ID("price", asset, "Binance", "spot"))
	o, ok := h.Store.Latest(d.ID)
	if !ok || o.Payload.Price == nil {
		return 0, nil, false
	}
	t := o.Time()
	rate, _, fx := h.Rate("USDT", now)
	if !fx {
		return 0, &t, false
	}
	return num(multiply(o.Payload.Price.Value, rate)), &t, o.Fresh(d, now)
}
func parseInt(q url.Values, key string, def, low, high int) int {
	v, e := strconv.Atoi(q.Get(key))
	if e != nil || v < low || v > high {
		return def
	}
	return v
}
func parseFloat(q url.Values, key string, def, low, high float64) float64 {
	v, e := strconv.ParseFloat(q.Get(key), 64)
	if e != nil || math.IsNaN(v) || math.IsInf(v, 0) || v < low || v > high {
		return def
	}
	return v
}
func (h *Hub) cached(ctx context.Context, key string, ttl time.Duration, compute func() (any, error)) (json.RawMessage, error) {
	h.viewMu.Lock()
	now := time.Now()
	if v, ok := h.views[key]; ok && now.Before(v.Until) && v.Epoch == h.Store.Epoch() {
		h.viewMu.Unlock()
		return v.Raw, nil
	}
	if f, ok := h.flights[key]; ok {
		h.viewMu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-f.done:
			return f.raw, f.err
		}
	}
	f := &viewFlight{done: make(chan struct{})}
	h.flights[key] = f
	h.viewMu.Unlock()
	var b []byte
	v, e := compute()
	if e == nil {
		b, e = json.Marshal(v)
	}
	if len(b) > ResultLimit {
		e = errors.New("结果过大，请缩小查询范围")
	}
	h.viewMu.Lock()
	defer h.viewMu.Unlock()
	if e == nil {
		for k, v := range h.views {
			if now.After(v.Until) {
				h.viewBytes -= len(v.Raw)
				delete(h.views, k)
			}
		}
		if h.viewBytes+len(b) > 32<<20 {
			h.views = map[string]cachedView{}
			h.viewBytes = 0
		}
		if old, ok := h.views[key]; ok {
			h.viewBytes -= len(old.Raw)
		}
		h.views[key] = cachedView{b, time.Now().Add(ttl), h.Store.Epoch()}
		h.viewBytes += len(b)
	}
	f.raw = b
	f.err = e
	delete(h.flights, key)
	close(f.done)
	return b, e
}
