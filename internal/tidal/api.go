package tidal

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/coder/websocket"
	"golang.org/x/crypto/bcrypt"
)

type Server struct {
	E            *Engine
	Store        *Store
	Whales       *Whales
	PasswordHash string
	Secure       bool
	WebDir       string
	Version      string
	mu           sync.Mutex
	attempts     map[string][]time.Time
	queries      chan struct{}
}

func NewServer(e *Engine, s *Store, w *Whales, hash, webdir, version string, secure bool) *Server {
	return &Server{E: e, Store: s, Whales: w, PasswordHash: hash, Secure: secure, WebDir: webdir, Version: version, attempts: map[string][]time.Time{}, queries: make(chan struct{}, 2)}
}
func jsonOut(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	json.NewEncoder(w).Encode(v)
}
func problem(w http.ResponseWriter, status int, msg string) {
	w.WriteHeader(status)
	jsonOut(w, map[string]string{"error": msg})
}
func tokenHash(t string) string { s := sha256.Sum256([]byte(t)); return hex.EncodeToString(s[:]) }
func (s *Server) authenticated(r *http.Request) bool {
	cookie, err := r.Cookie("tidal_session")
	if err != nil {
		return false
	}
	var expires int64
	err = s.Store.Meta.QueryRow("SELECT expires FROM sessions WHERE token=?", tokenHash(cookie.Value)).Scan(&expires)
	return err == nil && expires > time.Now().Unix()
}
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := s.Store.Meta.PingContext(r.Context()); err != nil {
			problem(w, 503, "database unavailable")
			return
		}
		jsonOut(w, map[string]any{"ok": true, "version": s.Version})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		for _, a := range []string{"BTC", "ETH"} {
			f := s.E.Frame(a)
			if f.Price == 0 || time.Since(f.At) > 10*time.Second {
				problem(w, 503, "waiting for valid market data")
				return
			}
		}
		jsonOut(w, map[string]bool{"ok": true})
	})
	mux.HandleFunc("POST /api/v1/login", s.login)
	mux.HandleFunc("GET /api/v1/session", func(w http.ResponseWriter, r *http.Request) {
		jsonOut(w, map[string]any{"authenticated": s.authenticated(r), "version": s.Version})
	})
	mux.HandleFunc("/api/", s.api)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" && r.Method != "HEAD" {
			problem(w, 405, "method not allowed")
			return
		}
		s.static(w, r)
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Referrer-Policy", "same-origin")
		if r.Method != "GET" && r.Method != "HEAD" {
			if origin := r.Header.Get("Origin"); origin != "" {
				u, err := url.Parse(origin)
				if err != nil || u.Host != r.Host {
					problem(w, 403, "跨站请求已拒绝")
					return
				}
			}
		}
		mux.ServeHTTP(w, r)
	})
}
func (s *Server) login(w http.ResponseWriter, r *http.Request) {
	ip, _, _ := net.SplitHostPort(r.RemoteAddr)
	if forwarded := r.Header.Get("X-Real-IP"); forwarded != "" && net.ParseIP(ip).IsPrivate() {
		ip = forwarded
	}
	s.mu.Lock()
	recent := []time.Time{}
	for _, t := range s.attempts[ip] {
		if time.Since(t) < 5*time.Minute {
			recent = append(recent, t)
		}
	}
	limited := len(recent) >= 10 || (len(s.attempts) >= 4096 && s.attempts[ip] == nil)
	if !limited {
		s.attempts[ip] = append(recent, time.Now())
	}
	if len(s.attempts) > 1000 {
		for k, ts := range s.attempts {
			if len(ts) == 0 || time.Since(ts[len(ts)-1]) > 5*time.Minute {
				delete(s.attempts, k)
			}
		}
	}
	s.mu.Unlock()
	if limited {
		problem(w, 429, "尝试过于频繁，请稍后再试")
		return
	}
	var req struct {
		Password string `json:"password"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&req); err != nil {
		problem(w, 400, "无效请求")
		return
	}
	if bcrypt.CompareHashAndPassword([]byte(s.PasswordHash), []byte(req.Password)) != nil {
		problem(w, 401, "密码不正确")
		return
	}
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		problem(w, 500, "session unavailable")
		return
	}
	token := hex.EncodeToString(b)
	expiry := time.Now().Add(12 * time.Hour)
	s.Store.Meta.Exec("DELETE FROM sessions WHERE expires<?", time.Now().Unix())
	if _, err := s.Store.Meta.Exec("INSERT INTO sessions(token,expires) VALUES(?,?)", tokenHash(token), expiry.Unix()); err != nil {
		problem(w, 500, "session unavailable")
		return
	}
	http.SetCookie(w, &http.Cookie{Name: "tidal_session", Value: token, Path: "/", Expires: expiry, HttpOnly: true, Secure: s.Secure, SameSite: http.SameSiteStrictMode})
	jsonOut(w, map[string]bool{"ok": true})
}
func (s *Server) static(w http.ResponseWriter, r *http.Request) {
	name := filepath.Clean("/" + r.URL.Path)
	p := filepath.Join(s.WebDir, name)
	if st, err := os.Stat(p); err != nil || st.IsDir() {
		p = filepath.Join(s.WebDir, "index.html")
	}
	http.ServeFile(w, r, p)
}
func (s *Server) api(w http.ResponseWriter, r *http.Request) {
	if !s.authenticated(r) {
		problem(w, 401, "请先登录")
		return
	}
	a := r.URL.Query().Get("asset")
	if a == "" {
		a = "BTC"
	}
	if a != "BTC" && a != "ETH" {
		problem(w, 400, "仅支持BTC或ETH")
		return
	}
	if r.Method != "GET" && r.URL.Path != "/api/v1/settings" && r.URL.Path != "/api/v1/annotations" && r.URL.Path != "/api/v1/watchlist" && r.URL.Path != "/api/v1/logout" {
		problem(w, 405, "method not allowed")
		return
	}
	switch r.URL.Path {
	case "/api/v1/logout":
		if r.Method != "POST" {
			problem(w, 405, "POST required")
			return
		}
		if cookie, err := r.Cookie("tidal_session"); err == nil {
			s.Store.Meta.Exec("DELETE FROM sessions WHERE token=?", tokenHash(cookie.Value))
		}
		http.SetCookie(w, &http.Cookie{Name: "tidal_session", Path: "/", Value: "", MaxAge: -1, HttpOnly: true, Secure: s.Secure, SameSite: http.SameSiteStrictMode})
		jsonOut(w, map[string]bool{"ok": true})
	case "/api/v1/overview", "/api/v1/levels":
		f := s.E.Frame(a)
		step := queryFloat(r, "step", Step(a), Step(a), Step(a)*100)
		span := queryFloat(r, "range", 2, .1, 10)
		age := int64(queryFloat(r, "minAge", 0, 0, 86400*30))
		f.Zones = GroupZones(f, step, span, age)
		f.Step = step
		jsonOut(w, f)
	case "/api/v1/stream":
		s.stream(w, r, a)
	case "/api/v1/health":
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		jsonOut(w, map[string]any{"runtime": map[string]any{"heapBytes": mem.HeapAlloc, "heapSysBytes": mem.HeapSys, "goroutines": runtime.NumGoroutine(), "uptimeSeconds": int64(time.Since(s.E.BootAt).Seconds())}, "feeds": s.E.Health(), "storage": s.Store.Report(), "startedAt": s.E.Started, "version": s.Version, "whales": s.Whales.Info(), "now": time.Now().UTC()})
	case "/api/v1/derivatives":
		s.derivativeHistory(w, r, a)
	case "/api/v1/whales":
		ws := s.E.Whales(a)
		limit := int(queryFloat(r, "limit", 10, 1, 50))
		side := r.URL.Query().Get("side")
		filtered := []Whale{}
		for _, item := range ws {
			if side == "" || side == "all" || side == item.Side {
				filtered = append(filtered, item)
			}
		}
		jsonOut(w, map[string]any{"items": filtered[:min(limit, len(filtered))], "count": len(filtered), "buckets": WhaleBuckets(ws, queryFloat(r, "step", Step(a)*4, Step(a), Step(a)*100), time.Now()), "monitor": s.Whales.Info(), "at": time.Now().UTC()})
	case "/api/v1/settings":
		if r.Method == "GET" {
			jsonOut(w, s.Store.Settings())
			return
		}
		if r.Method != "PUT" {
			problem(w, 405, "PUT required")
			return
		}
		var set Settings
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&set); err != nil {
			problem(w, 400, "无效配置")
			return
		}
		if err := s.Store.SetSettings(set); err != nil {
			problem(w, 400, err.Error())
			return
		}
		go s.Store.Cleanup(time.Now())
		jsonOut(w, set)
	case "/api/v1/watchlist":
		if r.Method == "GET" {
			jsonOut(w, s.Whales.Info())
			return
		}
		if r.Method != "POST" {
			problem(w, 405, "POST required")
			return
		}
		var req struct {
			Address string `json:"address"`
			Pinned  bool   `json:"pinned"`
		}
		if json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&req) != nil {
			problem(w, 400, "无效请求")
			return
		}
		if err := s.Whales.Pin(req.Address, req.Pinned); err != nil {
			problem(w, 400, err.Error())
			return
		}
		info := s.Whales.Info()
		s.Store.Put("whalePins", info["pinned"])
		jsonOut(w, info)
	case "/api/v1/annotations":
		s.annotations(w, r, a)
	case "/api/v1/history", "/api/v1/flow", "/api/v1/candles":
		select {
		case s.queries <- struct{}{}:
			defer func() { <-s.queries }()
		default:
			problem(w, 429, "历史查询处理中，请稍后重试")
			return
		}
		if r.URL.Path == "/api/v1/candles" {
			s.candleHistory(w, r, a)
		} else if r.URL.Path == "/api/v1/history" {
			s.history(w, r, a)
		} else {
			s.flow(w, r, a)
		}
	default:
		problem(w, 404, "未知接口")
	}
}
func queryFloat(r *http.Request, key string, def, low, high float64) float64 {
	if s := r.URL.Query().Get(key); s != "" {
		v, err := strconv.ParseFloat(s, 64)
		if err == nil && finite(v) && v >= low && v <= high {
			return v
		}
	}
	return def
}
func GroupZones(f Frame, step, span float64, age int64) []Zone {
	if f.Step <= 0 || f.Price <= 0 {
		return []Zone{}
	}
	step = math.Round(step/f.Step) * f.Step
	if step <= 0 {
		step = f.Step
	}
	group := map[string]*Zone{}
	for _, z := range f.Zones {
		if math.Abs(z.Price-f.Price)/math.Max(1, f.Price)*100 > span {
			continue
		}
		p := math.Floor(z.Price/step) * step
		k := fmt.Sprintf("%s/%.4f", z.Side, p)
		x := group[k]
		if x == nil {
			cp := z
			cp.Price = p
			cp.Step = step
			cp.Sources = map[string]int64{}
			cp.USDCents = 0
			cp.TradedCents = 0
			cp.ChangeCents = 0
			x = &cp
			group[k] = x
		}
		x.Sampled = x.Sampled || z.Sampled
		x.USDCents += z.USDCents
		x.TradedCents += z.TradedCents
		x.ChangeCents += z.ChangeCents
		if z.Seconds < x.Seconds {
			x.Seconds = z.Seconds
			x.Since = z.Since
		}
		x.Occupancy = math.Min(x.Occupancy, z.Occupancy)
		if z.TradedCents > 0 {
			x.Evidence = z.Evidence
		}
		for v, n := range z.Sources {
			x.Sources[v] += n
		}
	}
	out := []Zone{}
	for _, z := range group {
		if z.Seconds >= age {
			out = append(out, *z)
		}
	}
	gradeZones(out, f.Price)
	sort.Slice(out, func(i, j int) bool { return out[i].Price > out[j].Price })
	return out
}
func (s *Server) stream(w http.ResponseWriter, r *http.Request, a string) {
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer conn.CloseNow()
	ctx := conn.CloseRead(r.Context())
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			frame := s.E.Frame(a)
			frame.Zones = GroupZones(frame, queryFloat(r, "step", Step(a)*4, Step(a), Step(a)*100), queryFloat(r, "range", 2, .1, 10), int64(queryFloat(r, "minAge", 0, 0, 86400*30)))
			frame.Step = queryFloat(r, "step", Step(a)*4, Step(a), Step(a)*100)
			data, err := json.Marshal(frame)
			if err != nil {
				return
			}
			sendctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			err = conn.Write(sendctx, websocket.MessageText, data)
			cancel()
			if err != nil {
				return
			}
		}
	}
}
func (s *Server) history(w http.ResponseWriter, r *http.Request, a string) {
	hours := queryFloat(r, "hours", 24, .0833, 24*90)
	res := "1m"
	unit := int64(60)
	if hours <= 1 {
		res = "5s"
		unit = 5
	}
	if hours > 24 {
		res = "15m"
		unit = 900
	}
	now := time.Now().UTC()
	from := now.Add(-time.Duration(hours * float64(time.Hour)))
	stride := int64(math.Ceil(hours*3600/240/float64(unit))) * unit
	if stride < unit {
		stride = unit
	}
	price := queryFloat(r, "price", 0, 0, 1e9)
	step := queryFloat(r, "step", Step(a)*4, Step(a), Step(a)*100)
	span := queryFloat(r, "range", 2, .1, 10)
	heat := r.URL.Query().Get("heatmap") == "1"
	points := []map[string]any{}
	err := s.Store.Visit(r.Context(), res, "frame", a, from, now, stride, func(rec Record) error {
		var f Frame
		if err := json.Unmarshal(rec.Data, &f); err != nil {
			return err
		}
		p := map[string]any{"time": rec.TS, "price": f.Price, "candle": f.Candle}
		if price > 0 {
			var amount int64
			observed := false
			for _, z := range f.Zones {
				if z.Price >= price && z.Price < price+step {
					amount += z.USDCents
					observed = true
				}
			}
			if !observed {
				observed = covered(f, price, "bid") || covered(f, price, "ask")
			}
			if observed {
				p["usdCents"] = amount
			} else {
				p["usdCents"] = nil
			}
		}
		if heat {
			p["zones"] = GroupZones(f, step, span, 0)
		}
		if len(points) > 0 {
			last := points[len(points)-1]["time"].(int64)
			if rec.TS-last > stride+unit {
				points = append(points, map[string]any{"time": last + stride, "price": nil, "usdCents": nil, "zones": []Zone{}})
			}
		}
		points = append(points, p)
		return nil
	})
	if err != nil {
		problem(w, 500, "历史读取失败")
		return
	}
	jsonOut(w, map[string]any{"points": points, "resolution": res, "sampleSeconds": stride, "startedAt": s.E.Started, "from": from, "to": now, "note": "仅展示实际采集历史；空白表示未采集或未覆盖"})
}
func (s *Server) flow(w http.ResponseWriter, r *http.Request, a string) {
	hours := queryFloat(r, "hours", 1, .0833, 24*90)
	market := r.URL.Query().Get("market")
	if market != "perp" {
		market = "spot"
	}
	now := time.Now().UTC()
	res := "1m"
	unit := time.Minute
	if hours > 24 {
		res = "15m"
		unit = 15 * time.Minute
	}
	from := now.Add(-time.Duration(hours * float64(time.Hour))).Truncate(unit)
	current := now.Truncate(unit)
	buy, sell := int64(0), int64(0)
	qty, notional := 0.0, 0.0
	bins := map[int64][2]int64{}
	points := []map[string]any{}
	partial := false
	validPeriods := 0
	venues := map[string][2]int64{}
	add := func(ts int64, fs []Flow) {
		b, s := int64(0), int64(0)
		sources := map[string]bool{}
		incomplete := false
		for _, f := range fs {
			b += f.BuyCents
			s += f.SellCents
			qty += f.BaseQty
			notional += f.USDQty
			sources[f.Venue] = true
			incomplete = incomplete || f.Partial
			x := venues[f.Venue]
			x[0] += f.BuyCents
			x[1] += f.SellCents
			venues[f.Venue] = x
			for p, v := range f.PriceBins {
				x := bins[p]
				x[0] += v[0]
				x[1] += v[1]
				bins[p] = x
			}
		}
		expected := 5
		if market == "perp" {
			expected = 3
		}
		incomplete = incomplete || len(sources) < expected
		partial = partial || incomplete
		buy += b
		sell += s
		var cvd any = buy - sell
		if len(fs) == 0 {
			cvd = nil
		}
		if len(points) > 0 {
			last := points[len(points)-1]["time"].(int64)
			if ts-last > int64(unit.Seconds()) {
				points = append(points, map[string]any{"time": last + int64(unit.Seconds()), "cvdCents": nil, "partial": true})
				partial = true
			}
		}
		points = append(points, map[string]any{"time": ts, "buyCents": b, "sellCents": s, "cvdCents": cvd, "partial": incomplete})
		validPeriods++
	}
	err := s.Store.Visit(r.Context(), res, "flow:"+market, a, from, current.Add(-time.Second), 0, func(rec Record) error {
		var fs []Flow
		if err := json.Unmarshal(rec.Data, &fs); err != nil {
			return err
		}
		add(rec.TS, fs)
		return nil
	})
	if err != nil {
		problem(w, 500, "成交历史读取失败")
		return
	}
	add(current.Unix(), mergeFlows(s.E.Flows(a, market, current), current.Unix()))
	if validPeriods < int(now.Sub(from)/unit) {
		partial = true
	}
	vwap := 0.0
	if qty > 0 {
		vwap = notional / qty
	}
	jsonOut(w, map[string]any{"buyCents": buy, "sellCents": sell, "netCents": buy - sell, "vwap": vwap, "series": points, "footprint": bins, "venues": venues, "partial": partial, "step": Step(a), "market": market, "resolution": res, "from": from, "to": now, "startedAt": s.E.Started, "definition": "主动买入额 − 主动卖出额，不是充值提现；按完整分钟/15分钟边界统计已观察成交", "coverage": s.E.Health()})
}
func (s *Server) candleHistory(w http.ResponseWriter, r *http.Request, a string) {
	hours := queryFloat(r, "hours", 24, 1, 24*90)
	period := int64(queryFloat(r, "period", 900, 60, 86400))
	unit := int64(60)
	res := "1m"
	if hours > 24*30 {
		unit = 900
		res = "15m"
	}
	period = max(unit, period)
	period = max(period, int64(math.Ceil(hours*3600/1500/float64(unit)))*unit)
	now := time.Now().UTC()
	from := now.Add(-time.Duration(hours * float64(time.Hour)))
	cs := []Candle{}
	err := s.Store.Visit(r.Context(), res, "candle", a, from, now, 0, func(rec Record) error {
		var c Candle
		if err := json.Unmarshal(rec.Data, &c); err != nil {
			return err
		}
		if c.Open <= 0 {
			return nil
		}
		slot := rec.TS / period * period
		if len(cs) == 0 || cs[len(cs)-1].Time != slot {
			c.Time = slot
			cs = append(cs, c)
		} else {
			x := &cs[len(cs)-1]
			x.Close = c.Close
			x.High = math.Max(x.High, c.High)
			x.Low = math.Min(x.Low, c.Low)
		}
		return nil
	})
	if err != nil {
		problem(w, 500, "价格历史读取失败")
		return
	}
	jsonOut(w, map[string]any{"candles": cs, "period": period, "note": "五家有效现货报价中位数的观察区间 OHLC；空白为未采集"})
}
func (s *Server) derivativeHistory(w http.ResponseWriter, r *http.Request, a string) {
	hours := queryFloat(r, "hours", 1, .25, 24*90)
	now := time.Now().UTC()
	from := now.Add(-time.Duration(hours * float64(time.Hour)))
	res := "1m"
	unit := int64(60)
	if hours > 24 {
		res = "15m"
		unit = 900
	}
	stride := max(unit, int64(math.Ceil(hours*3600/120/float64(unit)))*unit)
	series := []map[string]any{}
	first := map[string]Derivative{}
	err := s.Store.Visit(r.Context(), res, "derivatives", a, from, now, stride, func(rec Record) error {
		var ds []Derivative
		if err := json.Unmarshal(rec.Data, &ds); err != nil {
			return err
		}
		for _, d := range ds {
			if !d.Valid || time.Unix(rec.TS, 0).Sub(d.At) > 90*time.Second {
				continue
			}
			if _, ok := first[d.Venue]; !ok {
				first[d.Venue] = d
			}
			series = append(series, map[string]any{"time": rec.TS, "venue": d.Venue, "oiUsdCents": d.OIUSDCents, "oiBase": d.OIBase, "funding": d.Funding, "basis": d.Mark - d.Index})
		}
		return nil
	})
	if err != nil {
		problem(w, 500, "合约历史读取失败")
		return
	}
	items := s.E.Derivatives(a)
	changes := map[string]any{}
	for _, d := range items {
		if old, ok := first[d.Venue]; ok && d.Valid {
			changes[d.Venue] = map[string]any{"oiBase": d.OIBase - old.OIBase, "oiUsdCents": d.OIUSDCents - old.OIUSDCents, "since": old.At}
		}
	}
	jsonOut(w, map[string]any{"items": items, "changes": changes, "series": series, "liquidations": s.E.Liquidations(a), "from": from, "to": now, "coverage": "仅已发布并接收到的清算；不同平台完整性及价格含义不同"})
}

func (s *Server) annotations(w http.ResponseWriter, r *http.Request, a string) {
	if r.Method == "GET" {
		rows, err := s.Store.Meta.Query("SELECT payload FROM annotations WHERE asset=? ORDER BY updated DESC LIMIT 20", a)
		if err != nil {
			problem(w, 500, "读取失败")
			return
		}
		defer rows.Close()
		out := []json.RawMessage{}
		for rows.Next() {
			var p string
			rows.Scan(&p)
			out = append(out, json.RawMessage(p))
		}
		jsonOut(w, out)
		return
	}
	if r.Method != "POST" && r.Method != "DELETE" {
		problem(w, 405, "method not allowed")
		return
	}
	var v struct {
		ID     string  `json:"id"`
		Asset  string  `json:"asset"`
		Entry  float64 `json:"entry"`
		Stop   float64 `json:"stop"`
		Target float64 `json:"target"`
		Side   string  `json:"side"`
	}
	if json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&v) != nil {
		problem(w, 400, "无效标注")
		return
	}
	if r.Method == "DELETE" {
		s.Store.Meta.Exec("DELETE FROM annotations WHERE id=?", v.ID)
		jsonOut(w, map[string]bool{"ok": true})
		return
	}
	if v.Asset != "BTC" && v.Asset != "ETH" || v.Entry <= 0 || v.Stop <= 0 || v.Target <= 0 || v.Entry == v.Stop || v.Side != "long" && v.Side != "short" {
		problem(w, 400, "请填写有效价格与方向")
		return
	}
	if v.Side == "long" && (v.Stop >= v.Entry || v.Target <= v.Entry) || v.Side == "short" && (v.Stop <= v.Entry || v.Target >= v.Entry) {
		problem(w, 400, "止盈止损方向与仓位方向不一致")
		return
	}
	if v.ID == "" {
		b := make([]byte, 8)
		rand.Read(b)
		v.ID = hex.EncodeToString(b)
	}
	b, _ := json.Marshal(v)
	_, err := s.Store.Meta.Exec("INSERT INTO annotations(id,asset,payload,updated) VALUES(?,?,?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload,updated=excluded.updated", v.ID, v.Asset, string(b), time.Now().Unix())
	if err != nil {
		problem(w, 500, "保存失败")
		return
	}
	s.Store.Meta.Exec("DELETE FROM annotations WHERE asset=? AND id NOT IN (SELECT id FROM annotations WHERE asset=? ORDER BY updated DESC LIMIT 20)", v.Asset, v.Asset)
	jsonOut(w, v)
}
