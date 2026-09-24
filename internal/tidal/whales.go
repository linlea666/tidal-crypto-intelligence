package tidal

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

var addressRE = regexp.MustCompile(`^0x[0-9a-fA-F]{40}$`)

type Candidate struct {
	Core    bool      `json:"-"`
	Address string    `json:"address"`
	Score   float64   `json:"score"`
	Last    time.Time `json:"last"`
	Pinned  bool      `json:"pinned"`
	Queried time.Time `json:"queried"`
}
type Whales struct {
	mu         sync.Mutex
	C          *Collector
	Candidates map[string]*Candidate
	budget     float64
	lastBudget time.Time
	marks      map[string]float64
	WSUsers    []string
}

func NewWhales(c *Collector) *Whales {
	return &Whales{C: c, Candidates: map[string]*Candidate{}, budget: 100, lastBudget: time.Now(), marks: map[string]float64{}}
}
func (w *Whales) budgetWait(ctx context.Context, cost float64) bool {
	for ctx.Err() == nil {
		w.mu.Lock()
		now := time.Now()
		w.budget = math.Min(800, w.budget+now.Sub(w.lastBudget).Seconds()*800/60)
		w.lastBudget = now
		if w.budget >= cost {
			w.budget -= cost
			w.mu.Unlock()
			return true
		}
		w.mu.Unlock()
		if !wait(ctx, 250*time.Millisecond) {
			return false
		}
	}
	return false
}
func (w *Whales) Add(address string, score float64, pinned bool) {
	address = strings.ToLower(address)
	if !addressRE.MatchString(address) || address == "0x0000000000000000000000000000000000000000" {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if c := w.Candidates[address]; c != nil {
		c.Score = math.Max(c.Score, score)
		c.Last = time.Now()
		c.Pinned = c.Pinned || pinned
		return
	}
	if len(w.Candidates) >= 1000 {
		var lowest *Candidate
		for _, c := range w.Candidates {
			if c.Pinned || c.Core {
				continue
			}
			if lowest == nil || c.Score < lowest.Score {
				lowest = c
			}
		}
		if lowest == nil || !pinned && lowest.Score >= score {
			return
		}
		delete(w.Candidates, lowest.Address)
		w.C.E.mu.Lock()
		delete(w.C.E.whales, lowest.Address)
		delete(w.C.E.whaleTimes, lowest.Address)
		delete(w.C.E.whaleCore, lowest.Address)
		w.C.E.mu.Unlock()
	}
	w.Candidates[address] = &Candidate{Address: address, Score: score, Last: time.Now(), Pinned: pinned}
}
func (w *Whales) Pin(address string, pin bool) error {
	if !addressRE.MatchString(address) || address == "0x0000000000000000000000000000000000000000" {
		return fmt.Errorf("无效地址")
	}
	if pin {
		w.mu.Lock()
		count := 0
		for _, c := range w.Candidates {
			if c.Pinned {
				count++
			}
		}
		w.mu.Unlock()
		if count >= 20 {
			return fmt.Errorf("最多关注20个地址")
		}
		w.Add(address, 1e12, true)
	} else {
		w.mu.Lock()
		if c := w.Candidates[strings.ToLower(address)]; c != nil {
			c.Pinned = false
		}
		w.mu.Unlock()
	}
	return nil
}
func (w *Whales) Info() map[string]any {
	w.mu.Lock()
	defer w.mu.Unlock()
	pins := []string{}
	for _, c := range w.Candidates {
		if c.Pinned {
			pins = append(pins, c.Address)
		}
	}
	return map[string]any{"candidates": len(w.Candidates), "limit": 1000, "coreLimit": 100, "websocketUsers": len(w.WSUsers), "pinned": pins, "scope": "Hyperliquid 原生 BTC/ETH 永续 · 已监控地址，不代表全市场", "refreshSeconds": 30}
}
func (w *Whales) Start(ctx context.Context) {
	go w.leaderboards(ctx)
	go w.poll(ctx)
	go w.C.reconnect(ctx, "hyperliquid:market", w.market)
	go w.C.reconnect(ctx, "hyperliquid:positions", w.positionsWS)
}
func (w *Whales) leaderboards(ctx context.Context) {
	for ctx.Err() == nil {
		err := w.leaderboard(ctx)
		w.C.E.SetHealth("hyperliquid:discovery", err == nil, "公开榜单仅用于候选发现，不作为仓位排名")
		if err != nil {
			w.C.E.SetHealth("hyperliquid:discovery", false, err.Error())
		}
		if !wait(ctx, time.Hour) {
			return
		}
	}
}
func (w *Whales) leaderboard(ctx context.Context) error {
	req, _ := http.NewRequestWithContext(ctx, "GET", "https://stats-data.hyperliquid.xyz/Mainnet/leaderboard", nil)
	resp, err := w.C.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("leaderboard HTTP %d", resp.StatusCode)
	}
	dec := json.NewDecoder(io.LimitReader(resp.Body, 80<<20))
	dec.UseNumber()
	if _, err = dec.Token(); err != nil {
		return err
	}
	for dec.More() {
		key, err := dec.Token()
		if err != nil {
			return err
		}
		if key != "leaderboardRows" {
			var discard json.RawMessage
			if err = dec.Decode(&discard); err != nil {
				return err
			}
			continue
		}
		if _, err = dec.Token(); err != nil {
			return err
		}
		for dec.More() {
			var row map[string]any
			if err = dec.Decode(&row); err != nil {
				return err
			}
			w.Add(str(row["ethAddress"]), number(row["accountValue"]), false)
		}
		if _, err = dec.Token(); err != nil {
			return err
		}
	}
	return nil
}
func (w *Whales) query(ctx context.Context, a string) error {
	if !w.budgetWait(ctx, 2) {
		return ctx.Err()
	}
	d, err := w.C.get(ctx, "https://api.hyperliquid.xyz/info", map[string]any{"type": "clearinghouseState", "user": a})
	if err != nil {
		return err
	}
	w.parse(a, obj(d))
	w.mu.Lock()
	if c := w.Candidates[a]; c != nil {
		c.Queried = time.Now()
	}
	w.mu.Unlock()
	return nil
}
func (w *Whales) parse(address string, m map[string]any) {
	fx, ok := w.C.E.Rate("USDC")
	if !ok {
		return
	}
	at := time.UnixMilli(num(m["time"]))
	if at.IsZero() || time.Since(at) > 90*time.Second || at.After(time.Now().Add(10*time.Second)) {
		return
	}
	out := []Whale{}
	e := w.C.E
	e.mu.Lock()
	old := e.whales[address]
	e.mu.Unlock()
	maxScore := 0.0
	for _, x := range arr(m["assetPositions"]) {
		p := obj(obj(x)["position"])
		a := str(p["coin"])
		if a != "BTC" && a != "ETH" {
			continue
		}
		size := number(p["szi"])
		if size == 0 {
			continue
		}
		side := "long"
		if size < 0 {
			side = "short"
		}
		lev := obj(p["leverage"])
		entry := str(p["entryPx"])
		if !finite(number(entry)) || number(entry) <= 0 {
			continue
		}
		mark := number(p["positionValue"]) / math.Abs(size)
		if !finite(mark) || mark <= 0 {
			continue
		}

		var lp *string
		var distance *float64
		if p["liquidationPx"] != nil && number(p["liquidationPx"]) > 0 {
			s := str(p["liquidationPx"])
			lp = &s
			d := math.Abs(number(s)-mark) / mark * 100
			distance = &d
		}
		change := "0"
		first := at
		for _, previous := range old {
			if previous.Asset == a {
				change = fmt.Sprintf("%.8f", size-number(previous.Size))
				first = previous.FirstSeen
			}
		}
		value := number(p["positionValue"]) * fx.Value
		maxScore = math.Max(maxScore, value)
		out = append(out, Whale{Address: address, Asset: a, Side: side, Size: str(p["szi"]), Entry: entry, USDCents: cents(value), Leverage: int(number(lev["value"])), Margin: str(lev["type"]), Liquidation: lp, Distance: distance, UnrealizedCents: cents(number(p["unrealizedPnl"]) * fx.Value), Mark: mark * fx.Value, At: at, ChangeSize: change, FirstSeen: first, Valid: true, Quote: "USDC", Rate: fx.USD})
	}
	e.mu.Lock()
	if previous := e.whaleTimes[address]; previous.After(at) {
		e.mu.Unlock()
		return
	}
	e.whaleTimes[address] = at
	e.whales[address] = out
	e.mu.Unlock()
	w.mu.Lock()
	if c := w.Candidates[address]; c != nil {
		c.Score = math.Max(c.Score, maxScore)
		c.Queried = time.Now()
	}
	w.mu.Unlock()
}
func (w *Whales) core() []string {
	w.C.E.mu.RLock()
	ws := []Whale{}
	for _, ps := range w.C.E.whales {
		ws = append(ws, ps...)
	}
	w.C.E.mu.RUnlock()
	sort.Slice(ws, func(i, j int) bool { return ws[i].USDCents > ws[j].USDCents })
	out := []string{}
	seen := map[string]bool{}
	counts := map[string]int{}
	for _, p := range ws {
		if counts[p.Asset] >= 50 {
			continue
		}
		counts[p.Asset]++
		if !seen[p.Address] {
			seen[p.Address] = true
			out = append(out, p.Address)
		}
	}
	w.mu.Lock()
	pins := []string{}
	for _, c := range w.Candidates {
		if c.Pinned {
			pins = append(pins, c.Address)
		}
	}
	w.mu.Unlock()
	sort.Strings(pins)
	for _, a := range pins {
		if !seen[a] {
			out = append([]string{a}, out...)
			seen[a] = true
		}
	}
	if len(out) > 100 {
		out = out[:100]
	}
	w.C.E.mu.Lock()
	w.C.E.whaleCore = map[string]bool{}
	for _, a := range out {
		w.C.E.whaleCore[a] = true
	}
	w.C.E.mu.Unlock()
	w.mu.Lock()
	for _, c := range w.Candidates {
		c.Core = false
	}
	for _, a := range out {
		if c := w.Candidates[a]; c != nil {
			c.Core = true
		}
	}
	w.mu.Unlock()
	return out
}
func (w *Whales) poll(ctx context.Context) {
	// Core refresh and discovery have separate, bounded workers and share the weight budget.
	go func() {
		for ctx.Err() == nil {
			started := time.Now()
			core := w.core()
			jobs := make(chan string)
			var workers sync.WaitGroup
			var errors sync.Map
			for n := 0; n < 3; n++ {
				workers.Add(1)
				go func() {
					defer workers.Done()
					for a := range jobs {
						if err := w.query(ctx, a); err != nil {
							errors.Store(a, err.Error())
						}
					}
				}()
			}
			for _, a := range core {
				select {
				case jobs <- a:
				case <-ctx.Done():
					close(jobs)
					workers.Wait()
					return
				}
			}
			close(jobs)
			workers.Wait()
			failed := false
			errors.Range(func(k, v any) bool {
				failed = true
				w.C.E.SetHealth("hyperliquid:poll", false, v.(string))
				return false
			})
			if len(core) > 0 && !failed {
				w.C.E.SetHealth("hyperliquid:poll", true, "30秒核心刷新；仅聚合90秒内成功更新的持仓")
			}
			if !wait(ctx, max(time.Second, 30*time.Second-time.Since(started))) {
				return
			}
		}
	}()
	for ctx.Err() == nil {
		w.mu.Lock()
		candidates := []Candidate{}
		for _, c := range w.Candidates {
			if time.Since(c.Queried) > 30*time.Minute {
				candidates = append(candidates, *c)
			}
		}
		w.mu.Unlock()
		sort.Slice(candidates, func(i, j int) bool {
			if candidates[i].Queried.Equal(candidates[j].Queried) {
				return candidates[i].Score > candidates[j].Score
			}
			return candidates[i].Queried.Before(candidates[j].Queried)
		})
		if len(candidates) > 0 {
			a := candidates[0].Address
			err := w.query(ctx, a)
			if err != nil {
				w.mu.Lock()
				if c := w.Candidates[a]; c != nil {
					c.Queried = time.Now().Add(-25 * time.Minute)
				}
				w.mu.Unlock()
				w.C.E.SetHealth("hyperliquid:discovery-query", false, err.Error())
				if !wait(ctx, 2*time.Second) {
					return
				}
			}
		}
		if !wait(ctx, 500*time.Millisecond) {
			return
		}
	}
}

func (w *Whales) market(ctx context.Context) error {
	conn, child, cancel, err := w.C.connect(ctx, "wss://api.hyperliquid.xyz/ws", []any{map[string]any{"method": "subscribe", "subscription": map[string]any{"type": "trades", "coin": "BTC"}}, map[string]any{"method": "subscribe", "subscription": map[string]any{"type": "trades", "coin": "ETH"}}, map[string]any{"method": "subscribe", "subscription": map[string]any{"type": "activeAssetCtx", "coin": "BTC"}}, map[string]any{"method": "subscribe", "subscription": map[string]any{"type": "activeAssetCtx", "coin": "ETH"}}}, map[string]any{"method": "ping"})
	if err != nil {
		return err
	}
	defer cancel()
	defer conn.CloseNow()
	for child.Err() == nil {
		m, err := read(child, conn)
		if err != nil {
			return err
		}
		switch m["channel"] {
		case "trades":
			for _, x := range arr(m["data"]) {
				d := obj(x)
				for _, a := range arr(d["users"]) {
					w.Add(str(a), number(d["px"])*number(d["sz"])*10, false)
				}
			}
		case "activeAssetCtx":
			d := obj(m["data"])
			v := obj(d["ctx"])
			w.mu.Lock()
			w.marks[str(d["coin"])] = number(v["markPx"])
			w.mu.Unlock()
		}
		w.C.E.SetHealth("hyperliquid:market", true, "公开成交地址发现与标记价格")
	}
	return child.Err()
}
func (w *Whales) positionsWS(ctx context.Context) error {
	core := w.core()
	if len(core) == 0 {
		if !wait(ctx, 10*time.Second) {
			return ctx.Err()
		}
		return fmt.Errorf("候选地址发现中")
	}
	if len(core) > 10 {
		core = core[:10]
	}
	w.mu.Lock()
	w.WSUsers = append([]string{}, core...)
	w.mu.Unlock()
	subs := []any{}
	for _, a := range core {
		subs = append(subs, map[string]any{"method": "subscribe", "subscription": map[string]any{"type": "clearinghouseState", "user": a}})
	}
	rotation, cancelRotation := context.WithTimeout(ctx, 5*time.Minute)
	defer cancelRotation()
	conn, child, cancel, err := w.C.connect(rotation, "wss://api.hyperliquid.xyz/ws", subs, map[string]any{"method": "ping"})
	if err != nil {
		return err
	}
	defer cancel()
	defer conn.CloseNow()
	for child.Err() == nil {
		m, err := read(child, conn)
		if err != nil {
			return err
		}
		if m["channel"] == "clearinghouseState" {
			d := obj(m["data"])
			w.parse(str(d["user"]), obj(d["clearinghouseState"]))
			w.C.E.SetHealth("hyperliquid:positions", true, "最多10个公开地址实时订阅")
		}
	}
	return child.Err()
}
