package datahub

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/coder/websocket"
	"github.com/shopspring/decimal"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type radarTrade struct {
	Coin  string   `json:"coin"`
	Px    string   `json:"px"`
	Sz    string   `json:"sz"`
	Time  int64    `json:"time"`
	TID   int64    `json:"tid"`
	Users []string `json:"users"`
}
type radarMinute struct {
	At    int64
	Value decimal.Decimal
}
type radarCandidate struct {
	First       time.Time
	Last        time.Time
	Queried     time.Time
	Minutes     [16]radarMinute
	Pending     bool
	Established bool
	Active      bool
}
type radarWeight struct {
	At time.Time
	N  int
	ID uint64
}
type radarRuntime struct {
	receipt    atomic.Int64
	mu         sync.Mutex
	candidates map[string]*radarCandidate
	seen       map[string]time.Time
	queue      chan string
	weights    []radarWeight
	nextWeight uint64
	retry      time.Time
	health     radarHealth
	client     *http.Client
	url        string
	ws         string
}

func newRadarRuntime() *radarRuntime {
	return &radarRuntime{candidates: map[string]*radarCandidate{}, seen: map[string]time.Time{}, queue: make(chan string, 128), client: &http.Client{Timeout: 15 * time.Second}, url: "https://api.hyperliquid.xyz/info", ws: "wss://api.hyperliquid.xyz/ws"}
}
func (h *Hub) radarError(err error, dropped bool) {
	if h.radar == nil || err == nil {
		return
	}
	r := h.radar
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now().UTC()
	r.health.LastError = err.Error()
	r.health.LastErrorAt = &now
	r.health.Gaps++
	if dropped {
		r.health.Dropped++
	}
}
func (r *radarRuntime) weightLocked(now time.Time) int {
	n := 0
	live := r.weights[:0]
	for _, v := range r.weights {
		if v.At.After(now.Add(-time.Minute)) {
			live = append(live, v)
			n += v.N
		}
	}
	r.weights = live
	r.health.Weight = n
	return n
}
func (r *radarRuntime) reserve(ctx context.Context, cost int) error {
	_, err := r.reserveID(ctx, cost)
	return err
}
func (r *radarRuntime) reserveID(ctx context.Context, cost int) (uint64, error) {
	for {
		now := time.Now()
		r.mu.Lock()
		if r.weightLocked(now)+cost <= 600 && !now.Before(r.retry) {
			r.nextWeight++
			id := r.nextWeight
			r.weights = append(r.weights, radarWeight{At: now, N: cost, ID: id})
			r.mu.Unlock()
			return id, nil
		}
		r.mu.Unlock()
		if !sleep(ctx, 250*time.Millisecond) {
			return 0, ctx.Err()
		}
	}
}
func (r *radarRuntime) settleWeight(id uint64, actual int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.weights {
		if r.weights[i].ID == id && actual < r.weights[i].N {
			r.weights[i].N = actual
			break
		}
	}
}
func (r *radarRuntime) query(ctx context.Context, body any, cost int, out any) error {
	reservation, err := r.reserveID(ctx, cost)
	if err != nil {
		return err
	}
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", r.url, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode == 429 {
		r.mu.Lock()
		r.retry = time.Now().Add(time.Minute)
		r.mu.Unlock()
	}
	if res.StatusCode != 200 {
		return fmt.Errorf("Hyperliquid HTTP %d", res.StatusCode)
	}
	b, err = io.ReadAll(io.LimitReader(res.Body, (2<<20)+1))
	if err != nil {
		return err
	}
	if len(b) > 2<<20 {
		return errors.New("Hyperliquid响应超过2MiB")
	}
	if err = json.Unmarshal(b, out); err != nil {
		return err
	}
	if cost == 120 {
		count := 0
		switch v := out.(type) {
		case *[]radarFill:
			count = len(*v)
		case *[]radarLedger:
			count = len(*v)
		case *[]radarTrade:
			count = len(*v)
		}
		r.settleWeight(reservation, 20+(count+19)/20)
	}
	return nil
}
func (l *radarLedger) UnmarshalJSON(b []byte) error {
	var v struct {
		Time  int64                      `json:"time"`
		Hash  string                     `json:"hash"`
		Delta map[string]json.RawMessage `json:"delta"`
	}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	l.Time = v.Time
	l.Hash = v.Hash
	for k, p := range map[string]*string{"type": &l.Delta.Type, "usdc": &l.Delta.USDC, "user": &l.Delta.User, "destination": &l.Delta.Destination} {
		if raw, ok := v.Delta[k]; ok {
			if err := json.Unmarshal(raw, p); err != nil {
				if k != "usdc" {
					return err
				}
				*p = string(raw)
			}
		}
	}
	return nil
}
func (h *Hub) radarDiscover(t radarTrade, now time.Time) error {
	if !ValidAsset(t.Coin) || len(t.Users) != 2 {
		return errors.New("无效公开成交")
	}
	px, ok := radarNumber(t.Px)
	sz, sok := radarNumber(t.Sz)
	if !ok || !sok || !px.IsPositive() || !sz.IsPositive() || t.TID < 0 || px.Mul(sz).GreaterThan(decimal.NewFromInt(1000000000000)) {
		return errors.New("无效成交金额")
	}
	at := radarTime(t.Time)
	if now.Sub(at) > time.Minute || at.After(now.Add(5*time.Second)) {
		return nil
	}
	rate, _, fx := h.Rate("USDC", now)
	fxRate, rateOK := radarNumber(rate)
	if !fx || !rateOK || !fxRate.IsPositive() {
		return errors.New("雷达美元换汇缺失，候选判断暂停")
	}
	r := h.radar
	r.mu.Lock()
	defer r.mu.Unlock()
	key := fmt.Sprintf("%s/%d/%d", t.Coin, t.Time, t.TID)
	if _, ok := r.seen[key]; ok {
		return nil
	}
	if len(r.seen) >= 20000 {
		for k, ts := range r.seen {
			if now.Sub(ts) > time.Minute {
				delete(r.seen, k)
			}
		}
		if len(r.seen) >= 20000 {
			return errors.New("成交去重窗口容量已满")
		}
	}
	r.seen[key] = now
	for _, raw := range t.Users {
		address := radarAddress(raw)
		if !publicAddress.MatchString(address) || address == "0x0000000000000000000000000000000000000000" {
			continue
		}
		c := r.candidates[address]
		if c == nil {
			if len(r.candidates) >= 5000 {
				for k, v := range r.candidates {
					if !v.Pending && now.Sub(v.Last) > 20*time.Minute {
						delete(r.candidates, k)
					}
				}
				if len(r.candidates) >= 5000 {
					return errors.New("地址候选容量已满")
				}
			}
			c = &radarCandidate{First: now}
			r.candidates[address] = c
		}
		c.Last = now
		minute := t.Time / 60000
		slot := minute % 16
		if c.Minutes[slot].At != minute {
			c.Minutes[slot] = radarMinute{At: minute}
		}
		c.Minutes[slot].Value = c.Minutes[slot].Value.Add(px.Mul(sz).Mul(fxRate))
		total := decimal.Zero
		for _, m := range c.Minutes {
			if m.At >= minute-14 {
				total = total.Add(m.Value)
			}
		}
		if total.GreaterThanOrEqual(decimal.NewFromInt(250000)) && !c.Pending && now.Sub(c.Queried) >= radarQueryInterval(c) {
			select {
			case r.queue <- address:
				c.Pending = true
			default:
				return errors.New("地址核验队列已满")
			}
		}
	}
	r.health.LastMessage = &now
	r.health.Candidates = len(r.candidates)
	r.health.Queued = len(r.queue)
	return nil
}
func (h *Hub) radarFetch(ctx context.Context, address string, now time.Time) error {
	r := h.radar
	var w RadarWallet
	err := radarLoad(ctx, h.Store.radar.db, "wallet", address, &w)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	initial := err == sql.ErrNoRows
	if initial {
		first := now
		r.mu.Lock()
		if c := r.candidates[address]; c != nil {
			first = c.First
		}
		r.mu.Unlock()
		w = RadarWallet{Address: address, FirstSeen: first, HistoryFrom: now.Add(-30 * 24 * time.Hour), Ledger: []radarLedger{}, Age: "unknown"}
	}
	from := now.Add(-30 * time.Minute)
	full := initial || now.Sub(w.HistoryThrough) > 24*time.Hour
	if full {
		from = now.Add(-30 * 24 * time.Hour)
	}
	// Obtain the current position before optional deep history consumes quota.
	// Five full pages alone cost 600 weight. A timed-out history lookup must
	// degrade age evidence, not make the mandatory account query unreachable.
	var a radarAccount
	if err = r.query(ctx, map[string]any{"type": "clearinghouseState", "user": address}, 2, &a); err != nil {
		return err
	}
	historyCtx, stopHistory := context.WithTimeout(ctx, 20*time.Second)
	fills := []radarFill{}
	complete := true
	cursor := from.UnixMilli()
	seen := map[string]radarFill{}
	for page := 0; page < 5; page++ {
		var batch []radarFill
		err = r.query(historyCtx, map[string]any{"type": "userFillsByTime", "user": address, "startTime": cursor, "endTime": now.UnixMilli(), "aggregateByTime": false}, 120, &batch)
		if err != nil {
			complete = false
			break
		}
		if len(batch) > 2000 {
			stopHistory()
			return errors.New("用户成交条数超过契约")
		}
		last := cursor
		added := 0
		for _, f := range batch {
			if f.Time < cursor || f.Time > now.UnixMilli() {
				complete = false
				continue
			}
			if f.Time > last {
				last = f.Time
			}
			k := radarFillID(f)
			if prior, exists := seen[k]; exists && prior != f {
				stopHistory()
				return errors.New("同次历史查询成交身份冲突")
			} else if !exists {
				seen[k] = f
				fills = append(fills, f)
				added++
				t := radarTime(f.Time)
				if w.Earliest == nil || t.Before(*w.Earliest) {
					w.Earliest = &t
				}
			}
		}
		if len(batch) < 2000 {
			break
		}
		if page == 4 || last <= cursor || added == 0 {
			complete = false
			break
		}
		cursor = last // inclusive replay avoids losing same-millisecond fills
	}
	stopHistory()
	if len(fills) == 0 && err != nil {
		return err
	}
	newOpening := false
	for _, f := range fills {
		if ValidAsset(f.Coin) && f.Time > w.LedgerThrough.UnixMilli() {
			before, after, _, fe := radarTransition(f)
			newOpening = newOpening || (fe == nil && (before.IsZero() || (!after.IsZero() && before.Sign() != after.Sign())))
		}
	}
	if full || newOpening || now.Sub(w.LedgerThrough) > 5*time.Minute {
		var ledger []radarLedger
		ledgerCtx, stopLedger := context.WithTimeout(ctx, 8*time.Second)
		le := r.query(ledgerCtx, map[string]any{"type": "userNonFundingLedgerUpdates", "user": address, "startTime": w.HistoryFrom.UnixMilli(), "endTime": now.UnixMilli()}, 120, &ledger)
		stopLedger()
		ledgerComplete := le == nil && len(ledger) < 2000
		if le != nil || len(ledger) >= 2000 {
			complete = false
		}
		sort.SliceStable(ledger, func(i, j int) bool { return ledger[i].Time < ledger[j].Time })
		unique := ledger[:0]
		ledgerSeen := map[string]bool{}
		for _, l := range ledger {
			if l.Time < w.HistoryFrom.UnixMilli() || l.Time > now.UnixMilli() {
				complete = false
				ledgerComplete = false
				continue
			}
			key := fmt.Sprintf("%d/%s", l.Time, l.Hash)
			if l.Hash == "" || ledgerSeen[key] {
				complete = false
				ledgerComplete = false
				continue
			}
			ledgerSeen[key] = true
			unique = append(unique, l)
		}
		ledger = unique
		if len(ledger) > 200 {
			complete = false
			ledgerComplete = false
			ledger = ledger[len(ledger)-200:]
		}
		w.LedgerComplete = ledgerComplete
		if le == nil {
			w.Ledger = ledger
			w.LedgerThrough = now
			for _, l := range ledger {
				t := radarTime(l.Time)
				if w.Earliest == nil || t.Before(*w.Earliest) {
					w.Earliest = &t
				}
			}
		}
		var role struct {
			Role string `json:"role"`
			Data struct {
				Master string `json:"master"`
			} `json:"data"`
		}
		if full || w.Role == "" {
			roleCtx, stopRole := context.WithTimeout(ctx, 5*time.Second)
			if re := r.query(roleCtx, map[string]any{"type": "userRole", "user": address}, 60, &role); re == nil {
				w.Role = role.Role
				w.Master = role.Data.Master
			}
			stopRole()
		}
		w.HistoryComplete = complete && (full || w.HistoryComplete)
		if full {
			w.HistoryFrom = from
			w.HistoryThrough = now
		}
		w.HistoryNote = "仅核验请求的30天范围；API最多最近10000笔成交，无法证明全链创建时间"
	} else if !complete {
		w.HistoryComplete = false
		w.HistoryNote = "增量成交查询缺口，历史完整度已降级"
	}
	if err = h.radarApply(ctx, w, fills, a, time.Now().UTC()); err != nil {
		return err
	}
	r.mu.Lock()
	t := time.Now().UTC()
	r.health.LastVerified = &t
	if c := r.candidates[address]; c != nil {
		radarAge(&w, t)
		c.Established = w.Age == "established"
		c.Active = !c.Established && len(a.Positions) > 0
	}
	r.mu.Unlock()
	return nil
}
func (h *Hub) radarWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case a := <-h.radar.queue:
			a = h.radar.prioritize(a)
			step, cancel := context.WithTimeout(ctx, 55*time.Second)
			err := h.radarFetch(step, a, time.Now().UTC())
			cancel()
			h.radarError(err, false)
			h.radar.mu.Lock()
			if c := h.radar.candidates[a]; c != nil {
				c.Pending = false
				c.Queried = time.Now()
			}
			h.radar.mu.Unlock()
		}
	}
}
func (h *Hub) radarStream(ctx context.Context, users []string) error {
	c, _, err := websocket.Dial(ctx, h.radar.ws, nil)
	if err != nil {
		return err
	}
	defer c.CloseNow()
	c.SetReadLimit(2 << 20)
	child, cancel := context.WithCancel(ctx)
	defer cancel()
	if len(users) == 0 {
		for _, coin := range Assets() {
			if err = c.Write(child, websocket.MessageText, []byte(`{"method":"subscribe","subscription":{"type":"trades","coin":"`+coin+`"}}`)); err != nil {
				return err
			}
		}
	} else {
		for _, a := range users {
			b, _ := json.Marshal(map[string]any{"method": "subscribe", "subscription": map[string]any{"type": "userFills", "user": a}})
			if err = c.Write(child, websocket.MessageText, b); err != nil {
				return err
			}
		}
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		tick := time.NewTicker(20 * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-child.Done():
				return
			case <-tick.C:
				send, stop := context.WithTimeout(child, 3*time.Second)
				e := c.Write(send, websocket.MessageText, []byte(`{"method":"ping"}`))
				stop()
				if e != nil {
					cancel()
					return
				}
			}
		}
	}()
	defer func() { cancel(); <-done }()
	if len(users) == 0 {
		h.radar.mu.Lock()
		h.radar.health.Connected = true
		if h.radar.health.GapFrom != nil && h.radar.health.GapThrough == nil {
			now := time.Now().UTC()
			h.radar.health.GapThrough = &now
			// Reconcile known active accounts over the overlapping REST window.
			// Unknown addresses in this interval remain an explicit coverage gap.
			for address, candidate := range h.radar.candidates {
				if !candidate.Active || candidate.Pending {
					continue
				}
				select {
				case h.radar.queue <- address:
					candidate.Pending = true
				default:
					h.radar.health.Dropped++
				}
			}
		}
		h.radar.mu.Unlock()
		defer func() { h.radar.mu.Lock(); h.radar.health.Connected = false; h.radar.mu.Unlock() }()
	}
	for {
		read, stop := context.WithTimeout(child, 45*time.Second)
		_, b, e := c.Read(read)
		stop()
		if e != nil {
			return e
		}
		var msg struct {
			Channel string          `json:"channel"`
			Data    json.RawMessage `json:"data"`
		}
		if err = json.Unmarshal(b, &msg); err != nil {
			return err
		}
		if msg.Channel == "trades" {
			var trades []radarTrade
			if err = json.Unmarshal(msg.Data, &trades); err != nil {
				return err
			}
			for _, t := range trades {
				if e = h.radarDiscover(t, time.Now().UTC()); e != nil {
					h.radarError(e, true)
				}
			}
		}
		if msg.Channel == "userFills" {
			var m struct {
				User string `json:"user"`
			}
			if e = json.Unmarshal(msg.Data, &m); e != nil {
				return e
			}
			a := strings.ToLower(m.User)
			h.radar.mu.Lock()
			candidate := h.radar.candidates[a]
			if candidate != nil && !candidate.Pending && time.Since(candidate.Queried) >= radarQueryInterval(candidate) {
				select {
				case h.radar.queue <- a:
					candidate.Pending = true
				default:
				}
			}
			h.radar.mu.Unlock()
		}
	}
}
func (h *Hub) radarCollector(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); h.radarWorker(ctx) }()
	}
	wg.Add(1)
	go func() { defer wg.Done(); h.radarUserStreams(ctx) }()
	defer wg.Wait()
	backoff := time.Second
	for ctx.Err() == nil {
		started := time.Now()
		err := h.radarStream(ctx, nil)
		if ctx.Err() != nil {
			return
		}
		h.radarError(err, false)
		h.radar.mu.Lock()
		if h.radar.health.GapFrom == nil || h.radar.health.GapThrough != nil {
			now := time.Now().UTC()
			h.radar.health.GapFrom = &now
			h.radar.health.GapThrough = nil
		}
		h.radar.mu.Unlock()
		if time.Since(started) > time.Minute {
			backoff = time.Second
		}
		if !sleep(ctx, backoff) {
			return
		}
		backoff = min(30*time.Second, backoff*2)
	}
}
func (h *Hub) radarUserStreams(ctx context.Context) {
	for ctx.Err() == nil {
		h.radar.mu.Lock()
		addresses := []string{}
		for a, c := range h.radar.candidates {
			if c.Active && !c.Queried.IsZero() && time.Since(c.Last) < 15*time.Minute {
				addresses = append(addresses, a)
			}
		}
		sort.Slice(addresses, func(i, j int) bool {
			return h.radar.candidates[addresses[i]].Last.After(h.radar.candidates[addresses[j]].Last)
		})
		if len(addresses) > 8 {
			addresses = addresses[:8]
		}
		h.radar.health.WSUsers = len(addresses)
		h.radar.mu.Unlock()
		if len(addresses) == 0 {
			if !sleep(ctx, 10*time.Second) {
				return
			}
			continue
		}
		step, cancel := context.WithTimeout(ctx, time.Minute)
		e := h.radarStream(step, addresses)
		cancel()
		if e != nil && !errors.Is(e, context.DeadlineExceeded) && ctx.Err() == nil {
			h.radarError(e, false)
		}
		if !sleep(ctx, 3*time.Second) {
			return
		}
	}
}

func radarQueryInterval(c *radarCandidate) time.Duration {
	if c.Established {
		return 5 * time.Minute
	}
	return 20 * time.Second
}
func (r *radarRuntime) prioritize(first string) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	all := []string{first}
	draining := true
	for draining {
		// A second worker can consume after len(queue) is read. Never wait on an
		// empty channel while holding the mutex that worker needs to proceed.
		select {
		case next := <-r.queue:
			all = append(all, next)
		default:
			draining = false
		}
	}
	sort.SliceStable(all, func(i, j int) bool {
		a, b := r.candidates[all[i]], r.candidates[all[j]]
		if a == nil || b == nil {
			return a != nil
		}
		if a.Established != b.Established {
			return !a.Established
		}
		if a.Queried.IsZero() != b.Queried.IsZero() {
			return a.Queried.IsZero()
		}
		return a.Queried.Before(b.Queried)
	})
	for _, a := range all[1:] {
		r.queue <- a
	}
	return all[0]
}
