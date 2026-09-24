package tidal

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/shopspring/decimal"
)

type Collector struct {
	E        *Engine
	HTTP     *http.Client
	cbMu     sync.Mutex
	cbHigh   map[string]int64
	cbCursor map[string]int64
	cbWake   chan struct{}
}

func NewCollector(e *Engine) *Collector {
	return &Collector{E: e, cbHigh: map[string]int64{}, cbCursor: map[string]int64{}, cbWake: make(chan struct{}, 1), HTTP: &http.Client{Timeout: 20 * time.Second, Transport: &http.Transport{MaxIdleConns: 100, MaxIdleConnsPerHost: 12, IdleConnTimeout: 90 * time.Second}}}
}
func (c *Collector) get(ctx context.Context, u string, body any) (any, error) {
	var r io.Reader
	method := "GET"
	if body != nil {
		bs, _ := json.Marshal(body)
		r = bytes.NewReader(bs)
		method = "POST"
	}
	req, err := http.NewRequestWithContext(ctx, method, u, r)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Tidal/1.0 public-market-data")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("HTTP %d from %s", resp.StatusCode, req.URL.Host)
	}
	d := json.NewDecoder(io.LimitReader(resp.Body, 64<<20))
	d.UseNumber()
	var result any
	err = d.Decode(&result)
	return result, err
}
func wait(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
func (c *Collector) Start(ctx context.Context) {
	go c.fx(ctx)
	for _, v := range []string{"okx", "binance", "coinbase", "kraken", "bybit"} {
		venue := v
		go c.reconnect(ctx, "spot:"+venue, func(ctx context.Context) error { return c.spot(ctx, venue) })
	}
	go c.okxDeep(ctx)
	go c.derivativePoll(ctx)
	for _, v := range []string{"okx", "binance", "bybit"} {
		venue := v
		go c.reconnect(ctx, "perp:"+venue, func(ctx context.Context) error { return c.perp(ctx, venue) })
	}
}
func (c *Collector) reconnect(ctx context.Context, key string, fn func(context.Context) error) {
	delay := time.Second
	gapFrom := time.Now()
	for ctx.Err() == nil {
		start := time.Now()
		parts := strings.Split(key, ":")
		if len(parts) == 2 && (parts[0] == "spot" || parts[0] == "perp") {
			c.E.MarkGap(parts[1], parts[0], gapFrom, start)
		}
		err := fn(ctx)
		if ctx.Err() != nil {
			return
		}
		msg := "connection ended"
		if err != nil {
			msg = err.Error()
		}
		c.E.SetHealth(key, false, msg)
		gapFrom = time.Now()
		if len(parts) == 2 && (parts[0] == "spot" || parts[0] == "perp") {
			c.E.MarkGap(parts[1], parts[0], gapFrom, gapFrom)
		}
		if strings.HasPrefix(key, "spot:") {
			for _, b := range c.E.Books {
				if b.Instrument.Venue == strings.TrimPrefix(key, "spot:") {
					b.Invalidate("连接中断，重建中")
				}
			}
		}
		slog.Warn("feed reconnect", "feed", key, "error", msg)
		if time.Since(start) > time.Minute {
			delay = time.Second
		}
		if !wait(ctx, delay) {
			return
		}
		if delay < 30*time.Second {
			delay *= 2
		}
	}
}
func (c *Collector) connect(ctx context.Context, u string, subs []any, ping any) (*websocket.Conn, context.Context, context.CancelFunc, error) {
	child, cancel := context.WithCancel(ctx)
	conn, _, err := websocket.Dial(child, u, &websocket.DialOptions{HTTPClient: &http.Client{Timeout: 20 * time.Second}})
	if err != nil {
		cancel()
		return nil, child, cancel, err
	}
	conn.SetReadLimit(32 << 20)
	for _, s := range subs {
		if err = send(child, conn, s); err != nil {
			conn.CloseNow()
			cancel()
			return nil, child, cancel, err
		}
	}
	if ping != nil {
		go func() {
			for wait(child, 15*time.Second) {
				if send(child, conn, ping) != nil {
					cancel()
					return
				}
			}
		}()
	}
	return conn, child, cancel, nil
}
func send(ctx context.Context, conn *websocket.Conn, v any) error {
	var b []byte
	if s, ok := v.(string); ok {
		b = []byte(s)
	} else {
		b, _ = json.Marshal(v)
	}
	wctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	return conn.Write(wctx, websocket.MessageText, b)
}
func read(ctx context.Context, conn *websocket.Conn) (map[string]any, error) {
	rctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	_, data, err := conn.Read(rctx)
	if err != nil {
		return nil, err
	}
	if string(data) == "pong" {
		return map[string]any{}, nil
	}
	var m map[string]any
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	err = d.Decode(&m)
	return m, err
}
func (c *Collector) fx(ctx context.Context) {
	for ctx.Err() == nil {
		d, err := c.get(ctx, "https://api.kraken.com/0/public/Ticker?pair=USDTUSD,USDCUSD", nil)
		if err == nil {
			result := obj(obj(d)["result"])
			for name, v := range result {
				m := obj(v)
				b, a := arr(m["b"]), arr(m["a"])
				if len(b) == 0 || len(a) == 0 {
					continue
				}
				q := "USDT"
				if strings.Contains(name, "USDC") {
					q = "USDC"
				}
				c.E.SetRate(q, fmt.Sprintf("%.10f", (number(b[0])+number(a[0]))/2), time.Now().UTC())
			}
			c.E.SetHealth("fx", len(result) == 2, "Kraken USD 买卖中间价")
		} else {
			c.E.SetHealth("fx", false, err.Error())
		}
		if !wait(ctx, 5*time.Second) {
			return
		}
	}
}
func (c *Collector) spot(ctx context.Context, v string) error {
	instruments := []Instrument{}
	books := map[string]*Book{}
	for _, i := range Instruments() {
		if i.Venue == v {
			instruments = append(instruments, i)
			books[i.Symbol] = c.E.Books[i.Key()]
		}
	}
	subs := []any{}
	var ping any
	wsURL := ""
	switch v {
	case "okx":
		wsURL = "wss://ws.okx.com:8443/ws/v5/public"
		args := []any{}
		for _, i := range instruments {
			for _, ch := range []string{"books", "trades"} {
				args = append(args, map[string]any{"channel": ch, "instId": i.Symbol})
			}
		}
		subs = append(subs, map[string]any{"op": "subscribe", "args": args})
		ping = "ping"
	case "binance":
		streams := []string{}
		for _, i := range instruments {
			for _, ch := range []string{"depth@100ms", "aggTrade"} {
				streams = append(streams, strings.ToLower(i.Symbol)+"@"+ch)
			}
		}
		wsURL = "wss://stream.binance.com:9443/stream?streams=" + strings.Join(streams, "/")
	case "coinbase":
		wsURL = "wss://ws-feed.exchange.coinbase.com"
		symbols := []string{}
		for _, i := range instruments {
			symbols = append(symbols, i.Symbol)
		}
		subs = append(subs, map[string]any{"type": "subscribe", "product_ids": symbols, "channels": []string{"level2_batch", "matches", "heartbeat"}})
	case "kraken":
		wsURL = "wss://ws.kraken.com/v2"
		symbols := []string{}
		for _, i := range instruments {
			symbols = append(symbols, i.Symbol)
		}
		subs = append(subs, map[string]any{"method": "subscribe", "params": map[string]any{"channel": "book", "symbol": symbols, "depth": 1000}}, map[string]any{"method": "subscribe", "params": map[string]any{"channel": "trade", "symbol": symbols}})
	case "bybit":
		wsURL = "wss://stream.bybit.com/v5/public/spot"
		args := []string{}
		for _, i := range instruments {
			args = append(args, "orderbook.full."+i.Symbol, "publicTrade."+i.Symbol)
		}
		for j := 0; j < len(args); j += 10 {
			end := min(j+10, len(args))
			subs = append(subs, map[string]any{"op": "subscribe", "args": args[j:end]})
		}
		ping = map[string]any{"op": "ping"}
	}
	conn, child, cancel, err := c.connect(ctx, wsURL, subs, ping)
	if err != nil {
		return err
	}
	defer cancel()
	defer conn.CloseNow()
	if v == "binance" || v == "bybit" {
		for _, i := range instruments {
			u := "https://api.binance.com/api/v3/depth?symbol=" + i.Symbol + "&limit=5000"
			if v == "bybit" {
				u = "https://api.bybit.com/v5/market/full_orderbook?category=spot&symbol=" + i.Symbol + "&limit=10000"
			}
			d, err := c.get(child, u, nil)
			if err != nil {
				return err
			}
			m := obj(d)
			bid, ask, seq := rows(m["bids"]), rows(m["asks"]), num(m["lastUpdateId"])
			if v == "bybit" {
				if num(m["retCode"]) != 0 {
					return fmt.Errorf("bybit snapshot: %v", m["retMsg"])
				}
				m = obj(m["result"])
				bid, ask, seq = rows(m["b"]), rows(m["a"]), num(m["u"])
			}
			if err = books[i.Symbol].Snapshot(bid, ask, seq); err != nil {
				return err
			}
		}
	}
	if v == "coinbase" {
		go c.coinbaseBackfill(child, instruments)
	}
	c.E.SetHealth("spot:"+v, true, "已连接，校验盘口中")
	for child.Err() == nil {
		m, err := read(child, conn)
		if err != nil {
			return err
		}
		if m["event"] == "error" || m["success"] == false || m["type"] == "error" {
			return fmt.Errorf("%s rejected subscription: %v", v, m)
		}
		switch v {
		case "okx":
			arg := obj(m["arg"])
			b := books[str(arg["instId"])]
			if b == nil {
				continue
			}
			for _, raw := range arr(m["data"]) {
				d := obj(raw)
				switch arg["channel"] {
				case "books":
					seq := num(d["seqId"])
					if m["action"] == "snapshot" {
						err = b.Snapshot(rows(d["bids"]), rows(d["asks"]), seq)
					} else {
						err = b.Delta(rows(d["bids"]), rows(d["asks"]), seq, num(d["prevSeqId"]), true, 400)
					}
				case "trades":
					c.E.Trade(b.Instrument, str(d["tradeId"]), str(d["side"]), str(d["px"]), str(d["sz"]), time.UnixMilli(num(d["ts"])), 1)
				}
			}
		case "binance":
			d := obj(m["data"])
			b := books[str(d["s"])]
			if b == nil {
				continue
			}
			switch d["e"] {
			case "depthUpdate":
				seq, first := num(d["u"]), num(d["U"])
				last := b.Seq()
				if seq <= last {
					continue
				}
				if first > last+1 {
					return fmt.Errorf("binance missing depth %d-%d", last, first)
				}
				err = b.Delta(rows(d["b"]), rows(d["a"]), seq, last, true, 0)
			case "aggTrade":
				side := "buy"
				if d["m"] == true {
					side = "sell"
				}
				c.E.Trade(b.Instrument, str(d["a"]), side, str(d["p"]), str(d["q"]), time.UnixMilli(num(d["T"])), 1)
			}
		case "coinbase":
			b := books[str(m["product_id"])]
			if b == nil {
				continue
			}
			switch m["type"] {
			case "snapshot":
				err = b.Snapshot(rows(m["bids"]), rows(m["asks"]), 0)
			case "l2update":
				bid, ask := [][]string{}, [][]string{}
				for _, r := range rows(m["changes"]) {
					if len(r) < 3 {
						continue
					}
					if r[0] == "buy" {
						bid = append(bid, r[1:])
					} else {
						ask = append(ask, r[1:])
					}
				}
				err = b.Delta(bid, ask, 0, 0, false, 0)
			case "heartbeat":
				b.Touch()
				c.cbMu.Lock()
				last := c.cbHigh[b.Instrument.Symbol]
				c.cbMu.Unlock()
				if num(m["last_trade_id"]) > last {
					select {
					case c.cbWake <- struct{}{}:
					default:
					}
				}
			case "match", "last_match":
				c.coinbaseTrade(b.Instrument, m)
			}
		case "kraken":
			for _, raw := range arr(m["data"]) {
				d := obj(raw)
				b := books[str(d["symbol"])]
				if b == nil {
					continue
				}
				if m["channel"] == "book" {
					bid, ask := krakenRows(d["bids"]), krakenRows(d["asks"])
					if m["type"] == "snapshot" {
						err = b.Snapshot(bid, ask, 0)
					} else {
						err = b.Delta(bid, ask, 0, 0, false, 1000)
					}
					if err == nil && d["checksum"] != nil {
						got := KrakenCRC(b)
						if got != uint32(num(d["checksum"])) {
							return fmt.Errorf("kraken CRC mismatch %s: %d != %v", b.Instrument.Symbol, got, d["checksum"])
						}
					}
				} else if m["channel"] == "trade" {
					t, _ := time.Parse(time.RFC3339Nano, str(d["timestamp"]))
					c.E.Trade(b.Instrument, str(d["trade_id"]), str(d["side"]), str(d["price"]), str(d["qty"]), t, 1)
				}
			}
			if m["channel"] == "heartbeat" {
				for _, b := range books {
					b.Touch()
				}
			}
		case "bybit":
			topic := str(m["topic"])
			parts := strings.Split(topic, ".")
			if len(parts) < 2 {
				continue
			}
			b := books[parts[len(parts)-1]]
			if b == nil {
				continue
			}
			if strings.HasPrefix(topic, "orderbook.") {
				d := obj(m["data"])
				seq := num(d["u"])
				last := b.Seq()
				if m["type"] == "snapshot" {
					err = b.Snapshot(rows(d["b"]), rows(d["a"]), seq)
				} else if seq > last {
					if seq != last+1 {
						return fmt.Errorf("bybit missing full depth %d -> %d", last, seq)
					}
					err = b.Delta(rows(d["b"]), rows(d["a"]), seq, last, true, 0)
				}
			} else if strings.HasPrefix(topic, "publicTrade.") {
				for _, raw := range arr(m["data"]) {
					d := obj(raw)
					c.E.Trade(b.Instrument, str(d["i"]), strings.ToLower(str(d["S"])), str(d["p"]), str(d["v"]), time.UnixMilli(num(d["T"])), 1)
				}
			}
		}
		if err != nil {
			return err
		}
	}
	return child.Err()
}
func (c *Collector) coinbaseTrade(i Instrument, m map[string]any) {
	c.cbMu.Lock()
	c.cbHigh[i.Symbol] = max(c.cbHigh[i.Symbol], num(m["trade_id"]))
	c.cbMu.Unlock()
	side := "buy"
	if m["side"] == "buy" {
		side = "sell"
	}
	t, _ := time.Parse(time.RFC3339Nano, str(m["time"]))
	c.E.Trade(i, str(m["trade_id"]), side, str(m["price"]), str(m["size"]), t, 1)
}
func (c *Collector) coinbaseBackfill(ctx context.Context, is []Instrument) {
	for ctx.Err() == nil {
		for _, i := range is {
			c.cbMu.Lock()
			previous := c.cbCursor[i.Symbol]
			c.cbMu.Unlock()
			newest := previous
			cursor := ""
			complete := false
			var fetchErr error
			for page := 0; page < 5; page++ {
				u := "https://api.exchange.coinbase.com/products/" + i.Symbol + "/trades?limit=1000"
				if cursor != "" {
					u += "&after=" + cursor
				}
				d, err := c.get(ctx, u, nil)
				if err != nil {
					fetchErr = err
					break
				}
				ts := arr(d)
				if len(ts) == 0 {
					complete = true
					break
				}
				for _, t := range ts {
					m := obj(t)
					id := num(m["trade_id"])
					newest = max(newest, id)
					at, _ := time.Parse(time.RFC3339Nano, str(m["time"]))
					if id <= previous || at.Before(c.E.BootAt) {
						complete = true
						continue
					}
					c.coinbaseTrade(i, m)
				}
				cursor = str(obj(ts[len(ts)-1])["trade_id"])
				if complete || len(ts) < 1000 {
					complete = true
					break
				}
				if !wait(ctx, 150*time.Millisecond) {
					return
				}
			}
			if fetchErr != nil {
				c.E.SetHealth("coinbase:trade-backfill:"+i.Symbol, false, fetchErr.Error())
			} else {
				c.cbMu.Lock()
				c.cbCursor[i.Symbol] = newest
				c.cbMu.Unlock()
				c.E.SetHealth("coinbase:trade-backfill:"+i.Symbol, complete, "心跳缺口触发回补，单次最多5000笔；未补齐时标为部分统计")
			}
			if fetchErr != nil || !complete {
				c.E.MarkGap("coinbase", "spot", time.Now().Add(-time.Minute), time.Now())
			}
		}
		// Heartbeat wakes are coalesced, and every cycle is bounded to avoid REST bursts.
		if !wait(ctx, 5*time.Second) {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-c.cbWake:
		case <-time.After(10 * time.Second):
		}
	}
}

func krakenRows(v any) [][]string {
	out := [][]string{}
	for _, r := range arr(v) {
		m := obj(r)
		out = append(out, []string{str(m["price"]), str(m["qty"])})
	}
	return out
}
func KrakenCRC(b *Book) uint32 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	var s strings.Builder
	for index, m := range []map[float64]RawLevel{b.Asks, b.Bids} {
		ps := []float64{}
		for p := range m {
			ps = append(ps, p)
		}
		sort.Float64s(ps)
		if index == 1 {
			sort.Sort(sort.Reverse(sort.Float64Slice(ps)))
		}
		for _, p := range ps[:min(10, len(ps))] {
			l := m[p]
			for _, raw := range []string{l.Price, l.Quantity} {
				if strings.ContainsAny(raw, "eE") {
					if d, err := decimal.NewFromString(raw); err == nil {
						raw = d.StringFixed(max(0, -d.Exponent()))
					}
				}
				s.WriteString(strings.TrimLeft(strings.ReplaceAll(raw, ".", ""), "0"))
			}
		}
	}
	return crc32.ChecksumIEEE([]byte(s.String()))
}
func (c *Collector) okxDeep(ctx context.Context) {
	for ctx.Err() == nil {
		for _, i := range Instruments() {
			if i.Venue != "okx" {
				continue
			}
			d, err := c.get(ctx, "https://www.okx.com/api/v5/market/books-full?instId="+url.QueryEscape(i.Symbol)+"&sz=5000", nil)
			if err == nil {
				data := arr(obj(d)["data"])
				if len(data) > 0 {
					m := obj(data[0])
					c.E.Books[i.Key()].SetDeep(rows(m["bids"]), rows(m["asks"]))
					c.E.SetHealth("okx:deep:"+i.Symbol, true, "10秒深度采样，浅层由实时盘口覆盖")
				}
			} else {
				c.E.SetHealth("okx:deep:"+i.Symbol, false, err.Error())
			}
		}
		if !wait(ctx, 10*time.Second) {
			return
		}
	}
}
