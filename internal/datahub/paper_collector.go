package datahub

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/shopspring/decimal"
)

const paperREST = "https://fapi.binance.com"
const paperBookWS = "wss://fstream.binance.com/public/ws/btcusdt@bookTicker"
const paperMarketWS = "wss://fstream.binance.com/market/stream?streams=btcusdt@markPrice@1s/btcusdt@kline_5m"

type paperFeed struct {
	Diagnostics *paperDiagnosticView `json:"diagnostics,omitempty"`
	Quote       *paperQuote          `json:"quote"`
	MarkAt      *time.Time           `json:"markAt"`
	Instrument  paperInstrument      `json:"instrument"`
	ATR         *decimal.Decimal     `json:"atr"`
	ATRThrough  *time.Time           `json:"atrThrough"`
	Mode        string               `json:"mode"`
	Bytes       int64                `json:"bytes"`
	Error       string               `json:"error"`
}
type paperMessage struct {
	Diagnostics *paperDiagnosticView
	Kind        string
	At          time.Time
	Raw         []byte
	Through     time.Time
	Err         error
}

// Binance wire keys are case-sensitive, while encoding/json also matches
// struct fields case-insensitively. Both e/E must have exact destinations so
// the numeric event clock cannot be decoded as the event-name string.
type paperStreamHeader struct {
	Event   string `json:"e"`
	EventAt int64  `json:"E"`
	Symbol  string `json:"s"`
	Type    int    `json:"st"`
}

// Independent limiter: one in-flight request, >=2s between requests. Upstream
// 429/418 carries a cooldown; it never consumes CoinGlass's request budget.
type paperHTTP struct {
	client *http.Client
	next   time.Time
}

func (c *paperHTTP) get(ctx context.Context, path string) ([]byte, error) {
	if !paperPublicURL(paperREST + path) {
		return nil, errors.New("non-public perpetual REST path refused")
	}
	if wait := time.Until(c.next); wait > 0 {
		t := time.NewTimer(wait)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-t.C:
		}
	}
	c.next = time.Now().Add(2 * time.Second)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, paperREST+path, nil)
	if err != nil {
		return nil, err
	}
	r, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		if r.StatusCode == 429 || r.StatusCode == 418 {
			seconds, _ := strconv.Atoi(r.Header.Get("Retry-After"))
			if seconds < 60 {
				seconds = 60
			}
			if seconds > 3600 {
				seconds = 3600
			}
			c.next = time.Now().Add(time.Duration(seconds) * time.Second)
		}
		return nil, fmt.Errorf("Binance public REST HTTP %d", r.StatusCode)
	}
	b, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
	if err != nil {
		return nil, err
	}
	if len(b) >= 4<<20 {
		return nil, errors.New("Binance response exceeds bound")
	}
	return b, nil
}
func paperSend(ctx context.Context, out chan<- paperMessage, msg paperMessage) bool {
	select {
	case <-ctx.Done():
		return false
	case out <- msg:
		return true
	}
}
func paperStream(ctx context.Context, address string, out chan<- paperMessage, traces ...*paperDiagnostics) {
	var trace *paperDiagnostics
	if len(traces) > 0 {
		trace = traces[0]
	}
	var failure *paperDiagnosticView
	if !paperPublicURL(address) {
		paperSend(ctx, out, paperMessage{Kind: "gap", At: time.Now().UTC(), Err: errors.New("unexpected perpetual stream URL")})
		return
	}
	backoff := time.Second
	for ctx.Err() == nil {
		dial, stop := context.WithTimeout(ctx, 8*time.Second)
		conn, _, err := websocket.Dial(dial, address, nil)
		stop()
		if err == nil {
			conn.SetReadLimit(16 << 10)
			started := time.Now()
			for ctx.Err() == nil {
				read, cancel := context.WithTimeout(ctx, 8*time.Second)
				_, raw, e := conn.Read(read)
				cancel()
				if e != nil {
					err = e
					break
				}
				at := time.Now().UTC()
				// Blocking a full bounded channel is itself a data gap, rather
				// than silently throwing away a path-dependent exit observation.
				failure = paperEnqueueStream(out, paperMessage{Kind: "stream", At: at, Raw: raw}, trace)
				if failure != nil {
					err = errors.New("perpetual quote queue overflow")
				}

				if err != nil {
					break
				}
			}
			conn.CloseNow()
			if time.Since(started) > time.Minute {
				backoff = time.Second
			}
		}
		if ctx.Err() != nil {
			return
		}
		if !paperSend(ctx, out, paperMessage{Kind: "gap", At: time.Now().UTC(), Err: err, Diagnostics: failure}) {
			return
		}
		failure = nil
		t := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
		backoff = min(backoff*2, 30*time.Second)
	}
}
func paperRESTWorker(ctx context.Context, p *paperStore, out chan<- paperMessage) {
	c := paperHTTP{client: &http.Client{Timeout: 5 * time.Second}}
	var instrumentAt, candlesAt time.Time
	for ctx.Err() == nil {
		now := time.Now().UTC()
		requests := []paperMessage{}
		if now.Sub(instrumentAt) > time.Hour {
			requests = append(requests, paperMessage{Kind: "instrument"})
		}
		if now.Sub(candlesAt) > 5*time.Minute {
			requests = append(requests, paperMessage{Kind: "candles"})
		}
		s := p.snapshot()
		from, through := paperFundingWindow(s, now)
		if through.After(from) {
			requests = append(requests, paperMessage{Kind: "funding", Through: through})
		}
		for _, r := range requests {
			path := ""
			switch r.Kind {
			case "instrument":
				path = "/fapi/v1/exchangeInfo"
			case "candles":
				path = "/fapi/v1/klines?symbol=BTCUSDT&interval=5m&limit=200"
			case "funding":
				path = "/fapi/v1/fundingRate?symbol=BTCUSDT&limit=1000&startTime=" + strconv.FormatInt(from.UnixMilli(), 10) + "&endTime=" + strconv.FormatInt(through.UnixMilli(), 10)
			}
			r.Raw, r.Err = c.get(ctx, path)
			r.At = time.Now().UTC()
			if r.Err == nil {
				if r.Kind == "instrument" {
					instrumentAt = r.At
				}
				if r.Kind == "candles" {
					candlesAt = r.At
				}
			}
			if !paperSend(ctx, out, r) {
				return
			}
		}
		t := time.NewTimer(30 * time.Second)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
	}
}
func paperFundingWindow(s paperState, now time.Time) (time.Time, time.Time) {
	// Revisit the last hour even after an apparently empty response. Known
	// overdue settlements are retried regardless of how far the watermark
	// advanced; their absence is never transformed into a zero funding charge.
	from := s.FundingThrough.Add(-time.Hour)
	if s.FundingThrough.IsZero() {
		from = now.Add(-24 * time.Hour)
	}
	for _, due := range s.ExpectedFunding {
		at := time.UnixMilli(due).UTC()
		if at.Before(now.Add(-paperFundingHistoryDelay)) && !paperSettlementObserved(s, due) && at.Before(from) {
			from = at.Add(-time.Second)
		}
	}
	return from, minTime(now.Add(-paperFundingHistoryDelay), from.Add(24*time.Hour))
}
func parsePaperInstrument(raw []byte, at time.Time) (paperInstrument, error) {
	var doc struct {
		Symbols []struct {
			Symbol, Status, ContractType, MarginAsset string
			Filters                                   []map[string]json.RawMessage
		}
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return paperInstrument{}, err
	}
	for _, v := range doc.Symbols {
		if v.Symbol != "BTCUSDT" {
			continue
		}
		if v.ContractType != "PERPETUAL" || v.MarginAsset != "USDT" {
			return paperInstrument{}, errors.New("unexpected contract specification")
		}
		m := paperInstrument{At: at, Status: v.Status}
		var lot, market map[string]json.RawMessage
		for _, f := range v.Filters {
			var kind string
			_ = json.Unmarshal(f["filterType"], &kind)
			if kind == "LOT_SIZE" {
				lot = f
			}
			if kind == "MARKET_LOT_SIZE" {
				market = f
			}
			if kind == "MIN_NOTIONAL" {
				var str string
				if err := json.Unmarshal(f["notional"], &str); err != nil {
					return m, err
				}
				d, err := paperDecimal(str, true)
				if err != nil {
					return m, err
				}
				m.MinNotional = d
			}
		}
		read := func(f map[string]json.RawMessage, key string) (decimal.Decimal, error) {
			var str string
			if err := json.Unmarshal(f[key], &str); err != nil {
				return decimal.Zero, err
			}
			return paperDecimal(str, true)
		}
		var err error
		m.Step, err = read(lot, "stepSize")
		if err != nil {
			return m, err
		}
		m.Minimum, err = read(lot, "minQty")
		if err != nil {
			return m, err
		}
		m.Maximum, err = read(lot, "maxQty")
		if err != nil {
			return m, err
		}
		if len(market) > 0 {
			step, e := read(market, "stepSize")
			if e != nil {
				return m, e
			}
			if !step.Mod(m.Step).IsZero() && !m.Step.Mod(step).IsZero() {
				return m, errors.New("incompatible quantity steps")
			}
			m.Step = decimal.Max(m.Step, step)
			low, e := read(market, "minQty")
			if e != nil {
				return m, e
			}
			high, e := read(market, "maxQty")
			if e != nil {
				return m, e
			}
			m.Minimum = decimal.Max(m.Minimum, low)
			m.Maximum = decimal.Min(m.Maximum, high)
		}
		if !m.MinNotional.IsPositive() {
			return m, errors.New("missing minimum notional")
		}
		return m, nil
	}
	return paperInstrument{}, errors.New("BTCUSDT perpetual not present")
}
func paperCandle(raw []json.RawMessage, now time.Time) (int64, Candle, error) {
	if len(raw) < 7 {
		return 0, Candle{}, errors.New("short kline")
	}
	var ts, end int64
	if err := json.Unmarshal(raw[0], &ts); err != nil {
		return 0, Candle{}, err
	}
	if err := json.Unmarshal(raw[6], &end); err != nil {
		return 0, Candle{}, err
	}
	if ts%300000 != 0 || end != ts+300000-1 || !time.UnixMilli(end).Before(now) {
		return 0, Candle{}, errors.New("incomplete five-minute candle")
	}
	numbers := [4]float64{}
	for i := 0; i < 4; i++ {
		var str string
		if err := json.Unmarshal(raw[i+1], &str); err != nil {
			return 0, Candle{}, err
		}
		d, err := paperDecimal(str, true)
		if err != nil {
			return 0, Candle{}, err
		}
		numbers[i], _ = d.Float64()
	}
	c := Candle{Open: numbers[0], High: numbers[1], Low: numbers[2], Close: numbers[3]}
	if c.Low > math.Min(c.Open, c.Close) || c.High < math.Max(c.Open, c.Close) || c.High < c.Low {
		return 0, Candle{}, errors.New("inconsistent candle OHLC")
	}
	return ts / 1000, c, nil
}
func (p *paperStore) publishFeed() {
	p.diagnostics.sampleCPU()
	f := paperFeed{Diagnostics: p.diagnostics.snapshot(), Mode: p.mode, Instrument: p.instrument, Bytes: p.size()}
	if p.quote != nil {
		q := *p.quote
		f.Quote = &q
	}
	if !p.markAt.IsZero() {
		at := p.markAt
		f.MarkAt = &at
	}
	if p.atr != nil {
		a, t := p.atr.ATR, p.atr.ATRThrough
		f.ATR, f.ATRThrough = &a, &t
	}
	p.mu.Lock()
	f.Error = p.err
	p.feed = f
	p.mu.Unlock()
}
func (h *Hub) paperWorker(ctx context.Context) {
	p := h.Store.paper
	if p == nil || p.mode == "off" || h.offline {
		return
	}
	sourceReader, err := openPaperPublicationReader(h.Store.root)
	if err != nil {
		p.failure(err, time.Now().UTC())
		return
	}
	defer sourceReader.Close()
	out := make(chan paperMessage, 256)
	child, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	defer func() { cancel(); wg.Wait() }()
	for _, f := range []func(){func() { paperStream(child, paperBookWS, out, &p.diagnostics) }, func() { paperStream(child, paperMarketWS, out, &p.diagnostics) }, func() { paperRESTWorker(child, p, out) }} {
		wg.Add(1)
		go func(f func()) { defer wg.Done(); f() }(f)
	}
	h.paperConsumeLoop(ctx, p, sourceReader, out)
}

// Production and burst replay use the identical queue consumer, including
// heartbeat, source reads, ledger writes, funding and maintenance.
func (h *Hub) paperConsumeLoop(ctx context.Context, p *paperStore, sourceReader *sql.DB, out <-chan paperMessage) {
	loopDone := p.diagnostics.stage("consumer_loop")
	defer loopDone()
	if err := p.discontinuity(ctx, time.Now().UTC(), "restart_gap"); err != nil {
		p.failure(err, time.Now().UTC())
		return
	}
	candles := map[int64]Candle{}
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	var lastMaintenance time.Time
	protected := false
	for ctx.Err() == nil {
		var err error
		now := time.Now().UTC()
		operation := "ledger"
		waiting := p.diagnostics.stage("consumer_wait")
		var dispatch func()
		select {
		case <-ctx.Done():
			waiting()
			return
		case msg := <-out:
			waiting()
			dispatch = p.diagnostics.stage("message_dispatch")
			operation = msg.Kind
			if msg.Kind == "stream" {
				p.diagnostics.processed(msg.At)
			}
			now = time.Now().UTC()
			if msg.Err != nil {
				err = msg.Err
				if msg.Kind == "gap" {
					err = p.discontinuity(ctx, now, "stream_disconnect")
					if err == nil {
						err = p.recordStreamFailure(ctx, msg.Err, now, msg.Diagnostics)
					}
				}
				break
			}
			done := p.diagnostics.stage(msg.Kind)
			switch msg.Kind {
			case "instrument":
				p.instrument, err = parsePaperInstrument(msg.Raw, msg.At)
			case "candles":
				var rows [][]json.RawMessage
				err = json.Unmarshal(msg.Raw, &rows)
				if err == nil {
					for _, row := range rows {
						if len(row) < 7 {
							err = errors.New("short klines response")
							break
						}
						var end int64
						if e := json.Unmarshal(row[6], &end); e != nil {
							err = e
							break
						}
						if end >= msg.At.UnixMilli() {
							continue
						}
						ts, c, e := paperCandle(row, msg.At)
						if e != nil {
							err = e
							break
						}
						candles[ts] = c
					}
				}
			case "funding":
				var rows []struct {
					Symbol                 string
					FundingTime            int64
					FundingRate, MarkPrice string
				}
				err = json.Unmarshal(msg.Raw, &rows)
				if len(rows) >= 1000 {
					err = errors.New("funding history page not complete")
				}
				if err == nil {
					records := []paperFunding{}
					for _, row := range rows {
						if row.Symbol != "BTCUSDT" {
							err = errors.New("wrong funding symbol")
							break
						}
						rate, e := paperDecimal(row.FundingRate, false)
						if e != nil {
							err = e
							break
						}
						mark, e := paperDecimal(row.MarkPrice, true)
						if e != nil {
							err = e
							break
						}
						records = append(records, paperFunding{At: time.UnixMilli(row.FundingTime).UTC(), Acquired: msg.At, Rate: rate, Mark: mark})
					}
					if err == nil {
						err = p.settle(ctx, records, msg.Through)
					}
				}
			case "stream":
				if now.Sub(msg.At) > time.Second {
					err = p.discontinuity(ctx, now, "processing_delay")
					break
				}
				err = p.streamMessage(ctx, msg, candles, protected)
			}
			done()
		case <-tick.C:
			waiting()
			dispatch = p.diagnostics.stage("tick_dispatch")
			now = time.Now().UTC()
			done := p.diagnostics.stage("heartbeat_prepare")
			protected = h.Store.Status().Paused || p.size() >= PaperBudget-paperReserve
			for ts := range candles {
				if ts < now.Add(-17*time.Hour).Unix() {
					delete(candles, ts)
				}
			}
			p.atr = nil
			if atr := hourlyATR(candles, now); atr != nil && *atr > 0 && completeCandles(candles, now.Truncate(time.Hour).Add(-14*time.Hour-5*time.Minute), now.Truncate(time.Hour)) {
				p.atr = &paperIntent{ATR: decimal.NewFromFloat(*atr).Round(12), ATRThrough: now.Truncate(time.Hour)}
			}
			done()
			done = p.diagnostics.stage("heartbeat")
			now, err = p.liveHeartbeat(ctx, protected)
			done()
			if err == nil {
				s := p.snapshot()
				read, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
				done = p.diagnostics.stage("source_read")
				pubs, source, end, e := readPaperPublications(read, sourceReader, s.Cursor)
				cancel()
				done()
				now = time.Now().UTC() // First actionable read completion, not tick time.
				err = e
				if err == nil {
					p.sourceAt = now
					if s.Origin == nil {
						if p.mode == "run" && !s.Gap && !protected && p.instrument.valid(now) && p.atr != nil {
							s.Origin = &now
							s.Cursor = end
							s.Source = source
							s.ObservedSeconds, s.CoveredSeconds = 0, 0
							err = p.commit(ctx, s, paperBatch{Events: []paperEvent{{At: now, Kind: "activated", Reason: PaperRules}}})
						}
					} else if source != s.Source || end < s.Cursor {
						err = p.discontinuity(ctx, now, "source_restore_boundary")
						if err == nil {
							s = p.snapshot()
							s.Source = source
							s.Cursor = end
							err = p.commit(ctx, s, paperBatch{})
						}
					} else if len(pubs) > 0 {
						err = p.consume(ctx, pubs, now, protected)
					}
				}
			}
			if now.Sub(lastMaintenance) >= time.Minute {
				lastMaintenance = now
				// Equity is accounting evidence: preserve it with the ledger.
				maintenance, stop := context.WithTimeout(ctx, 200*time.Millisecond)
				done = p.diagnostics.stage("checkpoint")
				_, e := p.db.ExecContext(maintenance, "PRAGMA wal_checkpoint(TRUNCATE)")
				done()
				if err == nil {
					err = e
				}
				stop()
			}
			done = p.diagnostics.stage("publish_feed")
			p.publishFeed()
			done()
		}
		if ctx.Err() != nil {
			dispatch()
			return
		}
		if err != nil {
			failed := p.diagnostics.stage("failure_handling")
			if operation == "funding" || operation == "candles" || operation == "instrument" {
				if e := p.recordFailure(ctx, err, now, operation); e != nil {
					p.failure(e, now)
				}
			} else {
				p.failure(err, now)
			}
			failed()
		}
		dispatch()
	}
}

func (p *paperStore) streamMessage(ctx context.Context, msg paperMessage, candles map[int64]Candle, protected bool) error {
	var envelope struct{ Data json.RawMessage }
	if err := json.Unmarshal(msg.Raw, &envelope); err != nil {
		return err
	}
	raw := msg.Raw
	if len(envelope.Data) > 0 {
		raw = envelope.Data
	}
	var head paperStreamHeader
	if err := json.Unmarshal(raw, &head); err != nil {
		return err
	}
	if head.Symbol != "BTCUSDT" || head.Type != 0 && head.Type != 1 {
		return errors.New("unexpected perpetual stream symbol/type")
	}
	switch head.Event {
	case "bookTicker":
		var v struct {
			paperStreamHeader
			ID     int64  `json:"u"`
			Bid    string `json:"b"`
			Ask    string `json:"a"`
			BidQty string `json:"B"`
			AskQty string `json:"A"`
		}
		if err := json.Unmarshal(raw, &v); err != nil {
			return err
		}
		q := paperQuote{ID: v.ID, At: msg.At, EventAt: time.UnixMilli(v.EventAt).UTC()}
		var err error
		q.Bid, err = paperDecimal(v.Bid, true)
		if err != nil {
			return err
		}
		q.Ask, err = paperDecimal(v.Ask, true)
		if err != nil {
			return err
		}
		q.BidQty, err = paperDecimal(v.BidQty, true)
		if err != nil {
			return err
		}
		q.AskQty, err = paperDecimal(v.AskQty, true)
		if err != nil {
			return err
		}
		return p.onQuote(ctx, q, protected)
	case "markPriceUpdate":
		var v struct {
			paperStreamHeader
			Next      int64           `json:"T"`
			Price     string          `json:"p"`
			Estimated json.RawMessage `json:"P"` // Distinct from the mark price p.
		}
		if err := json.Unmarshal(raw, &v); err != nil {
			return err
		}
		if _, err := paperDecimal(v.Price, true); err != nil {
			return err
		}
		if msg.At.Sub(time.UnixMilli(v.EventAt)) > 5*time.Second || time.UnixMilli(v.EventAt).After(msg.At.Add(time.Second)) {
			return errors.New("stale mark price")
		}
		p.markAt = msg.At
		s := p.snapshot()
		if v.Next > 0 && !paperHasTime(s.ExpectedFunding, v.Next) {
			s.ExpectedFunding = append(s.ExpectedFunding, v.Next)
			return p.commit(ctx, s, paperBatch{})
		}
	case "kline":
		var v struct {
			K struct {
				Start    int64  `json:"t"`
				End      int64  `json:"T"`
				Closed   bool   `json:"x"`
				Interval string `json:"i"`
				Open     string `json:"o"`
				High     string `json:"h"`
				Low      string `json:"l"`
				LastID   int64  `json:"L"` // Exact destination prevents L from matching price l.
				Close    string `json:"c"`
			} `json:"k"`
		}
		if err := json.Unmarshal(raw, &v); err != nil {
			return err
		}
		if !v.K.Closed {
			return nil
		}
		if v.K.Interval != "5m" {
			return errors.New("unexpected kline interval")
		}
		b, _ := json.Marshal([]any{v.K.Start, v.K.Open, v.K.High, v.K.Low, v.K.Close, "0", v.K.End})
		var row []json.RawMessage
		if err := json.Unmarshal(b, &row); err != nil {
			return err
		}
		ts, c, err := paperCandle(row, msg.At)
		if err != nil {
			return err
		}
		candles[ts] = c
	default:
		return fmt.Errorf("unexpected market event %q", head.Event)
	}
	return nil
}

// Kept here to make the public-only URL boundary auditable by tests.
func paperPublicURL(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.User != nil {
		return false
	}
	if raw == paperBookWS || raw == paperMarketWS {
		return true
	}
	return u.Scheme == "https" && u.Host == "fapi.binance.com" && (u.Path == "/fapi/v1/exchangeInfo" || u.Path == "/fapi/v1/klines" || u.Path == "/fapi/v1/fundingRate")
}
