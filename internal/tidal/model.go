package tidal

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/shopspring/decimal"
)

type Instrument struct {
	Venue  string `json:"venue"`
	Asset  string `json:"asset"`
	Quote  string `json:"quote"`
	Symbol string `json:"symbol"`
	Market string `json:"market"`
}

func (i Instrument) Key() string { return i.Venue + ":" + i.Symbol + ":" + i.Market }
func Instruments() []Instrument {
	out := []Instrument{}
	for _, a := range []string{"BTC", "ETH"} {
		for _, v := range []string{"okx", "binance", "coinbase", "kraken", "bybit"} {
			qs := []string{"USD", "USDT", "USDC"}
			if v == "okx" || v == "coinbase" {
				qs = []string{"USD", "USDT"}
			}
			if v == "binance" {
				qs = []string{"USDT", "USDC"}
			}
			for _, q := range qs {
				sep := ""
				if v == "okx" || v == "coinbase" {
					sep = "-"
				}
				if v == "kraken" {
					sep = "/"
				}
				out = append(out, Instrument{v, a, q, a + sep + q, "spot"})
			}
		}
	}
	return out
}

type RawLevel struct {
	Price    string  `json:"price"`
	Quantity string  `json:"quantity"`
	P        float64 `json:"-"`
	Q        float64 `json:"-"`
	Value    float64 `json:"-"`
}

func level(p, q string) (RawLevel, bool) {
	pd, err := decimal.NewFromString(p)
	if err != nil || !pd.IsPositive() {
		return RawLevel{}, false
	}
	qd, err := decimal.NewFromString(q)
	if err != nil || qd.IsNegative() {
		return RawLevel{}, false
	}
	pf, _ := pd.Float64()
	qf, _ := qd.Float64()
	nf, _ := pd.Mul(qd).Float64()
	return RawLevel{p, q, pf, qf, nf}, finite(pf) && finite(qf) && finite(nf)
}

type Book struct {
	mu                 sync.RWMutex
	Instrument         Instrument
	Bids, Asks         map[float64]RawLevel
	Sequence           int64
	Seen, Changed      time.Time
	Valid              bool
	Error              string
	LowBid, HighAsk    float64
	Resyncs            int64
	DeepBids, DeepAsks map[float64]RawLevel
	DeepAt             time.Time
}

func newBook(i Instrument) *Book {
	return &Book{Instrument: i, Bids: map[float64]RawLevel{}, Asks: map[float64]RawLevel{}, Error: "等待首次快照"}
}
func applySide(dst map[float64]RawLevel, rows [][]string) {
	for _, r := range rows {
		if len(r) < 2 {
			continue
		}
		l, ok := level(r[0], r[1])
		if !ok {
			continue
		}
		if l.Q == 0 {
			delete(dst, l.P)
		} else {
			dst[l.P] = l
		}
	}
}
func (b *Book) Snapshot(bids, asks [][]string, seq int64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.Bids = map[float64]RawLevel{}
	b.Asks = map[float64]RawLevel{}
	applySide(b.Bids, bids)
	applySide(b.Asks, asks)
	b.Sequence = seq
	b.Seen = time.Now().UTC()
	b.Changed = b.Seen
	b.LowBid = math.Inf(1)
	b.HighAsk = 0
	for p := range b.Bids {
		b.LowBid = math.Min(b.LowBid, p)
	}
	for p := range b.Asks {
		b.HighAsk = math.Max(b.HighAsk, p)
	}
	return b.validateLocked()
}
func (b *Book) validateLocked() error {
	bid, ask := 0.0, math.Inf(1)
	for p := range b.Bids {
		if p > bid {
			bid = p
		}
	}
	for p := range b.Asks {
		if p < ask {
			ask = p
		}
	}
	if bid == 0 || math.IsInf(ask, 1) || bid >= ask {
		b.Valid = false
		b.Error = "盘口为空或交叉，重建中"
		return fmt.Errorf("invalid book %s", b.Instrument.Key())
	}
	b.Valid = true
	b.Error = ""
	return nil
}
func (b *Book) Delta(bids, asks [][]string, seq, prev int64, strict bool, depth int) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.Valid {
		return fmt.Errorf("book not initialized")
	}
	if strict && seq > 0 && seq < b.Sequence && prev == b.Sequence {
		b.Valid = false
		b.Error = "序列重置，重新获取快照"
		return fmt.Errorf("sequence reset")
	}
	if seq > 0 && seq <= b.Sequence && (!strict || prev <= b.Sequence) {
		b.Seen = time.Now().UTC()
		return nil
	}
	if strict && prev != b.Sequence {
		b.Valid = false
		b.Error = "增量序列缺口"
		return fmt.Errorf("sequence gap %s: %d != %d", b.Instrument.Key(), prev, b.Sequence)
	}
	if seq > 0 && seq <= b.Sequence {
		return nil
	}
	applySide(b.Bids, bids)
	applySide(b.Asks, asks)
	b.Sequence = seq
	b.Seen = time.Now().UTC()
	if len(bids)+len(asks) > 0 {
		b.Changed = b.Seen
	}
	if depth > 0 {
		trimSide(b.Bids, depth, true)
		trimSide(b.Asks, depth, false)
		b.LowBid, b.HighAsk = bounds(b.Bids, b.Asks)
	}
	if len(b.Bids)+len(b.Asks) > 100000 {
		b.Valid = false
		return fmt.Errorf("book memory bound reached")
	}
	return b.validateLocked()
}
func bounds(bids, asks map[float64]RawLevel) (float64, float64) {
	low, high := math.Inf(1), 0.0
	for p := range bids {
		low = math.Min(low, p)
	}
	for p := range asks {
		high = math.Max(high, p)
	}
	if math.IsInf(low, 1) {
		low = 0
	}
	return low, high
}
func trimSide(m map[float64]RawLevel, n int, bids bool) {
	if len(m) <= n {
		return
	}
	ps := make([]float64, 0, len(m))
	for p := range m {
		ps = append(ps, p)
	}
	sort.Float64s(ps)
	if bids {
		for _, p := range ps[:len(ps)-n] {
			delete(m, p)
		}
	} else {
		for _, p := range ps[n:] {
			delete(m, p)
		}
	}
}
func (b *Book) Invalidate(reason string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.Valid = false
	b.Error = reason
	b.Resyncs++
}
func (b *Book) Touch()     { b.mu.Lock(); b.Seen = time.Now().UTC(); b.mu.Unlock() }
func (b *Book) Seq() int64 { b.mu.RLock(); defer b.mu.RUnlock(); return b.Sequence }
func (b *Book) SetDeep(bids, asks [][]string) {
	bs, as := map[float64]RawLevel{}, map[float64]RawLevel{}
	applySide(bs, bids)
	applySide(as, asks)
	b.mu.Lock()
	b.DeepBids = bs
	b.DeepAsks = as
	b.DeepAt = time.Now().UTC()
	b.mu.Unlock()
}

type Rate struct {
	Quote      string    `json:"quote"`
	USD        string    `json:"usd"`
	ObservedAt time.Time `json:"observedAt"`
	Value      float64   `json:"-"`
}
type Coverage struct {
	Venue       string    `json:"venue"`
	Symbol      string    `json:"symbol"`
	Quote       string    `json:"quote"`
	Valid       bool      `json:"valid"`
	Reason      string    `json:"reason,omitempty"`
	ObservedAt  time.Time `json:"observedAt"`
	BidLow      float64   `json:"bidLow"`
	AskHigh     float64   `json:"askHigh"`
	Bid         float64   `json:"bid"`
	Ask         float64   `json:"ask"`
	Levels      int       `json:"levels"`
	DeepAt      time.Time `json:"deepAt,omitempty"`
	LiveBidLow  float64   `json:"liveBidLow"`
	LiveAskHigh float64   `json:"liveAskHigh"`
	Resyncs     int64     `json:"resyncs"`
}
type Zone struct {
	Price       float64          `json:"price"`
	Step        float64          `json:"step"`
	Side        string           `json:"side"`
	USDCents    int64            `json:"usdCents"`
	Sources     map[string]int64 `json:"sources"`
	Since       time.Time        `json:"since"`
	Seconds     int64            `json:"seconds"`
	Occupancy   float64          `json:"occupancy"`
	Evidence    string           `json:"evidence"`
	Grade       string           `json:"grade"`
	TradedCents int64            `json:"tradedCents"`
	Samples     int64            `json:"samples"`
	ChangeCents int64            `json:"changeCents"`
	Sampled     bool             `json:"sampled"`
}
type Candle struct {
	Time  int64   `json:"time"`
	Open  float64 `json:"open"`
	High  float64 `json:"high"`
	Low   float64 `json:"low"`
	Close float64 `json:"close"`
}
type Frame struct {
	Asset     string     `json:"asset"`
	At        time.Time  `json:"at"`
	Price     float64    `json:"price"`
	Step      float64    `json:"step"`
	Zones     []Zone     `json:"zones"`
	Coverage  []Coverage `json:"coverage"`
	Rates     []Rate     `json:"rates"`
	StartedAt time.Time  `json:"startedAt"`
	Candle    Candle     `json:"candle"`
}
type Flow struct {
	Partial   bool               `json:"partial"`
	Venue     string             `json:"venue"`
	Asset     string             `json:"asset"`
	Market    string             `json:"market"`
	Minute    int64              `json:"minute"`
	BuyCents  int64              `json:"buyCents"`
	SellCents int64              `json:"sellCents"`
	BaseQty   float64            `json:"baseQty"`
	USDQty    float64            `json:"usdQty"`
	Trades    int64              `json:"trades"`
	PriceBins map[int64][2]int64 `json:"priceBins"`
}
type Derivative struct {
	Venue         string    `json:"venue"`
	Asset         string    `json:"asset"`
	OIBase        float64   `json:"oiBase"`
	OIUSDCents    int64     `json:"oiUsdCents"`
	Mark          float64   `json:"mark"`
	Index         float64   `json:"index"`
	Funding       float64   `json:"funding"`
	IntervalHours float64   `json:"intervalHours"`
	NextFunding   int64     `json:"nextFunding"`
	At            time.Time `json:"at"`
	Valid         bool      `json:"valid"`
	Quote         string    `json:"quote"`
	Source        string    `json:"source"`
}
type Liquidation struct {
	Venue     string    `json:"venue"`
	Asset     string    `json:"asset"`
	Side      string    `json:"side"`
	Price     float64   `json:"price"`
	USDCents  int64     `json:"usdCents"`
	At        time.Time `json:"at"`
	PriceType string    `json:"priceType"`
	Coverage  string    `json:"coverage"`
}
type Whale struct {
	Address         string    `json:"address"`
	Asset           string    `json:"asset"`
	Side            string    `json:"side"`
	Size            string    `json:"size"`
	Entry           string    `json:"entry"`
	USDCents        int64     `json:"usdCents"`
	Leverage        int       `json:"leverage"`
	Margin          string    `json:"margin"`
	Liquidation     *string   `json:"liquidation"`
	Distance        *float64  `json:"distance"`
	UnrealizedCents int64     `json:"unrealizedCents"`
	Mark            float64   `json:"mark"`
	At              time.Time `json:"at"`
	ChangeSize      string    `json:"changeSize"`
	FirstSeen       time.Time `json:"firstSeen"`
	Valid           bool      `json:"valid"`
	Quote           string    `json:"quote"`
	Rate            string    `json:"rate"`
}
type Health struct {
	Component string    `json:"component"`
	At        time.Time `json:"at"`
	OK        bool      `json:"ok"`
	Detail    string    `json:"detail"`
	Errors    int64     `json:"errors"`
}

func number(v any) float64 {
	switch n := v.(type) {
	case string:
		x, _ := strconv.ParseFloat(n, 64)
		return x
	case float64:
		return n
	case json.Number:
		x, _ := n.Float64()
		return x
	case int:
		return float64(n)
	case int64:
		return float64(n)
	}
	return 0
}
func str(v any) string {
	switch s := v.(type) {
	case string:
		return s
	case json.Number:
		return string(s)
	case nil:
		return ""
	default:
		return fmt.Sprint(s)
	}
}
func num(v any) int64          { return int64(number(v)) }
func obj(v any) map[string]any { m, _ := v.(map[string]any); return m }
func arr(v any) []any          { a, _ := v.([]any); return a }
func rows(v any) [][]string {
	out := [][]string{}
	for _, r := range arr(v) {
		s := []string{}
		for _, n := range arr(r) {
			s = append(s, str(n))
		}
		out = append(out, s)
	}
	return out
}
func finite(f float64) bool { return !math.IsNaN(f) && !math.IsInf(f, 0) }
func cents(n float64) int64 {
	if !finite(n) || math.Abs(n) > 9e13 {
		return 0
	}
	return int64(math.Round(n * 100))
}
func Step(a string) float64 {
	if a == "BTC" {
		return 25
	}
	return 1
}
func assetOf(symbol string) string {
	if strings.HasPrefix(symbol, "BTC") || strings.HasPrefix(symbol, "XBT") || strings.HasPrefix(symbol, "XXBT") {
		return "BTC"
	}
	if strings.Contains(symbol, "ETH") {
		return "ETH"
	}
	return ""
}
