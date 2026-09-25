// Package datahub owns the V2 data boundary. Readers never perform upstream IO.
package datahub

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

const RulesVersion = "evidence-2.3"

func ResearchAssets() []string    { return []string{"BTC"} }
func researchAsset(a string) bool { return a == "BTC" }

const WhaleRefreshSeconds = 300
const WhaleTTLSeconds = 480

func freshWhale(w Whale, now time.Time) bool {
	return !w.At.IsZero() && now.Sub(w.At) <= WhaleTTLSeconds*time.Second && !w.At.After(now.Add(30*time.Second))
}

type Dataset struct {
	Disabled     bool              `json:"disabled,omitempty"`
	Contract     bool              `json:"requiresContractCheck,omitempty"`
	ID           string            `json:"id"`
	Kind         string            `json:"kind"`
	Asset        string            `json:"asset"`
	Market       string            `json:"market"`
	Venue        string            `json:"venue,omitempty"`
	Symbol       string            `json:"symbol,omitempty"`
	Quote        string            `json:"quote,omitempty"`
	Source       string            `json:"source"`
	Unit         string            `json:"unit"`
	Resolution   int               `json:"resolutionSeconds"`
	Refresh      int               `json:"refreshSeconds"`
	SoftDeadline int               `json:"softDeadlineSeconds"`
	TTL          int               `json:"ttlSeconds"`
	Priority     int               `json:"priority"`
	Path         string            `json:"-"`
	Params       map[string]string `json:"-"`
	Retention    string            `json:"retention"`
}

type Level struct {
	Price    string `json:"price"`
	Quantity string `json:"quantity"`
}
type Book struct {
	Bids []Level `json:"bids"`
	Asks []Level `json:"asks"`
	Low  float64 `json:"bidLow"`
	High float64 `json:"askHigh"`
}
type Flow struct {
	Buy  string `json:"buyUsd"`
	Sell string `json:"sellUsd"`
}
type Foot struct {
	Low       string `json:"low"`
	High      string `json:"high"`
	BuyBase   string `json:"buyBase"`
	SellBase  string `json:"sellBase"`
	BuyQuote  string `json:"buyQuote"`
	SellQuote string `json:"sellQuote"`
	BuyUSDT   string `json:"buyUSDT"`
	SellUSDT  string `json:"sellUSDT"`
	BuyCount  int64  `json:"buyCount"`
	SellCount int64  `json:"sellCount"`
}
type Interest struct {
	Venue string `json:"venue"`
	USD   string `json:"usd"`
	Base  string `json:"base"`
}
type Funding struct {
	Asset       string   `json:"asset"`
	Venue       string   `json:"venue"`
	RatePercent string   `json:"ratePercent"`
	Hours       *float64 `json:"intervalHours"`
	Margin      string   `json:"margin"`
}
type Liquidation struct {
	Long  string `json:"longUsd"`
	Short string `json:"shortUsd"`
}
type Whale struct {
	Address       string     `json:"address"`
	Asset         string     `json:"asset"`
	Size          string     `json:"size"`
	Entry         string     `json:"entry"`
	Mark          string     `json:"mark"`
	USD           string     `json:"usd"`
	Leverage      string     `json:"leverage"`
	Margin        string     `json:"margin"`
	Liquidation   *string    `json:"liquidation"`
	PnL           string     `json:"pnl"`
	MarginBalance string     `json:"marginBalance"`
	FundingFee    string     `json:"fundingFee"`
	At            time.Time  `json:"at"`
	Created       *time.Time `json:"createdAt"`
}
type LargeOrder struct {
	ID               string     `json:"id"`
	Side             string     `json:"side"`
	Price            string     `json:"price"`
	Quantity         string     `json:"quantity"`
	ReportedUSD      string     `json:"reportedUsd"`
	ExecutedUSD      string     `json:"executedUsd"`
	State            string     `json:"state"`
	Start            *time.Time `json:"startAt"`
	Changed          *time.Time `json:"changedAt"`
	Trades           int64      `json:"trades"`
	InitialQuantity  *string    `json:"initialQuantity"`
	InitialUSD       *string    `json:"initialUsd"`
	ExecutedQuantity *string    `json:"executedQuantity"`
	RawState         int        `json:"rawState"`
	End              *time.Time `json:"endAt"`
}
type ModelBin struct {
	Price    float64 `json:"price"`
	Strength float64 `json:"strength"`
	Venue    string  `json:"venue,omitempty"`
}
type Model struct {
	Bins           []ModelBin   `json:"bins"`
	Prices         []float64    `json:"prices,omitempty"`
	Times          []int64      `json:"times,omitempty"`
	Cells          [][3]float64 `json:"cells,omitempty"`
	ReferencePrice float64      `json:"referencePrice"`
	Unit           string       `json:"unit"`
	Model          string       `json:"model"`
	Range          string       `json:"range"`
}
type Candle struct {
	Open   float64 `json:"open"`
	High   float64 `json:"high"`
	Low    float64 `json:"low"`
	Close  float64 `json:"close"`
	Volume float64 `json:"volume"`
}
type Price struct {
	Value string `json:"value"`
	Quote string `json:"quote"`
}
type Rate struct {
	Quote string `json:"quote"`
	USD   string `json:"usd"`
}
type Distribution struct {
	Price        float64 `json:"price"`
	Kind         string  `json:"kind"`
	Side         string  `json:"side"`
	USD          int64   `json:"usdCents"`
	Addresses    int     `json:"addresses"`
	LargestShare float64 `json:"largestShare"`
}

type Payload struct {
	Balances      []Balance      `json:"balances,omitempty"`
	ETF           *ETFRecord     `json:"etf,omitempty"`
	Premium       *Premium       `json:"premium,omitempty"`
	Distributions []Distribution `json:"distributions,omitempty"`

	Book        *Book           `json:"book,omitempty"`
	Flow        *Flow           `json:"flow,omitempty"`
	Foot        []Foot          `json:"footprint,omitempty"`
	OI          []Interest      `json:"openInterest,omitempty"`
	Funding     []Funding       `json:"funding,omitempty"`
	Liquidation *Liquidation    `json:"liquidation,omitempty"`
	Whales      []Whale         `json:"whales,omitempty"`
	Large       []LargeOrder    `json:"largeOrders,omitempty"`
	Model       *Model          `json:"model,omitempty"`
	Candle      *Candle         `json:"candle,omitempty"`
	Price       *Price          `json:"price,omitempty"`
	Rates       []Rate          `json:"rates,omitempty"`
	Wallet      json.RawMessage `json:"wallet,omitempty"`
}
type Observation struct {
	FirstFetchedAt   *time.Time        `json:"firstFetchedAt,omitempty"`
	PublishedAt      *time.Time        `json:"publishedAt,omitempty"`
	PublicationKnown bool              `json:"publicationKnown"`
	Dependencies     map[string]string `json:"dependencies,omitempty"`

	Dataset         string     `json:"dataset"`
	Source          string     `json:"source"`
	ObservedAt      *time.Time `json:"observedAt"`
	FetchedAt       time.Time  `json:"fetchedAt"`
	TimeBasis       string     `json:"timeBasis"`
	Resolution      int        `json:"resolutionSeconds"`
	Revision        string     `json:"revision"`
	Quality         string     `json:"quality"`
	Reason          string     `json:"reason,omitempty"`
	WindowStart     *time.Time `json:"windowStart,omitempty"`
	WindowEnd       *time.Time `json:"windowEnd,omitempty"`
	Samples         int        `json:"samples,omitempty"`
	ExpectedSamples int        `json:"expectedSamples,omitempty"`
	Payload         Payload    `json:"data"`
}

func (o Observation) Time() time.Time {
	if o.ObservedAt != nil {
		return *o.ObservedAt
	}
	return o.FetchedAt
}
func (o Observation) Fresh(d Dataset, now time.Time) bool {
	return o.Quality != "missing" && !o.Time().After(now.Add(30*time.Second)) && now.Sub(o.Time()) <= time.Duration(d.TTL)*time.Second && now.Sub(o.FetchedAt) <= time.Duration(d.TTL)*time.Second
}
func (o Observation) Status(d Dataset, now time.Time) string {
	if o.Quality == "missing" {
		return "missing"
	}
	if !o.Fresh(d, now) {
		return "stale"
	}
	if o.ObservedAt == nil {
		return "retrieval_only"
	}
	return "fresh"
}
func Assets() []string         { return []string{"BTC", "ETH"} }
func ValidAsset(a string) bool { return a == "BTC" || a == "ETH" }
func ID(kind, asset, venue, market string) string {
	return strings.Trim(strings.Join([]string{kind, strings.ToLower(asset), strings.ToLower(venue), market}, "."), ".")
}
func Registry() []Dataset {
	var out []Dataset
	add := func(kind, asset, market, venue, quote, path, unit string, res, every, ttl, priority int, params map[string]string) {
		symbol := params["symbol"]
		out = append(out, Dataset{ID: ID(kind, asset, venue, market), Kind: kind, Asset: asset, Market: market, Venue: venue, Symbol: symbol, Quote: quote, Source: "coinglass", Unit: unit, Resolution: res, Refresh: every, SoftDeadline: every + every/2, TTL: ttl, Priority: priority, Path: "/v4/api/" + path, Params: params, Retention: "30天细数据 / 90天降采样"})
	}
	for _, a := range Assets() {
		for _, v := range []string{"Binance", "OKX", "Coinbase", "Kraken", "Bitfinex"} {
			q := "USD"
			if v == "Binance" || v == "OKX" {
				q = "USDT"
			}
			sym := a + q
			if v == "OKX" || v == "Coinbase" {
				sym = a + "-" + q
			}
			if v == "Kraken" {
				b := a
				if a == "BTC" {
					b = "XBT"
				}
				sym = b + "/" + q
			}
			add("book", a, "spot", v, q, "spot/orderbook/history", "base", 60, 120, 300, 0, map[string]string{"exchange": v, "symbol": sym, "interval": "1m", "limit": "3"})
			add("large", a, "spot", v, q, "spot/orderbook/large-limit-order", "base", 0, 300, 720, 1, map[string]string{"exchange": v, "symbol": sym})
			for _, state := range []string{"2", "3"} {
				add("large-history", a, "spot", v, q, "spot/orderbook/v2/large-limit-order-history", "base", 0, 3600, 7800, 4, map[string]string{"exchange": v, "symbol": sym, "limit": "100", "state": state})
				if state == "3" {
					out[len(out)-1].ID += ".revoked"
				}
			}
		}
		add("oi", a, "futures", "", "USD", "futures/open-interest/exchange-list", "USD", 0, 300, 720, 1, map[string]string{"symbol": a})
		add("oi-history", a, "futures", "", "USD", "futures/open-interest/aggregated-history", "USD", 300, 300, 720, 2, map[string]string{"symbol": a, "unit": "usd", "interval": "5m", "limit": "12"})
		out[len(out)-1].Contract = true
		if a == "ETH" {
			out[len(out)-1].Disabled = true
		}
		add("balance-list", a, "chain", "", a, "exchange/balance/list", a, 0, 3600, 9000, 3, map[string]string{"symbol": a})
		out[len(out)-1].Contract = true
		add("balance-history", a, "chain", "", a, "exchange/balance/chart", a, 86400, 21600, 172800, 3, map[string]string{"symbol": a})
		out[len(out)-1].Contract = true
		coin := "bitcoin"
		if a == "ETH" {
			coin = "ethereum"
		}
		add("etf", a, "fund", "", "USD", "etf/"+coin+"/flow-history", "USD", 86400, 86400, 345600, 3, map[string]string{})
		out[len(out)-1].Contract = true
		for _, m := range []string{"spot", "futures"} {
			venues := "Binance,OKX,Bybit"
			if m == "spot" {
				venues = "Binance,OKX,Coinbase,Kraken,Bitfinex"
			}
			add("flow", a, m, "", "USD", m+"/aggregated-taker-buy-sell-volume/history", "USD", 60, 300, 720, 1, map[string]string{"symbol": a, "exchange_list": venues, "interval": "1m", "limit": "12"})
			for _, v := range []string{"Binance", "OKX", "Bybit"} {
				sym := a + "USDT"
				if v == "OKX" {
					sym = a + "-USDT"
				}
				if m == "futures" && v == "OKX" {
					sym = a + "-USDT-SWAP"
				}
				add("footprint", a, m, v, "USDT", m+"/volume/footprint-history", "base/quote/USDT", 300, 900, 2100, 2, map[string]string{"exchange": v, "symbol": sym, "interval": "5m", "limit": "6"})
			}
		}
		add("liquidations", a, "futures", "", "USD", "futures/liquidation/aggregated-history", "USD", 60, 300, 720, 1, map[string]string{"symbol": a, "exchange_list": "Binance,OKX,Bybit", "interval": "1m", "limit": "12"})
		add("map", a, "futures", "", "", "futures/liquidation/aggregated-map", "relative", 0, 900, 2100, 2, map[string]string{"symbol": a, "range": "1d"})
		add("heatmap", a, "futures", "", "", "futures/liquidation/aggregated-heatmap/model1", "relative", 300, 900, 2100, 3, map[string]string{"symbol": a, "range": "24h"})
	}
	add("premium", "BTC", "spot", "Coinbase", "USD", "coinbase-premium-index", "USD/raw rate", 300, 300, 720, 2, map[string]string{"interval": "5m", "limit": "12"})
	out[len(out)-1].Contract = true
	add("whales", "ALL", "futures", "Hyperliquid", "USD", "hyperliquid/whale-position", "USD", 0, WhaleRefreshSeconds, WhaleTTLSeconds, 2, map[string]string{})
	add("funding", "ALL", "futures", "", "", "futures/funding-rate/exchange-list", "percent", 0, 600, 1500, 2, map[string]string{})
	for _, a := range Assets() {
		out = append(out, Dataset{ID: ID("whale-distribution", a, "Hyperliquid", "futures"), Kind: "whale-distribution", Asset: a, Market: "futures", Venue: "Hyperliquid", Source: "derived/coinglass", Quote: "USD", Unit: "USD", Resolution: 60, Refresh: 60, TTL: WhaleTTLSeconds, Retention: "分钟30天 / 1小时90天"})
		out = append(out, Dataset{ID: ID("price", a, "Binance", "spot"), Kind: "price", Asset: a, Market: "spot", Venue: "Binance", Symbol: a + "USDT", Quote: "USDT", Source: "binance", Unit: "USDT", Refresh: 1, TTL: 15, Retention: "仅最新"})
		out = append(out, Dataset{ID: ID("candles", a, "Binance", "spot"), Kind: "candles", Asset: a, Market: "spot", Venue: "Binance", Symbol: a + "USDT", Quote: "USDT", Source: "binance", Unit: "USDT", Resolution: 300, Refresh: 300, TTL: 900, Retention: "5分钟30天 / 1小时90天"})
	}
	out = append(out, Dataset{ID: "fx.usd.kraken", Kind: "fx", Asset: "ALL", Source: "kraken", Venue: "Kraken", Unit: "USD", Refresh: 5, TTL: 30, Retention: "分钟30天 / 小时90天"})
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
func FindDataset(id string) (Dataset, error) {
	for _, d := range Registry() {
		if d.ID == id {
			return d, nil
		}
	}
	return Dataset{}, fmt.Errorf("unknown dataset")
}
