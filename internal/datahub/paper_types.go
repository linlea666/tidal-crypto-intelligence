package datahub

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/shopspring/decimal"
)

// Changing an experiment parameter requires a new version, never rewriting an
// existing account. No account credentials or order API exist in this module.
const PaperRules = "btc-perpetual-paper-1.0"
const PaperBudget int64 = 128 << 20
const paperReserve int64 = 16 << 20

var paperFee = decimal.RequireFromString("0.0005")
var paperSlip = decimal.RequireFromString("0.0002")
var paperNotional = decimal.NewFromInt(1000)
var paperInitial = decimal.NewFromInt(10000)

type paperQuote struct {
	ID      int64           `json:"id"`
	At      time.Time       `json:"receivedAt"`
	EventAt time.Time       `json:"eventAt"`
	Bid     decimal.Decimal `json:"bid"`
	Ask     decimal.Decimal `json:"ask"`
	BidQty  decimal.Decimal `json:"bidQty"`
	AskQty  decimal.Decimal `json:"askQty"`
}

func (q paperQuote) valid(now time.Time) bool {
	return q.ID > 0 && q.Bid.IsPositive() && q.Ask.GreaterThanOrEqual(q.Bid) && q.BidQty.IsPositive() && q.AskQty.IsPositive() &&
		!q.At.After(now) && now.Sub(q.At) <= 5*time.Second && !q.EventAt.After(now.Add(time.Second)) && now.Sub(q.EventAt) <= 5*time.Second
}

type paperInstrument struct {
	At          time.Time       `json:"at"`
	Status      string          `json:"status"`
	Step        decimal.Decimal `json:"step"`
	Minimum     decimal.Decimal `json:"minimumQuantity"`
	Maximum     decimal.Decimal `json:"maximumQuantity"`
	MinNotional decimal.Decimal `json:"minimumNotional"`
}

func (m paperInstrument) valid(at time.Time) bool {
	return m.Status == "TRADING" && m.Step.IsPositive() && m.Maximum.IsPositive() && !m.At.After(at) && at.Sub(m.At) <= 6*time.Hour
}

type paperIntent struct {
	Signal     Signal          `json:"signal"`
	Seen       time.Time       `json:"actionableAt"`
	After      time.Time       `json:"eligibleAfter"`
	ATR        decimal.Decimal `json:"atr"`
	ATRThrough time.Time       `json:"atrThrough"`
}
type paperTrade struct {
	ID         string           `json:"id"`
	Group      string           `json:"group"`
	Signal     Signal           `json:"signal"`
	Seen       time.Time        `json:"actionableAt"`
	Entered    time.Time        `json:"enteredAt"`
	Exited     *time.Time       `json:"exitedAt"`
	Entry      decimal.Decimal  `json:"entryPrice"`
	Quantity   decimal.Decimal  `json:"quantity"`
	Remaining  decimal.Decimal  `json:"remaining"`
	ATR        decimal.Decimal  `json:"atr"`
	ATRThrough time.Time        `json:"atrThrough"`
	Stop       *decimal.Decimal `json:"stop"`
	Target     *decimal.Decimal `json:"target"`
	Gross      decimal.Decimal  `json:"grossPnl"`
	Fees       decimal.Decimal  `json:"fees"`
	Slippage   decimal.Decimal  `json:"slippageCost"`
	Funding    decimal.Decimal  `json:"funding"`
	MFE        decimal.Decimal  `json:"mfePerUnit"`
	MAE        decimal.Decimal  `json:"maePerUnit"`
	MFEAt      time.Time        `json:"mfeAt"`
	MAEAt      time.Time        `json:"maeAt"`
	Quality    []string         `json:"quality"`
	ExitReason string           `json:"exitReason"`
	ExitAfter  time.Time        `json:"exitEligibleAfter"`
	FillCount  int              `json:"fillCount"`
}

func (t paperTrade) sign() decimal.Decimal {
	if t.Signal.Direction == "sell" {
		return decimal.NewFromInt(-1)
	}
	return decimal.NewFromInt(1)
}
func (t *paperTrade) flag(reason string) {
	for _, v := range t.Quality {
		if v == reason {
			return
		}
	}
	t.Quality = append(t.Quality, reason)
}

type paperFill struct {
	ID       string          `json:"id"`
	TradeID  string          `json:"tradeId"`
	Group    string          `json:"group"`
	Kind     string          `json:"kind"`
	Side     string          `json:"side"`
	At       time.Time       `json:"at"`
	Price    decimal.Decimal `json:"price"`
	Quantity decimal.Decimal `json:"quantity"`
	Fee      decimal.Decimal `json:"fee"`
	Slippage decimal.Decimal `json:"slippageCost"`
	Quote    paperQuote      `json:"quote"`
}
type paperFunding struct {
	At       time.Time       `json:"settledAt"`
	Acquired time.Time       `json:"acquiredAt"`
	Rate     decimal.Decimal `json:"rate"`
	Mark     decimal.Decimal `json:"markPrice"`
}
type paperFundingEntry struct {
	TradeID  string          `json:"tradeId"`
	Group    string          `json:"group"`
	Funding  paperFunding    `json:"settlement"`
	Quantity decimal.Decimal `json:"quantity"`
	Amount   decimal.Decimal `json:"amount"`
}
type paperIntake struct {
	ID       string    `json:"id"`
	Group    string    `json:"group"`
	SignalID string    `json:"signalId"`
	At       time.Time `json:"at"`
	State    string    `json:"state"`
	Reason   string    `json:"reason"`
	Signal   Signal    `json:"signal"`
}
type paperAccount struct {
	Group    string          `json:"group"`
	Gross    decimal.Decimal `json:"grossPnl"`
	Fees     decimal.Decimal `json:"fees"`
	Funding  decimal.Decimal `json:"funding"`
	Position *paperTrade     `json:"position"`
	Pending  *paperIntent    `json:"pending"`
}

func (a paperAccount) cash() decimal.Decimal {
	return paperInitial.Add(a.Gross).Sub(a.Fees).Add(a.Funding)
}

type paperState struct {
	Parameters      json.RawMessage `json:"parameters"`
	Version         string          `json:"version"`
	Generation      string          `json:"generation"`
	Origin          *time.Time      `json:"origin"`
	Cursor          int64           `json:"cursor"`
	Source          string          `json:"sourceGeneration"`
	At              time.Time       `json:"at"`
	Accounts        []paperAccount  `json:"accounts"`
	FundingThrough  time.Time       `json:"fundingThrough"`
	FundingConflict *time.Time      `json:"fundingConflictFrom"`
	ExpectedFunding []int64         `json:"expectedFunding"`
	SettledFunding  []int64         `json:"settledFunding"`
	GoodSince       time.Time       `json:"goodSince"`
	Gap             bool            `json:"gap"`
	Pause           string          `json:"pauseReason"`
	LastFailure     string          `json:"lastFailure"`
	LastFailureAt   *time.Time      `json:"lastFailureAt"`
	ObservedSeconds int64           `json:"observedSeconds"`
	CoveredSeconds  int64           `json:"coveredSeconds"`
	LastSample      time.Time       `json:"lastSample"`
	LastTick        time.Time       `json:"lastTick"`
	EquityGap       bool            `json:"equityGap"`
}

func paperParameters() json.RawMessage {
	b, _ := json.Marshal(map[string]any{"version": PaperRules, "contract": "Binance BTCUSDT USDT perpetual", "initialCapital": paperInitial, "notional": paperNotional, "feePerSide": paperFee, "adverseSlippage": paperSlip, "latencySeconds": 1, "quoteMaxAgeSeconds": 5, "signalMaxAgeSeconds": 30, "recoverySeconds": 30, "riskStopATR": 1, "riskTargetATR": 2, "riskMaxHoldHours": 4, "atrHours": 14, "leverageSimulation": false, "groups": []string{"opposite", "risk"}})
	return b
}

type paperEvent struct {
	At     time.Time `json:"at"`
	Kind   string    `json:"kind"`
	Reason string    `json:"reason"`
}
type paperEquity struct {
	At              time.Time        `json:"at"`
	Group           string           `json:"group"`
	BeforeFunding   *decimal.Decimal `json:"beforeFunding"`
	Unrealized      *decimal.Decimal `json:"unrealized"`
	Occupied        bool             `json:"occupied"`
	CoveredSeconds  int64            `json:"coveredSeconds"`
	ObservedSeconds int64            `json:"observedSeconds"`
}

func paperDecimal(v string, positive bool) (decimal.Decimal, error) {
	d, err := decimal.NewFromString(v)
	if err != nil || d.Exponent() < -18 || d.Exponent() > 12 || len(v) > 48 || (positive && !d.IsPositive()) {
		return decimal.Zero, errors.New("invalid perpetual decimal")
	}
	return d, nil
}
