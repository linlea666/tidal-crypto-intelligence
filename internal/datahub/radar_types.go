package datahub

import (
	"github.com/shopspring/decimal"
	"time"
)

const RadarRules = "hl-radar-1"
const radarBudget int64 = 512 << 20
const radarNoticeTTL = 5 * time.Minute

type radarFill struct {
	Coin  string `json:"coin"`
	Px    string `json:"px"`
	Sz    string `json:"sz"`
	Side  string `json:"side"`
	Start string `json:"startPosition"`
	Time  int64  `json:"time"`
	TID   int64  `json:"tid"`
	Hash  string `json:"hash"`
}
type radarLedger struct {
	Time  int64  `json:"time"`
	Hash  string `json:"hash"`
	Delta struct {
		Type        string `json:"type"`
		USDC        string `json:"usdc"`
		User        string `json:"user"`
		Destination string `json:"destination"`
	} `json:"delta"`
}
type radarAccount struct {
	Time   int64 `json:"time"`
	Margin struct {
		Equity string `json:"accountValue"`
		Total  string `json:"totalNtlPos"`
	} `json:"marginSummary"`
	Positions []struct {
		Position struct {
			Coin        string  `json:"coin"`
			Size        string  `json:"szi"`
			Value       string  `json:"positionValue"`
			Entry       string  `json:"entryPx"`
			Liquidation *string `json:"liquidationPx"`
			Leverage    struct {
				Value int    `json:"value"`
				Type  string `json:"type"`
			} `json:"leverage"`
		} `json:"position"`
	} `json:"assetPositions"`
}
type RadarWallet struct {
	LedgerComplete  bool          `json:"ledgerComplete"`
	LedgerThrough   time.Time     `json:"ledgerThrough"`
	Address         string        `json:"address"`
	FirstSeen       time.Time     `json:"firstSeenLocal"`
	Earliest        *time.Time    `json:"earliestActivityObserved"`
	HistoryFrom     time.Time     `json:"historyFrom"`
	HistoryThrough  time.Time     `json:"historyThrough"`
	HistoryComplete bool          `json:"historyComplete"`
	Age             string        `json:"age"`
	Role            string        `json:"role"`
	Master          string        `json:"master,omitempty"`
	Updated         time.Time     `json:"updatedAt"`
	Ledger          []radarLedger `json:"ledger"`
	HistoryNote     string        `json:"historyNote"`
}
type RadarEvent struct {
	ID                 string     `json:"id"`
	Rules              string     `json:"rulesVersion"`
	Address            string     `json:"address"`
	Asset              string     `json:"asset"`
	Side               string     `json:"side"`
	Opened             time.Time  `json:"positionOpenedAt"`
	Detected           time.Time  `json:"detectedAt"`
	Updated            time.Time  `json:"verifiedAt"`
	Through            time.Time  `json:"dataThrough"`
	Threshold          *time.Time `json:"thresholdAt"`
	Closed             *time.Time `json:"closedAt"`
	Size               string     `json:"size"`
	OpenedQuantity     string     `json:"openedQuantity"`
	OpenedNative       string     `json:"openedNotionalUSDC"`
	Native             string     `json:"notionalUSDC"`
	USDCents           *int64     `json:"usdCents"`
	Entry              string     `json:"entry"`
	Liquidation        *string    `json:"liquidation"`
	FX                 string     `json:"fx"`
	FXAt               *time.Time `json:"fxAt"`
	ReferenceUSD       string     `json:"referenceUSD"`
	ConfiguredLeverage int        `json:"configuredLeverage"`
	EffectiveLeverage  *string    `json:"effectiveLeverage"`
	Concentration      *string    `json:"concentration"`
	DepositCents       *int64     `json:"depositCents"`
	Rapid              bool       `json:"rapidFunding"`
	Concentrated       bool       `json:"concentrated"`
	Age                string     `json:"age"`
	HistoryComplete    bool       `json:"historyComplete"`
	LiveOpening        bool       `json:"liveOpening"`
	Verified           bool       `json:"verified"`
	Level              string     `json:"level"`
	Group              string     `json:"groupId,omitempty"`
	Members            []string   `json:"members"`
	Reasons            []string   `json:"reasons"`
	InitialRecorded    bool       `json:"initialRecorded"`
	UpgradeRecorded    bool       `json:"upgradeRecorded"`
	Context            []string   `json:"context"`
}
type radarPosition struct {
	Size  string `json:"size"`
	Event string `json:"event"`
	Last  int64  `json:"last"`
}
type radarNotice struct {
	ID      string     `json:"id"`
	Kind    string     `json:"kind"`
	At      time.Time  `json:"at"`
	Expires time.Time  `json:"expires"`
	Event   RadarEvent `json:"event"`
}
type RadarSettings struct {
	EmailEnabled bool `json:"emailEnabled"`
}
type radarHealth struct {
	GapFrom      *time.Time `json:"gapFrom"`
	GapThrough   *time.Time `json:"gapThrough"`
	Connected    bool       `json:"connected"`
	LastMessage  *time.Time `json:"lastMessage"`
	LastVerified *time.Time `json:"lastVerified"`
	LastError    string     `json:"lastError"`
	LastErrorAt  *time.Time `json:"lastErrorAt"`
	Gaps         int64      `json:"gaps"`
	Dropped      int64      `json:"dropped"`
	Candidates   int        `json:"candidates"`
	Queued       int        `json:"queued"`
	WSUsers      int        `json:"websocketUsers"`
	Weight       int        `json:"weightLastMinute"`
}

func radarNumber(s string) (decimal.Decimal, bool) {
	if len(s) == 0 || len(s) > 64 {
		return decimal.Zero, false
	}
	n, e := decimal.NewFromString(s)
	return n, e == nil && len(s) < 100 && n.Exponent() >= -18 && n.Exponent() <= 18 && n.Abs().LessThanOrEqual(decimal.NewFromInt(1000000000000))
}
func radarMoney(n decimal.Decimal) (int64, bool) {
	// Bounded before IntPart: neither overflow nor invalid amounts become zero.
	if n.IsNegative() || n.GreaterThan(decimal.NewFromInt(1000000000000)) {
		return 0, false
	}
	return n.Mul(decimal.NewFromInt(100)).Round(0).IntPart(), true
}
func radarTime(t int64) time.Time { return time.UnixMilli(t).UTC() }
func radarSide(n decimal.Decimal) string {
	if n.IsNegative() {
		return "short"
	}
	return "long"
}
