package datahub

import (
	"fmt"
	"github.com/shopspring/decimal"
	"time"
)

// History stores the exact strengths plus native identity inputs. Display-only
// ratios, dollar conversions, repeated contributions and derived IDs are rebuilt
// on read; they are never inputs to a historical decision.
type compactLiquidationZone struct {
	Low        float64    `json:"p"`
	Side       string     `json:"s"`
	Strength   string     `json:"n"`
	First      time.Time  `json:"f"`
	Last       time.Time  `json:"l"`
	Continuous time.Time  `json:"c"`
	Samples    int        `json:"a"`
	Missing    int        `json:"m,omitempty"`
	State      string     `json:"t"`
	Trend      string     `json:"d"`
	Change     *float64   `json:"v,omitempty"`
	Touch      *time.Time `json:"h,omitempty"`
	Cross      *time.Time `json:"x,omitempty"`
	Reclaim    *time.Time `json:"r,omitempty"`
	Uncertain  string     `json:"u,omitempty"`
}
type compactLiquidationHistory struct {
	Schema    int                      `json:"schema"`
	Asset     string                   `json:"asset"`
	Dataset   string                   `json:"dataset"`
	Rule      string                   `json:"rulesVersion"`
	Model     string                   `json:"model"`
	Contract  string                   `json:"contract"`
	Period    string                   `json:"period"`
	Revision  string                   `json:"revision"`
	Reference float64                  `json:"reference"`
	Quote     string                   `json:"quote"`
	Maximum   string                   `json:"maximum"`
	Coverage  []string                 `json:"coverage"`
	SourceAt  *time.Time               `json:"sourceAt"`
	Fetched   time.Time                `json:"fetchedAt"`
	Available time.Time                `json:"availableAt"`
	Zones     []compactLiquidationZone `json:"distribution"`
}

func compactLiquidationSnapshot(s LiquidationMapSnapshot) compactLiquidationHistory {
	h := compactLiquidationHistory{Schema: 1, Asset: s.Asset, Dataset: s.Dataset, Rule: s.Rule, Model: s.Model, Contract: s.Contract, Period: s.Period, Revision: s.Revision, Reference: s.Reference, Quote: s.Quote, Maximum: s.Maximum, Coverage: s.Coverage, SourceAt: s.SourceAt, Fetched: s.Fetched, Available: s.Available, Zones: []compactLiquidationZone{}}
	for _, z := range s.Zones {
		h.Zones = append(h.Zones, compactLiquidationZone{Low: z.Low, Side: z.Side, Strength: z.Strength, First: z.First, Last: z.Last, Continuous: z.Continuous, Samples: z.Samples, Missing: z.Missing, State: z.State, Trend: z.Trend, Change: z.Change, Touch: z.Touch, Cross: z.Cross, Reclaim: z.Reclaim, Uncertain: z.Uncertain})
	}
	return h
}
func decodeLiquidationHistory(b []byte) ([]LiquidationZone, error) {
	var h compactLiquidationHistory
	if e := liquidationDecode(b, &h); e != nil {
		return nil, e
	}
	if h.Schema != 1 {
		return nil, fmt.Errorf("不支持的区域历史契约 %d", h.Schema)
	}
	out := make([]LiquidationZone, 0, len(h.Zones))
	for _, c := range h.Zones {
		key := h.Quote + "/" + decimal.NewFromFloat(c.Low).String()
		z := LiquidationZone{ID: liquidationHash(h.Dataset + "/" + key + "/" + c.First.Format(time.RFC3339Nano)), Key: key, Quote: h.Quote, Low: c.Low, High: c.Low + liquidationStep(h.Asset), Side: c.Side, Strength: c.Strength, First: c.First, Last: c.Last, Continuous: c.Continuous, Samples: c.Samples, Missing: c.Missing, State: c.State, Trend: c.Trend, Change: c.Change, Touch: c.Touch, Cross: c.Cross, Reclaim: c.Reclaim, Uncertain: c.Uncertain}
		if dec(h.Maximum).IsPositive() {
			z.Relative, _ = dec(z.Strength).Div(dec(h.Maximum)).Mul(decimal.NewFromInt(100)).Float64()
		}
		out = append(out, z)
	}
	return out, nil
}
