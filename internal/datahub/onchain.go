package datahub

// This module describes last-moved output supply, not executed trades or entities.
import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/shopspring/decimal"
)

const OnchainRules = "onchain-cost-1"
const onchainSource = "BlockHorizon"
const onchainURL = "https://zbvrubdalcojapjygbcz.supabase.co/functions/v1/bundle_handler"
const onchainBudget int64 = 128 << 20
const onchainMethod = "blockhorizon-last-moved-positive-acquisition-cohorts-155-v1"
const onchainMethodNote = "按最后移动取得日收盘价归桶；STH 为最近155个仍有正余额的取得日分组，LTH 为更早分组。分类会随币龄变化；不是实际交易成本，未核实实体调整。"
const onchainSourceDescription = "Remaining output supply grouped by the closing price on its acquisition day. STH contains the 155 most recent acquisition-day cohorts with positive remaining balances; LTH contains the older cohorts. Bucket widths adapt to the price range. This is last-moved cost basis, not a record of trades. The side profile and price marker use the same observation date. Updated automatically by the daily engine after each completed UTC day. Replay preserves historical monthly frames and completed weekly Sunday frames, plus one temporary point for the latest completed day. Each new day replaces the previous temporary point."

type CostCohort struct {
	Start  string   `json:"start"`
	Step   string   `json:"step"`
	Values []string `json:"values"`
	Total  string   `json:"total"`
}
type CostFrame struct {
	Date      string     `json:"date"`
	Price     string     `json:"price"`
	STH       CostCohort `json:"sth"`
	LTH       CostCohort `json:"lth"`
	Method    string     `json:"method"`
	Revision  string     `json:"revision"`
	FirstSeen time.Time  `json:"firstSeen"`
	BuiltAt   *time.Time `json:"builtAt"`
	Origin    string     `json:"origin"`
}
type CostPrice struct {
	Date      string    `json:"date"`
	Value     string    `json:"value"`
	Revision  string    `json:"revision"`
	FirstSeen time.Time `json:"firstSeen"`
}
type CostBounds struct {
	Lower string `json:"lower"`
	Upper string `json:"upper"`
}
type CostSupply struct {
	STH   CostBounds `json:"sth"`
	LTH   CostBounds `json:"lth"`
	Total CostBounds `json:"total"`
}
type CostBin struct {
	Low   string `json:"low"`
	High  string `json:"high"`
	STH   string `json:"sth"`
	LTH   string `json:"lth"`
	Total string `json:"total"`
}
type CostZone struct {
	Side   string `json:"side"`
	Low    string `json:"low"`
	High   string `json:"high"`
	Supply string `json:"supply"`
}
type CostMetrics struct {
	Concentration     map[string]CostBounds `json:"concentration"`
	Denominator       string                `json:"denominator"`
	Volatility        *float64              `json:"volatility"`
	ConcentrationRank *CostBounds           `json:"concentrationRank"`
	VolatilityRank    *float64              `json:"volatilityRank"`
	BaselineSamples   int                   `json:"baselineSamples"`
	VolatilitySamples int                   `json:"volatilitySamples"`
	Zones             []CostZone            `json:"zones"`
}
type CostSettings struct {
	EmailEnabled bool `json:"emailEnabled"`
}
type CostFeed struct {
	Version     string     `json:"version"`
	ETag        string     `json:"etag"`
	FullETag    string     `json:"fullEtag"`
	LastCheck   *time.Time `json:"lastCheck"`
	LastAttempt *time.Time `json:"lastAttempt"`
	LastFull    *time.Time `json:"lastFull"`
	NextAttempt *time.Time `json:"nextAttempt"`
	LastDate    string     `json:"lastDate"`
	LastError   string     `json:"lastError"`
	Disabled    bool       `json:"disabled"`
	Failures    int        `json:"failures"`
}

func costDay(s string) (time.Time, error) {
	t, e := time.Parse("2006-01-02", s)
	if e != nil || t.Year() < 2009 || t.Year() > 2200 {
		return time.Time{}, errors.New("无效链上日期")
	}
	return t, nil
}
func costDate(t time.Time) string { return t.UTC().Format("2006-01-02") }
func costHash(v any) string {
	b, _ := json.Marshal(v)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:16])
}
func costNumber(s string, positive bool) (decimal.Decimal, error) {
	if len(s) == 0 || len(s) > 32 {
		return decimal.Zero, errors.New("链上数值长度异常")
	}
	d, e := decimal.NewFromString(s)
	if e != nil || d.IsNegative() || (positive && !d.IsPositive()) || d.Exponent() < -12 || d.GreaterThan(decimal.NewFromInt(1e12)) {
		return decimal.Zero, errors.New("链上数值范围异常")
	}
	return d, nil
}
func validateCostCohort(c CostCohort) error {
	if len(c.Values) == 0 || len(c.Values) > 20000 {
		return errors.New("缺少成本桶或超过桶数上限")
	}
	if _, e := costNumber(c.Start, false); e != nil {
		return e
	}
	if _, e := costNumber(c.Step, true); e != nil {
		return e
	}
	total, e := costNumber(c.Total, false)
	if e != nil {
		return e
	}
	sum := decimal.Zero
	for _, v := range c.Values {
		n, e := costNumber(v, false)
		if e != nil {
			return e
		}
		sum = sum.Add(n)
	}
	if !sum.Equal(total) {
		return errors.New("分桶供给与序列总量不一致")
	}
	return nil
}
func validateCostFrame(f CostFrame) error {
	if _, e := costDay(f.Date); e != nil {
		return e
	}
	if _, e := costNumber(f.Price, true); e != nil {
		return e
	}
	if f.Method != onchainMethod {
		return errors.New("链上成本方法版本未知")
	}
	if e := validateCostCohort(f.STH); e != nil {
		return e
	}
	if e := validateCostCohort(f.LTH); e != nil {
		return e
	}
	total := dec(f.STH.Total).Add(dec(f.LTH.Total))
	if !total.IsPositive() || total.GreaterThan(decimal.NewFromInt(21000000)) {
		return errors.New("供给分母异常")
	}
	return nil
}

// Bounds account for whole buckets only. No uniform-within-bucket assumption.
func costInterval(c CostCohort, low, high decimal.Decimal) CostBounds {
	lo, hi := decimal.Zero, decimal.Zero
	start, step := dec(c.Start), dec(c.Step)
	for i, v := range c.Values {
		a := start.Add(step.Mul(decimal.NewFromInt(int64(i))))
		b := a.Add(step)
		if a.GreaterThanOrEqual(high) {
			break
		}
		if b.LessThanOrEqual(low) {
			continue
		}
		n := dec(v)
		hi = hi.Add(n)
		if a.GreaterThanOrEqual(low) && b.LessThanOrEqual(high) {
			lo = lo.Add(n)
		}
	}
	return CostBounds{lo.String(), hi.String()}
}
func costIntervalSupply(f CostFrame, low, high decimal.Decimal) CostSupply {
	a, b := costInterval(f.STH, low, high), costInterval(f.LTH, low, high)
	return CostSupply{a, b, CostBounds{dec(a.Lower).Add(dec(b.Lower)).String(), dec(a.Upper).Add(dec(b.Upper)).String()}}
}
func costConcentration(f CostFrame, percent string) CostBounds {
	p, w := dec(f.Price), dec(percent).Div(decimal.NewFromInt(100))
	s := costIntervalSupply(f, p.Mul(decimal.NewFromInt(1).Sub(w)), p.Mul(decimal.NewFromInt(1).Add(w))).Total
	d := dec(f.STH.Total).Add(dec(f.LTH.Total))
	return CostBounds{dec(s.Lower).Div(d).Mul(decimal.NewFromInt(100)).String(), dec(s.Upper).Div(d).Mul(decimal.NewFromInt(100)).String()}
}
func costDifference(a, b CostSupply) CostSupply {
	diff := func(x, y CostBounds) CostBounds {
		return CostBounds{dec(x.Lower).Sub(dec(y.Upper)).String(), dec(x.Upper).Sub(dec(y.Lower)).String()}
	}
	return CostSupply{diff(a.STH, b.STH), diff(a.LTH, b.LTH), diff(a.Total, b.Total)}
}
func costBins(f CostFrame, requested string) ([]CostBin, string, error) {
	step := dec(requested)
	origin := decimal.Zero
	if !step.IsPositive() {
		return nil, "", errors.New("无效桶宽")
	}
	// If the requested grid is incompatible, preserve the source grid only when
	// both cohorts share it. Never smear a bucket across display boundaries.
	for _, c := range []CostCohort{f.STH, f.LTH} {
		if !step.Mod(dec(c.Step)).IsZero() || !dec(c.Start).Mod(dec(c.Step)).IsZero() || !dec(c.Start).Mod(step).IsZero() {
			if f.STH.Start != f.LTH.Start || f.STH.Step != f.LTH.Step {
				return nil, "", errors.New("两类原生桶网格不同，不能精确合并")
			}
			step = dec(f.STH.Step)
			origin = dec(f.STH.Start)
			break
		}
	}
	bins := map[string]*CostBin{}
	for j, c := range []CostCohort{f.STH, f.LTH} {
		for i, v := range c.Values {
			if dec(v).IsZero() {
				continue
			}
			a := dec(c.Start).Add(dec(c.Step).Mul(decimal.NewFromInt(int64(i))))
			low := a.Sub(origin).Div(step).Floor().Mul(step).Add(origin)
			if a.Add(dec(c.Step)).GreaterThan(low.Add(step)) {
				return nil, "", errors.New("原生桶边界无法精确聚合")
			}
			k := low.String()
			b := bins[k]
			if b == nil {
				b = &CostBin{Low: k, High: low.Add(step).String(), STH: "0", LTH: "0", Total: "0"}
				bins[k] = b
			}
			if j == 0 {
				b.STH = dec(b.STH).Add(dec(v)).String()
			} else {
				b.LTH = dec(b.LTH).Add(dec(v)).String()
			}
			b.Total = dec(b.Total).Add(dec(v)).String()
		}
	}
	out := make([]CostBin, 0, len(bins))
	for _, v := range bins {
		out = append(out, *v)
	}
	sort.Slice(out, func(i, j int) bool { return dec(out[i].Low).LessThan(dec(out[j].Low)) })
	return out, step.String(), nil
}
func costZones(f CostFrame) []CostZone {
	out := []CostZone{}
	bins, step, e := costBins(f, "1000")
	if e != nil || step != "1000" {
		return out
	}
	byLow := map[string]string{}
	starts := map[string]decimal.Decimal{}
	for _, b := range bins {
		if !dec(b.Low).Mod(dec("1000")).IsZero() {
			return out
		}
		byLow[b.Low] = b.Total
		// A positive three-bucket window includes at least one observed nonzero
		// bucket. Enumerating these starts bounds work even for extreme prices.
		for j := int64(0); j < 3; j++ {
			low := dec(b.Low).Sub(decimal.NewFromInt(j * 1000))
			starts[low.String()] = low
		}
	}
	p := dec(f.Price)
	k := decimal.NewFromInt(1000)
	min, max := p.Mul(dec("0.8")), p.Mul(dec("1.2"))
	for _, side := range []string{"above", "below"} {
		var best *CostZone
		bestDistance := decimal.Zero
		for _, low := range starts {
			high := low.Add(dec("3000"))
			if low.LessThan(min) || high.GreaterThan(max) {
				continue
			}
			if side == "above" && low.LessThan(p) || side == "below" && high.GreaterThan(p) {
				continue
			}
			sum := decimal.Zero
			for j := int64(0); j < 3; j++ {
				sum = sum.Add(dec(byLow[low.Add(k.Mul(decimal.NewFromInt(j))).String()]))
			}
			if !sum.IsPositive() {
				continue
			}
			distance := low.Sub(p).Abs()
			if side == "below" {
				distance = p.Sub(high).Abs()
			}
			if best == nil || sum.GreaterThan(dec(best.Supply)) || sum.Equal(dec(best.Supply)) && distance.LessThan(bestDistance) {
				best = &CostZone{side, low.String(), high.String(), sum.String()}
				bestDistance = distance
			}
		}
		if best != nil {
			out = append(out, *best)
		}
	}
	return out
}
func costVolatility(day string, prices map[string]string) *float64 {
	end, e := costDay(day)
	if e != nil {
		return nil
	}
	r := make([]float64, 0, 21)
	for i := 21; i > 0; i-- {
		a, ok := prices[costDate(end.AddDate(0, 0, -i))]
		b, ok2 := prices[costDate(end.AddDate(0, 0, -i+1))]
		if !ok || !ok2 {
			return nil
		}
		x, y := dec(a).InexactFloat64(), dec(b).InexactFloat64()
		if x <= 0 || y <= 0 {
			return nil
		}
		r = append(r, math.Log(y/x))
	}
	mean := 0.
	for _, v := range r {
		mean += v
	}
	mean /= 21
	s := 0.
	for _, v := range r {
		s += (v - mean) * (v - mean)
	}
	v := math.Sqrt(s/20) * math.Sqrt(365) * 100
	return &v
}

func fmtCostFloat(v float64) string { return strconv.FormatFloat(v, 'f', 8, 64) }
