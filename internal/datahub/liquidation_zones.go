package datahub

// Liquidation risk is deliberately independent of spot walls and mail rules.
// Native quotes define identity; USD is only a frozen presentation conversion.
import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/shopspring/decimal"
)

const LiquidationContract = "coinglass-intensity-native-v1"
const LiquidationRules = "liquidation-zones-1"
const liquidationBudget = 64 << 20
const liquidationWorkLimit = 16 << 20
const liquidationRowLimit = 1 << 20

type ZoneContribution struct {
	Venue      string `json:"venue"`
	Instrument string `json:"instrument"`
	Quote      string `json:"quote"`
	Strength   string `json:"strength"`
}
type LiquidationZone struct {
	ID             string             `json:"id"`
	Key            string             `json:"key"`
	Low            float64            `json:"low"`
	High           float64            `json:"high"`
	Quote          string             `json:"quote"`
	Side           string             `json:"side"`
	Strength       string             `json:"strength"`
	Relative       float64            `json:"relative"`
	Contributions  []ZoneContribution `json:"contributions"`
	First          time.Time          `json:"firstVisibleAt"`
	Last           time.Time          `json:"lastSeenAt"`
	Continuous     time.Time          `json:"continuousSince"`
	Samples        int                `json:"samples"`
	Missing        int                `json:"missingSamples"`
	State          string             `json:"state"`
	Trend          string             `json:"trend"`
	Change         *float64           `json:"changePercent"`
	Comparable     string             `json:"comparable"`
	Touch          *time.Time         `json:"touchedAt"`
	Cross          *time.Time         `json:"crossedAt"`
	Reclaim        *time.Time         `json:"reclaimedAt"`
	Uncertain      string             `json:"uncertain,omitempty"`
	LastBar        time.Time          `json:"lastBar"`
	PreviousClose  float64            `json:"previousClose"`
	FarCloses      int                `json:"farCloses"`
	NearCloses     int                `json:"nearCloses"`
	Distance       *float64           `json:"distancePercent"`
	DistanceNative *float64           `json:"distanceNative"`
	LowUSD         *float64           `json:"lowUsd"`
	HighUSD        *float64           `json:"highUsd"`
	WhaleCents     *int64             `json:"whaleCents"`
	WhaleCount     *int               `json:"whaleCount"`
	Eligible       bool               `json:"eligible"`
}
type LiquidationMapSnapshot struct {
	Asset       string            `json:"asset"`
	Period      string            `json:"period"`
	Dataset     string            `json:"dataset"`
	Revision    string            `json:"revision"`
	Rule        string            `json:"rulesVersion"`
	Model       string            `json:"model"`
	Contract    string            `json:"contract"`
	Quote       string            `json:"quote"`
	Reference   float64           `json:"reference"`
	MarketPrice *float64          `json:"marketPrice"`
	MarketAt    *time.Time        `json:"marketAt"`
	Fetched     time.Time         `json:"fetchedAt"`
	SourceAt    *time.Time        `json:"sourceAt"`
	Available   time.Time         `json:"availableAt"`
	Complete    bool              `json:"complete"`
	Reason      string            `json:"reason"`
	Coverage    []string          `json:"coverage"`
	Comparable  string            `json:"comparable"`
	Maximum     string            `json:"maximum"`
	Zones       []LiquidationZone `json:"zones"`
}

func liquidationHash(v string) string { return fmt.Sprintf("%x", sha256.Sum256([]byte(v)))[:24] }
func liquidationStep(asset string) float64 {
	if asset == "ETH" {
		return 10
	}
	return 250
}
func liquidationPeriod(d Dataset) string {
	if strings.HasSuffix(d.ID, "@7d") {
		return "7d"
	}
	if strings.HasSuffix(d.ID, "@30d") {
		return "30d"
	}
	return "24h"
}

// Only the contract with explicit quote/instrument identity is eligible. Legacy
// snapshots remain readable, but never acquire invented units or availability.
func makeLiquidationMap(d Dataset, o Observation, available time.Time) (LiquidationMapSnapshot, error) {
	s := LiquidationMapSnapshot{Asset: d.Asset, Period: liquidationPeriod(d), Dataset: d.ID, Revision: o.Revision, Rule: LiquidationRules, Fetched: o.FetchedAt, SourceAt: o.ObservedAt, Available: available, Zones: []LiquidationZone{}, Coverage: []string{}, Maximum: "0"}
	m := o.Payload.Model
	if m == nil {
		return s, errors.New("模型缺失")
	}
	if len(m.Bins) > 12000 {
		return s, errors.New("模型超出有界计算容量")
	}
	s.Model, s.Contract, s.Reference = m.Model, m.Contract, m.ReferencePrice
	s.Complete = m.CoverageComplete && m.Contract == LiquidationContract && s.Reference > 0 && o.Quality != "missing"
	groups := map[string]*LiquidationZone{}
	seen := map[string]string{}
	coverage := map[string]bool{}
	quotes := map[string]bool{}
	for _, b := range m.Bins {
		pv, sv := b.NativePrice, b.RawStrength
		if pv == "" {
			pv = fmt.Sprint(b.Price)
		}
		if sv == "" {
			sv = fmt.Sprint(b.Strength)
		}
		p, pe := decimal.NewFromString(pv)
		n, ne := decimal.NewFromString(sv)
		if pe != nil || ne != nil || !p.IsPositive() || n.IsNegative() || n.GreaterThan(decimal.NewFromInt(1e15)) {
			return s, errors.New("非法模型数值")
		}
		if b.Quote == "" || b.Instrument == "" || b.Venue == "" {
			s.Complete = false
		}
		source := b.Venue + "/" + b.Instrument + "/" + b.Quote
		k := source + "/" + p.String()
		if old, ok := seen[k]; ok {
			if old != n.String() {
				return s, errors.New("重复合约强度冲突")
			}
			continue
		}
		seen[k] = n.String()
		coverage[source] = true
		quotes[b.Quote] = true
		step := decimal.NewFromFloat(liquidationStep(d.Asset))
		low := p.Div(step).Floor().Mul(step)
		key := b.Quote + "/" + low.String()
		z := groups[key]
		if z == nil {
			v, _ := low.Float64()
			z = &LiquidationZone{Key: key, Low: v, High: v + liquidationStep(d.Asset), Quote: b.Quote, Strength: "0", Contributions: []ZoneContribution{}}
			groups[key] = z
		}
		z.Strength = dec(z.Strength).Add(n).String()
		found := false
		for i, c := range z.Contributions {
			if c.Venue == b.Venue && c.Instrument == b.Instrument {
				z.Contributions[i].Strength = dec(c.Strength).Add(n).String()
				found = true
				break
			}
		}
		if !found {
			z.Contributions = append(z.Contributions, ZoneContribution{b.Venue, b.Instrument, b.Quote, n.String()})
		}
	}
	if len(groups) > 2000 {
		return s, errors.New("区域数量超过容量上限")
	}
	for c := range coverage {
		s.Coverage = append(s.Coverage, c)
	}
	sort.Strings(s.Coverage)
	if len(quotes) != 1 {
		s.Complete = false
		s.Reason = "混合报价，暂停跨币种合并与重点判断"
	} else {
		for q := range quotes {
			s.Quote = q
		}
	}
	for _, z := range groups {
		z.Side = "neutral"
		if s.Reference > 0 && z.Low > s.Reference {
			z.Side = "short"
		} else if s.Reference > 0 && z.High < s.Reference {
			z.Side = "long"
		}
		sort.Slice(z.Contributions, func(i, j int) bool {
			return z.Contributions[i].Venue+z.Contributions[i].Instrument < z.Contributions[j].Venue+z.Contributions[j].Instrument
		})
		if dec(z.Strength).GreaterThan(dec(s.Maximum)) {
			s.Maximum = z.Strength
		}
		s.Zones = append(s.Zones, *z)
	}
	sort.Slice(s.Zones, func(i, j int) bool {
		if s.Zones[i].Quote != s.Zones[j].Quote {
			return s.Zones[i].Quote < s.Zones[j].Quote
		}
		return s.Zones[i].Low < s.Zones[j].Low
	})
	extent := ""
	if len(s.Zones) > 0 {
		extent = fmt.Sprintf("%g/%g", s.Zones[0].Low, s.Zones[len(s.Zones)-1].High)
	}
	s.Comparable = liquidationHash(s.Model + "/" + s.Contract + "/" + s.Period + "/" + s.Quote + "/" + strings.Join(s.Coverage, ",") + "/" + extent)
	for i := range s.Zones {
		z := &s.Zones[i]
		if dec(s.Maximum).IsPositive() {
			z.Relative, _ = dec(z.Strength).Div(dec(s.Maximum)).Mul(decimal.NewFromInt(100)).Float64()
		}
		z.Comparable = s.Comparable
		z.ID = liquidationHash(d.ID + "/" + z.Key + "/legacy")
		z.State = "untracked"
	}
	if s.Reference <= 0 {
		s.Reason = "模型参考价缺失，暂停方向判断"
	}
	if !s.Complete && s.Reason == "" {
		s.Reason = "缺少报价、合约或新版本单位契约；仅供历史浏览"
	}
	return s, nil
}

func evolveLiquidationZone(old *LiquidationZone, current LiquidationZone, s LiquidationMapSnapshot) LiquidationZone {
	z := current
	if old == nil || old.Missing >= 2 {
		z.ID = liquidationHash(s.Dataset + "/" + z.Key + "/" + s.Available.Format(time.RFC3339Nano))
		z.First, z.Continuous, z.Last = s.Available, s.Available, s.Available
		z.Samples = 1
		z.State = "new"
		z.Trend = "new"
		return z
	}
	z.ID, z.Side, z.First, z.Continuous, z.Samples = old.ID, old.Side, old.First, old.Continuous, old.Samples
	z.Touch, z.Cross, z.Reclaim, z.Uncertain = old.Touch, old.Cross, old.Reclaim, old.Uncertain
	z.LastBar, z.PreviousClose, z.FarCloses, z.NearCloses = old.LastBar, old.PreviousClose, old.FarCloses, old.NearCloses
	z.Last = s.Available
	z.State = old.State
	z.Trend = "unknown"
	if !s.Available.After(old.Last) {
		return *old
	}
	if s.Available.Sub(old.Last) > 35*time.Minute || old.Comparable != s.Comparable || old.Missing > 0 {
		z.Samples = 1
		z.Continuous = s.Available
		z.Trend = "incomparable"
	} else {
		z.Samples++
		if dec(old.Strength).IsPositive() {
			v, _ := dec(z.Strength).Div(dec(old.Strength)).Sub(decimal.NewFromInt(1)).Mul(decimal.NewFromInt(100)).Float64()
			z.Change = &v
			z.Trend = "stable"
			if v >= 20 {
				z.Trend = "stronger"
			}
			if v <= -20 {
				z.Trend = "weaker"
			}
		} else {
			z.Trend = "from_zero"
		}
	}
	if z.Touch == nil {
		z.State = "new"
		if z.Samples >= 3 && z.Last.Sub(z.Continuous) >= 30*time.Minute {
			z.State = "persistent"
		}
	}
	return z
}
func zoneDistance(z LiquidationZone, price float64) float64 {
	return math.Max(0, math.Max(z.Low-price, price-z.High))
}

// Called only with completed 5m bars whose open is at/after first visibility.
// Ambiguous OHLC paths and gaps are retained; no synthetic price path is assumed.
func validLiquidationCandle(c Candle) bool {
	for _, v := range []float64{c.Open, c.High, c.Low, c.Close} {
		if v <= 0 || math.IsNaN(v) || math.IsInf(v, 0) {
			return false
		}
	}
	return c.High >= math.Max(c.Open, c.Close) && c.Low <= math.Min(c.Open, c.Close) && c.High >= c.Low
}
func applyLiquidationCandle(z *LiquidationZone, at time.Time, c Candle) {
	if at.Before(z.First) || !at.After(z.LastBar) || z.Side == "neutral" {
		return
	}
	first := z.First.Truncate(5 * time.Minute)
	if first.Before(z.First) {
		first = first.Add(5 * time.Minute)
	}
	if z.LastBar.IsZero() && at.After(first) {
		z.Uncertain = "首次可见后的K线不完整"
	}
	if !z.LastBar.IsZero() && at.Sub(z.LastBar) != 5*time.Minute {
		z.Uncertain = "缺少连续5分钟K线"
		z.FarCloses = 0
		z.NearCloses = 0
	}
	if z.PreviousClose > 0 && ((z.PreviousClose < z.Low && c.Open > z.High) || (z.PreviousClose > z.High && c.Open < z.Low)) {
		z.Uncertain = "跳空跨越区域，无法确认区间成交"
	}
	hit := c.Low <= z.High && c.High >= z.Low
	if hit && z.Touch == nil {
		end := at.Add(5 * time.Minute)
		z.Touch = &end
		z.State = "touched"
		if c.Low < z.Low && c.High > z.High {
			z.Uncertain = "同根K线内触及与穿越先后不明"
		}
	}
	far, near := c.Close > z.High, c.Close < z.Low
	if z.Side == "long" {
		far, near = c.Close < z.Low, c.Close > z.High
	}
	if far {
		z.FarCloses++
	} else {
		z.FarCloses = 0
	}
	if near && z.Touch != nil {
		z.NearCloses++
	} else {
		z.NearCloses = 0
	}
	end := at.Add(5 * time.Minute)
	if z.FarCloses >= 2 {
		if z.Touch == nil {
			z.Uncertain = "未观察到触及便出现远端收盘"
		}
		if z.Cross == nil {
			z.Cross = &end
		}
		z.State = "crossed"
	}
	if z.Touch != nil && z.NearCloses >= 2 {
		if z.Reclaim == nil {
			z.Reclaim = &end
		}
		z.State = "reclaimed"
	}
	z.LastBar, z.PreviousClose = at, c.Close
}
func liquidationJSON(v any) ([]byte, error) {
	b, e := json.Marshal(v)
	if e != nil {
		return nil, e
	}
	if len(b) > liquidationRowLimit {
		return nil, errors.New("清算研究单项超过1MiB")
	}
	var out bytes.Buffer
	var z *gzip.Writer
	select {
	case z = <-encoders:
		z.Reset(&out)
	default:
		z, _ = gzip.NewWriterLevel(&out, gzip.BestSpeed)
	}
	defer func() {
		z.Reset(io.Discard)
		select {
		case encoders <- z:
		default:
		}
	}()
	if _, e = z.Write(b); e != nil {
		return nil, e
	}
	if e = z.Close(); e != nil {
		return nil, e
	}
	return out.Bytes(), nil
}
func liquidationDecode(b []byte, v any) error {
	if len(b) > liquidationRowLimit {
		return errors.New("清算记录过大")
	}
	z, e := gzip.NewReader(bytes.NewReader(b))
	if e != nil {
		return e
	}
	defer z.Close()
	raw, e := io.ReadAll(io.LimitReader(z, liquidationRowLimit+1))
	if e != nil {
		return e
	}
	if len(raw) > liquidationRowLimit {
		return errors.New("清算解压工作集上限")
	}
	return json.Unmarshal(raw, v)
}
