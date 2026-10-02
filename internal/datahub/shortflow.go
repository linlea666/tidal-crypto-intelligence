package datahub

// Short flow is a display and forward-observation experiment, never a mail rule.
import (
	"fmt"
	"math"
	"time"
)

const ShortFlowRules = "flow-observation-v1"
const shortFlowBudget = 32 << 20
const shortFlowWorkLimit = 8 << 20
const shortFlowRowLimit = shortFlowWorkLimit / 8
const shortFlowEventLimit = 32 << 10

type ShortThreshold struct {
	BuyP95       *float64 `json:"buyP95Cents"`
	SellP95      *float64 `json:"sellP95Cents"`
	MedianVolume *float64 `json:"medianVolumeCents"`
	Samples      int      `json:"samples"`
}
type ShortBaseline struct {
	From     time.Time                 `json:"from"`
	To       time.Time                 `json:"to"`
	AsOf     time.Time                 `json:"asOf"`
	Coverage float64                   `json:"coverage"`
	Dates    int                       `json:"validDates"`
	Valid    bool                      `json:"valid"`
	Windows  map[string]ShortThreshold `json:"windows"`
}
type ShortHint struct {
	Minutes   int         `json:"minutes"`
	Direction string      `json:"direction"`
	Active    bool        `json:"active"`
	Checks    []CoreCheck `json:"checks"`
}
type ShortPrice struct {
	From         time.Time `json:"from"`
	To           time.Time `json:"to"`
	Return       *float64  `json:"returnPercent"`
	Displacement *float64  `json:"displacementAtr"`
}
type ShortZone struct {
	ID        string    `json:"id"`
	Side      string    `json:"side"`
	Low       float64   `json:"low"`
	High      float64   `json:"high"`
	Quote     string    `json:"quote"`
	Strength  string    `json:"strength"`
	Relative  float64   `json:"relativeStrength"`
	Distance  float64   `json:"distancePercent"`
	Revision  string    `json:"revision"`
	Available time.Time `json:"availableAt"`
	Fetched   time.Time `json:"fetchedAt"`
	Reference float64   `json:"referencePrice"`
}
type ShortObservation struct {
	Pipeline          string                `json:"pipelineVersion,omitempty"`
	InputVersion      string                `json:"inputVersion,omitempty"`
	FirstGenerated    *time.Time            `json:"firstGeneratedAt"`
	Refreshed         *time.Time            `json:"refreshedAt,omitempty"`
	InputAvailable    *time.Time            `json:"inputAvailableAt"`
	FirstDelay        *float64              `json:"firstPublishDelaySeconds"`
	ArrivalDelay      *float64              `json:"sourceArrivalDelaySeconds"`
	Diagnostics       *ShortRuntime         `json:"diagnostics,omitempty"`
	Rules             string                `json:"rulesVersion"`
	At                time.Time             `json:"at"`
	Through           time.Time             `json:"dataThrough"`
	Available         *time.Time            `json:"availableAt"`
	Fresh             bool                  `json:"fresh"`
	Age               float64               `json:"ageSeconds"`
	Delay             *float64              `json:"processingDelaySeconds"`
	Windows           map[string]FlowWindow `json:"windows"`
	Segments          []FlowWindow          `json:"segments"`
	Baseline          ShortBaseline         `json:"baseline"`
	Hints             []ShortHint           `json:"hints"`
	Prices            map[string]ShortPrice `json:"prices"`
	ATR               *float64              `json:"priorAtr1h"`
	Following         map[string]bool       `json:"following"`
	Zones             []ShortZone           `json:"zones"`
	ZoneNote          string                `json:"zoneNote"`
	BackgroundAt      *time.Time            `json:"backgroundAt"`
	BackgroundThrough *time.Time            `json:"backgroundDataThrough"`
	Background        *DerivativeContext    `json:"background"`
	CoverageRequested string                `json:"coverageRequested"`
	CoverageNote      string                `json:"coverageNote"`
	ResearchPaused    bool                  `json:"researchPaused"`
	ResearchReason    string                `json:"researchReason"`
}

func shortBaselineCoverage(bars map[int64]FlowBar, from, to, asOf time.Time) ShortBaseline {
	b := ShortBaseline{From: from, To: to, AsOf: asOf, Windows: map[string]ShortThreshold{}}
	dates := map[string]int{}
	valid := 0
	for _, v := range bars {
		if !v.At.Before(from) && v.At.Before(to) {
			dates[v.At.Format("2006-01-02")]++
			valid++
		}
	}
	for _, n := range dates {
		if n >= 274 {
			b.Dates++
		}
	}
	if to.After(from) {
		b.Coverage = float64(valid) / (to.Sub(from).Minutes() / 5)
	}
	b.Valid = b.Coverage >= .95 && b.Dates >= 21
	return b
}
func shortThreshold(bars map[int64]FlowBar, from, to time.Time, m int) ShortThreshold {
	buy, sell, volume := make([]float64, 0, 8640), make([]float64, 0, 8640), make([]float64, 0, 8640)
	return shortThresholdScratch(bars, from, to, m, buy, sell, volume)
}
func shortThresholdScratch(bars map[int64]FlowBar, from, to time.Time, m int, buy, sell, volume []float64) ShortThreshold {
	for end := from.Add(time.Duration(m) * time.Minute); !end.After(to); end = end.Add(5 * time.Minute) {
		v, ok := sumBars(bars, end, m/5)
		if !ok {
			continue
		}
		volume = append(volume, float64(v.Buy+v.Sell))
		if v.Net() > 0 {
			buy = append(buy, float64(v.Net()))
		}
		if v.Net() < 0 {
			sell = append(sell, -float64(v.Net()))
		}
	}
	th := ShortThreshold{Samples: len(volume)}
	if len(volume) > 0 {
		th.MedianVolume = flowPtr(percentile(volume, .5))
	}
	if len(buy) > 0 {
		th.BuyP95 = flowPtr(percentile(buy, .95))
	}
	if len(sell) > 0 {
		th.SellP95 = flowPtr(percentile(sell, .95))
	}
	return th
}
func shortBaseline(bars map[int64]FlowBar, from, to, asOf time.Time) ShortBaseline {
	b := shortBaselineCoverage(bars, from, to, asOf)
	buy, sell, volume := make([]float64, 0, 8640), make([]float64, 0, 8640), make([]float64, 0, 8640)
	for _, m := range []int{5, 10, 15, 60, 240} {
		b.Windows[fmt.Sprint(m)] = shortThresholdScratch(bars, from, to, m, buy[:0], sell[:0], volume[:0])
	}
	return b
}

func buildShortObservation(bars map[int64]FlowBar, candles map[int64]Candle, end, now time.Time, baseline ShortBaseline) ShortObservation {
	s := ShortObservation{Rules: ShortFlowRules, At: now, Through: end, Age: now.Sub(end).Seconds(), Windows: map[string]FlowWindow{}, Segments: []FlowWindow{}, Baseline: baseline, Hints: []ShortHint{}, Prices: map[string]ShortPrice{}, Zones: []ShortZone{}, ZoneNote: "缺少当时有效且报价一致的清算区域", CoverageRequested: "Binance / OKX / Coinbase / Kraken / Bitfinex", CoverageNote: "请求聚合五家交易所；上游未提供逐家返回完整性，时间覆盖不等于交易所覆盖"}
	_, complete := bars[end.Add(-5*time.Minute).Unix()]
	s.Fresh = complete && !end.After(now) && now.Sub(end) <= 5*time.Minute
	baseOK := baseline.Valid && baseline.To.Equal(end.Truncate(time.Hour).Add(-time.Hour)) && !baseline.AsOf.After(now)
	s.Baseline.Valid = baseOK
	s.ATR = hourlyATR(candles, end.Add(-time.Hour))
	s.Following = map[string]bool{}
	for at := end.Add(-4 * time.Hour); !at.After(end); at = at.Add(5 * time.Minute) {
		for _, side := range []string{"buy", "sell"} {
			s.Following[side] = s.Following[side] || priceCross(candles, at, side)
		}
	}
	for _, m := range []int{5, 10, 15, 60, 240} {
		key := fmt.Sprint(m)
		th := baseline.Windows[key]
		median := 0.0
		if baseOK && th.MedianVolume != nil {
			median = *th.MedianVolume
		}
		s.Windows[key] = flowWindow(bars, end, m, median)
		p := ShortPrice{From: end.Add(-time.Duration(m) * time.Minute), To: end}
		first, ok1 := candles[p.From.Unix()]
		last, ok2 := candles[end.Add(-5*time.Minute).Unix()]
		all := ok1 && ok2 && first.Open > 0
		for t := p.From; t.Before(end); t = t.Add(5 * time.Minute) {
			if _, ok := candles[t.Unix()]; !ok {
				all = false
			}
		}
		if all {
			p.Return = flowPtr((last.Close/first.Open - 1) * 100)
			if s.ATR != nil && *s.ATR > 0 {
				p.Displacement = flowPtr((last.Close - first.Open) / *s.ATR)
			}
		}
		s.Prices[key] = p
	}
	for i := 5; i >= 0; i-- {
		s.Segments = append(s.Segments, flowWindow(bars, end.Add(-time.Duration(i)*5*time.Minute), 5, 0))
	}
	for _, m := range []int{5, 10} {
		for _, side := range []string{"buy", "sell"} {
			w := s.Windows[fmt.Sprint(m)]
			th := baseline.Windows[fmt.Sprint(m)]
			p95, share := th.BuyP95, w.BuyShare
			if side == "sell" {
				p95, share = th.SellP95, w.SellShare
			}
			h := ShortHint{Minutes: m, Direction: side, Checks: []CoreCheck{{Name: "完整且截止不超过5分钟", Known: true, Passed: s.Fresh && w.Net != nil}, {Name: "30天基线覆盖≥95%且≥21个有效日期", Known: true, Passed: baseOK}}}
			check := func(name string, actual, required *float64) {
				c := CoreCheck{Name: name, Actual: actual, Required: required, Known: actual != nil && required != nil}
				c.Passed = c.Known && *actual >= *required
				h.Checks = append(h.Checks, c)
			}
			var net *float64
			if w.Net != nil {
				net = flowPtr(float64(*w.Net * sideSign(side)))
			}
			check("同方向净额达到本周期P95", net, p95)
			check("本周期量比≥1.5", w.VolumeRatio, flowPtr(1.5))
			check("主导方占比≥55%", share, flowPtr(55.0))
			if m == 10 {
				n := 0
				for i := 1; i <= 2; i++ {
					if b, ok := bars[end.Add(-time.Duration(i)*5*time.Minute).Unix()]; ok && b.Net()*sideSign(side) > 0 {
						n++
					}
				}
				check("两根独立5分钟同向", flowPtr(float64(n)), flowPtr(2.0))
			}
			h.Active = true
			for _, c := range h.Checks {
				h.Active = h.Active && c.Passed
			}
			s.Hints = append(s.Hints, h)
		}
	}
	return s
}

// Read-time aging does not change the persisted first-observation evidence.
func (s *ShortObservation) age(now time.Time) {
	s.Age = math.Max(0, now.Sub(s.Through).Seconds())
	s.Fresh = s.Fresh && !s.Through.After(now) && now.Sub(s.Through) <= 5*time.Minute && now.Sub(s.At) <= 90*time.Second
	if !s.Fresh {
		for i := range s.Hints {
			s.Hints[i].Active = false
		}
	}
}
