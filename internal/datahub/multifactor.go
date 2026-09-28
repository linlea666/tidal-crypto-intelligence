package datahub

import (
	"fmt"
	"math"
	"sort"
	"time"
)

const MultifactorRules = "flow-multifactor-v1"

// Conditional distributions use only positive magnitudes of the specified side.
// Their coverage is still measured over ALL expected bars, not only that side.
type DirectionThreshold struct {
	FastSamples int      `json:"fastSamples"`
	HourSamples int      `json:"hourSamples"`
	FastP95     *float64 `json:"fastP95Cents"`
	HourP90     *float64 `json:"hourP90Cents"`
	HourP95     *float64 `json:"hourP95Cents"`
}
type DirectionalBaseline struct {
	Buy  DirectionThreshold `json:"buy"`
	Sell DirectionThreshold `json:"sell"`
}

func flowPtr[T any](v T) *T { return &v }
func sideSign(side string) int64 {
	if side == "sell" {
		return -1
	}
	return 1
}
func directionalBaseline(fast, hour []float64) *DirectionalBaseline {
	makeSide := func(sign float64) DirectionThreshold {
		f, h := []float64{}, []float64{}
		for _, x := range fast {
			if x*sign > 0 {
				f = append(f, x*sign)
			}
		}
		for _, x := range hour {
			if x*sign > 0 {
				h = append(h, x*sign)
			}
		}
		r := DirectionThreshold{FastSamples: len(f), HourSamples: len(h)}
		if len(f) > 0 {
			r.FastP95 = flowPtr(percentile(f, .95))
		}
		if len(h) > 0 {
			r.HourP90 = flowPtr(percentile(h, .9))
			r.HourP95 = flowPtr(percentile(h, .95))
		}
		return r
	}
	return &DirectionalBaseline{Buy: makeSide(1), Sell: makeSide(-1)}
}

type FlowWindow struct {
	Minutes     int       `json:"minutes"`
	From        time.Time `json:"from"`
	To          time.Time `json:"to"`
	Coverage    float64   `json:"coverage"`
	Net         *int64    `json:"netCents"`
	Buy         *int64    `json:"buyCents"`
	Sell        *int64    `json:"sellCents"`
	Volume      *int64    `json:"volumeCents"`
	BuyShare    *float64  `json:"buyShare"`
	SellShare   *float64  `json:"sellShare"`
	VolumeRatio *float64  `json:"volumeRatio"`
}

func flowWindow(bars map[int64]FlowBar, end time.Time, minutes int, median float64) FlowWindow {
	w := FlowWindow{Minutes: minutes, From: end.Add(-time.Duration(minutes) * time.Minute), To: end}
	count := 0
	for t := w.From; t.Before(end); t = t.Add(5 * time.Minute) {
		if _, ok := bars[t.Unix()]; ok {
			count++
		}
	}
	w.Coverage = float64(count) / float64(minutes/5)
	b, ok := sumBars(bars, end, minutes/5)
	if !ok {
		return w
	}
	w.Net, w.Buy, w.Sell, w.Volume = flowPtr(b.Net()), flowPtr(b.Buy), flowPtr(b.Sell), flowPtr(b.Buy+b.Sell)
	if *w.Volume > 0 {
		w.BuyShare, w.SellShare = flowPtr(b.Share()*100), flowPtr((1-b.Share())*100)
	}
	if median > 0 {
		w.VolumeRatio = flowPtr(float64(*w.Volume) / median)
	}
	return w
}

type FlowEvidence struct {
	Factor string `json:"factor"`
	State  string `json:"state"` // support, conflict, neutral, missing
	Text   string `json:"text"`
	Scope  string `json:"scope"`
}
type CoreCheck struct {
	Name     string   `json:"name"`
	Passed   bool     `json:"passed"`
	Known    bool     `json:"known"`
	Actual   *float64 `json:"actual"`
	Required *float64 `json:"required"`
}
type FlowAssessment struct {
	Direction       string      `json:"direction"`
	Fast            bool        `json:"fast"`
	Sustained       bool        `json:"sustained"`
	Large           bool        `json:"large"`
	Supported       bool        `json:"supported"`
	FastChecks      []CoreCheck `json:"fastChecks"`
	SustainedChecks []CoreCheck `json:"sustainedChecks"`
}

func (a FlowAssessment) pattern() string {
	if a.Fast && a.Sustained {
		return "fast_and_sustained"
	}
	if a.Sustained {
		return "sustained"
	}
	if a.Fast {
		return "fast"
	}
	return ""
}

type PriceContext struct {
	Following       map[string]bool `json:"following"`
	Close           *float64        `json:"closeUsdt"`
	Return1H        *float64        `json:"return1h"`
	PriorATR        *float64        `json:"priorAtr1h"`
	DisplacementATR *float64        `json:"displacementAtr"`
	High            *float64        `json:"high4hUsdt"`
	Low             *float64        `json:"low4hUsdt"`
}
type FlowSnapshot struct {
	Rules       string                `json:"rulesVersion"`
	At          time.Time             `json:"at"`
	DataThrough time.Time             `json:"dataThrough"`
	Fresh       bool                  `json:"fresh"`
	Headline    string                `json:"headline"`
	Detail      string                `json:"detail"`
	Direction   string                `json:"direction"`
	Spot        map[string]FlowWindow `json:"spot"`
	Quarters    []FlowWindow          `json:"quarters"`
	Hours       []FlowWindow          `json:"hours"`
	Baseline    SignalBaseline        `json:"baseline"`
	Assessments []FlowAssessment      `json:"assessments"`
	Context     DerivativeContext     `json:"context"`
	Price       PriceContext          `json:"price"`
	Evidence    []FlowEvidence        `json:"evidence"`
}

func coreAssessment(s FlowSnapshot, side string, bars map[int64]FlowBar) FlowAssessment {
	a := FlowAssessment{Direction: side}
	sign := sideSign(side)
	v15, v60 := s.Spot["15"], s.Spot["60"]
	var th DirectionThreshold
	if s.Baseline.Directional != nil {
		th = s.Baseline.Directional.Buy
		if side == "sell" {
			th = s.Baseline.Directional.Sell
		}
	}
	check := func(name string, actual, required *float64, known bool) CoreCheck {
		c := CoreCheck{Name: name, Actual: actual, Required: required, Known: known && actual != nil && required != nil}
		c.Passed = c.Known && *actual >= *required
		return c
	}
	net := func(w FlowWindow) *float64 {
		if w.Net == nil {
			return nil
		}
		return flowPtr(float64(*w.Net * sign))
	}
	share := func(w FlowWindow) *float64 {
		if sign < 0 {
			return w.SellShare
		}
		return w.BuyShare
	}
	consecutive, quarters := 0, 0
	for i := 1; i <= 3; i++ {
		if b, ok := bars[s.DataThrough.Add(-time.Duration(i)*5*time.Minute).Unix()]; ok && b.Net()*sign > 0 {
			consecutive++
		}
	}
	for _, q := range s.Quarters {
		if q.Net != nil && *q.Net*sign > 0 {
			quarters++
		}
	}
	base := CoreCheck{Name: "30天基线覆盖≥95%且≥21个有效日期", Known: true, Passed: s.Baseline.Valid}
	oneSame := CoreCheck{Name: "一小时净额同向", Known: v60.Net != nil, Passed: v60.Net != nil && *v60.Net*sign > 0}
	a.FastChecks = []CoreCheck{base, check("15分钟规模≥1000万美元", net(v15), flowPtr(1e9), true), check("该方向15分钟P95", net(v15), th.FastP95, s.Baseline.Valid), check("15分钟量比≥1.5", v15.VolumeRatio, flowPtr(1.5), true), check("主导方占比≥55%", share(v15), flowPtr(55.0), true), check("连续三根5分钟同向", flowPtr(float64(consecutive)), flowPtr(3.0), v15.Net != nil), oneSame}
	a.SustainedChecks = []CoreCheck{base, check("一小时规模≥3000万美元", net(v60), flowPtr(3e9), true), check("该方向一小时P90", net(v60), th.HourP90, s.Baseline.Valid), check("一小时量比≥1.5", v60.VolumeRatio, flowPtr(1.5), true), check("主导方占比≥55%", share(v60), flowPtr(55.0), true), check("四段15分钟至少三段同向", flowPtr(float64(quarters)), flowPtr(3.0), v60.Net != nil)}
	all := func(checks []CoreCheck) bool {
		for _, c := range checks {
			if !c.Passed {
				return false
			}
		}
		return true
	}
	a.Fast, a.Sustained = all(a.FastChecks), all(a.SustainedChecks)
	a.Large = th.HourP95 != nil && net(v60) != nil && *net(v60) >= 5e9 && *net(v60) >= *th.HourP95
	return a
}
func buildFlowSnapshot(bars map[int64]FlowBar, candles map[int64]Candle, end, now time.Time, baseline SignalBaseline, fresh bool) FlowSnapshot {
	s := FlowSnapshot{Rules: MultifactorRules, At: now, DataThrough: end, Fresh: fresh, Baseline: baseline, Spot: map[string]FlowWindow{}, Quarters: []FlowWindow{}, Hours: []FlowWindow{}, Evidence: []FlowEvidence{}}
	s.Price.Following = map[string]bool{}
	for t := end.Add(-4 * time.Hour); !t.After(end); t = t.Add(5 * time.Minute) {
		for _, side := range []string{"buy", "sell"} {
			if !s.Price.Following[side] && priceCross(candles, t, side) {
				s.Price.Following[side] = true
			}
		}
	}
	for _, m := range []int{5, 15, 30, 60, 240} {
		median := 0.0
		if baseline.Valid {
			if m == 15 {
				median = baseline.Median15
			}
			if m == 60 {
				median = baseline.Median60
			}
		}
		s.Spot[fmt.Sprint(m)] = flowWindow(bars, end, m, median)
	}
	for i := 3; i >= 0; i-- {
		s.Quarters = append(s.Quarters, flowWindow(bars, end.Add(-time.Duration(i)*15*time.Minute), 15, 0))
		s.Hours = append(s.Hours, flowWindow(bars, end.Add(-time.Duration(i)*time.Hour), 60, 0))
	}
	hi, lo, close, ok := candleBounds(candles, end)
	if ok {
		s.Price.High, s.Price.Low, s.Price.Close = flowPtr(hi), flowPtr(lo), flowPtr(close)
	}
	s.Price.PriorATR = hourlyATR(candles, end.Add(-time.Hour))
	prev, pok := candles[end.Add(-65*time.Minute).Unix()]
	last, lok := candles[end.Add(-5*time.Minute).Unix()]
	if pok && lok && prev.Close > 0 {
		s.Price.Return1H = flowPtr((last.Close/prev.Close - 1) * 100)
		if s.Price.PriorATR != nil && *s.Price.PriorATR > 0 {
			s.Price.DisplacementATR = flowPtr((last.Close - prev.Close) / *s.Price.PriorATR)
		}
	}
	for _, side := range []string{"buy", "sell"} {
		s.Assessments = append(s.Assessments, coreAssessment(s, side, bars))
	}
	return s
}

func (s *FlowSnapshot) explain(side string) {
	if side == "" {
		if w := s.Spot["60"]; w.Net != nil && *w.Net != 0 {
			side = "buy"
			if *w.Net < 0 {
				side = "sell"
			}
		}
	}
	s.Direction = side
	sign := sideSign(side)
	name, pressure := "买盘", "买入"
	if side == "sell" {
		name, pressure = "卖压", "卖出"
	}
	var a FlowAssessment
	for _, v := range s.Assessments {
		if v.Direction == side {
			a = v
		}
	}
	s.Evidence = []FlowEvidence{}
	add := func(factor, state, text, scope string) {
		s.Evidence = append(s.Evidence, FlowEvidence{factor, state, text, scope})
	}
	spotScope := "现货：Binance / OKX / Coinbase / Kraken / Bitfinex，美元主动成交"
	if s.Spot["15"].Net == nil || s.Spot["60"].Net == nil || !s.Baseline.Valid {
		add("spot", "missing", "当前窗口或历史基线不完整，不能认定资金异动", spotScope)
	} else if a.pattern() != "" {
		add("spot", "support", name+"规模、相对分位、量比和持续性达到资金异动门槛", spotScope)
	} else {
		add("spot", "neutral", "存在成交偏向，但资金规模、放量和持续性尚未全部达标", spotScope)
	}
	perp := s.Context.Futures["60"]
	perpSame, perpOpposite := false, false
	if perp.Net == nil || perp.VolumeRatio == nil || perp.BuyShare == nil {
		add("futures", "missing", "合约窗口或同口径量比基线不足", "合约：Binance / OKX / Bybit")
	} else {
		dominant := math.Max(*perp.BuyShare, 100-*perp.BuyShare)
		significant := dominant >= 55 && *perp.VolumeRatio >= 1.5
		perpSame = significant && *perp.Net*sign > 0
		perpOpposite = significant && *perp.Net*sign < 0
		state, txt := "neutral", "合约方向尚未达到显著同向或反向条件"
		if perpSame {
			state, txt = "support", "现货与合约放量同向"
		}
		if perpOpposite {
			state, txt = "conflict", "合约存在显著相反压力"
		}
		add("futures", state, txt, "合约：Binance / OKX / Bybit")
	}
	oiKnown := s.Context.OI.Coin1H != nil && s.Context.OI.AbsP75 != nil
	if !oiKnown {
		add("oi", "missing", "币计价 OI 或变化基线不足；美元 OI 含价格影响", "CoinGlass 聚合 OI；范围与现货不同")
	} else {
		txt := "币计价持仓变化未超历史异常阈值"
		if s.Context.OI.Regime == "expanding" {
			txt = "持仓明显扩张，每份合约同时存在多空双方"
		}
		if s.Context.OI.Regime == "contracting" {
			txt = "持仓明显收缩，存在平仓背景"
		}
		add("oi", "neutral", txt, "CoinGlass 聚合币计价 OI")
	}
	fundingKnown, crowded := s.Context.fundingWarning(side)
	if !fundingKnown {
		add("funding", "missing", "费率类型、结算周期或可比历史不足，拥挤程度未知", "逐交易所、保证金类别、费率类型和周期比较")
	} else if crowded {
		add("funding", "conflict", pressure+"方向至少两家交易所付费处于各自历史高位", "Binance / OKX / Bybit 分别统计")
	} else {
		add("funding", "neutral", "可比交易所尚未达到同向高付费条件", "Binance / OKX / Bybit 分别统计")
	}
	liq := s.Context.Liquidations["60"]
	liquidated := false
	if liq.Long == nil || liq.Short == nil || liq.LongP95 == nil || liq.ShortP95 == nil {
		add("liquidations", "missing", "清算闭合窗口或历史基线不足", "三家合约交易所已发生清算")
	} else {
		txt := "已发生清算未达到同侧历史 P95"
		if *liq.Long > 0 && float64(*liq.Long) >= *liq.LongP95 {
			txt = "多单清算集中"
			liquidated = side == "sell"
		}
		if *liq.Short > 0 && float64(*liq.Short) >= *liq.ShortP95 {
			if txt == "多单清算集中" {
				txt += "，空单清算亦集中"
			} else {
				txt = "空单清算集中"
			}
			liquidated = liquidated || side == "buy"
		}
		add("liquidations", "neutral", txt+"；不与合约成交重复计分", "三家合约交易所已发生清算")
	}
	extended := s.Price.DisplacementATR != nil && *s.Price.DisplacementATR*float64(sign) > 1.5
	priceConflict := s.Price.Return1H != nil && *s.Price.Return1H*float64(sign) <= 0
	if s.Price.Return1H == nil {
		add("price", "missing", "价格窗口不完整", "Binance BTC/USDT 已闭合五分钟 K 线")
	} else if extended {
		add("price", "conflict", "一小时同向位移超过此前 ATR 的 1.5 倍，限制升级", "Binance BTC/USDT")
	} else if priceConflict {
		add("price", "conflict", name+"尚未获得同窗价格响应", "Binance BTC/USDT")
	} else {
		add("price", "support", "同窗价格同向；固定区间确认进度另行跟踪", "Binance BTC/USDT")
	}
	fourSame := s.Spot["240"].Net != nil && *s.Spot["240"].Net*sign > 0
	a.Supported = a.Sustained && a.Large && fourSame && perpSame && oiKnown && fundingKnown && !crowded && !extended && !priceConflict && s.Price.DisplacementATR != nil
	for i := range s.Assessments {
		if s.Assessments[i].Direction == side {
			s.Assessments[i] = a
		}
	}
	s.Headline = name + "偏向尚未达到资金异动条件"
	if a.pattern() != "" {
		s.Headline = name + "异动，等待固定区间价格确认"
	}
	if a.Supported {
		s.Headline = name + "异动获得多因素支持，效果验证中"
	}
	if a.pattern() != "" && s.Context.OI.Regime == "contracting" && liquidated && s.Price.Return1H != nil && *s.Price.Return1H*float64(sign) > 0 {
		s.Headline = name + "异动伴随平仓和清算，持续性仍需观察"
	}
	if a.pattern() != "" && priceConflict {
		s.Headline = name + "明显，但价格响应存在分歧"
	}
	if v15, v4 := s.Spot["15"], s.Spot["240"]; v15.Net != nil && v4.Net != nil && *v15.Net != 0 && *v4.Net != 0 && (*v15.Net > 0) != (*v4.Net > 0) {
		if *v15.Net > 0 {
			s.Headline = "短线买盘回升，较长窗口仍有卖压"
		} else {
			s.Headline = "短线卖压增加，较长窗口仍为净买入"
		}
	}
	if side == "" {
		s.Headline = "暂无明确资金方向"
	}
	if a.pattern() == "" && s.Price.DisplacementATR != nil && math.Abs(*s.Price.DisplacementATR) > 1.5 && s.Spot["60"].VolumeRatio != nil && *s.Spot["60"].VolumeRatio < 1.5 {
		s.Headline = "价格位移明显，成交量尚未支持资金异动"
	}
	if !s.Fresh {
		s.Headline = "数据延迟或缺失，当前判断暂停"
		for i := range s.Evidence {
			s.Evidence[i].State = "missing"
			s.Evidence[i].Text = "历史快照：" + s.Evidence[i].Text
		}
		for i := range s.Assessments {
			s.Assessments[i].Supported = false
		}
	}
	s.Detail = "主动净买卖是主动买入额减主动卖出额，不等同充值提现或新增资金。确认仅描述已发生突破，不保证后续延续。"
}

type PriceProgress struct {
	At              time.Time  `json:"at"`
	DataThrough     time.Time  `json:"dataThrough"`
	Line            float64    `json:"lineUsdt"`
	Closes          []*float64 `json:"closesUsdt"`
	Outside         int        `json:"outsideCloses"`
	FlowSame        *bool      `json:"flowSame"`
	Status          string     `json:"status"`
	ObservationEnds *time.Time `json:"observationEnds"`
}

func priceProgress(s Signal, bars map[int64]FlowBar, candles map[int64]Candle, end, now time.Time, fresh bool) PriceProgress {
	if s.ConfirmedThrough != nil {
		end = minTime(end, s.ConfirmedThrough.Add(4*time.Hour))
	}
	p := PriceProgress{At: now, DataThrough: end, Line: s.FrozenHigh, Closes: []*float64{}, Status: "waiting"}
	if s.Direction == "sell" {
		p.Line = s.FrozenLow
	}
	if s.ConfirmedThrough != nil {
		p.ObservationEnds = flowPtr(s.ConfirmedThrough.Add(4 * time.Hour))
	}
	for i := 2; i >= 1; i-- {
		t := end.Add(-time.Duration(i) * 5 * time.Minute)
		c, ok := candles[t.Unix()]
		if !ok || t.Before(s.DataThrough) {
			p.Closes = append(p.Closes, nil)
			continue
		}
		p.Closes = append(p.Closes, flowPtr(c.Close))
		if (c.Close-p.Line)*float64(sideSign(s.Direction)) > 0 {
			p.Outside++
		}
	}
	if v, ok := sumBars(bars, end, 3); ok {
		p.FlowSame = flowPtr(v.Net()*sideSign(s.Direction) > 0)
	}
	if s.ConfirmedAt == nil && fresh && end.Before(s.DataThrough.Add(10*time.Minute)) {
		p.Status = "waiting"
	} else if !fresh || p.FlowSame == nil || len(p.Closes) != 2 || p.Closes[0] == nil || p.Closes[1] == nil {
		p.Status = "unknown"
	} else if s.ConfirmedAt != nil {
		switch {
		case p.Outside == 0:
			p.Status = "reclaimed"
		case !*p.FlowSame:
			p.Status = "flow_reversed"
		case p.Outside == 2:
			p.Status = "holding"
		default:
			p.Status = "near_line"
		}
	} else if !now.Before(s.Expires) {
		p.Status = "expired"
	}
	if s.ConfirmedAt != nil && s.ConfirmedThrough == nil {
		p.Status = "not_recorded"
	}
	// End the follow-up using data time; outage cannot leave monitoring open forever.
	if p.ObservationEnds != nil && !now.Before(*p.ObservationEnds) {
		if !fresh || end.Before(*p.ObservationEnds) || p.Status == "unknown" {
			p.Status = "ended_with_gap"
		} else {
			p.Status = "completed_" + p.Status
		}
	}
	return p
}

func sortedTimes(bars map[int64]FlowBar) []int64 {
	a := make([]int64, 0, len(bars))
	for t := range bars {
		a = append(a, t)
	}
	sort.Slice(a, func(i, j int) bool { return a[i] < a[j] })
	return a
}
