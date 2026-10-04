package datahub

import (
	"context"
	"database/sql"
	"encoding/json"
	"sort"
	"time"
)

type costTrial struct {
	ID             string     `json:"id"`
	Rules          string     `json:"rulesVersion"`
	Group          string     `json:"group"`
	Kind           string     `json:"kind"`
	Direction      string     `json:"direction"`
	EpisodeID      string     `json:"episodeId"`
	CaseID         string     `json:"caseId"`
	DetectedAt     time.Time  `json:"detectedAt"`
	Date           string     `json:"date"`
	PreVolatility  *float64   `json:"preVolatility"`
	DiscoveryPrice *string    `json:"discoveryPrice"`
	AttentionAt    *time.Time `json:"attentionAt"`
	PriceRevision  string     `json:"priceRevision"`
	FrameRevision  string     `json:"frameRevision"`
	Shadow         bool       `json:"shadow"`
}
type costDaily struct {
	ID            string       `json:"id"`
	Date          string       `json:"date"`
	FirstSeen     time.Time    `json:"firstSeen"`
	Rules         string       `json:"rulesVersion"`
	Structure     string       `json:"structure"`
	Price         string       `json:"price"`
	Reason        string       `json:"reason"`
	FrameRevision string       `json:"frameRevision"`
	PriceRevision string       `json:"priceRevision"`
	Metrics       *CostMetrics `json:"metrics"`
}

func costTrialFromEvent(e CostEvent, s costState) costTrial {
	t := costTrial{ID: e.ID, Rules: e.Rules, Group: e.Group, Kind: e.Kind, Direction: e.Direction, EpisodeID: e.EpisodeID, CaseID: e.CaseID, DetectedAt: e.DetectedAt, Date: e.Date, DiscoveryPrice: e.DiscoveredMarketPrice, FrameRevision: e.Revision, PriceRevision: e.PriceRevision, Shadow: e.Shadow}
	if e.Metrics != nil {
		t.PreVolatility = e.Metrics.Volatility
	}
	for _, c := range s.Cases {
		if c.ID == e.CaseID {
			t.AttentionAt = c.FirstAttentionAt
			if c.Buffer != "0" {
				t.Group = "onchain_buffer_" + c.Buffer
			}
		}
	}
	if t.Group == "" {
		t.Group = e.Kind
	}
	return t
}
func costDailyResearch(s *costState, f *CostFrame, m *CostMetrics, p *CostPrice, prices map[string]string, day string, now time.Time, structure bool) ([]costTrial, costDaily) {
	d := costDaily{Date: day, FirstSeen: now, Rules: OnchainRules, Structure: "missing", Price: "missing", Reason: "等待当日实际可得输入", Metrics: m}
	if f != nil {
		d.FrameRevision = f.Revision
	}
	if p != nil {
		d.PriceRevision = p.Revision
		d.Price = "available"
	}
	if structure && m != nil {
		if b := costCompressed(*m); b == nil {
			d.Structure = "uncertain"
			d.Reason = "参考样本或分桶精度不足"
		} else if *b {
			d.Structure = "met"
			d.Reason = "结构条件满足，连续性由episode独立判断"
		} else {
			d.Structure = "not_met"
			d.Reason = "结构条件明确未满足"
		}
	}
	if s.Initial {
		d.Reason = "初始化状态，不建立前向事件样本"
	}
	d.ID = costHash([]string{OnchainRules, day, d.Structure, d.Price, d.FrameRevision, d.PriceRevision, d.Reason})
	trials := []costTrial{}
	if p == nil || s.LowLastDate == day {
		return trials, d
	}
	priceInitial := s.LowLastDate == ""
	consecutive := costDayAdd(s.LowLastDate, 1) == day
	if !consecutive {
		s.LowMeet = 0
		s.LowClear = 0
	}
	s.LowLastDate = day
	vol := costVolatility(day, prices)
	vm := CostMetrics{Volatility: vol, Concentration: map[string]CostBounds{}}
	costRanks(&vm, day, onchainMethod, nil, prices)
	makeTrial := func(group, episode string) costTrial {
		return costTrial{ID: costHash([]string{OnchainRules, group, day}), Rules: OnchainRules, Group: group, Kind: group, EpisodeID: episode, DetectedAt: now, Date: day, PreVolatility: vol, PriceRevision: p.Revision, Shadow: true}
	}
	if !s.Initial && !priceInitial {
		trials = append(trials, makeTrial("unconditional", costHash([]string{"unconditional", day})))
	}
	if vm.VolatilityRank == nil {
		s.LowMeet = 0
		s.LowClear = 0
		return trials, d
	}
	if *vm.VolatilityRank <= 20 {
		s.LowMeet++
		s.LowClear = 0
		if s.Initial || priceInitial {
			s.LowActive = true
		} else if s.LowMeet >= 2 && consecutive && !s.LowActive {
			s.LowActive = true
			s.LowEpisode = costHash([]string{OnchainRules, "low_volatility", day})
			trials = append(trials, makeTrial("low_volatility", s.LowEpisode))
		}
	} else {
		s.LowMeet = 0
		s.LowClear++
		if s.LowClear >= 2 {
			s.LowActive = false
		}
	}
	return trials, d
}
func costInsertDaily(ctx context.Context, tx *sql.Tx, d costDaily) error {
	raw, e := json.Marshal(d)
	if e != nil {
		return e
	}
	_, e = tx.ExecContext(ctx, "INSERT OR IGNORE INTO daily VALUES(?,?,?,?)", d.ID, d.Date, d.FirstSeen.UnixNano(), raw)
	return e
}
func (h *Hub) costSaveDaily(ctx context.Context, d costDaily) error {
	var n int
	if e := h.Store.onchain.db.QueryRowContext(ctx, "SELECT count(*) FROM daily WHERE id=?", d.ID).Scan(&n); e != nil {
		return e
	}
	if n > 0 {
		return nil
	}
	tx, e := h.Store.onchain.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	if e = costInsertDaily(ctx, tx, d); e != nil {
		return e
	}
	return h.Store.onchain.commit(ctx, tx)
}

type costTrialResult struct {
	CostResult
	Rules                  string   `json:"rulesVersion"`
	Group                  string   `json:"group"`
	Direction              string   `json:"direction"`
	EpisodeID              string   `json:"episodeId"`
	DirectionalReturn      *float64 `json:"directionalReturn"`
	Favorable              *float64 `json:"favorable"`
	Adverse                *float64 `json:"adverse"`
	VolatilityRatio        *float64 `json:"volatilityRatio"`
	AnchorDate             string   `json:"anchorDate"`
	AnchorPrice            *string  `json:"anchorPrice"`
	DiscoveryToAnchor      *float64 `json:"discoveryToAnchor"`
	AttentionDelayHours    *float64 `json:"attentionDelayHours"`
	InvalidatedWithin7Days *bool    `json:"invalidatedWithin7Days"`
	Selected               bool     `json:"selected"`
}

func costTrialOutcome(t costTrial, n int, prices map[string]string, now time.Time, c *CostCase) costTrialResult {
	base := costOutcome(CostEvent{ID: t.ID, Kind: t.Kind, DetectedAt: t.DetectedAt}, n, prices, now)
	r := costTrialResult{CostResult: base, Rules: t.Rules, Group: t.Group, Direction: t.Direction, EpisodeID: t.EpisodeID, AnchorDate: costDate(t.DetectedAt)}
	if base.Status != "complete" {
		return r
	}
	a := prices[r.AnchorDate]
	r.AnchorPrice = &a
	if t.DiscoveryPrice != nil && dec(*t.DiscoveryPrice).IsPositive() {
		v := dec(a).Div(dec(*t.DiscoveryPrice)).Sub(dec("1")).Mul(dec("100")).InexactFloat64()
		r.DiscoveryToAnchor = &v
	}
	if t.Direction == "up" || t.Direction == "down" {
		v, hi, lo := *r.Return, *r.PathMax, *r.PathMin
		if t.Direction == "down" {
			v, hi, lo = -v, -lo, -hi
		}
		r.DirectionalReturn = &v
		r.Favorable = &hi
		r.Adverse = &lo
	}
	if t.PreVolatility != nil && *t.PreVolatility > 0 && r.Volatility != nil {
		v := *r.Volatility / *t.PreVolatility
		r.VolatilityRatio = &v
	}
	if t.AttentionAt != nil && !t.AttentionAt.After(t.DetectedAt) {
		v := t.DetectedAt.Sub(*t.AttentionAt).Hours()
		r.AttentionDelayHours = &v
	}
	if c != nil && c.ConfirmedDate != "" {
		through := costDayAdd(c.ConfirmedDate, 7)
		if c.InvalidatedDate != "" && c.InvalidatedDate <= through && c.InvalidatedAt != nil && !c.InvalidatedAt.After(r.Cutoff) {
			v := true
			r.InvalidatedWithin7Days = &v
		} else if !now.Before(costClose(through).Add(6 * time.Hour)) {
			complete := c.LastDate >= through && (c.FirstTrackingGap == "" || c.FirstTrackingGap > through)
			for i := 1; i <= 7; i++ {
				if _, ok := prices[costDayAdd(c.ConfirmedDate, i)]; !ok {
					complete = false
				}
			}
			if complete {
				v := false
				r.InvalidatedWithin7Days = &v
			}
		}
	}
	return r
}
func (h *Hub) processCostResearch(ctx context.Context, now time.Time) error {
	if e := h.processCostLegacyResearch(ctx, now); e != nil {
		return e
	}
	s := h.Store.onchain
	if e := s.writable(); e != nil {
		return e
	}
	rows, e := s.db.QueryContext(ctx, `SELECT t.payload,h.n FROM trials t CROSS JOIN (SELECT 7 n UNION ALL SELECT 14 UNION ALL SELECT 30 UNION ALL SELECT 60) h WHERE (t.detected / 86400000000000 + h.n + 1)*86400000000000+21600000000000 <= ? AND NOT EXISTS(SELECT 1 FROM trial_results r WHERE r.trial_id=t.id AND r.horizon=h.n) ORDER BY t.detected + h.n*86400000000000,h.n LIMIT 64`, now.UnixNano())
	if e != nil {
		return e
	}
	type task struct {
		t costTrial
		n int
	}
	tasks := []task{}
	for rows.Next() {
		var raw []byte
		var x task
		if e = rows.Scan(&raw, &x.n); e != nil {
			rows.Close()
			return e
		}
		if e = json.Unmarshal(raw, &x.t); e != nil {
			rows.Close()
			return e
		}
		tasks = append(tasks, x)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	var cached map[string]string
	var lastCutoff time.Time
	for _, x := range tasks {
		pending := costTrialOutcome(x.t, x.n, nil, now, nil)
		if !pending.Cutoff.Equal(lastCutoff) {
			cached, e = s.prices(ctx, pending.Cutoff)
			if e != nil {
				return e
			}
			lastCutoff = pending.Cutoff
		}
		var c *CostCase
		if x.t.CaseID != "" {
			c, e = s.caseByID(ctx, x.t.CaseID)
			if e != nil {
				return e
			}
		}
		result := costTrialOutcome(x.t, x.n, cached, now, c)
		raw, e := json.Marshal(result)
		if e != nil {
			return e
		}
		tx, e := s.db.BeginTx(ctx, nil)
		if e != nil {
			return e
		}
		_, e = tx.ExecContext(ctx, "INSERT OR IGNORE INTO trial_results VALUES(?,?,?)", x.t.ID, x.n, raw)
		if e == nil {
			e = s.commit(ctx, tx)
		}
		tx.Rollback()
		if e != nil {
			return e
		}
		s.epoch.Add(1)
	}
	return nil
}

type costValidationGroup struct {
	Rules                        string    `json:"rulesVersion"`
	Kind                         string    `json:"kind"`
	Side                         string    `json:"side"`
	Horizon                      int       `json:"horizon"`
	Complete                     int       `json:"complete"`
	Independent                  int       `json:"independent"`
	Waiting                      int       `json:"waiting"`
	Missing                      int       `json:"missing"`
	Overlapping                  int       `json:"overlapping"`
	Rate                         *float64  `json:"riseRate"`
	RateLabel                    string    `json:"rateLabel"`
	Interval                     []float64 `json:"confidenceInterval"`
	Median                       *float64  `json:"median"`
	Quartiles                    []float64 `json:"quartiles"`
	InvalidatedRate              *float64  `json:"invalidatedRate"`
	InvalidatedSamples           int       `json:"invalidatedSamples"`
	AttentionDelayMedian         *float64  `json:"attentionDelayMedianHours"`
	values                       []float64
	delays                       []float64
	positive, rateN, invalidated int
}

func costPercentile(v []float64, q float64) float64 {
	pos := q * float64(len(v)-1)
	i := int(pos)
	j := min(i+1, len(v)-1)
	return v[i] + (v[j]-v[i])*(pos-float64(i))
}
func (h *Hub) costResearchView(ctx context.Context, now time.Time, mode string, offset, limit int) (any, error) {
	if mode == "historical" || mode == "legacy" {
		if mode == "legacy" {
			mode = "forward"
		}
		return h.costLegacyResearchView(ctx, now, mode, offset, limit)
	}
	s := h.Store.onchain
	// Stream in discovery order; reserve overlap before inspecting completeness.
	rows, e := s.db.QueryContext(ctx, `SELECT t.payload,h.n,r.payload FROM trials t CROSS JOIN (SELECT 7 n UNION ALL SELECT 14 UNION ALL SELECT 30 UNION ALL SELECT 60) h LEFT JOIN trial_results r ON r.trial_id=t.id AND r.horizon=h.n ORDER BY t.detected,t.id,h.n LIMIT 80001`)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	groups := map[string]*costValidationGroup{}
	last := map[string]string{}
	episodes := map[string]bool{}
	items := []costTrialResult{}
	total := 0
	for rows.Next() {
		var raw, res []byte
		var n int
		if e = rows.Scan(&raw, &n, &res); e != nil {
			return nil, e
		}
		total++
		if total > 80000 {
			return nil, errorsNewCostResearchLimit()
		}
		var t costTrial
		if e = json.Unmarshal(raw, &t); e != nil {
			return nil, e
		}
		r := costTrialOutcome(t, n, nil, now, nil)
		if len(res) > 0 {
			if e = json.Unmarshal(res, &r); e != nil {
				return nil, e
			}
		} else {
			r.Status = "waiting"
		}
		key := t.Rules + "/" + t.Group + "/" + t.Direction + "/" + itoa(n)
		g := groups[key]
		if g == nil {
			g = &costValidationGroup{Rules: t.Rules, Kind: t.Group, Side: t.Direction, Horizon: n, Interval: []float64{}, Quartiles: []float64{}, RateLabel: "方向调整后收盘收益为正比例"}
			if t.Direction == "" {
				g.RateLabel = "未来波动高于此前21日波动比例"
			}
			groups[key] = g
		}
		episode := key + "/" + t.EpisodeID
		r.Selected = r.From > last[key] && !episodes[episode]
		if r.Selected {
			last[key] = r.Through
			episodes[episode] = true
		} else {
			g.Overlapping++
		}
		switch r.Status {
		case "complete":
			g.Complete++
			if r.Selected {
				g.Independent++
				v := r.DirectionalReturn
				if t.Direction == "" {
					v = r.VolatilityRatio
				}
				if v != nil {
					g.values = append(g.values, *v)
					g.rateN++
					threshold := 0.
					if t.Direction == "" {
						threshold = 1
					}
					if *v > threshold {
						g.positive++
					}
				}
				if r.InvalidatedWithin7Days != nil {
					g.InvalidatedSamples++
					if *r.InvalidatedWithin7Days {
						g.invalidated++
					}
				}
				if r.AttentionDelayHours != nil {
					g.delays = append(g.delays, *r.AttentionDelayHours)
				}
			}
		case "missing":
			g.Missing++
		default:
			g.Waiting++
		}
		if total > offset && len(items) < limit {
			items = append(items, r)
		}
	}
	if e = rows.Err(); e != nil {
		return nil, e
	}
	rows.Close()
	out := []costValidationGroup{}
	keys := []string{}
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		g := groups[k]
		if g.Independent >= 30 && g.rateN >= 30 {
			v := float64(g.positive) / float64(g.rateN)
			g.Rate = &v
			ci := liquidationWilson(g.positive, g.rateN)
			g.Interval = []float64{ci[0], ci[1]}
			sort.Float64s(g.values)
			med := costPercentile(g.values, .5)
			g.Median = &med
			g.Quartiles = []float64{costPercentile(g.values, .25), costPercentile(g.values, .75)}
		}
		if g.InvalidatedSamples >= 30 {
			v := float64(g.invalidated) / float64(g.InvalidatedSamples)
			g.InvalidatedRate = &v
		}
		if len(g.delays) >= 30 {
			sort.Float64s(g.delays)
			v := costPercentile(g.delays, .5)
			g.AttentionDelayMedian = &v
		}
		out = append(out, *g)
	}
	daily := []costDaily{}
	dr, e := s.db.QueryContext(ctx, "SELECT payload FROM daily ORDER BY seen DESC,id LIMIT 60")
	if e != nil {
		return nil, e
	}
	for dr.Next() {
		var b []byte
		var d costDaily
		if e = dr.Scan(&b); e != nil {
			break
		}
		if e = json.Unmarshal(b, &d); e != nil {
			break
		}
		daily = append(daily, d)
	}
	re := dr.Err()
	dr.Close()
	if e != nil {
		return nil, e
	}
	if re != nil {
		return nil, re
	}
	var dailyCount int
	if e = s.db.QueryRowContext(ctx, "SELECT count(DISTINCT day) FROM daily").Scan(&dailyCount); e != nil {
		return nil, e
	}
	var next any
	if offset+len(items) < total {
		next = offset + len(items)
	}
	return map[string]any{"mode": "forward", "groups": out, "items": items, "total": total, "offset": offset, "nextOffset": next, "daily": daily, "dailyCount": dailyCount, "primaryHorizon": 14, "baselines": []string{"unconditional", "low_volatility", "price_only"}, "note": "主期限14日；7/30/60日为次要观察。无条件、低波动、20日收盘通道分别建立前向样本。按发现顺序先冻结去重选择，缺失不替换成其他样本；30个去重完整且指标可算样本才展示比例与Wilson95%区间。非重叠不保证统计独立，比例不是胜率。旧版结果在原规则入口单列。"}, nil
}
func errorsNewCostResearchLimit() error {
	return &costProtocolError{"研究达到有界查询上限，请归档研究分期；未返回截断统计"}
}
