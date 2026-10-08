package datahub

import (
	"context"
	"database/sql"
	"errors"
	"math/rand"
	"net/url"
	"sort"
	"strings"
	"time"
)

func (h *Hub) bookFlowView(ctx context.Context, q url.Values, now time.Time) (any, error) {
	if q.Get("asset") != "" && q.Get("asset") != "BTC" {
		return nil, errors.New("挂单承接观察仅支持BTC")
	}
	side, id := q.Get("direction"), q.Get("id")
	if side != "" && side != "buy" && side != "sell" {
		return nil, errors.New("无效方向")
	}
	if len(id) > 240 {
		return nil, errors.New("无效事件ID")
	}
	from, to, err := auditRange(q, now)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if h.Store.bookFlowError != "" {
		return nil, errors.New(h.Store.bookFlowError)
	}
	s := bookFlowState{}
	if err = h.Store.bookFlowLoad(ctx, "state", BookFlowRules, &s); err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	contracts := []minuteContract{}
	for _, venue := range []string{"Binance", "OKX"} {
		c := minuteContract{Venue: venue}
		err = h.Store.bookFlowLoad(ctx, "contract", minuteFootID(venue), &c)
		if err != nil && err != sql.ErrNoRows {
			return nil, err
		}
		contracts = append(contracts, c)
	}
	limit, offset := parseInt(q, "limit", 20, 1, 50), parseInt(q, "offset", 0, 0, 10000)
	events, err := paperRows[bookEvent](ctx, h.Store.shortDB(), `SELECT e.payload FROM book_flow e JOIN book_flow c ON c.kind='clock' AND c.id=e.id||'/enhanced' WHERE e.kind='event' AND (?='' OR e.id=?) AND (?='' OR json_extract(e.payload,'$.direction')=?) AND (?<>'' OR (c.at>=? AND c.at<?)) ORDER BY c.at DESC,e.id LIMIT ? OFFSET ?`, id, id, side, side, id, from.Unix(), to.Unix(), limit+1, offset)
	if err != nil {
		return nil, err
	}
	more := len(events) > limit
	if more {
		events = events[:limit]
	}
	items := []any{}
	for eventIndex, e := range events {
		var readable time.Time
		if err = h.Store.bookFlowLoad(ctx, "clock", e.ID+"/enhanced", &readable); err != nil {
			return nil, err
		}
		e.At = readable
		e.Expires = readable.Add(30 * time.Minute)
		events[eventIndex] = e
		published := []bookUpdate{}
		for _, u := range e.Updates {
			var clock time.Time
			ce := h.Store.bookFlowLoad(ctx, "clock", e.ID+"/"+u.Kind, &clock)
			if ce == sql.ErrNoRows {
				continue
			}
			if ce != nil {
				return nil, ce
			}
			u.At = clock
			published = append(published, u)
		}
		e.Updates = published
		trials, er := paperRows[ShortTrial](ctx, h.Store.shortDB(), "SELECT payload FROM book_flow WHERE kind='trial' AND id LIKE ? ORDER BY at,id", e.ID+"/%")
		if er != nil {
			return nil, er
		}
		observations := []any{}
		for _, t := range trials {
			observations = append(observations, bookTrialView(t))
		}
		item := map[string]any{"event": e, "observations": observations}
		if id != "" {
			// Relations are temporal associations, never proof of causality.
			formal, er := paperRows[Signal](ctx, h.Store.shortDB(), "SELECT payload FROM documents WHERE kind='signal' AND asset='BTC' AND at>=? AND at<? ORDER BY at LIMIT 6", e.At.Add(-30*time.Minute).Unix(), e.Expires.Unix())
			if er != nil {
				return nil, er
			}
			linked := []any{}
			ids := []string{}
			for _, sig := range formal {
				ids = append(ids, sig.ID)
			}
			mails, er := h.signalMailResults(ctx, ids)
			if er != nil {
				return nil, er
			}
			related := []auditEvent{}
			for _, sig := range formal {
				x := auditSignal(sig)
				notices := []SignalMailResult{}
				for _, m := range mails {
					if m.SignalID == sig.ID {
						notices = append(notices, m)
					}
				}
				x["notifications"] = notices
				trades, er := h.auditPaper(ctx, sig.ID, &related)
				if er != nil {
					return nil, er
				}
				x["paper"] = trades
				linked = append(linked, x)
			}
			item["formalEvents"] = linked
			item["relatedTimeline"] = related
		}
		items = append(items, item)
	}
	var used int64
	if err = h.Store.shortDB().QueryRowContext(ctx, "SELECT used FROM book_flow_budget WHERE id=1").Scan(&used); err != nil {
		return nil, err
	}
	h.mu.RLock()
	runtime := h.bookFlowRuntime
	h.mu.RUnlock()
	runtime.Skipped = nil
	var origin *time.Time
	if !s.Origin.IsZero() {
		origin = &s.Origin
	}
	var last *time.Time
	if !s.Last.IsZero() {
		last = &s.Last
	}
	current := map[string]any{}
	for venue, p := range s.Current {
		current[venue] = map[string]any{"snapshot": p, "fresh": now.Sub(p.At) <= 3*time.Minute && now.Sub(p.Fetched) <= 3*time.Minute && p.FX != nil, "ageSeconds": max(0, now.Sub(p.At).Seconds())}
	}
	report, err := h.bookFlowReport(ctx, s, now)
	if err != nil {
		return nil, err
	}
	chartFrom, chartTo := from, to
	if id != "" && len(events) > 0 {
		chartFrom = events[0].At.Add(-time.Hour)
		chartTo = minTime(now, events[0].At.Add(4*time.Hour))
	}
	prices := []any{}
	byTime := map[int64]*string{}
	err = factsAsOf(ctx, h.Store.shortDB(), ID("candles", "BTC", "Binance", "spot"), chartFrom.Truncate(5*time.Minute), chartTo, now, func(o Observation) error {
		at := recordTime(o)
		if o.Quality == "valid" && o.Resolution == 300 && o.Payload.Candle != nil && !at.Add(5*time.Minute).After(chartTo) {
			byTime[at.Add(5*time.Minute).Unix()] = finiteDecimal(&o.Payload.Candle.Close)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	for at := chartFrom.Truncate(5 * time.Minute).Add(5 * time.Minute); !at.After(chartTo); at = at.Add(5 * time.Minute) {
		prices = append(prices, map[string]any{"at": at, "closeUsdt": byTime[at.Unix()]})
	}
	jobs := []any{}
	scheduler := h.Scheduler.State()
	for _, j := range scheduler["jobs"].([]Job) {
		if coreBook(j.Dataset) || minuteFoot(j.Dataset) || coreFiveFoot(j.Dataset) {
			jobs = append(jobs, j)
		}
	}
	return map[string]any{"prices": prices, "at": now, "mode": h.bookFlowMode, "rulesVersion": BookFlowRules, "collectionVersion": BookFlowCollection, "origin": origin, "lastProcessedAt": last, "runtime": runtime, "contracts": contracts, "current": current, "items": items, "more": more, "limit": limit, "offset": offset, "from": from, "to": to, "storageBytes": used, "budgetBytes": bookFlowBudget, "scheduler": map[string]any{"jobs": jobs, "limitPerMinute": scheduler["limitPerMinute"], "usedLastMinute": scheduler["usedLastMinute"], "rateLimited": scheduler["rateLimited"]}, "report": report, "note": "原始盘口快照与同交易所成交的承接线索；不证明同一委托、撤单或资金身份，不是做多做空指令。站内观察，不发邮件、不进入模拟。", "metricDefinitions": map[string]string{"baseline": "此前30分钟同交易所同固定区域数量中位数，至少29个有效分钟", "return": "阶段实际发布后下一根完整5分钟现货K线开盘，无费用，非可成交价", "coverage": "实验起点后完整源分钟，两家盘口及足迹各自首次在三分钟内取得；等待取得期限结束后冻结，缺口不追认，最初不足一分钟不纳入", "independent": "每方向每阶段按时间取最早事件，其后4小时窗口内其他事件单列为重叠样本", "absorption": "五个完整分钟成交冲击且快照深度保持；不能确定成交与后续挂单来自同一订单"}}, nil
}
func bookTrialView(t ShortTrial) any {
	outcomes := []any{}
	for _, o := range t.Outcomes {
		outcomes = append(outcomes, map[string]any{"minutes": o.Minutes, "state": o.State, "coverage": o.Coverage, "returnPercent": finiteDecimal(o.Return), "mfePercent": finiteDecimal(o.MFE), "maePercent": finiteDecimal(o.MAE), "mfeAt": o.MFEAt, "maeAt": o.MAEAt})
	}
	return map[string]any{"id": t.ID, "stage": strings.TrimPrefix(t.Rule, BookFlowRules+"/"), "publishedAt": t.At, "start": t.Start, "referenceUsdt": finiteDecimal(t.Reference), "done": t.Done, "outcomes": outcomes}
}

// Report limits are explicit: saturation makes stage eligibility false instead
// of silently presenting a truncated subset as the full forward experiment.
func (h *Hub) bookFlowReport(ctx context.Context, s bookFlowState, now time.Time) (any, error) {
	trials, err := paperRows[ShortTrial](ctx, h.Store.shortDB(), "SELECT payload FROM book_flow WHERE kind='trial' ORDER BY at,id LIMIT 3001")
	if err != nil {
		return nil, err
	}
	truncated := len(trials) > 3000
	if truncated {
		trials = trials[:3000]
	}
	quality := map[string]bool{}
	qr, err := h.Store.shortDB().QueryContext(ctx, "SELECT id,json_extract(payload,'$.completePath') FROM book_flow WHERE kind='event' ORDER BY at,id LIMIT 3001")
	if err != nil {
		return nil, err
	}
	for qr.Next() {
		var id string
		var complete bool
		if err = qr.Scan(&id, &complete); err != nil {
			break
		}
		quality[id] = complete
	}
	if err == nil {
		err = qr.Err()
	}
	qr.Close()
	if err != nil {
		return nil, err
	}
	minutes, timely, bookTimely := 0, 0, 0
	for _, d := range s.Days {
		minutes += d.Samples
		timely += d.Timely
		bookTimely += d.BookTimely
	}
	coverage := float64(timely) / float64(max(1, minutes))
	var coverageValue *float64
	if minutes > 0 {
		coverageValue = &coverage
	}
	groups := []any{}
	priceEvents := []PriceEpisode{}
	formal := []Signal{}
	if !s.Origin.IsZero() {
		priceEvents, err = loadPriceEvents(ctx, h.Store.shortDB(), "BTC", s.Origin, now.Add(-4*time.Hour))
		if err != nil {
			return nil, err
		}
		kept := []PriceEpisode{}
		for _, e := range priceEvents {
			if e.ReconstructedAt == nil && !e.Start.Before(s.Origin) {
				kept = append(kept, e)
			}
		}
		priceEvents = kept
		formal, err = paperRows[Signal](ctx, h.Store.shortDB(), "SELECT payload FROM documents WHERE kind='signal' AND asset='BTC' AND at>=? AND json_extract(payload,'$.rulesVersion')=? ORDER BY at LIMIT 3001", s.Origin.Unix(), MultifactorRules)
		if err != nil {
			return nil, err
		}
		if len(formal) > 3000 {
			truncated = true
			formal = formal[:3000]
		}
	}
	for _, stage := range []string{"enhanced", "absorption", "following"} {
		all, mature, positive, buy, sell, overlap := 0, 0, 0, 0, 0, 0
		qualityN, qualityBuy, qualitySell, qualityPositive, qualitySum := 0, 0, 0, 0, 0.0
		next := map[string]time.Time{}
		values := []float64{}
		daily := map[string][]float64{}
		stageSignals := []Signal{}
		for _, t := range trials {
			if t.Rule != BookFlowRules+"/"+stage {
				continue
			}
			all++
			stageSignals = append(stageSignals, Signal{ID: t.ID, Direction: t.Direction, At: t.At})
			if t.Start.Before(next[t.Direction]) {
				overlap++
				continue
			}
			next[t.Direction] = t.Start.Add(4 * time.Hour)
			for _, o := range t.Outcomes {
				if o.Minutes != 240 || o.State != "complete" || o.Return == nil {
					continue
				}
				mature++
				if quality[strings.TrimSuffix(t.ID, "/"+stage)] {
					qualityN++
					qualitySum += *o.Return
					if *o.Return > 0 {
						qualityPositive++
					}
					if t.Direction == "buy" {
						qualityBuy++
					} else {
						qualitySell++
					}
				}
				values = append(values, *o.Return)
				daily[t.At.UTC().Format("2006-01-02")] = append(daily[t.At.UTC().Format("2006-01-02")], *o.Return)
				if *o.Return > 0 {
					positive++
				}
				if t.Direction == "buy" {
					buy++
				} else {
					sell++
				}
			}
		}
		eligible := !s.Origin.IsZero() && now.Sub(s.Origin) >= 30*24*time.Hour && qualityN >= 100 && qualityBuy >= 30 && qualitySell >= 30 && coverage >= .95 && !truncated
		var mean, median, rate *string
		var interval any
		if len(values) > 0 {
			sum := 0.0
			for _, v := range values {
				sum += v
			}
			mean = finiteDecimal(flowPtr(sum / float64(len(values))))
			median = finiteDecimal(flowPtr(percentile(append([]float64(nil), values...), .5)))
			rate = finiteDecimal(flowPtr(100 * float64(positive) / float64(mature)))
		}
		if eligible {
			interval = bookDailyInterval(daily, s.Origin, now)
		}
		var qualityMean *string
		if qualityN > 0 {
			qualityMean = finiteDecimal(flowPtr(qualitySum / float64(qualityN)))
		}
		groups = append(groups, map[string]any{"relativeFormal": bookRelativeFormal(priceEvents, stageSignals, formal, now), "completeEvidence": map[string]any{"independent4h": qualityN, "buy": qualityBuy, "sell": qualitySell, "positive": qualityPositive, "meanPercent": qualityMean}, "marketCoverage": bookMarketCoverage(priceEvents, stageSignals, now), "stage": stage, "all": all, "overlapping": overlap, "completeIndependent4h": mature, "buy": buy, "sell": sell, "positive4h": positive, "nonPositive4h": mature - positive, "positivePercent": rate, "meanPercent": mean, "medianPercent": median, "eligible": eligible, "interval95": interval})
	}
	return map[string]any{"formalMarketCoverage": bookMarketCoverage(priceEvents, formal, now), "marketCoverageNote": "复用原两根5分钟突破行情事件、一对一匹配、4小时窗口；仅包含已登记且非重构行情事件，未匹配不等同亏损，缺少行情事实不能当全市场覆盖。", "groups": groups, "observedMinutes": minutes, "timelyMinutes": timely, "bookTimelyMinutes": bookTimely, "coverage": coverageValue, "truncated": truncated, "assessment": "阶段门槛前仅描述样本，不判断策略有效；价格观察不等于成本后交易收益。", "minimumDays": 30, "minimumIndependent": 100, "minimumPerSide": 30, "minimumCoverage": .95}, nil
}
func bookDailyInterval(daily map[string][]float64, from, to time.Time) any {
	blocks := [][]float64{}
	for d := from.UTC().Truncate(24 * time.Hour); !d.After(to.UTC().Truncate(24 * time.Hour)); d = d.Add(24 * time.Hour) {
		blocks = append(blocks, daily[d.Format("2006-01-02")])
	}
	if len(blocks) == 0 {
		return nil
	}
	rng := rand.New(rand.NewSource(1))
	means := []float64{}
	for i := 0; i < 2000; i++ {
		sum, n := 0.0, 0
		for range blocks {
			for _, v := range blocks[rng.Intn(len(blocks))] {
				sum += v
				n++
			}
		}
		if n > 0 {
			means = append(means, sum/float64(n))
		}
	}
	if len(means) == 0 {
		return nil
	}
	sort.Float64s(means)
	return map[string]any{"lowPercent": finiteDecimal(flowPtr(percentile(means, .025))), "highPercent": finiteDecimal(flowPtr(percentile(means, .975))), "method": "UTC日块，含无样本日，2000次，固定随机种子"}
}

func bookMarketCoverage(events []PriceEpisode, signals []Signal, now time.Time) any {
	r := matchEpisodes(events, signals, now)
	matches := []any{}
	for _, m := range r.Matches {
		matches = append(matches, map[string]any{"eventId": m.EventID, "signalId": m.SignalID, "timing": m.Timing, "leadMinutes": finiteDecimal(m.LeadMinutes)})
	}
	return map[string]any{"priceEpisodes": r.PriceEpisodes, "early": r.Early, "following": r.Following, "missed": r.Missed, "unmatched": r.Unmatched, "pending": r.Pending, "matches": matches}
}

// Positive lead means this stage was visible before the formal signal matched
// to the very same price episode; unmatched episodes never get an invented lead.
func bookRelativeFormal(events []PriceEpisode, signals, formal []Signal, now time.Time) any {
	a, b := matchEpisodes(events, signals, now), matchEpisodes(events, formal, now)
	times := map[string]time.Time{}
	for _, s := range append(append([]Signal(nil), signals...), formal...) {
		times[s.ID] = s.At
	}
	matched := map[string]string{}
	for _, m := range b.Matches {
		matched[m.EventID] = m.SignalID
	}
	values := []float64{}
	pairs := []any{}
	for _, m := range a.Matches {
		f, ok := matched[m.EventID]
		if !ok {
			continue
		}
		lead := times[f].Sub(times[m.SignalID]).Minutes()
		values = append(values, lead)
		pairs = append(pairs, map[string]any{"eventId": m.EventID, "observationId": m.SignalID, "formalId": f, "leadMinutes": finiteDecimal(&lead)})
	}
	var median *string
	if len(values) > 0 {
		median = finiteDecimal(flowPtr(percentile(values, .5)))
	}
	return map[string]any{"matchedPairs": len(values), "medianLeadMinutes": median, "pairs": pairs, "definition": "同一行情事件各自一对一匹配；正数表示该观察更早；未匹配不计算提前量"}
}

// Aggregate-only, bounded diagnostics for the existing five-minute sampler.
// Never include raw levels, credentials, events or unbounded research results.
func (h *Hub) bookFlowHealth(now time.Time) map[string]any {
	h.mu.RLock()
	runtime := h.bookFlowRuntime
	h.mu.RUnlock()
	runtime.Skipped = nil
	v := map[string]any{"mode": h.bookFlowMode, "rulesVersion": BookFlowRules, "collectionVersion": BookFlowCollection, "runtime": runtime, "origin": nil, "coverage": nil}
	if h.bookFlowMode == "off" {
		return v
	}
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	s := bookFlowState{}
	if err := h.Store.bookFlowLoad(ctx, "state", BookFlowRules, &s); err != nil && err != sql.ErrNoRows {
		v["error"] = err.Error()
		return v
	}
	if !s.Origin.IsZero() {
		v["origin"] = s.Origin
		v["lastProcessedAt"] = s.Last
	}
	observed, timely := 0, 0
	for _, d := range s.Days {
		observed += d.Samples
		timely += d.Timely
	}
	v["observedMinutes"], v["timelyMinutes"] = observed, timely
	if observed > 0 {
		v["coverage"] = float64(timely) / float64(observed)
	}
	contracts := []minuteContract{}
	for _, venue := range []string{"Binance", "OKX"} {
		c := minuteContract{Venue: venue}
		if err := h.Store.bookFlowLoad(ctx, "contract", minuteFootID(venue), &c); err != nil && err != sql.ErrNoRows {
			v["error"] = err.Error()
			return v
		}
		contracts = append(contracts, c)
	}
	v["contracts"] = contracts
	jobs := []map[string]any{}
	for _, j := range h.Scheduler.State()["jobs"].([]Job) {
		if j.Mode != "live" || (!coreBook(j.Dataset) && !minuteFoot(j.Dataset)) {
			continue
		}
		jobs = append(jobs, map[string]any{"id": j.ID, "refreshSeconds": j.Dataset.Refresh, "lastSuccess": j.LastSuccess, "next": j.Next, "disabled": j.Disabled, "failures": j.Failures, "successIntervalSeconds": j.SuccessIntervalSeconds, "sourceLagSeconds": j.SourceLagSeconds, "queueWaitSeconds": j.QueueWaitSeconds, "fetchSeconds": j.FetchSeconds, "observedSamples": j.ObservedSamples, "timelySamples": j.TimelySamples, "lastFailureAt": j.LastFailureAt, "lastFailure": j.LastFailure})
	}
	v["jobs"] = jobs
	return v
}
