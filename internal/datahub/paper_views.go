package datahub

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math/rand"
	"net/url"
	"time"

	"github.com/shopspring/decimal"
)

type paperStats struct {
	Closed             int              `json:"closed"`
	Complete           int              `json:"complete"`
	PathComplete       int              `json:"pathComplete"`
	PathLongs          int              `json:"pathLongs"`
	PathShorts         int              `json:"pathShorts"`
	PendingFunding     int              `json:"pendingFunding"`
	Abnormal           int              `json:"abnormal"`
	Wins               int              `json:"wins"`
	Longs              int              `json:"longs"`
	Shorts             int              `json:"shorts"`
	Net                *decimal.Decimal `json:"net"`
	Expectancy         *decimal.Decimal `json:"expectancy"`
	WinRate            *decimal.Decimal `json:"winRate"`
	PayoffRatio        *decimal.Decimal `json:"payoffRatio"`
	ProfitFactor       *decimal.Decimal `json:"profitFactor"`
	AverageHoldSeconds *decimal.Decimal `json:"averageHoldSeconds"`
	NotionalReturn     *decimal.Decimal `json:"notionalReturn"`
	DoubleCostNet      *decimal.Decimal `json:"doubleCostNet"`
	Eligible           bool             `json:"eligible"`
	ConfidenceLow      *decimal.Decimal `json:"confidenceLow"`
	ConfidenceHigh     *decimal.Decimal `json:"confidenceHigh"`
	Gate               string           `json:"gate"`
}
type paperDay struct {
	sum   decimal.Decimal
	count int
}

func paperStatistics(trades []paperTrade, s paperState, now time.Time, clean bool) paperStats {
	r := paperStats{Gate: "样本不足：至少30天、100笔完整平仓、多空各30笔、行情覆盖95%"}
	sum, profit, loss, fees, notional, held := decimal.Zero, decimal.Zero, decimal.Zero, decimal.Zero, decimal.Zero, decimal.Zero
	winning, losing := 0, 0
	days := map[int64]paperDay{}
	for _, t := range trades {
		if t.Exited == nil {
			continue
		}
		if clean && len(t.Quality) > 0 {
			continue
		}
		r.Closed++
		if len(t.Quality) > 0 {
			r.Abnormal++
		}
		if !paperFundingKnown(s, *t.Exited, false) {
			r.PendingFunding++
			continue
		}
		net := t.Gross.Sub(t.Fees).Add(t.Funding)
		sum = sum.Add(net)
		fees = fees.Add(t.Fees).Add(t.Slippage)
		notional = notional.Add(t.Entry.Mul(t.Quantity))
		held = held.Add(decimal.NewFromInt(int64(t.Exited.Sub(t.Entered).Seconds())))
		r.Complete++
		if len(t.Quality) == 0 {
			r.PathComplete++
			if t.Signal.Direction == "buy" {
				r.PathLongs++
			} else {
				r.PathShorts++
			}
		}
		if t.Signal.Direction == "buy" {
			r.Longs++
		} else {
			r.Shorts++
		}
		if net.IsPositive() {
			profit = profit.Add(net)
			winning++
			r.Wins++
		} else if net.IsNegative() {
			loss = loss.Sub(net)
			losing++
		}
		day := t.Entered.UTC().Truncate(24 * time.Hour).Unix()
		v := days[day]
		v.sum = v.sum.Add(net)
		v.count++
		days[day] = v
	}
	if r.Complete == 0 {
		return r
	}
	n := decimal.NewFromInt(int64(r.Complete))
	expect := sum.Div(n)
	rate := decimal.NewFromInt(int64(r.Wins)).Div(n).Mul(decimal.NewFromInt(100))
	hold := held.Div(n)
	cost := sum.Sub(fees)
	r.Net, r.Expectancy, r.WinRate, r.AverageHoldSeconds, r.DoubleCostNet = &sum, &expect, &rate, &hold, &cost
	if notional.IsPositive() {
		ret := sum.Div(notional).Mul(decimal.NewFromInt(100))
		r.NotionalReturn = &ret
	}
	if loss.IsPositive() {
		v := profit.Div(loss)
		r.ProfitFactor = &v
		if winning > 0 && losing > 0 {
			v := profit.Div(decimal.NewFromInt(int64(winning))).Div(loss.Div(decimal.NewFromInt(int64(losing))))
			r.PayoffRatio = &v
		}
	}
	coverage := float64(s.CoveredSeconds) / float64(max(s.ObservedSeconds, 1))
	r.Eligible = s.Origin != nil && now.Sub(*s.Origin) >= 30*24*time.Hour && r.PathComplete >= 100 && r.PathLongs >= 30 && r.PathShorts >= 30 && coverage >= .95
	if !r.Eligible {
		return r
	}
	// Fixed deterministic seed, 2,000 UTC-day blocks, including zero-trade days.
	// Resampling daily sums AND daily counts preserves clusters of same-day
	// correlated trades without pretending every fill is independent evidence.
	type bootstrapDay struct {
		sum   float64
		count int
	}
	blocks := []bootstrapDay{}
	for at := s.Origin.UTC().Truncate(24 * time.Hour); !at.After(now); at = at.Add(24 * time.Hour) {
		v := days[at.Unix()]
		sum, _ := v.sum.Float64()
		blocks = append(blocks, bootstrapDay{sum: sum, count: v.count})
	}
	rng := rand.New(rand.NewSource(20261006))
	samples := []float64{}
	for i := 0; i < 2000; i++ {
		total := 0.0
		n := 0
		for range blocks {
			v := blocks[rng.Intn(len(blocks))]
			total += v.sum
			n += v.count
		}
		if n > 0 {
			samples = append(samples, total/float64(n))
		}
	}
	if len(samples) > 0 {
		low := decimal.NewFromFloat(percentile(samples, .025)).Round(8)
		high := decimal.NewFromFloat(percentile(samples, .975)).Round(8)
		r.ConfidenceLow, r.ConfidenceHigh = &low, &high
		r.Gate = "阶段性观察：展示按日分块的95%净期望区间，不据此宣称可实盘"
	}
	return r
}

type paperEquityPoint struct {
	At    time.Time        `json:"at"`
	Value *decimal.Decimal `json:"value"`
}
type paperCurve struct {
	Points                 []paperEquityPoint `json:"points"`
	MaximumDrawdown        *decimal.Decimal   `json:"maximumObservedDrawdown"`
	MaximumDrawdownPercent *decimal.Decimal   `json:"maximumObservedDrawdownPercent"`
	ExposedSeconds         int64              `json:"exposedSeconds"`
	GapSamples             int                `json:"gapSamples"`
	Resolution             int                `json:"resolutionSeconds"`
}

func (p *paperStore) equityCurve(ctx context.Context, s paperState, group string, now time.Time) (paperCurve, error) {
	return paperEquityCurve(ctx, p.readDB, s, group, now)
}
func paperEquityCurve(ctx context.Context, reader paperQuerier, s paperState, group string, now time.Time) (paperCurve, error) {
	curve := paperCurve{Points: []paperEquityPoint{}, Resolution: 60}
	funding, err := paperRows[paperFundingEntry](ctx, reader, "SELECT payload FROM paper_funding WHERE group_id=? ORDER BY at,trade_id", group)
	if err != nil {
		return curve, err
	}
	r, err := reader.QueryContext(ctx, "SELECT payload FROM paper_equity WHERE group_id=? ORDER BY at", group)
	if err != nil {
		return curve, err
	}
	defer r.Close()
	var peak *decimal.Decimal
	maxDrop, maxPercent, funded := decimal.Zero, decimal.Zero, decimal.Zero
	index := 0
	var last time.Time
	valid := 0
	for r.Next() {
		var raw []byte
		var e paperEquity
		if err = r.Scan(&raw); err != nil {
			return curve, err
		}
		if err = json.Unmarshal(raw, &e); err != nil {
			return curve, err
		}
		for index < len(funding) && !funding[index].Funding.At.After(e.At) {
			funded = funded.Add(funding[index].Amount)
			index++
		}
		point := paperEquityPoint{At: e.At}
		gap := e.BeforeFunding == nil || !paperFundingKnown(s, e.At, false) || !last.IsZero() && e.At.Sub(last) > 65*time.Second
		if e.Occupied && !last.IsZero() && e.At.Sub(last) == time.Minute {
			curve.ExposedSeconds += 60
		}
		if gap {
			curve.GapSamples++
			peak = nil
		} else {
			v := e.BeforeFunding.Add(funded)
			point.Value = &v
			valid++
			if peak == nil || v.GreaterThan(*peak) {
				peak = &v
			}
			drop := peak.Sub(v)
			if drop.GreaterThan(maxDrop) {
				maxDrop = drop
			}
			if peak.IsPositive() {
				pct := drop.Div(*peak).Mul(decimal.NewFromInt(100))
				if pct.GreaterThan(maxPercent) {
					maxPercent = pct
				}
			}
		}
		last = e.At
		// Chart keeps the last 24h at native 60s resolution. The drawdown
		// calculation above uses the entire retained accounting series.
		if !e.At.Before(now.Add(-24 * time.Hour)) {
			curve.Points = append(curve.Points, point)
		}
	}
	if valid > 0 {
		curve.MaximumDrawdown, curve.MaximumDrawdownPercent = &maxDrop, &maxPercent
	}
	return curve, r.Err()
}

func paperTradeView(t paperTrade, s paperState) map[string]any {
	var net, ret *decimal.Decimal
	pending := t.Exited == nil || !paperFundingKnown(s, *t.Exited, false)
	if !pending {
		v := t.Gross.Sub(t.Fees).Add(t.Funding)
		net = &v
		den := t.Entry.Mul(t.Quantity)
		if den.IsPositive() {
			r := v.Div(den).Mul(decimal.NewFromInt(100))
			ret = &r
		}
	}
	return map[string]any{"trade": t, "netPnl": net, "notionalReturnPercent": ret, "fundingPending": pending}
}
func (h *Hub) paperRead(ctx context.Context, path string, q url.Values) (json.RawMessage, error) {
	if path != "paper" && path != "paper/trades" && path != "paper/trade" {
		return nil, errors.New("未知模拟仓位接口")
	}
	if q.Get("asset") != "" && q.Get("asset") != "BTC" {
		return nil, errors.New("模拟仓位仅支持BTCUSDT永续合约")
	}
	p := h.Store.paper
	if p == nil {
		if path == "paper/trade" {
			return nil, errors.New("模拟仓位账本未启用")
		}
		if path == "paper/trades" {
			return json.Marshal(map[string]any{"items": []any{}, "more": false, "enabled": false, "error": h.paperError})
		}
		return json.Marshal(map[string]any{"enabled": false, "mode": "off", "error": h.paperError, "version": PaperRules, "contract": "Binance BTCUSDT USDT本位永续", "accounts": []any{}})
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	now := time.Now().UTC()
	if path == "paper" {
		return h.cached(ctx, "paper/summary", 5*time.Second, func() (any, error) { return p.summary(ctx, now) })
	}
	tx, err := p.readDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var s paperState
	var raw []byte
	if err = tx.QueryRowContext(ctx, "SELECT payload FROM paper_state WHERE id=1").Scan(&raw); err != nil {
		return nil, err
	}
	if err = json.Unmarshal(raw, &s); err != nil {
		return nil, err
	}
	switch path {
	case "paper/trade":
		id := q.Get("id")
		if id == "" || len(id) > 240 {
			return nil, errors.New("无效模拟仓位ID")
		}
		trades, err := paperRows[paperTrade](ctx, tx, "SELECT payload FROM paper_trades WHERE id=?", id)
		if err != nil {
			return nil, err
		}
		if len(trades) != 1 {
			return nil, errors.New("模拟仓位不存在")
		}
		fills, err := paperRows[paperFill](ctx, tx, "SELECT payload FROM paper_fills WHERE trade_id=? ORDER BY at,id", id)
		if err != nil {
			return nil, err
		}
		funding, err := paperRows[paperFundingEntry](ctx, tx, "SELECT payload FROM paper_funding WHERE trade_id=? ORDER BY at", id)
		if err != nil {
			return nil, err
		}
		v := paperTradeView(trades[0], s)
		v["fills"], v["fundingEntries"] = fills, funding
		return json.Marshal(v)
	case "paper/trades":
		limit, offset := parseInt(q, "limit", 25, 1, 100), parseInt(q, "offset", 0, 0, 1000000)
		group := q.Get("group")
		if group != "" && group != "risk" && group != "opposite" {
			return nil, errors.New("无效实验组")
		}
		trades, err := paperRows[paperTrade](ctx, tx, "SELECT payload FROM paper_trades WHERE (?='' OR group_id=?) ORDER BY entered DESC,id DESC LIMIT ? OFFSET ?", group, group, limit+1, offset)
		if err != nil {
			return nil, err
		}
		more := len(trades) > limit
		if more {
			trades = trades[:limit]
		}
		items := []any{}
		for _, t := range trades {
			items = append(items, paperTradeView(t, s))
		}
		return json.Marshal(map[string]any{"items": items, "more": more, "offset": offset, "limit": limit})
	default:
		return nil, errors.New("未知模拟仓位接口")
	}
}
func (p *paperStore) summary(ctx context.Context, now time.Time) (any, error) {
	tx, err := p.readDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var s paperState
	var raw []byte
	if err = tx.QueryRowContext(ctx, "SELECT payload FROM paper_state WHERE id=1").Scan(&raw); err != nil {
		return nil, err
	}
	if err = json.Unmarshal(raw, &s); err != nil {
		return nil, err
	}
	p.mu.RLock()
	feed := p.feed
	feed.Error = p.err
	p.mu.RUnlock()
	trades, err := paperRows[paperTrade](ctx, tx, `SELECT json_object('id',id,'group',group_id,'enteredAt',json_extract(payload,'$.enteredAt'),'exitedAt',json_extract(payload,'$.exitedAt'),'entryPrice',json_extract(payload,'$.entryPrice'),'quantity',json_extract(payload,'$.quantity'),'grossPnl',json_extract(payload,'$.grossPnl'),'fees',json_extract(payload,'$.fees'),'slippageCost',json_extract(payload,'$.slippageCost'),'funding',json_extract(payload,'$.funding'),'quality',json_extract(payload,'$.quality'),'signal',json_object('id',json_extract(payload,'$.signal.id'),'direction',json_extract(payload,'$.signal.direction'),'level',json_extract(payload,'$.signal.level'))) FROM paper_trades ORDER BY entered,id`)
	if err != nil {
		return nil, err
	}
	groups := map[string][]paperTrade{}
	common := map[string]int{}
	closedCommon := map[string]int{}
	for _, t := range trades {
		groups[t.Group] = append(groups[t.Group], t)
		common[t.Signal.ID]++
		if t.Exited != nil && paperFundingKnown(s, *t.Exited, false) {
			closedCommon[t.Signal.ID]++
		}
	}
	accounts := []any{}
	for _, a := range s.Accounts {
		all := groups[a.Group]
		paired := []paperTrade{}
		directions := map[string][]paperTrade{"buy": {}, "sell": {}}
		levels := map[string][]paperTrade{}
		for _, t := range all {
			if common[t.Signal.ID] == 2 && closedCommon[t.Signal.ID] == 2 {
				paired = append(paired, t)
			}
			directions[t.Signal.Direction] = append(directions[t.Signal.Direction], t)
			level := t.Signal.Level
			if level == "" {
				level = "unknown"
			}
			levels[level] = append(levels[level], t)
		}
		byDirection := map[string]paperStats{}
		byLevel := map[string]paperStats{}
		for k, v := range directions {
			byDirection[k] = paperStatistics(v, s, now, false)
		}
		for k, v := range levels {
			byLevel[k] = paperStatistics(v, s, now, false)
		}
		var unrealized, equity, ret *decimal.Decimal
		if a.Position == nil || feed.Quote != nil && feed.Quote.valid(now) {
			u := decimal.Zero
			if a.Position != nil {
				u = paperCloseQuote(a.Position, *feed.Quote).Sub(a.Position.Entry).Mul(a.Position.Remaining).Mul(a.Position.sign())
			}
			unrealized = &u
			if paperFundingKnown(s, now, true) {
				e := a.cash().Add(u)
				r := e.Sub(paperInitial).Div(paperInitial).Mul(decimal.NewFromInt(100))
				equity, ret = &e, &r
			}
		}
		curve, err := paperEquityCurve(ctx, tx, s, a.Group, now)
		if err != nil {
			return nil, err
		}
		curve.ExposedSeconds = 0
		for _, t := range all {
			end := now
			if t.Exited != nil {
				end = *t.Exited
			}
			if end.After(t.Entered) {
				curve.ExposedSeconds += int64(end.Sub(t.Entered).Seconds())
			}
		}
		accounts = append(accounts, map[string]any{"account": a, "initialCapital": paperInitial, "unrealizedPnl": unrealized, "equity": equity, "accountReturnPercent": ret, "fundingPending": !paperFundingKnown(s, now, true), "all": paperStatistics(all, s, now, false), "clean": paperStatistics(all, s, now, true), "commonEntries": paperStatistics(paired, s, now, false), "byDirection": byDirection, "byPublishedLevel": byLevel, "curve": curve})
	}
	quality, err := paperRows[paperIntake](ctx, tx, "SELECT payload FROM paper_intakes ORDER BY at DESC,id DESC LIMIT 100")
	if err != nil {
		return nil, err
	}
	counts := map[string]int{}
	rows, err := tx.QueryContext(ctx, "SELECT json_extract(payload,'$.state'),count(*) FROM paper_intakes GROUP BY json_extract(payload,'$.state')")
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var k string
		var n int
		if err = rows.Scan(&k, &n); err != nil {
			rows.Close()
			return nil, err
		}
		counts[k] = n
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	events, err := paperRows[paperEvent](ctx, tx, "SELECT payload FROM paper_events ORDER BY at DESC LIMIT 100")
	if err != nil {
		return nil, err
	}
	coverage := float64(s.CoveredSeconds) / float64(max(s.ObservedSeconds, 1)) * 100
	return map[string]any{"enabled": p.mode != "off", "mode": p.mode, "contract": "Binance BTCUSDT USDT本位永续", "state": s, "feed": feed, "coveragePercent": coverage, "accounts": accounts, "quality": quality, "events": events, "intakeCounts": counts, "at": now, "parameters": s.Parameters}, nil
}
