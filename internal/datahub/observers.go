package datahub

// Low-frequency context never changes the signal score. These parsers keep
// source facts separate from display interpretations and tolerate nulls, not
// malformed numbers or conflicting aliases.
import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	_ "time/tzdata"
)

type Balance struct {
	Venue     string  `json:"venue"`
	RawVenue  string  `json:"rawVenue"`
	Value     *string `json:"balance"`
	Change1D  *string `json:"change1d,omitempty"`
	Change7D  *string `json:"change7d,omitempty"`
	Change30D *string `json:"change30d,omitempty"`
}
type ETFRecord struct {
	Date          string             `json:"date"`
	USD           *string            `json:"flowUsd"`
	Funds         map[string]*string `json:"funds"`
	Reconciled    bool               `json:"reconciled"`
	ExplicitFinal bool               `json:"explicitFinal"`
}
type Premium struct {
	USD           string `json:"premiumUsd"`
	RawRate       string `json:"rawRate"`
	CoinbasePrice string `json:"coinbasePrice"`
	// Official examples disagree on rate scaling. Preserve the raw field until
	// its unit is independently verified; never silently multiply by 100.
	RateUnit string `json:"rateUnit"`
}

func canonicalVenue(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "gate", "gate.io":
		return "Gate.io"
	case "coinbase pro", "coinbasepro", "coinbase":
		return "Coinbase"
	}
	return strings.TrimSpace(v)
}
func nullableNumber(v any, signed bool) (*string, error) {
	if v == nil {
		return nil, nil
	}
	n, e := validNumber(v, signed)
	if e != nil {
		return nil, e
	}
	return &n, nil
}
func uniqueBalances(rows []Balance) ([]Balance, error) {
	seen := map[string]Balance{}
	for _, b := range rows {
		if b.Venue == "" {
			return nil, errors.New("钱包交易所名称缺失")
		}
		if p, ok := seen[b.Venue]; ok {
			if p.Value == nil && b.Value == nil {
				continue
			}
			if p.Value == nil || b.Value == nil || *p.Value != *b.Value {
				return nil, errors.New("交易所别名余额冲突，停止聚合")
			}
			continue
		}
		seen[b.Venue] = b
	}
	out := make([]Balance, 0, len(seen))
	for _, b := range seen {
		out = append(out, b)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Venue < out[j].Venue })
	return out, nil
}
func normalizeObservers(d Dataset, data any, fetched time.Time) ([]Observation, error) {
	out := []Observation{}
	obs := func(at *time.Time, p Payload) Observation {
		basis := "source"
		if at == nil {
			basis = "retrieval"
		}
		return Observation{Dataset: d.ID, Source: d.Source, ObservedAt: at, FetchedAt: fetched, Resolution: d.Resolution, TimeBasis: basis, Quality: "valid", Payload: p}
	}
	switch d.Kind {
	case "balance-list":
		rows := []Balance{}
		for _, v := range array(data) {
			m := object(v)
			b := Balance{RawVenue: str(m["exchange_name"]), Venue: canonicalVenue(str(m["exchange_name"]))}
			var e error
			if b.Value, e = nullableNumber(m["total_balance"], false); e != nil {
				return nil, e
			}
			if b.Change1D, e = nullableNumber(m["balance_change_1d"], true); e != nil {
				return nil, e
			}
			if b.Change7D, e = nullableNumber(m["balance_change_7d"], true); e != nil {
				return nil, e
			}
			if b.Change30D, e = nullableNumber(m["balance_change_30d"], true); e != nil {
				return nil, e
			}
			rows = append(rows, b)
		}
		rows, e := uniqueBalances(rows)
		if e != nil {
			return nil, e
		}
		if len(rows) == 0 {
			return nil, ErrNoData
		}
		out = append(out, obs(nil, Payload{Balances: rows}))
	case "balance-history":
		m := object(data)
		times := array(m["time_list"])
		series := object(m["data_map"])
		if len(times) == 0 || len(series) == 0 {
			return nil, errors.New("钱包历史契约不匹配：缺少 time_list/data_map")
		}
		for venue, vals := range series {
			if len(array(vals)) != len(times) {
				return nil, fmt.Errorf("钱包历史序列长度不一致: %s", venue)
			}
		}
		for i, t := range times {
			at := timestamp(t)
			if at == nil {
				return nil, errors.New("钱包历史时间缺失")
			}
			rows := []Balance{}
			for v, vals := range series {
				n, e := nullableNumber(array(vals)[i], false)
				if e != nil {
					return nil, e
				}
				rows = append(rows, Balance{Venue: canonicalVenue(v), RawVenue: v, Value: n})
			}
			rows, e := uniqueBalances(rows)
			if e != nil {
				return nil, e
			}
			out = append(out, obs(at, Payload{Balances: rows}))
		}
	case "etf":
		for _, v := range array(data) {
			m := object(v)
			at := timestamp(m["timestamp"])
			if at == nil {
				return nil, errors.New("ETF交易日缺失")
			}
			n, e := nullableNumber(m["flow_usd"], true)
			if e != nil {
				return nil, e
			}
			r := ETFRecord{Date: at.Format("2006-01-02"), USD: n, Funds: map[string]*string{}, ExplicitFinal: m["is_final"] == true}
			sum := decimal.Zero
			complete := true
			for _, f := range array(m["etf_flows"]) {
				fm := object(f)
				ticker := str(fm["etf_ticker"])
				if ticker == "" {
					return nil, errors.New("ETF基金名称缺失")
				}
				if _, ok := r.Funds[ticker]; ok {
					return nil, errors.New("ETF基金重复")
				}
				val, e := nullableNumber(fm["flow_usd"], true)
				if e != nil {
					return nil, e
				}
				r.Funds[ticker] = val
				if val == nil {
					complete = false
				} else {
					sum = sum.Add(dec(*val))
				}
			}
			r.Reconciled = complete && len(r.Funds) > 0 && n != nil && sum.Sub(dec(*n)).Abs().LessThanOrEqual(decimal.NewFromInt(1))
			out = append(out, obs(at, Payload{ETF: &r}))
		}
	case "oi-history":
		for _, v := range array(data) {
			m := object(v)
			at := timestamp(m["time"])
			n, e := validNumber(m["close"], false)
			if e != nil || at == nil {
				return nil, errors.New("OI历史缺少有效时间/close")
			}
			if at.Add(time.Duration(d.Resolution) * time.Second).After(fetched) {
				continue
			}
			out = append(out, obs(at, Payload{OI: []Interest{{Venue: "CoinGlass aggregate", USD: n, Base: ""}}}))
		}
	case "premium":
		for _, v := range array(data) {
			m := object(v)
			at := timestamp(m["time"])
			if at == nil {
				return nil, errors.New("溢价时间缺失")
			}
			n, e := validNumber(m["premium"], true)
			if e != nil {
				return nil, e
			}
			rate, e := validNumber(m["premium_rate"], true)
			if e != nil {
				return nil, e
			}
			p, e := validNumber(m["coinbase_price"], false)
			if e != nil || dec(p).IsZero() {
				return nil, errors.New("溢价参考价格无效")
			}
			if at.Add(time.Duration(d.Resolution) * time.Second).After(fetched) {
				continue
			}
			out = append(out, obs(at, Payload{Premium: &Premium{n, rate, p, "unverified"}}))
		}
	}
	if len(out) == 0 {
		return nil, ErrNoData
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Time().Before(out[j].Time()) })
	return out, nil
}

type BalanceChange struct {
	From     time.Time `json:"from"`
	To       time.Time `json:"to"`
	Hours    float64   `json:"hours"`
	Delta    *string   `json:"delta"`
	Percent  *float64  `json:"percent"`
	Venues   []string  `json:"venues"`
	Excluded []string  `json:"excluded"`
}

func comparableBalances(a, b Observation) BalanceChange {
	c := BalanceChange{From: a.Time(), To: b.Time(), Hours: b.Time().Sub(a.Time()).Hours(), Venues: []string{}, Excluded: []string{}}
	left, right := map[string]*string{}, map[string]*string{}
	for _, r := range a.Payload.Balances {
		left[r.Venue] = r.Value
	}
	for _, r := range b.Payload.Balances {
		right[r.Venue] = r.Value
	}
	names := map[string]bool{}
	for v := range left {
		names[v] = true
	}
	for v := range right {
		names[v] = true
	}
	before, after := decimal.Zero, decimal.Zero
	for v := range names {
		if left[v] == nil || right[v] == nil {
			c.Excluded = append(c.Excluded, v)
			continue
		}
		c.Venues = append(c.Venues, v)
		before = before.Add(dec(*left[v]))
		after = after.Add(dec(*right[v]))
	}
	sort.Strings(c.Venues)
	sort.Strings(c.Excluded)
	if len(c.Venues) > 0 && c.Hours > 0 {
		delta := after.Sub(before).String()
		c.Delta = &delta
		if !before.IsZero() {
			p := after.Sub(before).Div(before).Mul(decimal.NewFromInt(100)).InexactFloat64()
			c.Percent = &p
		}
	}
	return c
}
func (h *Hub) WalletTrends(ctx context.Context, a string) (any, error) {
	now := time.Now().UTC()
	d, _ := h.Dataset(ID("balance-history", a, "", "chain"))
	list, _ := h.Dataset(ID("balance-list", a, "", "chain"))
	o, ok := h.Store.Latest(list.ID)
	hist, histOK := h.Store.Latest(d.ID)
	rows := []Observation{}
	e := h.Store.Visit(ctx, d, 86400, now.Add(-90*24*time.Hour), now, func(o Observation) error { rows = append(rows, o); return nil })
	if e != nil {
		return nil, e
	}
	changes := []BalanceChange{}
	for i := 1; i < len(rows); i++ {
		changes = append(changes, comparableBalances(rows[i-1], rows[i]))
	}
	trends := map[string]any{}
	if len(rows) > 0 {
		last := rows[len(rows)-1]
		for _, days := range []int{1, 3, 7, 30} {
			cut := last.Time().Add(-time.Duration(days) * 24 * time.Hour)
			var found *Observation
			for i := range rows {
				r := &rows[i]
				if !r.Time().After(cut) {
					found = r
				}
			}
			if found != nil && cut.Sub(found.Time()) <= 26*time.Hour {
				trends[fmt.Sprint(days)] = comparableBalances(*found, last)
			} else {
				trends[fmt.Sprint(days)] = nil
			}
		}
	}
	total := decimal.Zero
	count := 0
	for _, b := range o.Payload.Balances {
		if b.Value != nil {
			count++
			total = total.Add(dec(*b.Value))
		}
	}
	var balanceTotal *string
	if count > 0 {
		value := total.String()
		balanceTotal = &value
	}
	return map[string]any{"asset": a, "name": "交易所钱包余额净变化", "weight": 0, "unit": a, "meta": metadata(list, o, ok), "historyMeta": metadata(d, hist, histOK), "balances": o.Payload.Balances, "balanceTotal": balanceTotal, "covered": count, "changes": changes, "trends": trends, "note": "净变化仅比较两端共同交易所；不等于充值/提现总流水，不作分钟预警。获取时间不是来源发布时间。"}, nil
}

func observedHoliday(t time.Time) time.Time {
	switch t.Weekday() {
	case time.Saturday:
		return t.AddDate(0, 0, -1)
	case time.Sunday:
		return t.AddDate(0, 0, 1)
	}
	return t
}
func tradingDay(t time.Time) bool {
	t = t.UTC()
	if t.Weekday() == time.Saturday || t.Weekday() == time.Sunday {
		return false
	}
	y := t.Year()
	date := t.Format("2006-01-02")
	for _, md := range [][2]int{{1, 1}, {7, 4}, {12, 25}, {6, 19}} {
		if md[0] == 6 && y < 2022 {
			continue
		}
		if observedHoliday(time.Date(y, time.Month(md[0]), md[1], 0, 0, 0, 0, time.UTC)).Format("2006-01-02") == date {
			return false
		}
	}
	if observedHoliday(time.Date(y+1, 1, 1, 0, 0, 0, 0, time.UTC)).Format("2006-01-02") == date {
		return false
	}
	nth := (t.Day()-1)/7 + 1
	if t.Weekday() == time.Monday && ((t.Month() == 1 && nth == 3) || (t.Month() == 2 && nth == 3) || (t.Month() == 5 && t.AddDate(0, 0, 7).Month() != 5) || (t.Month() == 9 && nth == 1)) {
		return false
	}
	if t.Month() == 11 && t.Weekday() == time.Thursday && nth == 4 {
		return false
	}
	// Gregorian Easter; US cash equities are closed on Good Friday.
	a := y % 19
	b := y / 100
	c := y % 100
	dd := b / 4
	e := b % 4
	f := (b + 8) / 25
	g := (b - f + 1) / 3
	hh := (19*a + b - dd - g + 15) % 30
	i := c / 4
	k := c % 4
	l := (32 + 2*e + 2*i - hh - k) % 7
	m := (a + 11*hh + 22*l) / 451
	easter := time.Date(y, time.Month((hh+l-7*m+114)/31), (hh+l-7*m+114)%31+1, 0, 0, 0, 0, time.UTC)
	return easter.AddDate(0, 0, -2).Format("2006-01-02") != date
}
func etfState(r ETFRecord, now time.Time) string {
	day, e := time.Parse("2006-01-02", r.Date)
	if e != nil {
		return "missing"
	}
	if !tradingDay(day) {
		return "non_trading_day"
	}
	loc, _ := time.LoadLocation("America/New_York")
	close := time.Date(day.Year(), day.Month(), day.Day(), 16, 0, 0, 0, loc)
	if now.Before(close) {
		return "unfinished"
	}
	if r.USD == nil {
		return "unreported"
	}
	if !r.Reconciled {
		return "unreconciled"
	}
	if dec(*r.USD).IsZero() && !r.ExplicitFinal {
		nonzero := false
		for _, v := range r.Funds {
			if v != nil && !dec(*v).IsZero() {
				nonzero = true
			}
		}
		if !nonzero {
			return "zero_unconfirmed"
		}
	}
	return "reported"
}
func (h *Hub) ETFView(ctx context.Context, a string) (any, error) {
	now := time.Now().UTC()
	d, _ := h.Dataset(ID("etf", a, "", "fund"))
	latest, ok := h.Store.Latest(d.ID)
	points := []map[string]any{}
	byDate := map[string]ETFRecord{}
	e := h.Store.Visit(ctx, d, 86400, now.Add(-90*24*time.Hour), now, func(o Observation) error {
		if r := o.Payload.ETF; r != nil {
			byDate[r.Date] = *r
		}
		return nil
	})
	if e != nil {
		return nil, e
	}
	dates := []string{}
	for date := range byDate {
		dates = append(dates, date)
	}
	sort.Strings(dates)
	// Keep missing calendar dates explicit, including the current US session.
	// Their synthetic records carry nil, never manufactured zero flows.
	if len(dates) > 0 {
		first, _ := time.Parse("2006-01-02", dates[0])
		loc, _ := time.LoadLocation("America/New_York")
		today := now.In(loc).Format("2006-01-02")
		for day := first; day.Format("2006-01-02") <= today; day = day.AddDate(0, 0, 1) {
			date := day.Format("2006-01-02")
			if _, ok := byDate[date]; !ok {
				byDate[date] = ETFRecord{Date: date, Funds: map[string]*string{}}
			}
		}
		dates = dates[:0]
		for date := range byDate {
			dates = append(dates, date)
		}
		sort.Strings(dates)
	}
	for _, date := range dates {
		r := byDate[date]
		points = append(points, map[string]any{"record": r, "state": etfState(r, now)})
	}
	latestDate := ""
	for i := len(dates) - 1; i >= 0; i-- {
		if etfState(byDate[dates[i]], now) == "reported" {
			latestDate = dates[i]
			break
		}
	}
	sums := map[string]any{}
	streak := 0
	direction := 0
	if latestDate != "" {
		end, _ := time.Parse("2006-01-02", latestDate)
		for _, n := range []int{5, 20} {
			sum := decimal.Zero
			complete := true
			count := 0
			for t := end; count < n; t = t.AddDate(0, 0, -1) {
				if !tradingDay(t) {
					continue
				}
				count++
				r, exists := byDate[t.Format("2006-01-02")]
				if !exists || etfState(r, now) != "reported" {
					complete = false
					continue
				}
				sum = sum.Add(dec(*r.USD))
			}
			if complete {
				sums[fmt.Sprint(n)] = sum.String()
			} else {
				sums[fmt.Sprint(n)] = nil
			}
		}
		for t := end; streak < 90; t = t.AddDate(0, 0, -1) {
			if !tradingDay(t) {
				continue
			}
			r, exists := byDate[t.Format("2006-01-02")]
			if !exists || etfState(r, now) != "reported" {
				break
			}
			s := dec(*r.USD).Sign()
			if s == 0 || direction != 0 && direction != s {
				break
			}
			direction = s
			streak++
		}
	}
	return map[string]any{"asset": a, "meta": metadata(d, latest, ok), "points": points, "latestReportedDate": latestDate, "totals": sums, "streak": streak, "direction": direction, "weight": 0, "note": "上游已报告，可能修订；未披露/未结束/全零待确认不记为真实零值。仅作独立日级观察。"}, nil
}
