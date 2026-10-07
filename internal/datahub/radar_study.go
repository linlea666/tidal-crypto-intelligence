package datahub

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/shopspring/decimal"
	"sort"
	"time"
)

type radarOutcome struct {
	Minutes  int     `json:"minutes"`
	State    string  `json:"state"`
	Return   *string `json:"returnPercent"`
	MFE      *string `json:"mfePercent"`
	MAE      *string `json:"maePercent"`
	Observed int     `json:"observedBars"`
	Expected int     `json:"expectedBars"`
}
type radarTrial struct {
	ReferenceSource string         `json:"referenceSource"`
	ReferenceAt     *time.Time     `json:"referenceAt"`
	ReferenceFX     string         `json:"referenceFx"`
	ReferenceFXAt   *time.Time     `json:"referenceFxAt"`
	Evidence        RadarEvent     `json:"evidence"`
	ID              string         `json:"id"`
	Asset           string         `json:"asset"`
	Side            string         `json:"side"`
	At              time.Time      `json:"at"`
	Price           string         `json:"referenceUSD"`
	Alert           bool           `json:"alert"`
	Group           string         `json:"groupId"`
	Outcomes        []radarOutcome `json:"outcomes"`
}

// Freeze when a verified opening first reaches the USD threshold, in the same
// transaction as its facts. Mail settings and later returns never select samples.
func (h *Hub) radarFreezeTrial(ctx context.Context, tx radarDB, v RadarEvent, now time.Time) error {
	if !v.LiveOpening || !v.Verified || v.Closed != nil || v.Threshold == nil || v.USDCents == nil || *v.USDCents < 100000000 {
		return nil
	}
	var prior radarTrial
	if err := radarLoad(ctx, tx, "study", v.ID, &prior); err == nil {
		return nil
	} else if err != sql.ErrNoRows {
		return err
	}
	t := radarTrial{ReferenceSource: "Binance spot / USD", Evidence: v, ID: v.ID, Asset: v.Asset, Side: v.Side, At: now, Alert: v.InitialRecorded, Group: v.Group, Outcomes: []radarOutcome{}}
	// The benchmark and all forward bars must use the same venue/instrument.
	// A missing discovery price remains missing; never backfill it with hindsight.
	d, _ := h.Dataset(ID("price", v.Asset, "Binance", "spot"))
	o, exists := h.Store.Latest(d.ID)
	rate, fxAt, fxOK := h.Rate("USDT", now)
	if exists && o.Fresh(d, now) && o.Payload.Price != nil && o.Payload.Price.Quote == "USDT" && fxOK {
		price, valid := radarNumber(o.Payload.Price.Value)
		fx, validFX := radarNumber(rate)
		if valid && validFX && price.IsPositive() && fx.IsPositive() {
			at := o.Time()
			t.Price = price.Mul(fx).String()
			t.ReferenceAt = &at
			t.ReferenceFX = rate
			t.ReferenceFXAt = fxAt
		}
	}
	if err := radarPut(ctx, tx, "study", t.ID, t.At, t, true); err != nil {
		return err
	}
	return radarPut(ctx, tx, "study_pending", t.ID, now.Add(25*time.Minute), t.ID, true)
}

func (h *Hub) radarStudy(ctx context.Context, now time.Time) error {
	r := h.Store.radar
	// Earliest due work first. Persistent due times prevent busy new wallets from
	// starving older 24-hour observations; cancellation leaves work retryable.
	rows, err := r.db.QueryContext(ctx, "SELECT id FROM records WHERE kind='study_pending' AND at<=? ORDER BY at,id LIMIT 8", now.UnixMilli())
	if err != nil {
		return err
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			break
		}
		ids = append(ids, id)
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return err
	}
	for _, id := range ids {
		var t radarTrial
		if err = radarLoad(ctx, r.db, "study", id, &t); err != nil {
			return err
		}
		done := map[int]bool{}
		for _, o := range t.Outcomes {
			done[o.Minutes] = true
		}
		next := time.Time{}
		for _, minutes := range []int{15, 60, 240, 1440} {
			if done[minutes] {
				continue
			}
			end := t.At.Add(time.Duration(minutes) * time.Minute)
			due := end.Add(10 * time.Minute)
			if now.Before(due) {
				next = due
				break
			}
			o := h.radarOutcome(ctx, t, minutes, end)
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if o.State == "retry" {
				return fmt.Errorf("雷达复盘暂时无法读取行情")
			}
			t.Outcomes = append(t.Outcomes, o)
		}
		r.mu.Lock()
		tx, e := r.db.BeginTx(ctx, nil)
		if e == nil {
			// Preserve a group assigned while the outcome computation was in progress.
			var latest radarTrial
			e = radarLoad(ctx, tx, "study", id, &latest)
			if e == nil {
				t.Group = latest.Group
				e = radarPut(ctx, tx, "study", id, t.At, t, false)
			}
			if e == nil {
				if next.IsZero() {
					_, e = tx.ExecContext(ctx, "DELETE FROM records WHERE kind='study_pending' AND id=?", id)
				} else {
					e = radarPut(ctx, tx, "study_pending", id, next, id, false)
				}
			}
			if e == nil {
				e = tx.Commit()
			} else {
				tx.Rollback()
			}
		}
		r.mu.Unlock()
		if e != nil {
			return e
		}
	}
	return nil
}
func (h *Hub) radarOutcome(ctx context.Context, t radarTrial, minutes int, end time.Time) radarOutcome {
	o := radarOutcome{Minutes: minutes, State: "incomplete"}
	start := t.At.Truncate(5 * time.Minute).Add(5 * time.Minute)
	stop := end.Truncate(5 * time.Minute)
	o.Expected = int(stop.Sub(start) / (5 * time.Minute))
	if o.Expected <= 0 {
		return o
	}
	d, _ := h.Dataset(ID("candles", t.Asset, "Binance", "spot"))
	candles := map[int64]Candle{}
	err := h.Store.Visit(ctx, d, 300, start, stop, func(v Observation) error {
		at := v.Time()
		if v.Payload.Candle != nil && !at.Before(start) && at.Before(stop) && v.Resolution == 300 {
			candles[at.Unix()] = *v.Payload.Candle
		}
		return nil
	})
	if err != nil {
		o.State = "retry"
		return o
	}
	o.Observed = len(candles)
	if o.Observed != o.Expected {
		return o
	}
	// Convert the same Binance spot instrument with historical FX, never current FX.
	ref := dec(t.Price)
	if !ref.IsPositive() {
		return o
	}
	best, worst, last := decimal.Zero, decimal.Zero, decimal.Zero
	rates := map[int64]decimal.Decimal{}
	known := map[int64]bool{}
	for at := start; at.Before(stop); at = at.Add(5 * time.Minute) {
		c, ok := candles[at.Unix()]
		if !ok {
			return o
		}
		hour := at.Truncate(time.Hour).Unix()
		rate, ok := rates[hour]
		if !known[hour] {
			rate, ok = h.radarHistoricalFX(ctx, at)
			known[hour] = true
			if ok {
				rates[hour] = rate
			}
		}
		if !ok {
			return o
		}
		hi := decimal.NewFromFloat(c.High).Mul(rate).Div(ref).Sub(decimal.NewFromInt(1)).Mul(decimal.NewFromInt(100))
		lo := decimal.NewFromFloat(c.Low).Mul(rate).Div(ref).Sub(decimal.NewFromInt(1)).Mul(decimal.NewFromInt(100))
		close := decimal.NewFromFloat(c.Close).Mul(rate).Div(ref).Sub(decimal.NewFromInt(1)).Mul(decimal.NewFromInt(100))
		if t.Side == "short" {
			hi, lo = lo.Neg(), hi.Neg()
			close = close.Neg()
		}
		if hi.GreaterThan(best) {
			best = hi
		}
		if lo.LessThan(worst) {
			worst = lo
		}
		last = close
	}
	a, b, c := last.String(), best.String(), worst.String()
	o.State = "complete"
	o.Return = &a
	o.MFE = &b
	o.MAE = &c
	return o
}
func (h *Hub) radarHistoricalFX(ctx context.Context, at time.Time) (decimal.Decimal, bool) {
	d, _ := h.Dataset("fx.usd.kraken")
	var result decimal.Decimal
	ok := false
	_ = h.Store.Visit(ctx, d, 3600, at.Truncate(time.Hour), at.Truncate(time.Hour).Add(time.Hour), func(o Observation) error {
		for _, r := range o.Payload.Rates {
			if r.Quote == "USDT" {
				n, valid := radarNumber(r.USD)
				if valid && n.IsPositive() {
					result = n
					ok = true
				}
			}
		}
		return nil
	})
	return result, ok
}

type radarStudyMetric struct {
	Minutes    int     `json:"minutes"`
	Population string  `json:"population"`
	Events     int     `json:"events"`
	Complete   int     `json:"complete"`
	Missing    int     `json:"missing"`
	Pending    int     `json:"pending"`
	MeanReturn *string `json:"meanReturnPercent"`
	MeanMAE    *string `json:"meanMaePercent"`
}

func (h *Hub) radarStudyView(ctx context.Context) (any, error) {
	r := h.Store.radar
	var origin time.Time
	if e := radarLoad(ctx, r.db, "origin", RadarRules, &origin); e != nil {
		return nil, e
	}
	rows, e := r.db.QueryContext(ctx, "SELECT payload FROM records WHERE kind='study' ORDER BY at DESC LIMIT 501")
	if e != nil {
		return nil, e
	}
	items := []radarTrial{}
	for rows.Next() {
		var b []byte
		if e = rows.Scan(&b); e != nil {
			break
		}
		var t radarTrial
		if e = json.Unmarshal(b, &t); e != nil {
			break
		}
		items = append(items, t)
	}
	if e == nil {
		e = rows.Err()
	}
	rows.Close()
	if e != nil {
		return nil, e
	}
	truncated := len(items) > 500
	if truncated {
		items = items[:500]
	}
	// First discovered member represents each group in both populations, including
	// losing, missing and unresolved outcomes. No best-performing member selection.
	sort.Slice(items, func(i, j int) bool {
		if items[i].At.Equal(items[j].At) {
			return items[i].ID < items[j].ID
		}
		return items[i].At.Before(items[j].At)
	})
	representatives := []radarTrial{}
	groups := map[string]bool{}
	mature := 0
	for _, t := range items {
		key := t.Group
		if key == "" {
			key = t.ID
		}
		if groups[key] {
			continue
		}
		groups[key] = true
		representatives = append(representatives, t)
		if len(t.Outcomes) == 4 {
			valid := true
			for _, o := range t.Outcomes {
				valid = valid && o.State == "complete"
			}
			if valid {
				mature++
			}
		}
	}
	metrics := []radarStudyMetric{}
	for _, pop := range []string{"large_opening_baseline", "radar_selected"} {
		for _, minutes := range []int{15, 60, 240, 1440} {
			m := radarStudyMetric{Minutes: minutes, Population: pop}
			sum, mae := decimal.Zero, decimal.Zero
			for _, t := range representatives {
				if pop == "radar_selected" && !t.Alert {
					continue
				}
				m.Events++
				found := false
				for _, o := range t.Outcomes {
					if o.Minutes != minutes {
						continue
					}
					found = true
					if o.State == "complete" && o.Return != nil && o.MAE != nil {
						m.Complete++
						sum = sum.Add(dec(*o.Return))
						mae = mae.Add(dec(*o.MAE))
					} else {
						m.Missing++
					}
				}
				if !found {
					m.Pending++
				}
			}
			if m.Complete > 0 {
				a, b := sum.Div(decimal.NewFromInt(int64(m.Complete))).String(), mae.Div(decimal.NewFromInt(int64(m.Complete))).String()
				m.MeanReturn = &a
				m.MeanMAE = &b
			}
			metrics = append(metrics, m)
		}
	}
	// The detail list is bounded independently of aggregate calculation and never
	// returns frozen raw evidence for 500 addresses in a single response.
	sort.Slice(representatives, func(i, j int) bool { return representatives[i].At.After(representatives[j].At) })
	detail := []map[string]any{}
	for i, t := range representatives {
		if i >= 50 {
			break
		}
		detail = append(detail, map[string]any{"id": t.ID, "asset": t.Asset, "side": t.Side, "at": t.At, "alert": t.Alert, "groupId": t.Group, "outcomes": t.Outcomes})
	}
	return map[string]any{"origin": origin, "days": time.Since(origin).Hours() / 24, "independentEvents": len(groups), "completeEvents": mature, "reviewReady": !truncated && time.Since(origin) >= 30*24*time.Hour && mature >= 30, "truncated": truncated, "metrics": metrics, "items": detail, "note": "首次核验到百万级仓位时冻结证据和同期Binance现货参考价，后续也用同市场行情，缺参考价不追填；同步组取最早发现成员，只算一个样本。大额基线包含全部已核验百万级新开仓，雷达组为其中满足地址规则的子集。最多汇总最近500条，超限明确标记且不宣称全量审查。从发现后下一根完整5分钟K线开始，缺行情/FX保留未知；最大不利波动未覆盖首尾不足5分钟区间，收益不含成本。至少30天/30个独立完整事件才进入描述性审查，不代表可跟单收益。"}, nil
}
