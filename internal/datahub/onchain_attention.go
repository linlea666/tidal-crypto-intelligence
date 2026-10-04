package datahub

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"
)

// Reuses quotes already received by the 5-second FX collector. Only the last
// 30 seconds before a four-hour close are relevant; no network work is added.
func (h *Hub) captureCostFX(ctx context.Context, rates []Rate, received time.Time) {
	if h.onchainDisabled || h.onchainEventsDisabled || h.Store.onchain.available() != nil {
		return
	}
	end := received.UTC().Truncate(4 * time.Hour).Add(4 * time.Hour)
	if end.Sub(received) > 30*time.Second {
		return
	}
	rate := ""
	for _, r := range rates {
		if r.Quote == "USDT" {
			rate = r.USD
		}
	}
	if !dec(rate).IsPositive() {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	s := h.Store.onchain
	if s.writable() != nil {
		return
	}
	_, _ = s.db.ExecContext(ctx, `INSERT INTO fx_boundary VALUES(?,?,?,?) ON CONFLICT(close_at) DO UPDATE SET sampled=excluded.sampled,available=excluded.available,rate=excluded.rate WHERE excluded.available>fx_boundary.available`, end.Unix(), received.UnixNano(), received.UnixNano(), rate)
}

type costReference struct {
	Value     string    `json:"value"`
	Native    string    `json:"nativePrice"`
	Source    string    `json:"source"`
	CloseAt   time.Time `json:"closeAt"`
	FirstSeen time.Time `json:"firstSeen"`
	FX        string    `json:"fx"`
	FXAt      time.Time `json:"fxAt"`
	Note      string    `json:"note"`
}

func (h *Hub) costFourHourPrice(ctx context.Context, end, now time.Time) (*costReference, error) {
	if end.After(now) || now.Sub(end) > 15*time.Minute || !end.After(h.boot) {
		return nil, nil
	}
	s := h.Store.onchain
	var sampled, available int64
	var rate string
	e := s.db.QueryRowContext(ctx, "SELECT sampled,available,rate FROM fx_boundary WHERE close_at=?", end.Unix()).Scan(&sampled, &available, &rate)
	if e == sql.ErrNoRows {
		return nil, nil
	}
	if e != nil {
		return nil, e
	}
	at := time.Unix(0, sampled).UTC()
	known := time.Unix(0, available).UTC()
	if at.After(end) || known.After(end) || end.Sub(at) > 30*time.Second || !dec(rate).IsPositive() {
		return nil, nil
	}
	var selected *costReference
	e = h.Store.FactsAsOf(ctx, ID("candles", "BTC", "Binance", "spot"), end.Add(-5*time.Minute), end, now, func(o Observation) error {
		if o.Quality != "valid" || o.Resolution != 300 || !recordTime(o).Equal(end.Add(-5*time.Minute)) || o.Payload.Candle == nil || o.Payload.Candle.Close <= 0 || o.FetchedAt.Before(end) {
			return nil
		}
		native := fmtCostFloat(o.Payload.Candle.Close)
		selected = &costReference{Value: dec(native).Mul(dec(rate)).String(), Native: native, Source: "Binance BTCUSDT × Kraken USDT/USD", CloseAt: end, FirstSeen: o.FetchedAt, FX: rate, FXAt: at, Note: "已完成末根5分钟K线的结束价，非完整4小时OHLC；边界前有效汇率折算参考"}
		return nil
	})
	return selected, e
}
func costAttention(c *CostCase, p costReference, now time.Time) *CostEvent {
	if !costCaseActive(*c) || c.Shadow || c.Legacy || !p.CloseAt.After(c.FrozenAt) || (c.AttentionAt != nil && !p.CloseAt.After(*c.AttentionAt)) {
		return nil
	}
	consecutive := c.AttentionAt != nil && p.CloseAt.Sub(*c.AttentionAt) == 4*time.Hour
	if c.AttentionClearByKind == nil {
		c.AttentionClearByKind = map[string]int{}
	}
	if !consecutive {
		for kind := range c.AttentionClearByKind {
			c.AttentionClearByKind[kind] = 0
		}
	}
	state := ""
	beyond := costBeyond(*c, p.Value)
	if c.State == "confirmed" && !beyond {
		state = "attention_returned"
	} else if beyond {
		state = "attention_outside"
	} else if dec(p.Value).Div(dec(c.Boundary)).Sub(dec("1")).Abs().LessThanOrEqual(dec("0.01")) {
		state = "attention_near"
	}
	at := p.CloseAt
	c.AttentionAt = &at
	for kind, n := range c.AttentionClearByKind {
		if kind != state {
			c.AttentionClearByKind[kind] = min(2, n+1)
		}
	}
	if state == "" {
		return nil
	}
	n, known := c.AttentionClearByKind[state]
	c.AttentionClearByKind[state] = 0
	c.AttentionState = state
	if known && n < 2 {
		return nil
	}
	if c.FirstAttentionAt == nil {
		first := now
		c.FirstAttentionAt = &first
	}
	e := costCaseEvent(*c, state, CostPrice{Date: costDate(p.CloseAt.Add(-time.Nanosecond)), Value: p.Value, FirstSeen: p.FirstSeen, ValidatedAt: &now}, now)
	e.ID = costHash([]string{c.ID, state, p.CloseAt.Format(time.RFC3339Nano)})
	e.OccurredAt = &at
	e.PriceSource = p.Source
	e.Note = costEventName(state) + "；仅站内参考，不替代正式日线条件。" + p.Note
	return &e
}
func (h *Hub) evaluateCostAttention(ctx context.Context, now time.Time) error {
	if h.onchainDisabled || h.onchainEventsDisabled {
		return nil
	}
	s := h.Store.onchain
	if e := s.writable(); e != nil {
		return e
	}
	end := now.UTC().Truncate(4 * time.Hour)
	p, e := h.costFourHourPrice(ctx, end, now)
	if e != nil || p == nil {
		return e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var state costState
	if e = costLoad(ctx, s.db, "observation", &state); e != nil {
		return e
	}
	before, _ := json.Marshal(state)
	events := []CostEvent{}
	for i := range state.Cases {
		if event := costAttention(&state.Cases[i], *p, now); event != nil {
			h.decorateCostEvent(event, now)
			events = append(events, *event)
		}
	}
	after, _ := json.Marshal(state)
	if string(before) == string(after) {
		return nil
	}
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	if e = costSave(ctx, tx, "observation", state); e != nil {
		return e
	}
	if e = costSave(ctx, tx, "four-hour-reference", p); e != nil {
		return e
	}
	for _, c := range state.Cases {
		raw, _ := json.Marshal(c)
		if _, e = tx.ExecContext(ctx, "INSERT INTO cases VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET updated=excluded.updated,payload=excluded.payload", c.ID, now.UnixNano(), raw); e != nil {
			return e
		}
	}
	for _, event := range events {
		if e = costInsertEvent(ctx, tx, event, false); e != nil {
			return e
		}
	}
	if e = s.commit(ctx, tx); e == nil {
		s.epoch.Add(1)
	}
	return e
}
func (h *Hub) onchainEvaluator(ctx context.Context) {
	lastResearch := time.Time{}
	for ctx.Err() == nil {
		now := time.Now().UTC()
		work, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := h.evaluateCostDay(work, now, false)
		if err == nil {
			err = h.evaluateCostAttention(work, now)
		}
		if err == nil && now.Sub(lastResearch) >= time.Hour {
			err = h.processCostResearch(work, now)
			if err == nil {
				lastResearch = now
			}
		}
		cancel()
		if err != nil {
			_ = h.Store.onchain.save(ctx, "evaluation-error", map[string]any{"at": now, "reason": err.Error(), "active": true})
		} else {
			var failure map[string]any
			if h.Store.onchain.available() == nil && costLoad(ctx, h.Store.onchain.db, "evaluation-error", &failure) == nil && failure["active"] == true {
				failure["active"], failure["recoveredAt"] = false, now
				_ = h.Store.onchain.save(ctx, "evaluation-error", failure)
			}
		}
		_ = h.processCostNotices(ctx, time.Now().UTC())
		if !sleep(ctx, time.Minute) {
			return
		}
	}
}
func (h *Hub) costCapabilities(ctx context.Context, now time.Time) any {
	s := h.Store.onchain
	if s.available() != nil {
		return map[string]any{"structure": false, "dailyPrice": false, "fourHourPrice": false, "note": "链上存储不可用"}
	}
	f, e := h.costFeed(ctx)
	status, _ := costFeedStatus(f, now)
	price, pe := s.price(ctx, costDate(now.AddDate(0, 0, -1)), now)
	allowed := !h.onchainDisabled && !h.onchainEventsDisabled && s.writable() == nil
	var ref costReference
	_ = costLoad(ctx, s.db, "four-hour-reference", &ref)
	return map[string]any{"structure": allowed && e == nil && status == "fresh", "dailyPrice": allowed && pe == nil && price != nil, "fourHourPrice": allowed && !ref.CloseAt.IsZero() && now.Sub(ref.CloseAt) <= 4*time.Hour+15*time.Minute, "dailyPriceSource": onchainSource, "note": "链上结构、正式日收盘与4小时参考分别判断；缺失价格时不声称确认仍有效"}
}
