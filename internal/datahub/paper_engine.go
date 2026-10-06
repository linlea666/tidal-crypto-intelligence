package datahub

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/shopspring/decimal"
)

func paperExecution(q paperQuote, side string) (decimal.Decimal, decimal.Decimal) {
	if side == "buy" {
		return q.Ask.Mul(decimal.NewFromInt(1).Add(paperSlip)), q.AskQty
	}
	return q.Bid.Mul(decimal.NewFromInt(1).Sub(paperSlip)), q.BidQty
}
func paperExitSide(t *paperTrade) string {
	if t.Signal.Direction == "buy" {
		return "sell"
	}
	return "buy"
}
func paperCloseQuote(t *paperTrade, q paperQuote) decimal.Decimal {
	if t.Signal.Direction == "buy" {
		return q.Bid
	}
	return q.Ask
}
func paperTrigger(t *paperTrade, reason string, at time.Time) {
	if t.ExitReason == "" {
		t.ExitReason, t.ExitAfter = reason, at.Add(time.Second)
	}
}
func paperMark(t *paperTrade, q paperQuote) {
	delta := paperCloseQuote(t, q).Sub(t.Entry).Mul(t.sign())
	if delta.GreaterThan(t.MFE) {
		t.MFE, t.MFEAt = delta, q.At
	}
	if delta.LessThan(t.MAE) {
		t.MAE, t.MAEAt = delta, q.At
	}
}
func paperFillTrade(t *paperTrade, kind, side string, qty, price decimal.Decimal, q paperQuote) paperFill {
	t.FillCount++
	reference := q.Bid
	if side == "buy" {
		reference = q.Ask
	}
	return paperFill{ID: fmt.Sprintf("%s/%d", t.ID, t.FillCount), TradeID: t.ID, Group: t.Group, Kind: kind, Side: side, At: q.At, Price: price, Quantity: qty, Fee: qty.Mul(price).Mul(paperFee), Slippage: reference.Mul(qty).Mul(paperSlip), Quote: q}
}
func (p *paperStore) admission(s paperState, intent paperIntent, now time.Time, protected bool) string {
	if p.mode != "run" {
		return "disabled"
	}
	if s.Origin == nil || s.Gap || s.GoodSince.IsZero() || now.Sub(s.GoodSince) < 30*time.Second {
		return "recovering"
	}
	if protected {
		return "storage_reserve"
	}
	if intent.Signal.At.After(now) || now.Sub(intent.Signal.At) > 30*time.Second {
		return "signal_expired"
	}
	if intent.Signal.Asset != "BTC" || intent.Signal.Rules != MultifactorRules || (intent.Signal.Direction != "buy" && intent.Signal.Direction != "sell") {
		return "ineligible_signal"
	}
	if !p.instrument.valid(now) {
		return "contract_unavailable"
	}
	if !intent.ATR.IsPositive() || intent.ATRThrough != now.Truncate(time.Hour) {
		return "atr_missing"
	}
	if p.quote == nil || !p.quote.valid(now) {
		return "quote_stale"
	}
	if p.markAt.IsZero() || now.Sub(p.markAt) > 5*time.Second {
		return "mark_stale"
	}
	if !paperFundingKnown(s, now, true) {
		return "funding_unsettled"
	}
	return ""
}

// consume freezes the intake clock exactly once. All pending intents, intake
// evidence and the source cursor are committed together, before any fill.
func (p *paperStore) consume(ctx context.Context, pubs []paperPublication, now time.Time, protected bool) error {
	s := p.snapshot()
	b := paperBatch{}
	for _, pub := range pubs {
		if pub.Seq <= s.Cursor {
			continue
		}
		intent := paperIntent{Signal: pub.Signal, Seen: now, After: now.Add(time.Second)}
		if p.atr != nil {
			intent.ATR, intent.ATRThrough = p.atr.ATR, p.atr.ATRThrough
		}
		for n := range s.Accounts {
			a := &s.Accounts[n]
			// A restored research database can reuse a sequence; the signal key
			// remains authoritative across cursor changes and source generations.
			var used int
			if err := p.db.QueryRowContext(ctx, "SELECT count(*) FROM paper_intakes WHERE id=?", fmt.Sprintf("%s/%s/%s/seen", s.Generation, a.Group, intent.Signal.ID)).Scan(&used); err != nil {
				return err
			}
			if used > 0 {
				continue
			}
			b.Intakes = append(b.Intakes, paperIntakeRecord(s, *a, intent, now, "seen", "first_publication"))
			if intent.Signal.Asset != "BTC" || intent.Signal.Rules != MultifactorRules || intent.Signal.Direction != "buy" && intent.Signal.Direction != "sell" {
				b.Intakes = append(b.Intakes, paperIntakeRecord(s, *a, intent, now, "skipped", "ineligible_signal"))
				continue
			}
			if s.Origin == nil || !pub.At.After(*s.Origin) {
				b.Intakes = append(b.Intakes, paperIntakeRecord(s, *a, intent, now, "skipped", "before_experiment"))
				continue
			}
			if a.Position != nil {
				if a.Position.Signal.Direction == intent.Signal.Direction {
					b.Intakes = append(b.Intakes, paperIntakeRecord(s, *a, intent, now, "associated", "same_direction"))
					continue
				}
				// Closing risk is allowed even when a fresh reverse entry fails
				// admission. Expired historical events must not drive exits.
				if now.Sub(intent.Signal.At) <= 30*time.Second && !intent.Signal.At.After(now) {
					paperTrigger(a.Position, "opposite_signal", now)
					b.Trades = append(b.Trades, a.Position)
				}
			}
			reason := p.admission(s, intent, now, protected)
			if reason != "" {
				b.Intakes = append(b.Intakes, paperIntakeRecord(s, *a, intent, now, "skipped", reason))
				continue
			}
			if a.Pending != nil {
				b.Intakes = append(b.Intakes, paperIntakeRecord(s, *a, *a.Pending, now, "skipped", "superseded_by_new_event"))
			}
			copy := intent
			a.Pending = &copy
		}
		s.Cursor = pub.Seq
	}
	s.At = now
	return p.commit(ctx, s, b)
}

func (p *paperStore) discontinuity(ctx context.Context, now time.Time, reason string) error {
	s := p.snapshot()
	b := paperBatch{}
	b.Events = append(b.Events, paperEvent{At: now, Kind: "gap", Reason: reason})
	s.Gap, s.GoodSince, s.Pause = true, time.Time{}, reason
	s.EquityGap = true
	s.LastFailure, s.LastFailureAt = reason, &now
	for n := range s.Accounts {
		a := &s.Accounts[n]
		if a.Pending != nil {
			b.Intakes = append(b.Intakes, paperIntakeRecord(s, *a, *a.Pending, now, "skipped", reason))
			a.Pending = nil
		}
		if a.Position != nil {
			a.Position.flag("data_gap")
			paperTrigger(a.Position, "data_gap", now)
			b.Trades = append(b.Trades, a.Position)
		}
	}
	s.At = now
	return p.commit(ctx, s, b)
}

func (p *paperStore) onQuote(ctx context.Context, q paperQuote, protected bool) error {
	if !q.valid(q.At) {
		return errors.New("invalid or delayed perpetual book ticker")
	}
	// Best bid/ask is a self-contained top-of-book snapshot, not an L2 delta.
	// Update IDs may skip; duplicate/older snapshots cannot supply more size.
	if q.ID <= p.lastQuoteID {
		return nil
	}
	if p.quote != nil && q.At.Sub(p.quote.At) > 5*time.Second {
		if err := p.discontinuity(ctx, q.At, "quote_gap"); err != nil {
			return err
		}
	}
	p.lastQuoteID = q.ID
	p.quote = &q
	s := p.snapshot()
	// Persist the quote identity in the same transaction as fills. Restarting
	// cannot replenish a partially consumed snapshot's visible quantity.
	s.LastQuoteID = q.ID
	b := paperBatch{}
	changed := false
	for n := range s.Accounts {
		a := &s.Accounts[n]
		t := a.Position
		if t != nil {
			oldReason := t.ExitReason
			paperMark(t, q)
			if s.Gap {
				t.flag("data_gap")
				paperTrigger(t, "data_gap", q.At)
			}
			if a.Group == "risk" && t.ExitReason == "" {
				delta := paperCloseQuote(t, q).Sub(t.Entry).Mul(t.sign())
				switch {
				case delta.LessThanOrEqual(t.ATR.Neg()):
					paperTrigger(t, "stop_loss", q.At)
				case delta.GreaterThanOrEqual(t.ATR.Mul(decimal.NewFromInt(2))):
					paperTrigger(t, "take_profit", q.At)
				case q.At.Sub(t.Entered) >= 4*time.Hour:
					paperTrigger(t, "time_limit", q.At)
				}
			}
			if t.ExitReason != "" && !q.At.Before(t.ExitAfter) && p.instrument.valid(q.At) {
				price, size := paperExecution(q, paperExitSide(t))
				qty := decimal.Min(size, t.Remaining)
				if qty.IsPositive() {
					f := paperFillTrade(t, "close", paperExitSide(t), qty, price, q)
					gross := price.Sub(t.Entry).Mul(qty).Mul(t.sign())
					t.Gross = t.Gross.Add(gross)
					t.Fees = t.Fees.Add(f.Fee)
					t.Slippage = t.Slippage.Add(f.Slippage)
					t.Remaining = t.Remaining.Sub(qty)
					a.Gross = a.Gross.Add(gross)
					a.Fees = a.Fees.Add(f.Fee)
					b.Fills = append(b.Fills, f)
					changed = true
					if t.Remaining.IsZero() {
						at := q.At
						t.Exited = &at
						a.Position = nil
						if a.Pending != nil {
							a.Pending.After = q.At.Add(time.Second)
						}
					}
				}
			}
			if t.ExitReason != oldReason {
				changed = true
			}
			b.Trades = append(b.Trades, t)
			// Each independent group's quote budget can be spent only once;
			// reversal is never filled on the same quote as the final close.
			continue
		}
		if a.Pending == nil || q.At.Before(a.Pending.After) {
			continue
		}
		intent := *a.Pending
		reason := p.admission(s, intent, q.At, protected)
		price, size := paperExecution(q, intent.Signal.Direction)
		var qty decimal.Decimal
		if reason == "" {
			qty = paperNotional.Div(price).Div(p.instrument.Step).Floor().Mul(p.instrument.Step)
			switch {
			case !qty.IsPositive() || qty.LessThan(p.instrument.Minimum) || qty.GreaterThan(p.instrument.Maximum) || qty.Mul(price).LessThan(p.instrument.MinNotional):
				reason = "quantity_filter"
			case qty.GreaterThan(size):
				reason = "top_size_insufficient"
			case a.cash().LessThan(qty.Mul(price).Mul(decimal.NewFromInt(1).Add(paperFee))):
				reason = "cash_insufficient"
			}
		}
		if reason != "" {
			b.Intakes = append(b.Intakes, paperIntakeRecord(s, *a, intent, q.At, "skipped", reason))
			a.Pending = nil
			changed = true
			continue
		}
		t = &paperTrade{ID: s.Generation + "/" + a.Group + "/" + intent.Signal.ID, Group: a.Group, Signal: intent.Signal, Seen: intent.Seen, Entered: q.At, Entry: price, Quantity: qty, Remaining: qty, ATR: intent.ATR, ATRThrough: intent.ATRThrough, Quality: []string{}, MFEAt: q.At, MAEAt: q.At}
		if a.Group == "risk" {
			stop := price.Sub(t.sign().Mul(t.ATR))
			target := price.Add(t.sign().Mul(t.ATR).Mul(decimal.NewFromInt(2)))
			t.Stop, t.Target = &stop, &target
		}
		f := paperFillTrade(t, "open", intent.Signal.Direction, qty, price, q)
		t.Fees = f.Fee
		t.Slippage = f.Slippage
		a.Fees = a.Fees.Add(f.Fee)
		a.Position = t
		a.Pending = nil
		paperMark(t, q)
		b.Fills = append(b.Fills, f)
		b.Trades = append(b.Trades, t)
		b.Intakes = append(b.Intakes, paperIntakeRecord(s, *a, intent, q.At, "opened", ""))
		changed = true
	}
	if changed {
		s.At = q.At
		return p.commit(ctx, s, b)
	}
	// Quote extrema are volatile until the next one-minute checkpoint. A crash
	// marks the position incomplete rather than claiming the lost path is known.
	p.mu.Lock()
	p.state = s
	p.mu.Unlock()
	return nil
}

func (p *paperStore) heartbeat(ctx context.Context, now time.Time, protected bool) error {
	s := p.snapshot()
	wasGap := s.Gap
	good := p.quote != nil && p.quote.valid(now) && !p.markAt.IsZero() && now.Sub(p.markAt) <= 5*time.Second
	if !good && !s.Gap {
		if err := p.discontinuity(ctx, now, "market_gap"); err != nil {
			return err
		}
		s = p.snapshot()
	}
	if good {
		if s.GoodSince.IsZero() {
			s.GoodSince = now
		}
		if now.Sub(s.GoodSince) >= 30*time.Second {
			s.Gap = false
			s.Pause = ""
		}
	} else {
		s.GoodSince = time.Time{}
		s.Gap = true
	}
	if protected {
		s.Pause = "storage_reserve"
	}
	if p.mode != "run" {
		s.Pause = "collection_only"
	}
	if !s.LastTick.IsZero() && now.After(s.LastTick) {
		seconds := now.Unix() - s.LastTick.Unix()
		// Counts represent wall time, including downtime (added on restart).
		if s.Origin != nil {
			s.ObservedSeconds += seconds
			if good && !s.Gap && seconds <= 2 && !p.sourceAt.IsZero() && now.Sub(p.sourceAt) <= 2*time.Second && p.instrument.valid(now) && p.atr != nil && p.atr.ATRThrough == now.Truncate(time.Hour) && paperFundingKnown(s, now, true) {
				s.CoveredSeconds += seconds
			}
		}
	}
	b := paperBatch{}
	if wasGap && !s.Gap {
		b.Events = append(b.Events, paperEvent{At: now, Kind: "recovered", Reason: "continuous_30s"})
	}
	if s.Origin != nil && now.Sub(s.LastSample) >= time.Minute {
		for _, a := range s.Accounts {
			if protected && a.Position == nil {
				continue
			}
			e := paperEquity{At: now, Group: a.Group, Occupied: a.Position != nil, CoveredSeconds: s.CoveredSeconds, ObservedSeconds: s.ObservedSeconds}
			if good && !s.Gap && !s.EquityGap {
				u := decimal.Zero
				if a.Position != nil {
					u = paperCloseQuote(a.Position, *p.quote).Sub(a.Position.Entry).Mul(a.Position.Remaining).Mul(a.Position.sign())
				}
				v := paperInitial.Add(a.Gross).Sub(a.Fees).Add(u)
				e.BeforeFunding, e.Unrealized = &v, &u
			}
			b.Equity = append(b.Equity, e)
			if a.Position != nil {
				b.Trades = append(b.Trades, a.Position)
			}
		}
		s.LastSample = now
		s.EquityGap = false
	}
	for n := range s.Accounts {
		a := &s.Accounts[n]
		if a.Pending != nil && now.Sub(a.Pending.Signal.At) > 30*time.Second {
			b.Intakes = append(b.Intakes, paperIntakeRecord(s, *a, *a.Pending, now, "skipped", "signal_expired"))
			a.Pending = nil
		}
	}
	s.At = now
	s.LastTick = now
	return p.commit(ctx, s, b)
}
