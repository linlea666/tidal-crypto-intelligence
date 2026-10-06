package datahub

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/shopspring/decimal"
)

const (
	paperFundingHistoryDelay = 2 * time.Minute
	// History intentionally stops two minutes behind wall time. Allow one
	// bounded poll/request cycle beyond that boundary for live admission;
	// overdue announced settlements still block below, and closed trades
	// require complete coverage through their exact exit instant.
	paperFundingPollTolerance = time.Minute
)

// Funding attribution uses the quantity actually held at the settlement
// instant, including partial exits: open <= settlement < close.
func paperFundingQuantity(fills []paperFill, at time.Time) decimal.Decimal {
	qty := decimal.Zero
	for _, f := range fills {
		if f.At.After(at) {
			continue
		}
		if f.Kind == "open" {
			qty = qty.Add(f.Quantity)
		} else {
			qty = qty.Sub(f.Quantity)
		}
	}
	return qty
}
func paperHasTime(times []int64, at int64) bool {
	for _, t := range times {
		if t == at {
			return true
		}
	}
	return false
}

// The mark stream announces a schedule, not the authoritative settlement
// timestamp. Public funding history can report that settlement a millisecond
// later. Match the announcement within one second, while attribution and
// accounting continue to use the exact official Funding.At without rounding.
func paperSettlementObserved(s paperState, due int64) bool {
	for _, at := range s.SettledFunding {
		if at >= due-1000 && at <= due+1000 {
			return true
		}
	}
	return false
}
func paperFundingKnown(s paperState, at time.Time, open bool) bool {
	if s.FundingConflict != nil && !at.Before(*s.FundingConflict) {
		return false
	}
	through := at
	if open {
		through = at.Add(-paperFundingHistoryDelay - paperFundingPollTolerance)
	}
	if s.FundingThrough.Before(through) {
		return false
	}
	for _, due := range s.ExpectedFunding {
		if due <= at.UnixMilli() && !paperSettlementObserved(s, due) {
			return false
		}
	}
	return true
}
func (p *paperStore) settle(ctx context.Context, records []paperFunding, through time.Time) error {
	s := p.snapshot()
	b := paperBatch{}
	for _, f := range records {
		if !f.Mark.IsPositive() || f.At.After(through) {
			return errors.New("invalid funding settlement")
		}
		existing, err := paperRows[paperFunding](ctx, p.db, "SELECT payload FROM paper_settlements WHERE at=?", f.At.UnixMilli())
		if err != nil {
			return err
		}
		if len(existing) > 0 {
			if !existing[0].Rate.Equal(f.Rate) || !existing[0].Mark.Equal(f.Mark) {
				state := p.snapshot()
				if state.FundingConflict == nil || f.At.Before(*state.FundingConflict) {
					at := f.At
					state.FundingConflict = &at
				}
				if err = p.commit(ctx, state, paperBatch{Events: []paperEvent{{At: f.Acquired, Kind: "funding_conflict", Reason: "official funding revision differs from immutable ledger"}}}); err != nil {
					return err
				}
				return errors.New("funding revision conflicts with immutable ledger")
			}
			continue
		}
		// SQL uses millisecond indexes; retain same-millisecond exits for the
		// precise timestamp/quantity check below, rather than truncating a
		// sub-millisecond held interval out of [entry,exit).
		trades, err := paperRows[paperTrade](ctx, p.db, "SELECT payload FROM paper_trades WHERE entered<=? AND (exited IS NULL OR exited>=?)", f.At.UnixMilli(), f.At.UnixMilli())
		if err != nil {
			return err
		}
		for n := range trades {
			t := &trades[n]
			// A batch can contain multiple settlements for the same trade.
			for _, updated := range b.Trades {
				if updated.ID == t.ID {
					copy := *updated
					t = &copy
				}
			}
			fills, err := paperRows[paperFill](ctx, p.db, "SELECT payload FROM paper_fills WHERE trade_id=? ORDER BY at,id", t.ID)
			if err != nil {
				return err
			}
			qty := paperFundingQuantity(fills, f.At)
			if !qty.IsPositive() {
				continue
			}
			amount := qty.Mul(f.Mark).Mul(f.Rate).Mul(t.sign()).Neg()
			t.Funding = t.Funding.Add(amount)
			b.Funding = append(b.Funding, paperFundingEntry{TradeID: t.ID, Group: t.Group, Funding: f, Quantity: qty, Amount: amount})
			b.Trades = append(b.Trades, t)
			for j := range s.Accounts {
				a := &s.Accounts[j]
				if a.Group == t.Group {
					a.Funding = a.Funding.Add(amount)
					if a.Position != nil && a.Position.ID == t.ID {
						a.Position.Funding = t.Funding /* preserve newer volatile extrema */
						b.Trades = append(b.Trades, a.Position)
					}
				}
			}
		}
		b.Settlements = append(b.Settlements, f)
		s.SettledFunding = append(s.SettledFunding, f.At.UnixMilli())
	}
	if through.After(s.FundingThrough) {
		s.FundingThrough = through
	}
	// Expected times are retained until the official record has arrived. Keep
	// recent settled times for live mark-stream duplicate messages.
	expected := []int64{}
	settled := []int64{}
	for _, at := range s.ExpectedFunding {
		if at > through.Add(-24*time.Hour).UnixMilli() || !paperSettlementObserved(s, at) {
			expected = append(expected, at)
		}
	}
	for _, at := range s.SettledFunding {
		if at > through.Add(-24*time.Hour).UnixMilli() {
			settled = append(settled, at)
		}
	}
	s.ExpectedFunding, s.SettledFunding = expected, settled
	return p.commit(ctx, s, b)
}

// A backup manifest reads the copied database, not the concurrently moving
// source. These values make the recovery boundary independently inspectable.
func paperBackupMetadata(ctx context.Context, dbPath string) (map[string]any, error) {
	db, err := sql.Open("sqlite", "file:"+dbPath+"?mode=ro")
	if err != nil {
		return nil, err
	}
	defer db.Close()
	var raw []byte
	if err = db.QueryRowContext(ctx, "SELECT payload FROM paper_state WHERE id=1").Scan(&raw); err != nil {
		return nil, err
	}
	var s paperState
	if err = json.Unmarshal(raw, &s); err != nil {
		return nil, err
	}
	return map[string]any{"version": s.Version, "generation": s.Generation, "sourceGeneration": s.Source, "cursor": s.Cursor, "lastQuoteId": s.LastQuoteID, "at": s.At, "recoveryRequired": true}, nil
}
