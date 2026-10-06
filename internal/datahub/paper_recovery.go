package datahub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/shopspring/decimal"
)

type paperAuditTrade struct {
	Trade    paperTrade
	Quantity decimal.Decimal
	Gross    decimal.Decimal
	Fees     decimal.Decimal
	Slippage decimal.Decimal
	Funding  decimal.Decimal
	Fills    int
}

// Check projections against immutable fills/charges before trusting restored
// cash. It does not repair a mismatch by silently replacing historic results.
func (p *paperStore) reconcile(ctx context.Context) error {
	trades := map[string]*paperAuditTrade{}
	read := func(query string, consume func([]byte) error) error {
		r, err := p.db.QueryContext(ctx, query)
		if err != nil {
			return err
		}
		defer r.Close()
		for r.Next() {
			var raw []byte
			if err = r.Scan(&raw); err != nil {
				return err
			}
			if err = consume(raw); err != nil {
				return err
			}
		}
		return r.Err()
	}
	if err := read("SELECT payload FROM paper_trades", func(raw []byte) error {
		var t paperTrade
		if err := json.Unmarshal(raw, &t); err != nil {
			return err
		}
		trades[t.ID] = &paperAuditTrade{Trade: t}
		return nil
	}); err != nil {
		return err
	}
	if err := read("SELECT payload FROM paper_fills ORDER BY at,id", func(raw []byte) error {
		var f paperFill
		if err := json.Unmarshal(raw, &f); err != nil {
			return err
		}
		if f.Quote.ID <= 0 || f.Quote.ID > p.state.LastQuoteID {
			return errors.New("paper quote consumption cursor does not cover committed fill")
		}
		v, ok := trades[f.TradeID]
		if !ok {
			return errors.New("orphan paper fill")
		}
		v.Fills++
		v.Fees = v.Fees.Add(f.Fee)
		v.Slippage = v.Slippage.Add(f.Slippage)
		if f.Kind == "open" {
			v.Quantity = v.Quantity.Add(f.Quantity)
		} else if f.Kind == "close" {
			v.Quantity = v.Quantity.Sub(f.Quantity)
			v.Gross = v.Gross.Add(f.Price.Sub(v.Trade.Entry).Mul(f.Quantity).Mul(v.Trade.sign()))
		} else {
			return errors.New("unknown fill kind")
		}
		if v.Quantity.IsNegative() {
			return errors.New("negative restored position")
		}
		return nil
	}); err != nil {
		return err
	}
	if err := read("SELECT payload FROM paper_funding", func(raw []byte) error {
		var f paperFundingEntry
		if err := json.Unmarshal(raw, &f); err != nil {
			return err
		}
		v, ok := trades[f.TradeID]
		if !ok {
			return errors.New("orphan paper funding")
		}
		v.Funding = v.Funding.Add(f.Amount)
		return nil
	}); err != nil {
		return err
	}
	accounts := map[string]paperAccount{"opposite": {Group: "opposite"}, "risk": {Group: "risk"}}
	for _, v := range trades {
		t := v.Trade
		if !t.Remaining.Equal(v.Quantity) || !t.Gross.Equal(v.Gross) || !t.Fees.Equal(v.Fees) || !t.Slippage.Equal(v.Slippage) || !t.Funding.Equal(v.Funding) || t.FillCount != v.Fills {
			return fmt.Errorf("paper trade projection mismatch: %s", t.ID)
		}
		a, ok := accounts[t.Group]
		if !ok {
			return errors.New("unknown paper account")
		}
		a.Gross = a.Gross.Add(t.Gross)
		a.Fees = a.Fees.Add(t.Fees)
		a.Funding = a.Funding.Add(t.Funding)
		if t.Exited == nil {
			if a.Position != nil || !t.Remaining.IsPositive() {
				return errors.New("inconsistent restored open position")
			}
			a.Position = &t
		} else if !t.Remaining.IsZero() {
			return errors.New("closed trade has remaining quantity")
		}
		accounts[t.Group] = a
	}
	for _, a := range p.state.Accounts {
		v := accounts[a.Group]
		if !a.Gross.Equal(v.Gross) || !a.Fees.Equal(v.Fees) || !a.Funding.Equal(v.Funding) {
			return errors.New("paper account totals do not match immutable ledger")
		}
		if (a.Position == nil) != (v.Position == nil) {
			return errors.New("paper open position missing from checkpoint")
		}
		if a.Position != nil && (a.Position.ID != v.Position.ID || !a.Position.Remaining.Equal(v.Position.Remaining)) {
			return errors.New("paper checkpoint position mismatch")
		}
	}
	return nil
}

func (p *paperStore) verifyRecovery() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return p.reconcile(ctx)
}
