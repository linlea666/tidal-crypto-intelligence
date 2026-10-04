package datahub

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// Synthetic load is deliberately restricted to tests; not a source adapter.
func costReplaySeed(t *testing.T, h *Hub, now time.Time) {
	t.Helper()
	ctx := context.Background()
	b := costBundle{}
	for i := 1460; i >= 1; i-- {
		d := now.AddDate(0, 0, -i)
		b.Prices = append(b.Prices, CostPrice{Date: costDate(d), Value: "85000"})
		if i != 1 && d.Weekday() != time.Sunday {
			continue
		}
		f := costTestFrame(costDate(d))
		f.STH.Start = "0"
		f.LTH.Start = "0"
		f.STH.Step = "50"
		f.LTH.Step = "50"
		f.STH.Values = make([]string, 2800)
		f.LTH.Values = make([]string, 2800)
		a, c := int64(0), int64(0)
		for j := range f.STH.Values {
			x, y := int64(50+(j+i)%193), int64(50+(2*j+i)%127)
			f.STH.Values[j] = fmt.Sprint(x)
			f.LTH.Values[j] = fmt.Sprint(y)
			a += x
			c += y
		}
		f.STH.Total = fmt.Sprint(a)
		f.LTH.Total = fmt.Sprint(c)
		b.Frames = append(b.Frames, f)
	}
	if e := h.Store.onchain.ingest(ctx, b, now, true); e != nil {
		t.Fatal(e)
	}
	costSeedFeed(t, h, now)
	if e := h.evaluateCostDay(ctx, now, true); e != nil {
		t.Fatal(e)
	}
	tx, e := h.Store.onchain.db.BeginTx(ctx, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback()
	for i := 0; i < 4; i++ {
		event := CostEvent{ID: fmt.Sprint("resource-cost-", i), Kind: "confirmed", Rules: OnchainRules, DetectedAt: now.AddDate(0, 0, -70-i), Date: costDate(now.AddDate(0, 0, -71-i)), Price: "85000"}
		if e = costInsertEvent(ctx, tx, event, false); e != nil {
			t.Fatal(e)
		}
	}
	if e = tx.Commit(); e != nil {
		t.Fatal(e)
	}
	if e = h.processCostResearch(ctx, now); e != nil {
		t.Fatal(e)
	}
	var count int
	if e = h.Store.onchain.db.QueryRow("SELECT count(*) FROM results").Scan(&count); e != nil || count != 16 {
		t.Fatal("onchain outcomes did not advance", count, e)
	}
}
