package datahub

import (
	"context"
	"testing"
	"time"
)

func TestCollectionCohortsKeepUnknownAndMissingOutcomes(t *testing.T) {
	sample := func(v string, n *float64) collectionSample {
		return collectionSample{v, ShortTrial{Direction: "buy", Outcomes: []ShortOutcome{{Minutes: 60, State: "complete", Return: n}, {Minutes: 240, State: "pending"}}}}
	}
	rows := collectionCohorts([]collectionSample{sample("", flowPtr(1.0)), sample(BookFlowCollection, flowPtr(-2.0)), sample(BookFlowCollection, nil)})
	if len(rows) != 2 {
		t.Fatal(rows)
	}
	for _, g := range rows {
		if g.Mean4H != nil || g.Complete4H != 0 {
			t.Fatal("missing maturity became zero return", g)
		}
		if g.Version == BookFlowCollection && (g.Count != 2 || g.Complete1H != 1 || g.Mean1H == nil || *g.Mean1H != "-2") {
			t.Fatal("new collection mixed with legacy", g)
		}
		if g.Version == "unrecorded" && (g.Count != 1 || *g.Mean1H != "1") {
			t.Fatal("old sample reclassified", g)
		}
	}
}
func TestLayeredCollectionCohortsReadOnly(t *testing.T) {
	h, err := Open(Config{Root: t.TempDir(), Offline: true, BookFlowMode: "collect"})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Store.Close()
	s, c, now := layeredFixture()
	d := evaluateLayered(s, c, now.Add(-2*time.Hour), now)
	d.Collection = BookFlowCollection
	if err = h.saveLayeredDecision(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	before := h.Scheduler.State()["calls"]
	rows, truncated, err := h.layeredCollectionCohorts(context.Background(), now.Add(-time.Minute), now.Add(time.Minute), "buy")
	if err != nil || truncated || len(rows) != 1 || rows[0].Count != 1 || rows[0].Version != BookFlowCollection || rows[0].Mean4H != nil {
		t.Fatal(rows, truncated, err)
	}
	if h.Scheduler.State()["calls"] != before {
		t.Fatal("cohort GET fetched upstream")
	}
}
func TestPaperCollectionProjectionKeepsFeesAndAbnormalTrades(t *testing.T) {
	p, now := paperFixture(t)
	sig := Signal{ID: "collection-case", Asset: "BTC", Rules: MultifactorRules, Direction: "buy", At: now, Collection: BookFlowCollection}
	if err := p.consume(context.Background(), []paperPublication{{Seq: 1, At: now, Signal: sig}}, now, false); err != nil {
		t.Fatal(err)
	}
	paperTick(t, p, 2, now.Add(time.Second), "9999", "10000", "1")
	paperTick(t, p, 3, now.Add(2*time.Second), "10300", "10301", "1")
	paperTick(t, p, 4, now.Add(3*time.Second), "10300", "10301", "1")
	if err := p.discontinuity(context.Background(), now.Add(4*time.Second), "test_gap"); err != nil {
		t.Fatal(err)
	}
	paperTick(t, p, 5, now.Add(5*time.Second), "10300", "10301", "1")
	v, err := p.summary(context.Background(), now.Add(6*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	for _, x := range v.(map[string]any)["accounts"].([]any) {
		a := x.(map[string]any)
		groups := a["byCollection"].(map[string]paperStats)
		if len(groups) != 1 {
			t.Fatal("signal collection missing from compact SQL", groups)
		}
		g, ok := groups[BookFlowCollection]
		if !ok {
			t.Fatal(groups)
		}
		all := a["all"].(paperStats)
		if a["account"].(paperAccount).Group == "opposite" && g.Abnormal != 1 {
			t.Fatal("abnormal trade omitted", g)
		}
		if g.Closed != all.Closed || g.Complete != all.Complete || g.Abnormal != all.Abnormal || g.Eligible {
			t.Fatal("cohort changes total or borrows eligibility", g, all)
		}
		if all.Net != nil && (g.Net == nil || !g.Net.Equal(*all.Net)) {
			t.Fatal("cohort changed costs", g, all)
		}
	}
}
