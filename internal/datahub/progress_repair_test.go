package datahub

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func progressFixture(t *testing.T, end time.Time, flowArrival, priceArrival time.Duration) (*Hub, Signal) {
	t.Helper()
	h := shortTestHub(t, end)
	s := Signal{ID: "progress-test", Asset: "BTC", Rules: MultifactorRules, Direction: "buy", State: "confirmed", At: end.Add(-5 * time.Hour), DataThrough: end.Add(-5 * time.Hour), ConfirmedAt: flowPtr(end.Add(-4*time.Hour + time.Minute)), ConfirmedThrough: flowPtr(end.Add(-4 * time.Hour)), FrozenHigh: 100, FrozenLow: 90}
	for _, kind := range []string{"flow", "candles"} {
		id := ID(kind, "BTC", "", "spot")
		if kind == "candles" {
			id = ID(kind, "BTC", "Binance", "spot")
		}
		d, _ := h.Dataset(id)
		for at := end.Add(-15 * time.Minute); at.Before(end); at = at.Add(5 * time.Minute) {
			o := Observation{Dataset: id, Source: d.Source, ObservedAt: flowPtr(at), FetchedAt: end.Add(flowArrival), Resolution: 300, Quality: "valid", Payload: Payload{Flow: &Flow{"200", "100"}}}
			if kind == "candles" {
				o.FetchedAt = end.Add(priceArrival)
				o.Payload = Payload{Candle: &Candle{Open: 101, High: 102, Low: 100, Close: 101, Volume: 1}}
			}
			if _, e := h.Store.Ingest(d, o); e != nil {
				t.Fatal(e)
			}
		}
	}
	return h, s
}

func TestProgressWaitsForTailAndFreezesDeadline(t *testing.T) {
	end := time.Now().UTC().Truncate(time.Hour).Add(-8 * time.Hour)
	h, s := progressFixture(t, end, 3*time.Minute, 4*time.Minute)
	ctx := context.Background()
	p, e := h.finalPriceProgress(ctx, h.Store.research, s, end.Add(time.Minute))
	if e != nil {
		t.Fatal(e)
	}
	if p.Status != "awaiting_data" || p.Deadline == nil || !p.Deadline.Equal(end.Add(20*time.Minute)) {
		t.Fatal(p)
	}
	p, e = h.finalPriceProgress(ctx, h.Store.research, s, end.Add(5*time.Minute))
	if e != nil {
		t.Fatal(e)
	}
	if p.Status != "completed_holding" || len(p.Evidence) != 5 || !p.DataThrough.Equal(end) {
		t.Fatal(p)
	}
	// A revision after the grace deadline cannot rewrite the terminal tail.
	d, _ := h.Dataset(ID("candles", "BTC", "Binance", "spot"))
	at := end.Add(-5 * time.Minute)
	_, e = h.Store.Ingest(d, Observation{Dataset: d.ID, Source: d.Source, ObservedAt: &at, FetchedAt: end.Add(21 * time.Minute), Resolution: 300, Quality: "valid", Payload: Payload{Candle: &Candle{Open: 80, High: 90, Low: 70, Close: 80, Volume: 1}}})
	if e != nil {
		t.Fatal(e)
	}
	after, e := h.finalPriceProgress(ctx, h.Store.research, s, end.Add(24*time.Hour))
	if e != nil {
		t.Fatal(e)
	}
	if after.Status != "completed_holding" || *after.Closes[1] != 101 {
		t.Fatal("late correction entered terminal", after)
	}
}

func TestProgressDeadlineBoundaryAndMissing(t *testing.T) {
	end := time.Now().UTC().Truncate(time.Hour).Add(-8 * time.Hour)
	for _, lag := range []time.Duration{20 * time.Minute, 20*time.Minute + time.Nanosecond} {
		h, s := progressFixture(t, end, 3*time.Minute, lag)
		p, e := h.finalPriceProgress(context.Background(), h.Store.research, s, end.Add(20*time.Minute))
		if e != nil {
			t.Fatal(e)
		}
		want := "completed_holding"
		if lag > 20*time.Minute {
			want = "ended_with_gap"
		}
		if p.Status != want {
			t.Fatalf("lag %s: %+v", lag, p)
		}
	}
}

func TestProgressRepairAppendOnlyRestartAndReadOnly(t *testing.T) {
	end := time.Now().UTC().Truncate(time.Hour).Add(-8 * time.Hour)
	h, s := progressFixture(t, end, 3*time.Minute, 4*time.Minute)
	s.Progress = &PriceProgress{At: end.Add(20 * time.Second), DataThrough: end.Add(-5 * time.Minute), Status: "ended_with_gap", ObservationEnds: &end}
	if e := h.Store.saveDocument("signal", s.ID, "BTC", s.At, s); e != nil {
		t.Fatal(e)
	}
	before, _ := json.Marshal(s)
	for i := 0; i < 2; i++ {
		if i == 1 {
			root := h.Store.Root()
			h.Store.Close()
			var e error
			h, e = Open(Config{Root: root, Offline: true})
			if e != nil {
				t.Fatal(e)
			}
			defer h.Store.Close()
		}
		if e := h.repairPriceProgress(context.Background(), end.Add(time.Hour)); e != nil {
			t.Fatal(e)
		}
	}
	var original Signal
	h.Store.document(context.Background(), "signal", s.ID, &original)
	after, _ := json.Marshal(original)
	if string(before) != string(after) {
		t.Fatal("rewrote original signal")
	}
	rows, e := h.progressRepairs(context.Background(), "BTC")
	if e != nil {
		t.Fatal(e)
	}
	r, ok := rows[s.ID]
	if !ok || r.Result.Status != "completed_holding" || !reflect.DeepEqual(r.Original, *s.Progress) {
		t.Fatal(rows)
	}
	var count int
	h.Store.research.QueryRow("SELECT count(*) FROM documents WHERE kind='signal-progress-repair'").Scan(&count)
	if count != 1 {
		t.Fatal(count)
	}
	h.Store.research.QueryRow("SELECT count(*) FROM notices").Scan(&count)
	if count != 0 {
		t.Fatal("repair created mail")
	}
	if !r.AsOf.Equal(end.Add(progressGrace)) {
		t.Fatal("unbounded repair asOf")
	}
}

func TestProgressFirstCompleteTerminalAndMailRemainFrozen(t *testing.T) {
	end := time.Now().UTC().Truncate(time.Hour).Add(-8 * time.Hour)
	h, s := progressFixture(t, end, 3*time.Minute, 4*time.Minute)
	ctx := context.Background()
	if e := h.Store.saveDocument("signal", s.ID, "BTC", s.At, s); e != nil {
		t.Fatal(e)
	}
	var updates []Signal
	// Current market data can be missing or stale; terminal tail is independent.
	if e := h.updatePriceProgress(ctx, "BTC", nil, nil, end.Add(-5*time.Minute), end.Add(5*time.Minute), false, &updates); e != nil {
		t.Fatal(e)
	}
	if len(updates) != 1 || updates[0].Progress.Status != "completed_holding" {
		t.Fatal(updates)
	}
	frozen := updates[0]
	if e := h.Store.saveDocument("signal", s.ID, "BTC", s.At, frozen); e != nil {
		t.Fatal(e)
	}
	d, _ := h.Dataset(ID("candles", "BTC", "Binance", "spot"))
	for _, at := range []time.Time{end.Add(-5 * time.Minute), end, end.Add(5 * time.Minute)} {
		if _, e := h.Store.Ingest(d, Observation{Dataset: d.ID, Source: d.Source, ObservedAt: flowPtr(at), FetchedAt: end.Add(15 * time.Minute), Resolution: 300, Quality: "valid", Payload: Payload{Candle: &Candle{Open: 80, High: 90, Low: 70, Close: 80, Volume: 1}}}); e != nil {
			t.Fatal(e)
		}
	}
	root := h.Store.Root()
	h.Store.Close()
	var e error
	h, e = Open(Config{Root: root, Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	updates = nil
	if e = h.updatePriceProgress(ctx, "BTC", nil, nil, end.Add(24*time.Hour), end.Add(24*time.Hour), true, &updates); e != nil {
		t.Fatal(e)
	}
	if len(updates) != 0 {
		t.Fatal("terminal rewritten after restart", updates)
	}
	var kept Signal
	h.Store.document(ctx, "signal", s.ID, &kept)
	if !reflect.DeepEqual(kept, frozen) {
		t.Fatal("frozen event changed")
	}
	var n int
	h.Store.research.QueryRow("SELECT count(*) FROM notices").Scan(&n)
	if n != 0 {
		t.Fatal("follow-up sent mail")
	}
}

func TestProgressUnknownFirstVisibilityCannotRepairGap(t *testing.T) {
	end := time.Now().UTC().Truncate(time.Hour).Add(-8 * time.Hour)
	h, s := progressFixture(t, end, 3*time.Minute, 4*time.Minute)
	// Simulate pre-ledger legacy facts whose first visible time is unprovable.
	rows, e := h.Store.research.Query("SELECT dataset,ts,res,revision,available,payload FROM facts")
	if e != nil {
		t.Fatal(e)
	}
	type fact struct {
		id    string
		ts    int64
		res   int
		rev   string
		avail int64
		raw   []byte
	}
	facts := []fact{}
	for rows.Next() {
		var f fact
		if e = rows.Scan(&f.id, &f.ts, &f.res, &f.rev, &f.avail, &f.raw); e != nil {
			t.Fatal(e)
		}
		facts = append(facts, f)
	}
	rows.Close()
	for _, f := range facts {
		o, e := unpack(f.raw)
		if e != nil {
			t.Fatal(e)
		}
		o.FirstFetchedAt = nil
		raw, e := pack(o)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = h.Store.research.Exec("UPDATE facts SET payload=? WHERE dataset=? AND ts=? AND res=? AND revision=? AND available=?", raw, f.id, f.ts, f.res, f.rev, f.avail); e != nil {
			t.Fatal(e)
		}
	}
	p, e := h.finalPriceProgress(context.Background(), h.Store.research, s, end.Add(24*time.Hour))
	if e != nil {
		t.Fatal(e)
	}
	if p.Status != "ended_with_gap" || len(p.Evidence) != 0 || p.FlowSame != nil {
		t.Fatal("invented availability", p)
	}
}
