package datahub

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"
)

// Optional local replay of a read-only public-fact export. The export remains
// outside Git; no credential, production write or upstream request is involved.
func TestShortProductionProfile(t *testing.T) {
	path := os.Getenv("TIDAL_SHORT_PROFILE")
	if path == "" {
		t.Skip("private read-only fact export not supplied")
	}
	raw, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	var source struct {
		Facts []struct {
			TS        int64  `json:"ts"`
			Res       int    `json:"res"`
			Revision  string `json:"revision"`
			Available int64  `json:"available"`
			Payload   []byte `json:"payload"`
		}
		Baseline ShortBaseline `json:"baseline"`
	}
	if e = json.Unmarshal(raw, &source); e != nil {
		t.Fatal(e)
	}
	end := source.Baseline.To.Add(time.Hour)
	now := time.Now().UTC()
	h := shortTestHub(t, now)
	tx, e := h.Store.research.Begin()
	if e != nil {
		t.Fatal(e)
	}
	stmt, e := tx.Prepare("INSERT INTO facts VALUES(?,?,?,?,?,?)")
	if e != nil {
		t.Fatal(e)
	}
	for _, f := range source.Facts {
		if _, e = stmt.Exec(ID("flow", "BTC", "", "spot"), f.TS, f.Res, f.Revision, f.Available, f.Payload); e != nil {
			t.Fatal(e)
		}
	}
	stmt.Close()
	if e = tx.Commit(); e != nil {
		t.Fatal(e)
	}
	ctx := context.Background()
	start := time.Now()
	_, e = h.Store.shortRangeVersion(ctx, ID("flow", "BTC", "", "spot"), source.Baseline.From, source.Baseline.To)
	if e != nil {
		t.Fatal(e)
	}
	t.Logf("range version: %s", time.Since(start))
	acc := newFlowAccumulator(300)
	var decode, aggregate time.Duration
	n := 0
	start = time.Now()
	rows, e := h.Store.shortDB().QueryContext(ctx, `SELECT f.payload FROM facts f WHERE f.dataset=? AND f.ts>=? AND f.ts<? AND f.available<=? AND f.available=(SELECT max(g.available) FROM facts g WHERE g.dataset=f.dataset AND g.ts=f.ts AND g.res=f.res AND g.available<=?) ORDER BY f.ts,f.res`, ID("flow", "BTC", "", "spot"), source.Baseline.To.Add(-48*time.Hour).Unix(), source.Baseline.To.Unix(), now.UnixNano(), now.UnixNano())
	if e != nil {
		t.Fatal(e)
	}
	for rows.Next() {
		var b []byte
		rows.Scan(&b)
		at := time.Now()
		o, e := unpack(b)
		decode += time.Since(at)
		if e != nil {
			t.Fatal(e)
		}
		at = time.Now()
		acc.add(o)
		aggregate += time.Since(at)
		n++
	}
	if e = rows.Err(); e != nil {
		t.Fatal(e)
	}
	rows.Close()
	t.Logf("48h total=%s rows=%d decode=%s aggregate=%s", time.Since(start), n, decode, aggregate)
	h.Store.SaveState("short-flow/current", ShortObservation{Through: end})
	for i := 0; i < 16; i++ {
		start = time.Now()
		e = h.shortBaselineAt(ctx, end, now)
		t.Logf("baseline slice %d duration=%s error=%v", i, time.Since(start), e)
		if e != nil {
			t.Fatal(e)
		}
	}
}
