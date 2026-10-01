package datahub

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestShortPagedBaselineRestartMatchesFrozenReference(t *testing.T) {
	end := time.Now().UTC().Truncate(time.Hour)
	h := shortTestHub(t, end)
	ctx := context.Background()
	to, asof := end.Add(-time.Hour), end
	from := to.Add(-30 * 24 * time.Hour)
	id := ID("flow", "BTC", "", "spot")
	tx, e := h.Store.research.Begin()
	if e != nil {
		t.Fatal(e)
	}
	stmt, e := tx.Prepare("INSERT INTO facts VALUES(?,?,?,?,?,?)")
	if e != nil {
		t.Fatal(e)
	}
	put := func(at time.Time, res int, rev string, avail time.Time, buy string) {
		t.Helper()
		o := Observation{Dataset: id, ObservedAt: flowPtr(at), FetchedAt: avail, FirstFetchedAt: flowPtr(avail), Resolution: res, Revision: rev, Quality: "valid", Payload: Payload{Flow: &Flow{buy, "10.12"}}}
		raw, e := pack(o)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = stmt.Exec(id, at.Unix(), res, rev, avail.UnixNano(), raw); e != nil {
			t.Fatal(e)
		}
	}
	for at := from; at.Before(to); at = at.Add(5 * time.Minute) {
		// Last day uses minute facts, with incomplete minutes and duplicates.
		if !at.Before(to.Add(-24 * time.Hour)) {
			for i := 0; i < 5; i++ {
				if at.Minute() == 0 && i == 2 {
					continue
				}
				put(at.Add(time.Duration(i)*time.Minute), 60, "original", asof.Add(-time.Minute), fmt.Sprint(10+i))
			}
		} else {
			put(at, 300, "original", asof.Add(-time.Minute), fmt.Sprint(10+at.Minute()))
		}
	}
	// A duplicate timestamp correction before asOf and a future revision.
	put(to.Add(-7*time.Minute), 60, "correction", asof.Add(-time.Second), "0")
	put(to.Add(-7*time.Minute), 60, "future", asof.Add(time.Second), "9999999")
	stmt.Close()
	if e = tx.Commit(); e != nil {
		t.Fatal(e)
	}
	acc := newFlowAccumulator(300)
	if e = factsAsOf(ctx, h.Store.shortDB(), id, from, to, asof, func(o Observation) error {
		if shortClosedFact(o) {
			acc.add(o)
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	want := shortBaseline(acc.finish(), from, to, asof)
	version, e := h.Store.shortRangeVersion(ctx, id, from, to)
	if e != nil {
		t.Fatal(e)
	}
	work := shortBaselineV2{shortBaselineWork: shortBaselineWork{From: from, To: to, Cursor: from, AsOf: asof, Version: version}, Phase: "read"}
	pages := 0
	for work.Cursor.Before(to) {
		page, next, e := h.shortFlowPage(ctx, work)
		if e != nil {
			t.Fatal(e)
		}
		if !next.After(work.Cursor) || next.Unix()%300 != 0 {
			t.Fatal("page cannot advance", next)
		}
		work.Bars = append(work.Bars, page...)
		work.Cursor = next
		pages++
		if e = h.Store.shortPut(ctx, "work-v2", ShortFlowRules, asof, work); e != nil {
			t.Fatal(e)
		}
		var restored shortBaselineV2
		if e = h.Store.shortLoad(ctx, "work-v2", ShortFlowRules, &restored); e != nil {
			t.Fatal(e)
		}
		if !reflect.DeepEqual(work, restored) {
			t.Fatal("checkpoint changed exact integer bars")
		}
		work = restored
		if pages == 2 {
			root := h.Store.Root()
			h.Store.Close()
			h, e = Open(Config{Root: root, Offline: true})
			if e != nil {
				t.Fatal(e)
			}
			defer h.Store.Close()
		}
	}
	if pages < 30 {
		t.Fatal("fixture did not cross page boundaries", pages)
	}
	if e = h.shortBaselineAt(ctx, end, asof); e != nil {
		t.Fatal(e)
	}
	var got ShortBaseline
	if e = h.Store.shortLoad(ctx, "baseline", ShortFlowRules, &got); e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("paged baseline drift\ngot %+v\nwant %+v", got, want)
	}
	if got.Coverage >= 1 || !got.Valid {
		t.Fatal("missing minutes filled or full baseline lost", got)
	}
	// Finished records were atomically published; the legacy work is untouched.
	var old shortBaselineWork
	if e = h.Store.shortLoad(ctx, "work", ShortFlowRules, &old); e != sql.ErrNoRows {
		t.Fatal("legacy checkpoint rewritten", e)
	}
}

func TestShortAtomicBaselineWriteFailure(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Hour)
	h := shortTestHub(t, now)
	ctx := context.Background()
	_, b := shortFixture(now)
	w := shortBaselineV2{shortBaselineWork: shortBaselineWork{From: b.From, To: b.To, AsOf: now, Cursor: b.To}, Phase: "ready", Horizon: 5, Baseline: b}
	if e := h.Store.shortPut(ctx, "baseline", ShortFlowRules, now, b); e != nil {
		t.Fatal(e)
	}
	if _, e := h.Store.shortDB().Exec("CREATE TRIGGER test_baseline_write BEFORE INSERT ON sf_records WHEN NEW.kind='baseline-v2' BEGIN SELECT RAISE(ABORT,'injected disk failure'); END"); e != nil {
		t.Fatal(e)
	}
	if e := h.Store.commitShortBaseline(ctx, w, now); shortErrorClass(e) != "write" {
		t.Fatal("write fault not classified", e)
	}
	var absent shortBaselineV2
	if e := h.Store.shortLoad(ctx, "work-v2", ShortFlowRules, &absent); e != sql.ErrNoRows {
		t.Fatal("partial checkpoint published", e)
	}
	var kept ShortBaseline
	h.Store.shortLoad(ctx, "baseline", ShortFlowRules, &kept)
	if !reflect.DeepEqual(kept, b) {
		t.Fatal("previous baseline changed")
	}
}

func TestShortPublicationClockSurvivesRefreshRestartAndUnknownLegacy(t *testing.T) {
	end := time.Now().UTC().Truncate(5 * time.Minute)
	h := shortTestHub(t, end)
	ctx := context.Background()
	h.Store.shortPut(ctx, "pipeline-origin", ShortPipeline, end, end.Add(-time.Hour))
	s := shortSnapshot(end)
	s.InputAvailable = flowPtr(end.Add(30 * time.Second))
	s.Available = s.InputAvailable
	p, e := h.shortPublication(ctx, &s, end.Add(45*time.Second))
	if e != nil {
		t.Fatal(e)
	}
	if s.FirstDelay == nil || *s.FirstDelay != 15 || s.ArrivalDelay == nil || *s.ArrivalDelay != 30 {
		t.Fatal(s)
	}
	h.Store.shortPut(ctx, "publication", s.InputVersion, s.Through, p)
	root := h.Store.Root()
	h.Store.Close()
	h, e = Open(Config{Root: root, Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	again := shortSnapshot(end)
	again.InputAvailable = s.InputAvailable
	again.Available = s.Available
	if _, e = h.shortPublication(ctx, &again, end.Add(2*time.Minute)); e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(again.FirstGenerated, s.FirstGenerated) || *again.FirstDelay != 15 {
		t.Fatal("refresh reset publication clock")
	}
	legacy := shortSnapshot(end.Add(-24 * time.Hour))
	legacy.InputAvailable = flowPtr(end.Add(-24 * time.Hour))
	if _, e = h.shortPublication(ctx, &legacy, end); e != nil {
		t.Fatal(e)
	}
	if legacy.FirstGenerated != nil || legacy.FirstDelay != nil {
		t.Fatal("invented legacy first visibility")
	}
}

func TestShortDiagnosticsYieldAndErrorsDoNotRewriteCoverage(t *testing.T) {
	now := time.Now().UTC().Truncate(5 * time.Minute)
	h := shortTestHub(t, now)
	ctx := context.Background()
	trace := &shortTrace{Operations: map[string]time.Duration{"baseline_page": time.Millisecond}, Yielded: true}
	h.Store.recordShortRuntime("baseline", now, time.Millisecond, trace, nil)
	r := h.Store.shortRuntimeView()
	if r.Stages["baseline"].Errors != 0 || r.Stages["baseline"].State != "yielded" || r.Stages["baseline"].Success != nil {
		t.Fatal(r)
	}
	h.Store.recordShortRuntime("baseline", now, time.Second, trace, context.DeadlineExceeded)
	r = h.Store.shortRuntimeView()
	if r.Stages["baseline"].Errors != 1 || r.Stages["baseline"].Class != "timeout" {
		t.Fatal(r)
	}
	h.Store.shortGap(now, context.DeadlineExceeded, false)
	var gap shortGap
	h.Store.LoadState("short-flow/gap", &gap)
	if gap.Paused || gap.Count != 1 {
		t.Fatal(gap)
	}
	h.Store.shortGap(now, shortWriteError(errors.New("disk")), true)
	h.Store.shortGap(now, context.DeadlineExceeded, false)
	h.Store.LoadState("short-flow/gap", &gap)
	if !gap.Paused || gap.Count != 3 {
		t.Fatal("read failure cleared write pause", gap)
	}
	origin := now.Add(-15 * time.Minute)
	old := ShortCoverage{Through: now.Add(-10 * time.Minute), Seen: now, Valid: false}
	h.Store.shortPut(ctx, "coverage", fmt.Sprint(old.Through.Unix()), old.Through, old)
	newer := ShortCoverage{Through: now.Add(-5 * time.Minute), Seen: now, Valid: false, Pipeline: ShortPipeline, Reasons: []string{"stale_flow", "baseline_unavailable"}}
	h.Store.shortPut(ctx, "coverage", fmt.Sprint(newer.Through.Unix()), newer.Through, newer)
	if e := h.buildShortReport(ctx, origin, now); e != nil {
		t.Fatal(e)
	}
	report := h.shortStudyView("BTC").(ShortStudyReport)
	if report.Expected != 3 || report.Observed != 0 || report.GapReasons["legacy_unknown"] != 1 || report.GapReasons["unrecorded_unknown"] != 1 || report.GapReasons["stale_flow"] != 1 {
		t.Fatal(report)
	}
	var untouched ShortCoverage
	h.Store.shortLoad(ctx, "coverage", fmt.Sprint(old.Through.Unix()), &untouched)
	a, _ := json.Marshal(old)
	b, _ := json.Marshal(untouched)
	if string(a) != string(b) {
		t.Fatal("diagnostics rewrote old coverage")
	}
}
