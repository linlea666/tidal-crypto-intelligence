package datahub

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestFrozenStudyInputSurvivesPruningRevisionRestartAndBackup(t *testing.T) {
	h, err := Open(Config{Root: t.TempDir(), Offline: true})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Store.Close()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Minute)
	s := Study{ID: "freeze", Asset: "BTC", From: now.Add(-89 * 24 * time.Hour), To: now, Created: now, Pipeline: studyPipeline}
	d, _ := FindDataset(ID("flow", "BTC", "", "spot"))
	for i := 0; i < 600; i++ {
		at := s.From.Add(time.Duration(i) * 5 * time.Minute)
		if _, err = h.Store.Ingest(d, Observation{Dataset: d.ID, ObservedAt: &at, FetchedAt: now, Resolution: 300, Quality: "valid", Payload: Payload{Flow: &Flow{Buy: "20", Sell: "10"}}}); err != nil {
			t.Fatal(err)
		}
	}
	m, err := h.Store.beginStudySnapshot(ctx, s, now)
	if err != nil {
		t.Fatal(err)
	}
	// One page only; maintenance crosses the normal retention boundary.
	m, err = h.Store.advanceStudySnapshot(ctx, m, now)
	if err != nil {
		t.Fatal(err)
	}
	if m.State != "building" || m.Rows >= 600 {
		t.Fatal("capture was not bounded", m)
	}
	if err = h.Store.maintainResearch(ctx, now.Add(2*24*time.Hour), 90); err != nil {
		t.Fatal(err)
	}
	// The deliberately expired lease must terminate rather than pin forever.
	m, err = h.Store.studySnapshot(ctx, m.ID)
	if err != nil || m.State != "failed" {
		t.Fatal(m, err)
	}
	var n int
	if err = h.Store.research.QueryRow("SELECT count(*) FROM facts").Scan(&n); err != nil || n != 312 {
		t.Fatal("expired lease retained source", n, err)
	}
	// A different study captures fresh facts, then source deletion/revision cannot
	// change its values or invalidate an in-progress comparison checkpoint.
	s.ID = "freeze-live"
	s.From = now.Add(-34 * 24 * time.Hour)
	for i := 0; i < 600; i++ {
		at := s.From.Add(time.Duration(i) * 5 * time.Minute)
		if _, err = h.Store.Ingest(d, Observation{Dataset: d.ID, ObservedAt: &at, FetchedAt: now, Resolution: 300, Quality: "valid", Payload: Payload{Flow: &Flow{Buy: "20", Sell: "10"}}}); err != nil {
			t.Fatal(err)
		}
	}
	m, err = h.Store.beginStudySnapshot(ctx, s, now)
	if err != nil {
		t.Fatal(err)
	}
	for m.State == "building" {
		m, err = h.Store.advanceStudySnapshot(ctx, m, now)
		if err != nil {
			t.Fatal(err)
		}
	}
	s.InputSnapshotID = m.ID
	s.InputFrozenAt = &m.AsOf
	v, err := h.studyInputVersion(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	r, err := h.evaluateStudy(ctx, s, now)
	if err != nil || r.MultifactorComparison.State != "calculating" {
		t.Fatal(r, err)
	}
	var first multiCheckpoint
	if err = h.Store.document(ctx, "multifactor-study-progress", s.ID, &first); err != nil {
		t.Fatal(err)
	}
	if _, err = h.Store.research.Exec("DELETE FROM facts"); err != nil {
		t.Fatal(err)
	}
	at := s.From
	if _, err = h.Store.Ingest(d, Observation{Dataset: d.ID, ObservedAt: &at, FetchedAt: now.Add(time.Minute), Resolution: 300, Quality: "valid", Payload: Payload{Flow: &Flow{Buy: "999", Sell: "0"}}}); err != nil {
		t.Fatal(err)
	}
	if next, e := h.studyInputVersion(ctx, s); e != nil || next != v {
		t.Fatal("input version changed", next, e)
	}
	frozen := context.WithValue(ctx, studySnapshotKey{}, m.ID)
	got := 0
	err = h.Store.FactsAsOf(frozen, d.ID, s.From, s.To, now.Add(time.Hour), func(o Observation) error {
		got++
		if o.Payload.Flow.Buy != "20" {
			return fmt.Errorf("future revision leaked")
		}
		return nil
	})
	if err != nil || got != 600 {
		t.Fatal(got, err)
	}
	r, err = h.evaluateStudy(ctx, s, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	var second multiCheckpoint
	if err = h.Store.document(ctx, "multifactor-study-progress", s.ID, &second); err != nil {
		t.Fatal(err)
	}
	if !second.Cursor.After(first.Cursor) || r.MultifactorComparison.State == "calculating" {
		t.Fatal("checkpoint reset", first.Cursor, second.Cursor)
	}
	target := filepath.Join(t.TempDir(), "research.sqlite")
	if err = BackupFile(ctx, filepath.Join(h.Store.Root(), "research.sqlite"), target); err != nil {
		t.Fatal(err)
	}
	db, err := database(target)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	restored := &Warehouse{research: db}
	got = 0
	if err = restored.FactsAsOf(frozen, d.ID, s.From, s.To, now, func(o Observation) error { got++; return nil }); err != nil || got != 600 {
		t.Fatal("backup lost frozen input", got, err)
	}
	if _, err = db.Exec("UPDATE study_input_chunks SET payload=x'0102' WHERE id=? AND n=0", m.ID); err != nil {
		t.Fatal(err)
	}
	if err = restored.FactsAsOf(frozen, d.ID, s.From, s.To, now, func(o Observation) error { return nil }); err == nil {
		t.Fatal("corrupted snapshot accepted")
	}
}

func TestStudyLeaseProtectsOnlyActiveCaptureAndBudgetIsTerminal(t *testing.T) {
	w := testStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	s := Study{ID: "lease", Asset: "BTC", From: now.Add(-91 * 24 * time.Hour), To: now, Created: now}
	if _, err := w.research.Exec("INSERT INTO facts VALUES('flow.btc..spot',?,300,'a',?,x'01')", s.From.Unix(), now.UnixNano()); err != nil {
		t.Fatal(err)
	}
	m, err := w.beginStudySnapshot(ctx, s, now)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.maintainResearch(ctx, now, 90); err != nil {
		t.Fatal(err)
	}
	var n int
	if err = w.research.QueryRow("SELECT count(*) FROM facts").Scan(&n); err != nil || n != 1 {
		t.Fatal("capture input pruned", n, err)
	}
	if _, err = w.research.Exec("UPDATE study_input_budget SET bytes=?", studySnapshotsLimit); err != nil {
		t.Fatal(err)
	}
	m, err = w.advanceStudySnapshot(ctx, m, now)
	if err != nil || m.State != "failed" {
		t.Fatal("capacity did not terminate", m, err)
	}
	if err = w.maintainResearch(ctx, now, 90); err != nil {
		t.Fatal(err)
	}
	if err = w.research.QueryRow("SELECT count(*) FROM facts").Scan(&n); err != nil || n != 0 {
		t.Fatal("failed capture pinned facts", n, err)
	}
}
func TestVersionQueryErrorsCannotBecomeEmptyVersion(t *testing.T) {
	w := testStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if v, err := w.factVersion(ctx, "BTC"); err == nil || v != "" {
		t.Fatal(v, err)
	}
	if v, err := w.datasetRangeVersion(ctx, "missing", time.Time{}, time.Now()); err == nil || v != "" {
		t.Fatal(v, err)
	}
}
func TestShortPublicationNoRewriteAndFailureSurvivesSuccess(t *testing.T) {
	now := time.Now().UTC()
	h := shortTestHub(t, now)
	now = time.Now().UTC().Add(time.Second)
	ctx := context.Background()
	s := ShortObservation{InputAvailable: &now}
	p, err := h.shortPublication(ctx, &s, now)
	if err != nil || p == nil {
		t.Fatal(p, err)
	}
	if err = h.Store.shortPut(ctx, "publication", s.InputVersion, now, p); err != nil {
		t.Fatal(err)
	}
	first := s.FirstGenerated
	if _, err = h.Store.shortResearch.Exec("CREATE TRIGGER reject_publication BEFORE UPDATE ON sf_records WHEN NEW.kind='publication' BEGIN SELECT RAISE(ABORT,'immutable'); END"); err != nil {
		t.Fatal(err)
	}
	p, err = h.shortPublication(ctx, &s, now.Add(time.Minute))
	if err != nil || p != nil || !s.FirstGenerated.Equal(*first) {
		t.Fatal("publication rewrites first clock", p, err)
	}
	trace := &shortTrace{Operations: map[string]time.Duration{}, LastOperation: "current_write"}
	h.Store.recordShortRuntime("observation", now, time.Millisecond, trace, errors.New("test failure"))
	h.Store.recordShortRuntime("observation", now.Add(time.Second), time.Millisecond, trace, nil)
	stage := h.Store.shortRuntimeView().Stages["observation"]
	if stage.Error != "" || stage.LastFailure != "test failure" || stage.LastFailureOperation != "current_write" || stage.Errors != 1 {
		t.Fatal(stage)
	}
	// A readable current snapshot with corrupt pause state must remain unknown.
	if err = h.Store.shortSaveState(ctx, "short-flow/current", s); err != nil {
		t.Fatal(err)
	}
	if _, err = h.Store.shortDB().Exec("INSERT INTO sf_records VALUES('state','short-flow/gap',0,x'01') ON CONFLICT(kind,id) DO UPDATE SET payload=x'01'"); err != nil {
		t.Fatal(err)
	}
	view := h.shortObservationView(now).(ShortObservation)
	if !view.ResearchPaused || view.ResearchReason != "研究暂停状态暂不可核验" {
		t.Fatal(view.ResearchReason)
	}
}
func TestBoundedShortWriteHonorsRemainingBudget(t *testing.T) {
	w := testStore(t)
	other, err := database(filepath.Join(w.Root(), "research.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	tx, err := other.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.Exec("INSERT INTO sf_records VALUES('state','held',0,x'01')"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	err = w.shortSaveState(ctx, "blocked", true)
	if err == nil || time.Since(start) > 400*time.Millisecond {
		t.Fatal("busy handler escaped stage budget", time.Since(start), err)
	}
	var timeout int
	if err = w.shortDB().QueryRow("PRAGMA busy_timeout").Scan(&timeout); err != nil || timeout != 1500 {
		t.Fatal("connection policy not restored", timeout, err)
	}
}

func TestIncompleteFrozenStudyTerminatesAndDoesNotAutoRecompute(t *testing.T) {
	h, err := Open(Config{Root: t.TempDir(), Offline: true})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Store.Close()
	now := time.Now().UTC().Truncate(time.Hour)
	ctx := context.Background()
	s := Study{ID: "deadline-study", Asset: "BTC", From: now.Add(-90 * 24 * time.Hour), To: now, Created: now.Add(-25 * time.Hour), State: "collecting", Pipeline: studyPipeline}
	if err = h.Store.saveDocument("study", s.ID, s.Asset, s.Created, s); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 45; i++ {
		if err = h.processStudies(ctx, now.Add(time.Duration(i)*time.Minute)); err != nil {
			t.Fatal(err)
		}
	}
	if err = h.Store.document(ctx, "study", s.ID, &s); err != nil {
		t.Fatal(err)
	}
	if s.State != "incomplete" || s.InputIntegrity != "partial" || s.InputSnapshotID == "" || s.Result == nil || s.Result.FlowCoverage != 0 {
		t.Fatal("missing input kept running or invented", s.State, s.InputIntegrity)
	}
	raw, err := h.Store.documents(ctx, "study", "BTC", 10)
	if err != nil {
		t.Fatal(err)
	}
	before := string(raw[0])
	if err = h.processStudies(ctx, now.Add(24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	raw, err = h.Store.documents(ctx, "study", "BTC", 10)
	if err != nil || string(raw[0]) != before {
		t.Fatal("terminal result changed", err)
	}
	revision, err := h.CreateStudy(StudyRequest{Asset: "BTC", ParentStudyID: s.ID})
	if err != nil {
		t.Fatal(err)
	}
	if revision.ID == s.ID || revision.ParentStudyID != s.ID || revision.InputSnapshotID != "" {
		t.Fatal("revision reused old evidence", revision.ID)
	}
}
