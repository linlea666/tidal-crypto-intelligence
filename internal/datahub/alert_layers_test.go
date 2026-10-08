package datahub

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func layeredFixture() (Signal, FlowSnapshot, time.Time) {
	now := time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)
	end := now.Add(-5 * time.Minute)
	current := FlowSnapshot{At: now.Add(-time.Minute), DataThrough: end, Fresh: true, Spot: map[string]FlowWindow{"60": {Net: flowPtr(int64(3000000000)), Coverage: 1, To: end}, "240": {Net: flowPtr(int64(2000000000)), Coverage: 1, To: end}}, Price: PriceContext{Close: flowPtr(83140.26), PriorATR: flowPtr(380.0), DisplacementATR: flowPtr(1.2297)}}
	s := Signal{ID: "case", Asset: "BTC", Rules: MultifactorRules, Direction: "buy", At: now.Add(-time.Hour), DataThrough: now.Add(-65 * time.Minute), Expires: now.Add(3 * time.Hour), FrozenHigh: 83283.28, FrozenLow: 82700, Multifactor: flowPtr(current), ConfirmedAt: flowPtr(now.Add(-2 * time.Minute)), ConfirmedThrough: &end, ConfirmationSnapshot: flowPtr(current)}
	return s, current, now
}
func TestLayeredCaseRegressions(t *testing.T) {
	s, c, now := layeredFixture()
	origin := now.Add(-2 * time.Hour)
	if d := evaluateLayered(s, c, origin, now); !d.Accepted {
		t.Fatal(d.Reasons)
	}
	// 10/8 15:30: aligned background alone cannot replace this event's own break.
	s.ConfirmedAt = nil
	s.ConfirmedThrough = nil
	s.ConfirmationSnapshot = nil
	d := evaluateLayered(s, c, origin, now)
	if d.Accepted || !strings.Contains(strings.Join(d.Reasons, ","), "own_breakout_unconfirmed") {
		t.Fatal(d)
	}
	// 10/8 23:02: real buying against the four-hour selling background.
	c.Spot["240"] = FlowWindow{Net: flowPtr(int64(-5821490000)), Coverage: 1, To: c.DataThrough}
	s.Multifactor = &c
	m := alertMeaning(s, false)
	if !strings.Contains(strings.Join(m.Labels, ","), "逆势买盘异动，反转未确认") {
		t.Fatal(m)
	}
	d = evaluateLayered(s, c, origin, now)
	if d.Accepted || !strings.Contains(strings.Join(d.Reasons, ","), "flow_direction_conflict") {
		t.Fatal(d)
	}
	// 10/7 10:02: a >3 ATR drop was already visible, not a top prediction.
	s.Direction = "sell"
	c.Price.DisplacementATR = flowPtr(-3.3815)
	s.Multifactor = &c
	if !strings.Contains(strings.Join(alertMeaning(s, false).Labels, ","), "行情已明显移动") {
		t.Fatal("missing chase warning")
	}
	d = evaluateLayered(s, c, origin, now)
	if !strings.Contains(strings.Join(d.Reasons, ","), "extended_move") {
		t.Fatal(d)
	}
}
func TestLayeredBoundaryMissingFutureAndInitialLevel(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*Signal, *FlowSnapshot, *time.Time)
		reason string
	}{
		{"pre_origin", func(s *Signal, c *FlowSnapshot, n *time.Time) { s.At = n.Add(-3 * time.Hour) }, "not_new_formal_publication"},
		{"expired", func(s *Signal, c *FlowSnapshot, n *time.Time) { s.Expires = *n }, "parent_expired"},
		{"stale", func(s *Signal, c *FlowSnapshot, n *time.Time) { c.Fresh = false }, "stale_input"},
		{"missing_atr", func(s *Signal, c *FlowSnapshot, n *time.Time) { c.Price.PriorATR = nil }, "price_atr_missing"},
		{"incomplete", func(s *Signal, c *FlowSnapshot, n *time.Time) { w := c.Spot["240"]; w.Coverage = .9; c.Spot["240"] = w }, "flow_window_missing"},
		{"future_confirmation", func(s *Signal, c *FlowSnapshot, n *time.Time) { s.ConfirmedAt = flowPtr(n.Add(time.Second)) }, "own_breakout_unconfirmed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, c, n := layeredFixture()
			tc.change(&s, &c, &n)
			d := evaluateLayered(s, c, n.Add(-2*time.Hour), n)
			if d.Accepted || !strings.Contains(strings.Join(d.Reasons, ","), tc.reason) {
				t.Fatal(d)
			}
		})
	}
	s, c, n := layeredFixture()
	c.Price.DisplacementATR = flowPtr(1.5)
	if !evaluateLayered(s, c, n.Add(-2*time.Hour), n).Accepted {
		t.Fatal("inclusive 1.5 boundary")
	}
	s.Multifactor.Assessments = []FlowAssessment{{Direction: "buy"}}
	u := *s.Multifactor
	u.Assessments = []FlowAssessment{{Direction: "buy", Large: true}}
	s.MultifactorUpgrade = &u
	s.Level = "large"
	m := alertMeaning(s, false)
	if m.PublishedLevel != "ordinary" || m.LaterLevel != "large" {
		t.Fatal(m)
	}
}
func TestLayeredForwardWorkerDedupeNoMailOrPaper(t *testing.T) {
	h, e := Open(Config{Root: t.TempDir(), Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	ctx := context.Background()
	s, c, n := layeredFixture()
	origin := n.Add(-2 * time.Hour)
	b, _ := json.Marshal(origin)
	if _, e = h.Store.research.Exec("INSERT INTO alert_audit VALUES('origin',?,'',?,?)", LayeredRules, origin.Unix(), b); e != nil {
		t.Fatal(e)
	}
	if e = h.Store.SaveState("signals/current/BTC", c); e != nil {
		t.Fatal(e)
	}
	if e = h.Store.saveDocument("signal", s.ID, "BTC", s.At, s); e != nil {
		t.Fatal(e)
	}
	// A reconstructed document alone is never a real first publication.
	if e = h.layeredStep(ctx, n); e != nil {
		t.Fatal(e)
	}
	var count int
	h.Store.research.QueryRow("SELECT count(*) FROM alert_audit WHERE kind='candidate'").Scan(&count)
	if count != 0 {
		t.Fatal("reconstructed candidate")
	}
	b, _ = json.Marshal(s)
	if _, e = h.Store.research.Exec("INSERT INTO signal_publications(id,at,payload) VALUES(?,?,?)", s.Rules+"/"+s.ID, s.At.UnixMilli(), b); e != nil {
		t.Fatal(e)
	}
	if e = h.layeredStep(ctx, n); e != nil {
		t.Fatal(e)
	}
	if e = h.layeredStep(ctx, n.Add(20*time.Second)); e != nil {
		t.Fatal(e)
	}
	for kind, want := range map[string]int{"candidate": 1, "decision": 1, "outcome": 1} {
		h.Store.research.QueryRow("SELECT count(*) FROM alert_audit WHERE kind=?", kind).Scan(&count)
		if count != want {
			t.Fatalf("%s=%d", kind, count)
		}
	}
	h.Store.research.QueryRow("SELECT count(*) FROM notices").Scan(&count)
	if count != 0 {
		t.Fatal("shadow sent mail")
	}
	if h.Store.paper != nil {
		t.Fatal("shadow started paper")
	}
	ds, e := paperRows[LayeredDecision](ctx, h.Store.research, "SELECT payload FROM alert_audit WHERE kind='candidate'")
	if e != nil || !ds[0].At.Equal(n) {
		t.Fatal(ds, e)
	}
	// Rejected snapshots cannot later be overwritten into successes.
	d := ds[0]
	d.ID += "/reject"
	d.Accepted = false
	d.Reasons = []string{"stale_input"}
	if e = h.saveLayeredDecision(ctx, d); e != nil {
		t.Fatal(e)
	}
	d.Accepted = true
	d.Reasons = []string{}
	if e = h.saveLayeredDecision(ctx, d); e != nil {
		t.Fatal(e)
	}
	var stored LayeredDecision
	h.Store.research.QueryRow("SELECT payload FROM alert_audit WHERE kind='decision' AND id=?", d.ID).Scan(&b)
	json.Unmarshal(b, &stored)
	if stored.Accepted {
		t.Fatal("rewrote first rejection")
	}
}
func TestAlertAuditReadOnlyRangeNullAndBudget(t *testing.T) {
	h, e := Open(Config{Root: t.TempDir(), Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	ctx := context.Background()
	s, _, n := layeredFixture()
	if e = h.Store.saveDocument("signal", s.ID, "BTC", s.At, s); e != nil {
		t.Fatal(e)
	}
	for _, q := range []url.Values{{}, {"layer": {"risk"}}, {"id": {s.ID}}} {
		v, e := h.alertAudit(ctx, q, n)
		if e != nil {
			t.Fatal(e)
		}
		m := v.(map[string]any)
		if m["origin"].(*time.Time) != nil {
			t.Fatal("GET seeded origin")
		}
	}
	var count int
	h.Store.research.QueryRow("SELECT count(*) FROM alert_audit").Scan(&count)
	if count != 0 {
		t.Fatal("GET wrote state")
	}
	for _, q := range []url.Values{{"from": {n.Add(-8 * 24 * time.Hour).Format(time.RFC3339)}}, {"layer": {"bogus"}}, {"asset": {"ETH"}}, {"direction": {"long"}}} {
		if _, e = h.alertAudit(ctx, q, n); e == nil {
			t.Fatal("accepted invalid range", q)
		}
	}
	d := LayeredDecision{ID: "budget", ParentID: s.ID, At: n, Rules: LayeredRules, Accepted: true, Reasons: []string{}}
	h.Store.research.Exec("UPDATE alert_audit_budget SET used=?", layeredBudget)
	if e = h.saveLayeredDecision(ctx, d); e == nil {
		t.Fatal("capacity ignored")
	}
	h.Store.research.QueryRow("SELECT count(*) FROM alert_audit").Scan(&count)
	if count != 0 {
		t.Fatal("partial ledger")
	}
}

// UI data is only generated by this opt-in test into a disposable local path.
func TestLayeredBrowserFixture(t *testing.T) {
	root := os.Getenv("TIDAL_LAYERED_UI_ROOT")
	if root == "" {
		t.Skip("isolated browser fixture")
	}
	if !filepath.IsAbs(root) || !strings.Contains(root, "/tmp/paper-ui-") {
		t.Fatal("disposable tmp/paper-ui- root required")
	}
	h, e := Open(Config{Root: root, Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	now := time.Now().UTC()
	end := now.Truncate(5 * time.Minute)
	bars, c, base := multifactorFixture(end, "sell")
	current := buildFlowSnapshot(bars, c, end, now, base, true)
	current.explain("sell")
	current.Headline = "离线验收数据 · 分层预警交互"
	h.Store.SaveState("signals/current/BTC", current)
	origin := now.Add(-24 * time.Hour)
	raw, _ := json.Marshal(origin)
	h.Store.research.Exec("INSERT OR IGNORE INTO alert_audit VALUES('origin',?,'',?,?)", LayeredRules, origin.Unix(), raw)
	for i := 0; i < 12; i++ {
		at := now.Add(-time.Duration(i+1) * time.Hour)
		side := "buy"
		if i%2 == 0 {
			side = "sell"
		}
		initial := current
		initial.At = at
		initial.DataThrough = at.Truncate(5 * time.Minute)
		sig := Signal{ID: fmt.Sprintf("offline-layered-qa-%d", i), Asset: "BTC", Rules: MultifactorRules, Direction: side, At: at, DataThrough: initial.DataThrough, Expires: at.Add(4 * time.Hour), State: "anomaly", Level: "ordinary", Multifactor: &initial, ReferencePrice: 83000, FrozenHigh: 83500, FrozenLow: 82500}
		if i%3 == 0 {
			sig.ConfirmedAt = flowPtr(at.Add(10 * time.Minute))
			sig.ConfirmedThrough = flowPtr(initial.DataThrough.Add(10 * time.Minute))
			confirmed := initial
			confirmed.At = *sig.ConfirmedAt
			confirmed.DataThrough = *sig.ConfirmedThrough
			sig.ConfirmationSnapshot = &confirmed
		}
		h.Store.saveDocument("signal", sig.ID, "BTC", at, sig)
		d := evaluateLayered(sig, current, origin, now)
		d.At = at.Add(15 * time.Minute)
		d.ID += "/ui"
		if i == 0 {
			d.Accepted = true
			d.Reasons = []string{}
		}
		if e = h.saveLayeredDecision(context.Background(), d); e != nil {
			t.Fatal(e)
		}
	}
	cd, _ := h.Dataset(ID("candles", "BTC", "Binance", "spot"))
	for ts, v := range c {
		at := time.Unix(ts, 0).UTC()
		o := Observation{Dataset: cd.ID, Source: cd.Source, ObservedAt: &at, FetchedAt: now, Resolution: 300, Quality: "valid", Payload: Payload{Candle: &v}}
		if _, e = h.Store.Ingest(cd, o); e != nil {
			t.Fatal(e)
		}
	}
	t.Log("local-only UI fixture; no production events or mail")
}
