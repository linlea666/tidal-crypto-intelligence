package datahub

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Explicit offline browser acceptance only; no worker, collector or SMTP.
func TestShortFlowBrowserFixture(t *testing.T) {
	root := os.Getenv("TIDAL_SF_QA_ROOT")
	if root == "" {
		t.Skip("manual browser fixture")
	}
	if !filepath.IsAbs(root) || !strings.Contains(root, "/tmp/short-flow-qa/") {
		t.Fatal("isolated QA root required")
	}
	h, err := Open(Config{Root: root, Offline: true})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Store.Close()
	now := time.Now().UTC()
	end := now.Truncate(5 * time.Minute)
	mode := os.Getenv("TIDAL_SF_QA_MODE")
	bars, candles, baseline := multifactorFixture(end.Add(-5*time.Minute), "buy")
	formal := buildFlowSnapshot(bars, candles, end.Add(-5*time.Minute), now, baseline, true)
	formal.explain("buy")
	event := Signal{ID: "offline-short-formal", Asset: "BTC", Rules: MultifactorRules, Direction: "buy", Pattern: "fast", State: "confirmed", At: now.Add(-40 * time.Minute), DataThrough: end.Add(-45 * time.Minute), Updated: now.Add(-20 * time.Minute), ConfirmedAt: flowPtr(now.Add(-20 * time.Minute)), ConfirmedThrough: flowPtr(end.Add(-25 * time.Minute)), Multifactor: &formal, FrozenHigh: 84000, FrozenLow: 83500, Net15: 5e9, Expires: now.Add(3 * time.Hour)}
	if err = h.Store.saveDocument("signal", event.ID, "BTC", event.At, event); err != nil {
		t.Fatal(err)
	}
	for i := range formal.Assessments {
		for j := range formal.Assessments[i].FastChecks {
			if strings.Contains(formal.Assessments[i].FastChecks[j].Name, "连续三根") {
				formal.Assessments[i].FastChecks[j].Actual = flowPtr(2.0)
				formal.Assessments[i].FastChecks[j].Passed = false
			}
		}
		for j := range formal.Assessments[i].SustainedChecks {
			if strings.Contains(formal.Assessments[i].SustainedChecks[j].Name, "四段15分钟") {
				formal.Assessments[i].SustainedChecks[j].Actual = flowPtr(1.0)
				formal.Assessments[i].SustainedChecks[j].Passed = false
			}
		}
		formal.Assessments[i].Fast = false
		formal.Assessments[i].Sustained = false
	}
	formal.Headline = "离线验收 · 买盘偏向尚未达到正式异动条件"
	if err = h.Store.SaveState("signals/current/BTC", formal); err != nil {
		t.Fatal(err)
	}
	bs, base := shortFixture(end)
	if mode == "stale" {
		end = end.Add(-20 * time.Minute)
		bs, base = shortFixture(end)
	}
	if mode == "missing" {
		delete(bs, end.Add(-10*time.Minute).Unix())
		base.Valid = false
	}
	bs[end.Add(-15*time.Minute).Unix()] = FlowBar{At: end.Add(-15 * time.Minute), Buy: 100000000, Sell: 300000000}
	s := buildShortObservation(bs, nil, end, now, base)
	s.Available = flowPtr(now.Add(-10 * time.Second))
	s.Zones = []ShortZone{{ID: "offline-short-zone", Side: "short", Low: 86000, High: 86250, Quote: "USDT", Strength: "312000000", Relative: 85, Distance: 1.2, Fetched: now.Add(-5 * time.Minute), Reference: 84980}}
	s.ZoneNote = "离线验收区域 · 模型强度与成交金额分开"
	if mode == "missing" {
		s.Zones = nil
		s.ZoneNote = "缺少当时有效且报价一致的清算区域"
	}
	if err = h.Store.SaveState("short-flow/current", s); err != nil {
		t.Fatal(err)
	}
	if mode == "empty" {
		if _, err = h.Store.db.Exec("DELETE FROM state WHERE key='short-flow/current'"); err != nil {
			t.Fatal(err)
		}
	}
	if err = h.buildShortReport(context.Background(), now.Add(-2*time.Hour), now); err != nil {
		t.Fatal(err)
	}
	t.Log("isolated QA fixture saved; no upstream or email")
}
