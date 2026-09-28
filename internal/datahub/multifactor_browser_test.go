package datahub

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Explicit, isolated browser fixture. Never run the live worker or SMTP here.
// Normal test runs skip it; the output must be under a named temporary QA path.
func TestMultifactorBrowserFixture(t *testing.T) {
	root := os.Getenv("TIDAL_MF_QA_ROOT")
	if root == "" {
		t.Skip("manual browser fixture")
	}
	if !filepath.IsAbs(root) || !strings.Contains(root, "/tmp/multifactor-qa/") {
		t.Fatal("fixture requires isolated temporary root")
	}
	h, e := Open(Config{Root: root, Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	now := time.Now().UTC()
	end := now.Truncate(5 * time.Minute)
	bars, c, base := multifactorFixture(end, "sell")
	for k, v := range c {
		v.Open, v.High, v.Low, v.Close = 83200, 83500, 83000, 83200
		c[k] = v
	}
	base.From, base.To = end.Add(-30*24*time.Hour-time.Hour), end.Add(-time.Hour)
	base.Coverage, base.Dates = 1, 30
	current := buildFlowSnapshot(bars, c, end, now, base, true)
	current.explain("sell")
	current.Headline = "离线验收数据 · 卖压异动，等待价格确认"
	if e = h.Store.SaveState("signals/current/BTC", current); e != nil {
		t.Fatal(e)
	}
	h.Store.SaveState("signals/multifactor-cutover", now.Add(-6*time.Hour))
	state := signalState{Active: map[string]string{}, Clear: map[string]*time.Time{}}
	updates := []Signal{}
	notices := map[string]string{}
	if e = h.processMultifactor(context.Background(), "BTC", &state, current, c, true, now, &updates, notices); e != nil {
		t.Fatal(e)
	}
	for i := range updates {
		updates[i].At = now.Add(-45 * time.Minute)
		updates[i].DataThrough = end.Add(-45 * time.Minute)
		updates[i].State = "confirmed"
		updates[i].ConfirmedAt = flowPtr(now.Add(-30 * time.Minute))
		updates[i].ConfirmedThrough = flowPtr(end.Add(-30 * time.Minute))
		updates[i].FrozenLow = 83050
		updates[i].Progress = flowPtr(priceProgress(updates[i], bars, c, end, now, true))
		updates[i].ConfirmationSnapshot = snapshotForSide(current, "sell")
		updates[i].Multifactor.Headline = "离线验收数据 · 发现时的卖压异动"
	}
	if e = h.commitSignals(context.Background(), "BTC", state, updates, notices, now); e != nil {
		t.Fatal(e)
	}
	fd, _ := h.Dataset(ID("candles", "BTC", "Binance", "spot"))
	for at, v := range c {
		ts := time.Unix(at, 0).UTC()
		o := Observation{Dataset: fd.ID, Source: fd.Source, ObservedAt: &ts, FetchedAt: now, Resolution: 300, Quality: "valid", Payload: Payload{Candle: &v}}
		if _, e = h.Store.Ingest(fd, o); e != nil {
			t.Fatal(e)
		}
	}
	t.Log("isolated fixture written; no upstream calls or email")
}
