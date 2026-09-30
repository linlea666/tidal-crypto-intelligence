package datahub

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Isolated UI acceptance fixture. Never feeds a running production collector.
func TestLiquidationBrowserFixture(t *testing.T) {
	root := os.Getenv("TIDAL_LZ_QA_ROOT")
	if root == "" {
		t.Skip("manual isolated browser fixture")
	}
	if !filepath.IsAbs(root) || !strings.Contains(root, "/tmp/liquidation-qa/") {
		t.Fatal("requires named isolated temporary path")
	}
	h, e := Open(Config{Root: root, Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	now := time.Now().UTC()
	ctx := context.Background()
	for _, asset := range Assets() {
		d, o := liquidationFixture(asset, now)
		o.Payload.Model.Model = "离线验收数据 · CoinGlass契约结构"
		o.Payload.Model.Bins[0].RawStrength = "312480691.630001"
		o.Payload.Model.Bins[0].Strength = 312480691.630001
		for _, m := range []int{-35, -20, -5} {
			s, e := makeLiquidationMap(d, o, now.Add(time.Duration(m)*time.Minute))
			if e != nil {
				t.Fatal(e)
			}
			s.Fetched = now
			s.Revision = "offline-qa"
			if e = h.processLiquidationMap(ctx, s); e != nil {
				t.Fatal(e)
			}
		}
		if _, e = h.Store.Ingest(d, o); e != nil {
			t.Fatal(e)
		}
		old := o
		old.FetchedAt = now.Add(-48 * time.Hour)
		dd := d
		dd.ID += "@7d"
		old.Dataset = dd.ID
		old.Payload.Model = &Model{Bins: o.Payload.Model.Bins, ReferencePrice: o.Payload.Model.ReferencePrice, Model: "离线验收历史", Contract: LiquidationContract, CoverageComplete: true, Range: "7d"}
		h.Store.Ingest(dd, old)
		liq, _ := h.Dataset(ID("liquidations", asset, "", "futures"))
		for i := 0; i < 60; i++ {
			at := now.Truncate(5 * time.Minute).Add(-time.Duration(60-i) * time.Minute)
			h.Store.Ingest(liq, Observation{Dataset: liq.ID, ObservedAt: &at, FetchedAt: now, Resolution: 60, Quality: "valid", Payload: Payload{Liquidation: &Liquidation{Long: "713333.33", Short: "425000"}}})
		}
	}
}
