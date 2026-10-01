package datahub

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"
)

// Optional realistic seven-day history read, using the same API deadline. The
// fixture is isolated from production and never calls an upstream endpoint.
func TestOrderZoneHistoryResource(t *testing.T) {
	if os.Getenv("TIDAL_ORDER_ZONE_REPLAY") != "1" {
		t.Skip("manual bounded history replay")
	}
	h := zoneHub(t)
	now := time.Now().UTC()
	zonePrice(t, h, "BTC", "1", "85000", now)
	b := &Book{Low: 82000, High: 88000}
	for i := 0; i < 2000; i++ {
		b.Bids = append(b.Bids, Level{fmt.Sprint(84000 - i), "1.23456789"})
		b.Asks = append(b.Asks, Level{fmt.Sprint(86000 + i), "2.34567891"})
	}
	fd, _ := h.Dataset("fx.usd.kraken")
	for i := 336; i > 0; i-- {
		at := now.Truncate(30 * time.Minute).Add(-time.Duration(i) * 30 * time.Minute)
		zoneIngest(t, h, fd, Observation{Dataset: fd.ID, ObservedAt: &at, FetchedAt: now, Resolution: 60, Quality: "valid", Payload: Payload{Rates: []Rate{{Quote: "USDT", USD: fmt.Sprintf("1.000%03d", i%91)}}}})
	}
	for _, v := range []string{"Coinbase", "Kraken", "Bitfinex", "Binance", "OKX"} {
		d, _ := h.Dataset(ID("book", "BTC", v, "spot"))
		for i := 336; i > 0; i-- {
			at := now.Truncate(30 * time.Minute).Add(-time.Duration(i) * 30 * time.Minute)
			zoneIngest(t, h, d, Observation{Dataset: d.ID, FetchedAt: now, ObservedAt: &at, Resolution: 300, Quality: "valid", Payload: Payload{Book: b}})
		}
	}
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	v, e := h.orderZoneHistory(ctx, "BTC", url.Values{}, now)
	t.Logf("7-day/6,720,000-level read: %v, slots=%d", time.Since(start), v.CoveredSlots)
	if e != nil {
		t.Fatal(e)
	}
	if v.CoveredSlots < 335 {
		t.Fatalf("incomplete stress fixture: %d covered slots", v.CoveredSlots)
	}
	if time.Since(start) > 8*time.Second {
		t.Fatal("production query deadline exceeded")
	}
}
