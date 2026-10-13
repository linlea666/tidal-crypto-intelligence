package datahub

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func priceObservation(d Dataset, at time.Time, value string) Observation {
	return Observation{Dataset: d.ID, Source: "binance", ObservedAt: &at, FetchedAt: at, Quality: "valid", Payload: Payload{Price: &Price{value, "USDT"}}}
}

func TestPriceIngestDoesNotWaitForPersistentWriter(t *testing.T) {
	// Memory-only store: price must neither touch a database nor wait for the
	// persistent ingest mutex shared by slow books, models and research facts.
	w := &Warehouse{latest: map[string]Observation{}, sizes: map[string]int{}}
	w.write.Lock()
	defer w.write.Unlock()
	done := make(chan error, 1)
	now := time.Now().UTC()
	go func() {
		for _, asset := range []string{"BTC", "ETH"} {
			d, _ := FindDataset(ID("price", asset, "Binance", "spot"))
			if _, err := w.Ingest(d, priceObservation(d, now, "123")); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("live prices waited behind persistent ingestion")
	}
	for _, asset := range []string{"BTC", "ETH"} {
		d, _ := FindDataset(ID("price", asset, "Binance", "spot"))
		latest, ok := w.Latest(d.ID)
		if !ok || !latest.Fresh(d, now) || d.TTL != 15 {
			t.Fatal("price freshness contract changed", asset)
		}
	}
}

func TestPriceIngestPreservesSourceOrderFreshnessAndHotBudget(t *testing.T) {
	w := &Warehouse{latest: map[string]Observation{}, sizes: map[string]int{}}
	d, _ := FindDataset(ID("price", "BTC", "Binance", "spot"))
	at := time.Now().UTC().Add(-time.Minute)
	o := priceObservation(d, at, "100")
	if changed, err := w.Ingest(d, o); err != nil || !changed {
		t.Fatal(changed, err)
	}
	o.FetchedAt = at.Add(time.Minute)
	if changed, err := w.Ingest(d, o); err != nil || changed {
		t.Fatal("duplicate identity changed", changed, err)
	}
	latest, _ := w.Latest(d.ID)
	if latest.Fresh(d, o.FetchedAt) || !latest.Time().Equal(at) {
		t.Fatal("retrieval refreshed an old source time")
	}
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			x := priceObservation(d, at.Add(time.Duration(i)*time.Millisecond), fmt.Sprint(i+100))
			if _, err := w.Ingest(d, x); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	latest, _ = w.Latest(d.ID)
	if !latest.Time().Equal(at.Add(99*time.Millisecond)) || latest.Payload.Price.Value != "199" {
		t.Fatal("concurrent older source replaced newer price")
	}
	before := latest.Revision
	w.mu.Lock()
	w.status.HotBytes = HotLimit
	w.mu.Unlock()
	if _, err := w.Ingest(d, priceObservation(d, at.Add(time.Second), "200.0000000001")); err != nil {
		t.Fatal(err)
	}
	latest, _ = w.Latest(d.ID)
	if latest.Revision != before || w.Status().Error == "" {
		t.Fatal("price bypassed hot-cache limit")
	}
}
