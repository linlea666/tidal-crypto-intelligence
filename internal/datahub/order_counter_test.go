package datahub

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestOrderCounterAllTablesMaintenanceAndRestore(t *testing.T) {
	w := testStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	check := func() {
		t.Helper()
		var tracked, actual int64
		if err := w.db.QueryRow("SELECT bytes FROM order_storage WHERE id=1").Scan(&tracked); err != nil {
			t.Fatal(err)
		}
		if err := w.db.QueryRow(`SELECT coalesce((SELECT sum(length(payload)+length(k)+length(order_key)+128) FROM order_events),0)+coalesce((SELECT sum(length(payload)+length(k)+128) FROM tracked_orders),0)+coalesce((SELECT sum(length(payload)+64) FROM order_hours),0)+coalesce((SELECT sum(length(payload)+length(k)+128) FROM liquidity_events),0)+coalesce((SELECT sum(length(payload)+length(dataset)+128) FROM liquidity_hours),0)+coalesce((SELECT sum(length(payload)+length(dataset)+128) FROM order_zone_samples),0)`).Scan(&actual); err != nil {
			t.Fatal(err)
		}
		if tracked != actual {
			t.Fatalf("counter %d != independent total %d", tracked, actual)
		}
	}
	queries := []string{
		`INSERT INTO tracked_orders VALUES('k','BTC','test',0,'{}')`,
		`INSERT INTO order_events VALUES('e','k','BTC',0,'test','{}')`,
		`INSERT INTO order_hours VALUES('BTC',0,'{}')`,
		`INSERT INTO liquidity_events VALUES('l','BTC','d',0,'{"side":"bid","step":10}')`,
		`INSERT INTO liquidity_hours VALUES('BTC','d','bid',10,0,'{}')`,
		`INSERT INTO order_zone_samples VALUES('d','BTC',0,'{}')`,
		`UPDATE tracked_orders SET payload='{"test":123}'`,
		`UPDATE order_events SET payload='{"test":123}'`,
		`UPDATE order_hours SET payload='{"test":123}'`,
		`UPDATE liquidity_events SET payload='{"side":"bid","step":10,"test":123}'`,
		`UPDATE liquidity_hours SET payload='{"test":123}'`,
	}
	for _, q := range queries {
		if _, err := w.db.Exec(q); err != nil {
			t.Fatal(q, err)
		}
		check()
	}
	backup := filepath.Join(t.TempDir(), "hub.sqlite")
	if err := BackupFile(ctx, filepath.Join(w.Root(), "hub.sqlite"), backup); err != nil {
		t.Fatal(err)
	}
	if err := w.maintainOrders(ctx, now, 90); err != nil {
		t.Fatal(err)
	}
	check()
	if w.Status().OrderBytes != 0 {
		t.Fatal("maintained status diverged", w.Status().OrderBytes)
	}
	db, err := database(backup)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	original := w.db
	w.db = db
	check()
	w.db = original
}
