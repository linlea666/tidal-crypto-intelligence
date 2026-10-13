package datahub

import (
	"context"
	"os"
	"testing"
	"time"
)

// Opt-in metadata-scale reconstruction of the production retention query.
// Payloads are synthetic storage ballast, never market input or research output.
func TestResearchRetentionProductionScaleProfile(t *testing.T) {
	if os.Getenv("TIDAL_RETENTION_PROFILE") != "1" {
		t.Skip("explicit retention profile")
	}
	w := testStore(t)
	ctx := context.Background()
	if _, err := w.research.ExecContext(ctx, "DROP INDEX facts_time"); err != nil {
		t.Fatal(err)
	}
	now := time.Now().Unix()
	if _, err := w.research.ExecContext(ctx, `WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n<331447)
 INSERT INTO facts SELECT 'flow.btc..spot',?-n,60,printf('%064d',n),?,zeroblob(96) FROM seq`, now, now*int64(time.Second)); err != nil {
		t.Fatal(err)
	}
	query := `SELECT dataset,ts,res,revision FROM facts WHERE ts<? AND NOT EXISTS(SELECT 1 FROM study_inputs i WHERE i.state='building' AND facts.ts>=i.from_ts AND facts.ts<i.to_ts) LIMIT 256`
	measure := func(label string) {
		t.Helper()
		start := time.Now()
		c, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		rows, err := w.shortDB().QueryContext(c, query, now-90*86400)
		if err == nil {
			for rows.Next() {
				t.Fatal("unexpired row selected")
			}
			err = rows.Err()
			rows.Close()
		}
		t.Logf("%s: rows=331447 elapsed=%s err=%v", label, time.Since(start), err)
		if label == "indexed" && err != nil {
			t.Fatal(err)
		}
	}
	measure("original")
	if _, err := w.research.ExecContext(ctx, "CREATE INDEX facts_time ON facts(ts)"); err != nil {
		t.Fatal(err)
	}
	measure("indexed")
}
