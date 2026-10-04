package datahub

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"time"
)

// QuiesceOnchain is run after stopping the app, before an older image is started.
// Both operations are idempotent. Any error blocks rollback, so an old generic
// mail worker can never pick up our pending notifications after a partial stop.
func QuiesceOnchain(ctx context.Context, root string) error {
	for _, spec := range []struct{ file, query string }{
		{"onchain.sqlite", `BEGIN IMMEDIATE; INSERT INTO state(key,payload) VALUES('settings','{"emailEnabled":false}') ON CONFLICT(key) DO UPDATE SET payload=excluded.payload; UPDATE outbox SET status='suppressed_rollback' WHERE status IN ('pending','handed_off'); INSERT INTO state(key,payload) SELECT 'rollback-' || key || '-' || hex(randomblob(8)),payload FROM state WHERE key IN ('feed','observation'); DELETE FROM state WHERE key IN ('feed','observation'); COMMIT;`},
		{"research.sqlite", `UPDATE notices SET status=CASE WHEN status='sending' THEN 'unknown_after_rollback' ELSE 'suppressed_rollback' END WHERE kind LIKE 'onchain-cost:%' AND status IN ('pending','sending');`},
	} {
		path := filepath.Join(root, spec.file)
		if _, err := os.Stat(path); os.IsNotExist(err) {
			continue
		} else if err != nil {
			return err
		}
		db, err := database(path)
		if err != nil {
			return err
		}
		_, err = db.ExecContext(ctx, spec.query)
		closeErr := db.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
	}
	return nil
}

// Restore is not an ordinary process restart. Run after restoring the selected
// online backups with the app stopped. Preserve the user's mail preference,
// but terminate all pre-restore intentions on BOTH sides before starting workers.
func RestoreOnchainBoundary(ctx context.Context, root string) error {
	now := time.Now().UTC()
	for _, spec := range []struct{ file, query string }{
		{"research.sqlite", `UPDATE notices SET status=CASE WHEN status='sending' OR attempted>0 THEN 'unknown_after_restore' ELSE 'suppressed_restore' END WHERE kind LIKE 'onchain-cost:%' AND status IN ('pending','sending')`},
		{"onchain.sqlite", `UPDATE outbox SET status='suppressed_restore' WHERE status IN ('pending','handed_off')`},
	} {
		path := filepath.Join(root, spec.file)
		if _, e := os.Stat(path); os.IsNotExist(e) {
			continue
		} else if e != nil {
			return e
		}
		db, e := database(path)
		if e != nil {
			return e
		}
		_, e = db.ExecContext(ctx, spec.query)
		if e == nil && spec.file == "onchain.sqlite" {
			raw, _ := json.Marshal(map[string]any{"at": now, "note": "恢复边界：恢复前投递状态不再视为可安全重发"})
			_, e = db.ExecContext(ctx, "INSERT INTO state VALUES('restore-boundary',?) ON CONFLICT(key) DO UPDATE SET payload=excluded.payload", raw)
		}
		ce := db.Close()
		if e != nil {
			return e
		}
		if ce != nil {
			return ce
		}
	}
	return nil
}
