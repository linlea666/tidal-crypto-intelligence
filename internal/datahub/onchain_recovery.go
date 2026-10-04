package datahub

import (
	"context"
	"os"
	"path/filepath"
)

// QuiesceOnchain is run after stopping the app, before an older image is started.
// Both operations are idempotent. Any error blocks rollback, so an old generic
// mail worker can never pick up our pending notifications after a partial stop.
func QuiesceOnchain(ctx context.Context, root string) error {
	for _, spec := range []struct{ file, query string }{
		{"onchain.sqlite", `BEGIN IMMEDIATE; INSERT INTO state(key,payload) VALUES('settings','{"emailEnabled":false}') ON CONFLICT(key) DO UPDATE SET payload=excluded.payload; UPDATE outbox SET status='suppressed_rollback' WHERE status='pending'; COMMIT;`},
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
