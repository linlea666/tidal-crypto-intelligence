package datahub

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// A SQLite busy handler may outlive context cancellation. Reserve time before
// entering it, on the existing single connection; restore its normal policy.
func boundedExec(ctx context.Context, db *sql.DB, normalMS int, query string, args ...any) (sql.Result, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	waitMS := min(normalMS, 100)
	if deadline, ok := ctx.Deadline(); ok {
		waitMS = min(waitMS, int(time.Until(deadline).Milliseconds())-5)
		if waitMS <= 0 {
			return nil, context.DeadlineExceeded
		}
	}
	if _, err = conn.ExecContext(ctx, fmt.Sprintf("PRAGMA busy_timeout=%d", waitMS)); err != nil {
		return nil, err
	}
	defer func() {
		reset, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		_, _ = conn.ExecContext(reset, fmt.Sprintf("PRAGMA busy_timeout=%d", normalMS))
	}()
	return conn.ExecContext(ctx, query, args...)
}
