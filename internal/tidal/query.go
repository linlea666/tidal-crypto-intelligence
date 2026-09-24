package tidal

import (
	"context"
	"database/sql"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Visit decodes one compressed row at a time, so 90-day queries never load 90 days of books into RAM.
func (s *Store) Visit(ctx context.Context, res, kind, asset string, from, to time.Time, stride int64, visit func(Record) error) error {
	paths, err := filepath.Glob(filepath.Join(s.Root, "history", storageRes(res, kind), "*.sqlite"))
	if err != nil {
		return err
	}
	sort.Strings(paths)
	for _, p := range paths {
		day, err := time.Parse("2006-01-02", strings.TrimSuffix(filepath.Base(p), ".sqlite"))
		if err != nil || day.After(to) || day.Add(24*time.Hour).Before(from) {
			continue
		}
		db, err := sql.Open("sqlite", "file:"+p+"?mode=ro&_pragma=busy_timeout(5000)")
		if err != nil {
			return err
		}
		q := "SELECT ts,payload FROM records WHERE kind=? AND asset=? AND ts>=? AND ts<=?"
		args := []any{kind, asset, from.Unix(), to.Unix()}
		if stride > 0 {
			q += " AND ts % ?=0"
			args = append(args, stride)
		}
		q += " ORDER BY ts"
		rows, err := db.QueryContext(ctx, q, args...)
		if err != nil {
			db.Close()
			return err
		}
		for rows.Next() {
			var ts int64
			var data []byte
			if err = rows.Scan(&ts, &data); err != nil {
				break
			}
			raw, er := decode(data)
			if er != nil {
				err = er
				break
			}
			if err = visit(Record{kind, asset, ts, raw}); err != nil {
				break
			}
		}
		rowErr := rows.Err()
		rows.Close()
		db.Close()
		if err != nil {
			return err
		}
		if rowErr != nil {
			return rowErr
		}
	}
	return nil
}
