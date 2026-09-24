package datahub

import (
	"context"
	"database/sql"
	"fmt"
	"modernc.org/sqlite"
	"os"
	"path/filepath"
)

// BackupFile uses SQLite's online backup API, including committed WAL pages.
// The destination must not already exist. No raw DB/WAL file copies are used.
func BackupFile(ctx context.Context, source, destination string) error {
	if _, err := os.Stat(destination); !os.IsNotExist(err) {
		return fmt.Errorf("backup destination must be new")
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0700); err != nil {
		return err
	}
	db, err := sql.Open("sqlite", "file:"+source+"?mode=ro&_pragma=busy_timeout(5000)")
	if err != nil {
		return err
	}
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	return conn.Raw(func(driver any) error {
		b, err := driver.(interface {
			NewBackup(string) (*sqlite.Backup, error)
		}).NewBackup(destination)
		if err != nil {
			return err
		}
		for {
			if ctx.Err() != nil {
				b.Finish()
				return ctx.Err()
			}
			more, e := b.Step(128)
			if e != nil {
				b.Finish()
				return e
			}
			if !more {
				break
			}
		}
		return b.Finish()
	})
}
