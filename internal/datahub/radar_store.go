package datahub

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type radarStore struct {
	db   *sql.DB
	mu   sync.Mutex
	path string
}

func openRadar(root string) (*radarStore, error) {
	p := filepath.Join(root, "radar.sqlite")
	db, e := database(p)
	if e != nil {
		return nil, e
	}
	_, e = db.Exec(`PRAGMA busy_timeout=200; PRAGMA cache_size=-1024; PRAGMA max_page_count=122880;
 CREATE TABLE IF NOT EXISTS records(kind TEXT,id TEXT,at INTEGER,payload BLOB NOT NULL,PRIMARY KEY(kind,id)) WITHOUT ROWID;
 CREATE INDEX IF NOT EXISTS radar_records_time ON records(kind,at,id);
 CREATE TABLE IF NOT EXISTS fills(address TEXT,coin TEXT,ts INTEGER,tid INTEGER,hash TEXT,payload BLOB,PRIMARY KEY(address,coin,ts,tid)) WITHOUT ROWID;
 CREATE INDEX IF NOT EXISTS radar_fills_time ON fills(ts);
 CREATE TABLE IF NOT EXISTS events(id TEXT PRIMARY KEY,address TEXT,asset TEXT,side TEXT,opened INTEGER,updated INTEGER,payload BLOB NOT NULL);
 CREATE INDEX IF NOT EXISTS radar_events_time ON events(updated,id);
 CREATE INDEX IF NOT EXISTS radar_events_opened ON events(opened,id);
 CREATE INDEX IF NOT EXISTS radar_events_address ON events(address,updated);
 CREATE TABLE IF NOT EXISTS outbox(id TEXT PRIMARY KEY,at INTEGER,payload BLOB NOT NULL,exported INTEGER NOT NULL DEFAULT 0);
 CREATE INDEX IF NOT EXISTS radar_outbox_pending ON outbox(exported,at);
 `)
	if e != nil {
		db.Close()
		return nil, e
	}
	r := &radarStore{db: db, path: p}
	e = r.put(context.Background(), "origin", RadarRules, time.Now().UTC(), time.Now().UTC(), true)
	if e != nil {
		db.Close()
		return nil, e
	}
	return r, nil
}

type radarDB interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func radarLoad(ctx context.Context, db radarDB, kind, id string, v any) error {
	var b []byte
	e := db.QueryRowContext(ctx, "SELECT payload FROM records WHERE kind=? AND id=?", kind, id).Scan(&b)
	if e != nil {
		return e
	}
	if len(b) > 512<<10 {
		return errors.New("雷达记录超限")
	}
	return json.Unmarshal(b, v)
}
func radarPut(ctx context.Context, db radarDB, kind, id string, at time.Time, v any, once bool) error {
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	if len(b) > 512<<10 {
		return errors.New("雷达记录超限")
	}
	q := "INSERT INTO records VALUES(?,?,?,?) ON CONFLICT(kind,id) DO UPDATE SET at=excluded.at,payload=excluded.payload"
	if once {
		q = "INSERT OR IGNORE INTO records VALUES(?,?,?,?)"
	}
	_, e = db.ExecContext(ctx, q, kind, id, at.UnixMilli(), b)
	return e
}
func (r *radarStore) put(ctx context.Context, kind, id string, at time.Time, v any, once bool) error {
	return radarPut(ctx, r.db, kind, id, at, v, once)
}
func (r *radarStore) size() int64 {
	var n int64
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if s, e := os.Stat(r.path + suffix); e == nil {
			n += s.Size()
		}
	}
	return n
}
func (r *radarStore) maintain(ctx context.Context, now time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, q := range []string{"DELETE FROM fills WHERE ts<?", "DELETE FROM records WHERE kind IN ('wallet','position') AND at<?"} {
		if _, e := r.db.ExecContext(ctx, q, now.Add(-30*24*time.Hour).UnixMilli()); e != nil {
			return e
		}
	}
	for _, q := range []string{"DELETE FROM events WHERE updated<?", "DELETE FROM outbox WHERE at<?", "DELETE FROM records WHERE kind IN ('group','study','study_pending') AND at<?"} {
		if _, e := r.db.ExecContext(ctx, q, now.Add(-90*24*time.Hour).UnixMilli()); e != nil {
			return e
		}
	}
	_, e := r.db.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)")
	return e
}
func radarReadEvent(ctx context.Context, db radarDB, id string) (RadarEvent, error) {
	var v RadarEvent
	var b []byte
	e := db.QueryRowContext(ctx, "SELECT payload FROM events WHERE id=?", id).Scan(&b)
	if e == nil {
		e = json.Unmarshal(b, &v)
	}
	return v, e
}
func radarSaveEvent(ctx context.Context, db radarDB, v RadarEvent) error {
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	_, e = db.ExecContext(ctx, `INSERT INTO events VALUES(?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET updated=excluded.updated,payload=excluded.payload`, v.ID, v.Address, v.Asset, v.Side, v.Opened.UnixMilli(), v.Updated.UnixMilli(), b)
	return e
}
func radarQueueNotice(ctx context.Context, db radarDB, e RadarEvent, kind string, now time.Time) error {
	n := radarNotice{e.ID + "/" + kind, kind, now, now.Add(radarNoticeTTL), e}
	if e.Threshold != nil && kind == "opening" {
		n.Expires = e.Threshold.Add(radarNoticeTTL)
	}
	b, err := json.Marshal(n)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, "INSERT OR IGNORE INTO outbox(id,at,payload) VALUES(?,?,?)", n.ID, now.UnixMilli(), b)
	return err
}
