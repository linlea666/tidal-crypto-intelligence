package datahub

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
)

const studySnapshotLimit = 64 << 20
const studySnapshotsLimit = 128 << 20
const snapshotChunkLimit = 1 << 20

type studySnapshotKey struct{}
type snapshotFact struct {
	Dataset    string
	TS         int64
	Resolution int
	Revision   string
	Available  int64
	Payload    []byte
}
type studySnapshot struct {
	ID         string    `json:"id"`
	From       time.Time `json:"from"`
	To         time.Time `json:"to"`
	AsOf       time.Time `json:"asOf"`
	Deadline   time.Time `json:"deadline"`
	State      string    `json:"state"`
	Reason     string    `json:"reason,omitempty"`
	Dataset    string    `json:"cursorDataset"`
	TS         int64     `json:"cursorTime"`
	Resolution int       `json:"cursorResolution"`
	Revision   string    `json:"cursorRevision"`
	Phase      string    `json:"phase"`
	Chunks     int       `json:"chunks"`
	Bytes      int64     `json:"bytes"`
	Rows       int64     `json:"rows"`
	Hash       string    `json:"hash"`
}

func (w *Warehouse) initStudySnapshots() error {
	_, err := w.research.Exec(`CREATE TABLE IF NOT EXISTS study_inputs(id TEXT PRIMARY KEY,state TEXT,from_ts INTEGER,to_ts INTEGER,deadline INTEGER,payload BLOB);
 CREATE TABLE IF NOT EXISTS study_input_chunks(id TEXT,n INTEGER,dataset TEXT,low INTEGER,high INTEGER,hash TEXT,payload BLOB,PRIMARY KEY(id,n)) WITHOUT ROWID;
 CREATE INDEX IF NOT EXISTS study_input_window ON study_input_chunks(id,dataset,low,high);
 CREATE TABLE IF NOT EXISTS study_input_budget(id INTEGER PRIMARY KEY,bytes INTEGER NOT NULL);
 INSERT OR IGNORE INTO study_input_budget VALUES(1,0);
 CREATE TRIGGER IF NOT EXISTS study_input_guard BEFORE INSERT ON study_input_chunks WHEN
 (SELECT bytes FROM study_input_budget WHERE id=1)+length(NEW.payload)+256>134217728 OR
 coalesce((SELECT sum(length(payload)+256) FROM study_input_chunks WHERE id=NEW.id),0)+length(NEW.payload)+256>67108864
 BEGIN SELECT RAISE(ABORT,'study snapshot capacity'); END;
 CREATE TRIGGER IF NOT EXISTS study_input_added AFTER INSERT ON study_input_chunks BEGIN UPDATE study_input_budget SET bytes=bytes+length(NEW.payload)+256 WHERE id=1; END;
 CREATE TRIGGER IF NOT EXISTS study_input_removed AFTER DELETE ON study_input_chunks BEGIN UPDATE study_input_budget SET bytes=bytes-length(OLD.payload)-256 WHERE id=1; END;`)
	return err
}
func (w *Warehouse) studySnapshot(ctx context.Context, id string) (studySnapshot, error) {
	var m studySnapshot
	var b []byte
	err := w.research.QueryRowContext(ctx, "SELECT payload FROM study_inputs WHERE id=?", id).Scan(&b)
	if err == nil {
		err = json.Unmarshal(b, &m)
	}
	return m, err
}
func writeSnapshot(ctx context.Context, tx *sql.Tx, m studySnapshot) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO study_inputs VALUES(?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET state=excluded.state,payload=excluded.payload`, m.ID, m.State, m.From.Unix(), m.To.Unix(), m.Deadline.Unix(), b)
	return err
}

// The lease is created in the same database as retention. New facts are append
// only and carry their first availability; a fixed cutoff plus the lease makes
// paged capture stable without holding a long SQLite transaction.
func (w *Warehouse) beginStudySnapshot(ctx context.Context, s Study, now time.Time) (studySnapshot, error) {
	id := s.ID + "/inputs-v1"
	if m, err := w.studySnapshot(ctx, id); err == nil {
		return m, nil
	} else if err != sql.ErrNoRows {
		return m, err
	}
	tx, err := w.research.BeginTx(ctx, nil)
	if err != nil {
		return studySnapshot{}, err
	}
	defer tx.Rollback()
	var n int
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM study_inputs WHERE state='building' AND deadline>?", now.Unix()).Scan(&n); err != nil {
		return studySnapshot{}, err
	}
	if n > 0 {
		return studySnapshot{}, errors.New("已有研究输入正在冻结")
	}
	m := studySnapshot{ID: id, From: s.From.Add(-48 * time.Hour), To: s.To, AsOf: now, Deadline: now.Add(24 * time.Hour), State: "building", Phase: "facts"}
	if err = writeSnapshot(ctx, tx, m); err == nil {
		err = tx.Commit()
	}
	return m, err
}
func compressSnapshot(rows []snapshotFact) ([]byte, error) {
	raw, err := json.Marshal(rows)
	if err != nil {
		return nil, err
	}
	if len(raw) > snapshotChunkLimit {
		return nil, errors.New("研究输入分块解码预算超限")
	}
	var out bytes.Buffer
	z, _ := gzip.NewWriterLevel(&out, gzip.BestSpeed)
	if _, err = z.Write(raw); err != nil {
		return nil, err
	}
	if err = z.Close(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}
func decodeSnapshot(b []byte) ([]snapshotFact, error) {
	z, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	defer z.Close()
	raw, err := io.ReadAll(io.LimitReader(z, snapshotChunkLimit+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > snapshotChunkLimit {
		return nil, errors.New("研究输入分块解码预算超限")
	}
	var rows []snapshotFact
	err = json.Unmarshal(raw, &rows)
	return rows, err
}
func (w *Warehouse) advanceStudySnapshot(ctx context.Context, m studySnapshot, now time.Time) (studySnapshot, error) {
	if m.State != "building" {
		return m, nil
	}
	fail := func(reason string) (studySnapshot, error) {
		m.State, m.Reason = "failed", reason
		tx, err := w.research.BeginTx(ctx, nil)
		if err != nil {
			return m, err
		}
		defer tx.Rollback()
		if err = writeSnapshot(ctx, tx, m); err == nil {
			err = tx.Commit()
		}
		return m, err
	}
	if !now.Before(m.Deadline) {
		return fail("输入冻结超过24小时；已有事实可能不完整")
	}
	if w.Status().ResearchPaused || w.Status().Paused {
		return fail("研究总容量保护")
	}
	var rows *sql.Rows
	var err error
	if m.Phase == "facts" {
		rows, err = w.research.QueryContext(ctx, `SELECT dataset,ts,res,revision,available,payload FROM facts
 WHERE ts>=? AND ts<? AND available<=? AND (dataset LIKE '%.btc.%' OR dataset='funding.all..futures')
 AND (dataset,ts,res,revision)>(?,?,?,?) ORDER BY dataset,ts,res,revision LIMIT 256`, m.From.Unix(), m.To.Unix(), m.AsOf.UnixNano(), m.Dataset, m.TS, m.Resolution, m.Revision)
	} else {
		rows, err = w.research.QueryContext(ctx, `SELECT '@shadow/BTC',ts,0,'',0,payload FROM shadow WHERE asset='BTC' AND ts>=? AND ts<? AND ts<=? AND ts>? ORDER BY ts LIMIT 256`, m.From.Unix(), m.To.Unix(), m.AsOf.Unix(), m.TS)
	}
	if err != nil {
		return m, err
	}
	items := []snapshotFact{}
	rawBytes := 0
	for rows.Next() {
		var f snapshotFact
		if err = rows.Scan(&f.Dataset, &f.TS, &f.Resolution, &f.Revision, &f.Available, &f.Payload); err != nil {
			break
		}
		// One dataset per chunk permits indexed reads and streaming vintage choice.
		if len(items) > 0 && (f.Dataset != items[0].Dataset || rawBytes+len(f.Payload) > 256<<10) {
			break
		}
		if len(f.Payload) > 256<<10 {
			err = errors.New("研究输入单行超过冻结预算")
			break
		}
		items = append(items, f)
		rawBytes += len(f.Payload) + 256
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return m, err
	}
	if len(items) == 0 {
		if m.Phase == "facts" {
			m.Phase = "shadow"
			m.TS = 0
		} else {
			m.State = "ready"
		}
	}
	var packed []byte
	var hash string
	if len(items) > 0 {
		packed, err = compressSnapshot(items)
		if err != nil {
			return fail(err.Error())
		}
		hash = fmt.Sprintf("%x", sha256.Sum256(packed))
		var used int64
		if err = w.research.QueryRowContext(ctx, "SELECT bytes FROM study_input_budget WHERE id=1").Scan(&used); err != nil {
			return m, err
		}
		size := int64(len(packed) + 256)
		if m.Bytes+size > studySnapshotLimit || used+size > studySnapshotsLimit {
			return fail("研究输入快照容量不足")
		}
		last := items[len(items)-1]
		m.Dataset, m.TS, m.Resolution, m.Revision = last.Dataset, last.TS, last.Resolution, last.Revision
		m.Bytes += size
		m.Rows += int64(len(items))
		m.Hash = fmt.Sprintf("%x", sha256.Sum256([]byte(m.Hash+hash)))
	}
	tx, err := w.research.BeginTx(ctx, nil)
	if err != nil {
		return m, err
	}
	defer tx.Rollback()
	if len(items) > 0 {
		_, err = tx.ExecContext(ctx, "INSERT INTO study_input_chunks VALUES(?,?,?,?,?,?,?)", m.ID, m.Chunks, items[0].Dataset, items[0].TS, items[len(items)-1].TS, hash, packed)
		if err != nil {
			return m, err
		}
		m.Chunks++
	}
	if err = writeSnapshot(ctx, tx, m); err == nil {
		err = tx.Commit()
	}
	return m, err
}

func (w *Warehouse) snapshotRows(ctx context.Context, id, dataset string, from, to time.Time, fn func(snapshotFact) error) error {
	m, err := w.studySnapshot(ctx, id)
	if err != nil {
		return err
	}
	if m.State != "ready" {
		return errors.New("研究输入快照未就绪")
	}
	var chunks int
	if err = w.research.QueryRowContext(ctx, "SELECT count(*) FROM study_input_chunks WHERE id=?", id).Scan(&chunks); err != nil {
		return err
	}
	if chunks != m.Chunks {
		return errors.New("研究输入分块缺失")
	}
	rows, err := w.research.QueryContext(ctx, "SELECT hash,payload FROM study_input_chunks WHERE id=? AND dataset=? AND low<? AND high>=? ORDER BY n", id, dataset, to.Unix(), from.Unix())
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var hash string
		var b []byte
		if err = rows.Scan(&hash, &b); err != nil {
			return err
		}
		if fmt.Sprintf("%x", sha256.Sum256(b)) != hash {
			return errors.New("研究输入校验失败")
		}
		facts, e := decodeSnapshot(b)
		if e != nil {
			return e
		}
		for _, f := range facts {
			if f.TS >= from.Unix() && f.TS < to.Unix() {
				if e = fn(f); e != nil {
					return e
				}
			}
		}
	}
	return rows.Err()
}
func (w *Warehouse) snapshotFacts(ctx context.Context, snapshot, id string, from, to, asOf time.Time, fn func(Observation) error) error {
	var pending *snapshotFact
	flush := func() error {
		if pending == nil {
			return nil
		}
		o, err := unpack(pending.Payload)
		if err != nil {
			return err
		}
		return fn(o)
	}
	err := w.snapshotRows(ctx, snapshot, id, from, to, func(f snapshotFact) error {
		if f.Available > asOf.UnixNano() {
			return nil
		}
		if pending != nil && (f.TS != pending.TS || f.Resolution != pending.Resolution) {
			if err := flush(); err != nil {
				return err
			}
			pending = nil
		}
		if pending == nil || f.Available > pending.Available {
			copy := f
			pending = &copy
		}
		return nil
	})
	if err != nil {
		return err
	}
	return flush()
}
func (w *Warehouse) pruneStudySnapshots(ctx context.Context, now time.Time) error {
	rows, err := w.research.QueryContext(ctx, "SELECT id FROM study_inputs WHERE state='building' AND deadline<=?", now.Unix())
	if err != nil {
		return err
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			break
		}
		ids = append(ids, id)
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return err
	}
	for _, id := range ids {
		m, e := w.studySnapshot(ctx, id)
		if e != nil {
			return e
		}
		if _, e = w.advanceStudySnapshot(ctx, m, now); e != nil {
			return e
		}
	}
	// Failed builds release their payloads; ready inputs are immutable and the
	// aggregate budget rejects new captures rather than evicting active evidence.
	tx, err := w.research.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, "DELETE FROM study_input_chunks WHERE id IN (SELECT id FROM study_inputs WHERE state='failed')")
	if err != nil {
		return err
	}
	return tx.Commit()
}
