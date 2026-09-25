package datahub

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"
	_ "modernc.org/sqlite"
)

const HotLimit = 128 << 20
const ResultLimit = 8 << 20

type StorageStatus struct {
	ResearchBytes  int64     `json:"researchBytes"`
	ResearchPaused bool      `json:"researchPaused"`
	OrderBytes     int64     `json:"orderHistoryBytes"`
	OrderPaused    bool      `json:"orderHistoryPaused"`
	Bytes          int64     `json:"usedBytes"`
	Free           int64     `json:"freeBytes"`
	WhaleBytes     int64     `json:"whaleBytes"`
	Paused         bool      `json:"fineHistoryPaused"`
	Error          string    `json:"error,omitempty"`
	LastCleanup    time.Time `json:"lastCleanup"`
	SQLite         string    `json:"sqlite"`
	HotBytes       int       `json:"hotBytes"`
}
type Warehouse struct {
	research                   *sql.DB
	root                       string
	db                         *sql.DB
	write                      sync.Mutex
	mu                         sync.RWMutex
	latest                     map[string]Observation
	sizes                      map[string]int
	epoch                      uint64
	status                     StorageStatus
	retention, budget, minFree int
}

func database(path string) (*sql.DB, error) {
	db, e := sql.Open("sqlite", path)
	if e != nil {
		return nil, e
	}
	db.SetMaxOpenConns(1)
	_, e = db.Exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA busy_timeout=5000; PRAGMA wal_autocheckpoint=256;`)
	if e != nil {
		db.Close()
		return nil, e
	}
	return db, nil
}
func OpenWarehouse(root string) (*Warehouse, error) {
	if e := os.MkdirAll(root, 0700); e != nil {
		return nil, e
	}
	db, e := database(filepath.Join(root, "hub.sqlite"))
	if e != nil {
		return nil, e
	}
	_, e = db.Exec(`CREATE TABLE IF NOT EXISTS latest(dataset TEXT PRIMARY KEY, ts INTEGER NOT NULL, revision TEXT NOT NULL, payload BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS state(key TEXT PRIMARY KEY, payload BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS dirty(dataset TEXT NOT NULL,ts INTEGER NOT NULL,res INTEGER NOT NULL,PRIMARY KEY(dataset,ts,res)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS progress(dataset TEXT PRIMARY KEY,first_ts INTEGER,last_ts INTEGER,updates INTEGER DEFAULT 0,corrections INTEGER DEFAULT 0);
CREATE TABLE IF NOT EXISTS rollups(dataset TEXT,res INTEGER,through_ts INTEGER,PRIMARY KEY(dataset,res)) WITHOUT ROWID;`)
	if e != nil {
		db.Close()
		return nil, e
	}
	w := &Warehouse{root: root, db: db, latest: map[string]Observation{}, sizes: map[string]int{}, retention: 90, budget: 20, minFree: 8}
	if e = w.initOrders(); e != nil {
		db.Close()
		return nil, e
	}
	if e = w.initResearch(); e != nil {
		db.Close()
		return nil, e
	}
	if e = db.QueryRow("SELECT sqlite_version()").Scan(&w.status.SQLite); e != nil {
		db.Close()
		return nil, e
	}
	rows, e := db.Query("SELECT dataset,payload FROM latest")
	if e != nil {
		db.Close()
		return nil, e
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var b []byte
		if e = rows.Scan(&id, &b); e != nil {
			db.Close()
			return nil, e
		}
		o, e := unpack(b)
		if e != nil {
			db.Close()
			return nil, e
		}
		w.putHot(id, o)
	}
	return w, rows.Err()
}
func (w *Warehouse) Close() error {
	w.write.Lock()
	defer w.write.Unlock()
	if w.research != nil {
		_, _ = w.research.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
		_ = w.research.Close()
	}
	_, _ = w.db.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
	return w.db.Close()
}
func (w *Warehouse) Root() string { return w.root }
func (w *Warehouse) Latest(id string) (Observation, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	o, ok := w.latest[id]
	return o, ok
}
func (w *Warehouse) Epoch() uint64         { w.mu.RLock(); defer w.mu.RUnlock(); return w.epoch }
func (w *Warehouse) Status() StorageStatus { w.mu.RLock(); defer w.mu.RUnlock(); return w.status }
func (w *Warehouse) SetLimits(days, budget, free int) {
	if days != 30 && days != 90 {
		return
	}
	w.mu.Lock()
	w.retention = days
	w.budget = budget
	w.minFree = free
	w.mu.Unlock()
}
func (w *Warehouse) putHot(id string, o Observation) {
	b, _ := json.Marshal(o)
	w.mu.Lock()
	defer w.mu.Unlock()
	size := len(b)
	old := w.sizes[id]
	if w.status.HotBytes-old+size > HotLimit {
		w.status.Error = "热数据缓存达到128MiB上限"
		return
	}
	w.status.HotBytes += size - old
	w.latest[id] = o
	w.sizes[id] = size
	w.epoch++
}
func (w *Warehouse) SaveState(key string, v any) error {
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	_, e = w.db.Exec("INSERT INTO state VALUES(?,?) ON CONFLICT(key) DO UPDATE SET payload=excluded.payload", key, b)
	return e
}
func (w *Warehouse) LoadState(key string, v any) bool {
	var b []byte
	if w.db.QueryRow("SELECT payload FROM state WHERE key=?", key).Scan(&b) != nil {
		return false
	}
	return json.Unmarshal(b, v) == nil
}

// Fixed-size codec pools bound retained buffers while avoiding a new 1.2MiB
// deflater per fact. Buffers returned to callers never belong to a pooled codec.
var encoders = make(chan *gzip.Writer, 2)

type pooledDecoder struct {
	z     *gzip.Reader
	input *bytes.Reader
}

var decoders = make(chan *pooledDecoder, 4)

func pack(o Observation) ([]byte, error) {
	b, e := json.Marshal(o)
	if e != nil {
		return nil, e
	}
	var out bytes.Buffer
	var z *gzip.Writer
	select {
	case z = <-encoders:
		z.Reset(&out)
	default:
		z, _ = gzip.NewWriterLevel(&out, gzip.BestSpeed)
	}
	defer func() {
		z.Reset(io.Discard)
		select {
		case encoders <- z:
		default:
		}
	}()
	if _, e = z.Write(b); e != nil {
		return nil, e
	}
	if e = z.Close(); e != nil {
		return nil, e
	}
	return out.Bytes(), nil
}
func unpack(b []byte) (Observation, error) {
	var o Observation
	var p *pooledDecoder
	var e error
	select {
	case p = <-decoders:
		p.input.Reset(b)
		e = p.z.Reset(p.input)
	default:
		p = &pooledDecoder{input: bytes.NewReader(b)}
		p.z, e = gzip.NewReader(p.input)
	}
	if e != nil {
		return o, e
	}
	defer func() {
		p.z.Close()
		p.input.Reset(nil)
		select {
		case decoders <- p:
		default:
		}
	}()
	e = json.NewDecoder(io.LimitReader(p.z, 16<<20)).Decode(&o)
	return o, e
}
func digest(o Observation) string {
	o.FetchedAt = time.Time{}
	o.FirstFetchedAt = nil
	o.Revision = ""
	b, _ := json.Marshal(o)
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}
func nativeRes(d Dataset) int {
	if d.Kind == "whales" {
		return 300
	}
	if d.Kind == "fx" {
		return 60
	}
	if d.Resolution > 0 {
		return d.Resolution
	}
	return max(60, d.Refresh)
}
func recordTime(o Observation) time.Time {
	if o.WindowStart != nil {
		return *o.WindowStart
	}
	return o.Time()
}
func (w *Warehouse) partition(res int, at time.Time, whales bool) string {
	folder := "history"
	if whales {
		folder = "whales"
	}
	return filepath.Join(w.root, folder, fmt.Sprintf("r%d", res), at.UTC().Format("2006-01-02")+".sqlite")
}
func openPartition(path string) (*sql.DB, error) {
	if e := os.MkdirAll(filepath.Dir(path), 0700); e != nil {
		return nil, e
	}
	db, e := database(path)
	if e != nil {
		return nil, e
	}
	_, e = db.Exec(`CREATE TABLE IF NOT EXISTS records(dataset TEXT,ts INTEGER,revision TEXT,payload BLOB,PRIMARY KEY(dataset,ts)) WITHOUT ROWID;`)
	if e != nil {
		db.Close()
		return nil, e
	}
	return db, nil
}
func historyObservation(d Dataset, o Observation) Observation {
	if d.Kind == "whales" {
		p := o.Payload
		p.Whales = nil
		for _, a := range Assets() {
			var rows []Whale
			for _, r := range o.Payload.Whales {
				if r.Asset == a {
					rows = append(rows, r)
				}
			}
			sort.Slice(rows, func(i, j int) bool { return dec(rows[i].USD).GreaterThan(dec(rows[j].USD)) })
			p.Whales = append(p.Whales, rows[:min(100, len(rows))]...)
		}
		o.Payload = p
	}
	if d.Kind == "heatmap" && o.Payload.Model != nil {
		m := *o.Payload.Model
		m.Cells = nil
		m.Prices = nil
		m.Times = nil
		o.Payload.Model = &m
	}
	return o
}

// Ingest is idempotent by dataset/source-time/content. A successful read of an old
// timestamp updates fetchedAt only; it cannot rejuvenate observedAt.
func (w *Warehouse) Ingest(d Dataset, o Observation) (bool, error) {
	if o.Dataset != d.ID || o.Time().IsZero() || o.Time().After(o.FetchedAt.Add(30*time.Second)) {
		return false, errors.New("invalid observation identity/time")
	}
	o.Revision = digest(o)
	w.write.Lock()
	defer w.write.Unlock()
	if err := w.recordFact(d, &o); err != nil {
		return false, err
	}
	if d.Kind == "large" || d.Kind == "large-history" {
		if err := w.ingestOrders(d, o); err != nil {
			return false, err
		}
	}
	previous, exists := w.Latest(d.ID)
	changed := !exists || previous.Revision != o.Revision
	if !exists || !o.Time().Before(previous.Time()) {
		if d.Kind != "price" {
			b, e := pack(o)
			if e != nil {
				return false, e
			}
			if _, e = w.db.Exec("INSERT INTO latest VALUES(?,?,?,?) ON CONFLICT(dataset) DO UPDATE SET ts=excluded.ts,revision=excluded.revision,payload=excluded.payload", d.ID, o.Time().UnixNano(), o.Revision, b); e != nil {
				return false, e
			}
		}
		w.putHot(d.ID, o)
	}
	if d.Kind == "price" || d.Kind == "wallet" || d.Kind == "large-history" || d.Kind == "large" {
		return changed, nil
	}
	w.mu.RLock()
	paused := w.status.Paused
	days := w.retention
	whaleFull := w.status.WhaleBytes >= 2<<30
	w.mu.RUnlock()
	if paused || (d.Kind == "whales" && whaleFull) {
		return changed, nil
	}
	if d.Contract && o.Time().Before(o.FetchedAt.Add(-time.Duration(days)*24*time.Hour)) {
		return changed, nil
	}
	res := nativeRes(d)
	if o.Resolution > res {
		res = o.Resolution
	} // historical backfills retain their actual resolution
	t := recordTime(o)
	if o.ObservedAt == nil {
		t = t.Truncate(time.Duration(res) * time.Second)
	}
	for _, target := range targets(d) {
		if target <= res {
			continue
		}
		start := t.Truncate(time.Duration(target) * time.Second)
		if _, e := w.db.Exec("INSERT OR IGNORE INTO dirty VALUES(?,?,?)", d.ID, start.Unix(), target); e != nil {
			return false, e
		}
	}
	h := historyObservation(d, o)
	b, e := pack(h)
	if e != nil {
		return false, e
	}
	db, e := openPartition(w.partition(res, t, d.Kind == "whales"))
	if e != nil {
		return false, e
	}
	defer db.Close()
	var old string
	e = db.QueryRow("SELECT revision FROM records WHERE dataset=? AND ts=?", d.ID, t.Unix()).Scan(&old)
	if e != nil && e != sql.ErrNoRows {
		return false, e
	}
	if old == o.Revision {
		return false, nil
	}
	_, e = db.Exec("INSERT INTO records VALUES(?,?,?,?) ON CONFLICT(dataset,ts) DO UPDATE SET revision=excluded.revision,payload=excluded.payload", d.ID, t.Unix(), o.Revision, b)
	if e != nil {
		return false, e
	}
	correction := 0
	if old != "" {
		correction = 1
	}
	_, e = w.db.Exec("INSERT INTO progress VALUES(?,?,?,1,?) ON CONFLICT(dataset) DO UPDATE SET first_ts=min(first_ts,excluded.first_ts),last_ts=max(last_ts,excluded.last_ts),updates=updates+1,corrections=corrections+excluded.corrections", d.ID, t.Unix(), t.Unix(), correction)
	if e != nil {
		return false, e
	}

	return true, nil
}
func targets(d Dataset) []int {
	if d.Resolution >= 86400 || d.Kind == "balance-list" {
		return nil
	}
	if d.Kind == "book" {
		return []int{300, 3600}
	}
	return []int{900, 3600}
}

// Visit uses a separate read connection and decodes one row at a time. No hot-cache
// lock or writer lock is held while doing history IO or decompressing.
func (w *Warehouse) Visit(ctx context.Context, d Dataset, res int, from, to time.Time, fn func(Observation) error) error {
	dir := filepath.Dir(w.partition(res, from, d.Kind == "whales"))
	paths, e := filepath.Glob(filepath.Join(dir, "*.sqlite"))
	if e != nil {
		return e
	}
	sort.Strings(paths)
	for _, path := range paths {
		day, e := time.Parse("2006-01-02", strings.TrimSuffix(filepath.Base(path), ".sqlite"))
		if e != nil || !day.Before(to) || !day.Add(24*time.Hour).After(from) {
			continue
		}
		db, e := sql.Open("sqlite", "file:"+path+"?mode=ro&_pragma=busy_timeout(1500)")
		if e != nil {
			return e
		}
		db.SetMaxOpenConns(1)
		rows, e := db.QueryContext(ctx, "SELECT payload FROM records WHERE dataset=? AND ts>=? AND ts<? ORDER BY ts", d.ID, from.Unix(), to.Unix())
		if e != nil {
			db.Close()
			return e
		}
		for rows.Next() {
			var b []byte
			if e = rows.Scan(&b); e != nil {
				break
			}
			var o Observation
			o, e = unpack(b)
			if e != nil {
				break
			}
			if e = fn(o); e != nil {
				break
			}
		}
		if e == nil {
			e = rows.Err()
		}
		rows.Close()
		db.Close()
		if e != nil {
			return e
		}
	}
	return nil
}

var errPage = errors.New("page complete")

func (w *Warehouse) Query(ctx context.Context, d Dataset, res int, from, to time.Time, limit int) ([]Observation, bool, error) {
	out := []Observation{}
	size := 0
	more := false
	e := w.Visit(ctx, d, res, from, to, func(o Observation) error {
		b, _ := json.Marshal(o)
		if len(out) >= limit || size+len(b) > ResultLimit {
			more = true
			return errPage
		}
		size += len(b)
		out = append(out, o)
		return nil
	})
	if errors.Is(e, errPage) {
		e = nil
	}
	return out, more, e
}
func (w *Warehouse) Progress(id string) map[string]any {
	var first, last, updates, corrections int64
	_ = w.db.QueryRow("SELECT first_ts,last_ts,updates,corrections FROM progress WHERE dataset=?", id).Scan(&first, &last, &updates, &corrections)
	return map[string]any{"first": first, "last": last, "updates": updates, "corrections": corrections}
}
func (w *Warehouse) Rollup(ctx context.Context, registry map[string]Dataset, now time.Time) error {
	rows, e := w.db.QueryContext(ctx, "SELECT dataset,ts,res FROM dirty WHERE ts+res<? ORDER BY ts DESC LIMIT 64", now.Unix())
	if e != nil {
		return e
	}
	type job struct {
		id  string
		ts  int64
		res int
	}
	var jobs []job
	for rows.Next() {
		var j job
		if e = rows.Scan(&j.id, &j.ts, &j.res); e != nil {
			rows.Close()
			return e
		}
		jobs = append(jobs, j)
	}
	rows.Close()
	for _, j := range jobs {
		d, ok := registry[j.id]
		if !ok {
			continue
		}
		if e := func() error {
			w.write.Lock()
			defer w.write.Unlock()
			start := time.Unix(j.ts, 0).UTC()
			end := start.Add(time.Duration(j.res) * time.Second)
			var items []Observation
			if e := w.Visit(ctx, d, nativeRes(d), start, end, func(o Observation) error { items = append(items, o); return nil }); e != nil {
				return e
			}
			if len(items) > 0 {
				o := Aggregate(d, items, start, end, j.res)
				o.Revision = digest(o)
				b, e := pack(o)
				if e != nil {
					return e
				}
				db, e := openPartition(w.partition(j.res, start, d.Kind == "whales"))
				if e != nil {
					return e
				}
				// An authoritative upstream coarse interval cannot be replaced by
				// the incomplete tail of a locally collected fine interval.
				var existing []byte
				if db.QueryRow("SELECT payload FROM records WHERE dataset=? AND ts=?", d.ID, start.Unix()).Scan(&existing) == nil {
					old, err := unpack(existing)
					if err == nil && old.Quality == "valid" && o.Quality == "partial" {
						db.Close()
						_, err = w.db.Exec("DELETE FROM dirty WHERE dataset=? AND ts=? AND res=?", d.ID, j.ts, j.res)
						return err
					}
				}
				_, e = db.Exec("INSERT INTO records VALUES(?,?,?,?) ON CONFLICT(dataset,ts) DO UPDATE SET revision=excluded.revision,payload=excluded.payload", d.ID, start.Unix(), o.Revision, b)
				db.Close()
				if e != nil {
					return e
				}
				if _, e = w.db.Exec("INSERT INTO rollups VALUES(?,?,?) ON CONFLICT(dataset,res) DO UPDATE SET through_ts=max(through_ts,excluded.through_ts)", d.ID, j.res, end.Unix()); e != nil {
					return e
				}
			}
			_, e := w.db.Exec("DELETE FROM dirty WHERE dataset=? AND ts=? AND res=?", d.ID, j.ts, j.res)
			return e
		}(); e != nil {
			return e
		}
	}

	return nil
}

// Aggregate distinguishes flows from stocks. Even coarse stock rows retain their
// original observation time and effective sample count.
func Aggregate(d Dataset, items []Observation, start, end time.Time, res int) Observation {
	sort.Slice(items, func(i, j int) bool { return items[i].Time().Before(items[j].Time()) })
	last := items[len(items)-1]
	o := last
	o.WindowStart = &start
	o.WindowEnd = &end
	o.Resolution = res
	o.Samples = 0
	o.ExpectedSamples = max(1, res/nativeRes(d))
	o.Payload = Payload{}
	for _, r := range items {
		if r.Quality != "missing" {
			o.Samples++
		}
	}
	if o.Samples == 0 {
		o.Quality = "missing"
		return o
	}
	switch d.Kind {
	case "flow":
		f := Flow{"0", "0"}
		for _, r := range items {
			if r.Payload.Flow != nil {
				f.Buy = dec(f.Buy).Add(dec(r.Payload.Flow.Buy)).String()
				f.Sell = dec(f.Sell).Add(dec(r.Payload.Flow.Sell)).String()
			}
		}
		o.Payload.Flow = &f
	case "liquidations":
		f := Liquidation{"0", "0"}
		for _, r := range items {
			if r.Payload.Liquidation != nil {
				f.Long = dec(f.Long).Add(dec(r.Payload.Liquidation.Long)).String()
				f.Short = dec(f.Short).Add(dec(r.Payload.Liquidation.Short)).String()
			}
		}
		o.Payload.Liquidation = &f
	case "footprint":
		bins := map[string]Foot{}
		for _, r := range items {
			for _, f := range r.Payload.Foot {
				k := f.Low + ":" + f.High
				p := bins[k]
				p.Low = f.Low
				p.High = f.High
				p.BuyBase = dec(p.BuyBase).Add(dec(f.BuyBase)).String()
				p.SellBase = dec(p.SellBase).Add(dec(f.SellBase)).String()
				p.BuyQuote = dec(p.BuyQuote).Add(dec(f.BuyQuote)).String()
				p.SellQuote = dec(p.SellQuote).Add(dec(f.SellQuote)).String()
				p.BuyUSDT = dec(p.BuyUSDT).Add(dec(f.BuyUSDT)).String()
				p.SellUSDT = dec(p.SellUSDT).Add(dec(f.SellUSDT)).String()
				p.BuyCount += f.BuyCount
				p.SellCount += f.SellCount
				bins[k] = p
			}
		}
		for _, p := range bins {
			o.Payload.Foot = append(o.Payload.Foot, p)
		}
		sort.Slice(o.Payload.Foot, func(i, j int) bool { return num(o.Payload.Foot[i].Low) < num(o.Payload.Foot[j].Low) })
	case "candles":
		var c *Candle
		for _, r := range items {
			p := r.Payload.Candle
			if p == nil {
				continue
			}
			if c == nil {
				cp := *p
				c = &cp
			} else {
				c.High = max(c.High, p.High)
				c.Low = min(c.Low, p.Low)
				c.Close = p.Close
				c.Volume += p.Volume
			}
		}
		o.Payload.Candle = c
	default:
		o.Payload = last.Payload // OI, books, positions, funding and model intensities are never time-summed.
	}
	if o.Samples < o.ExpectedSamples {
		o.Quality = "partial"
		o.Reason = "汇总包含采样缺口"
	}
	return o
}
func (w *Warehouse) Maintain(ctx context.Context, registry map[string]Dataset, now time.Time) error {
	if e := w.Rollup(ctx, registry, now); e != nil {
		return e
	}
	w.mu.RLock()
	days, budget, free := w.retention, w.budget, w.minFree
	w.mu.RUnlock()
	if err := w.maintainOrders(ctx, now, days); err != nil {
		return err
	}
	if err := w.maintainResearch(ctx, now, days); err != nil {
		return err
	}
	var used, whales int64
	root := filepath.Dir(w.root)
	_ = filepath.WalkDir(root, func(path string, d os.DirEntry, e error) error {
		if e != nil || d.IsDir() {
			return nil
		}
		if st, e := d.Info(); e == nil {
			used += st.Size()
			if strings.Contains(path, string(os.PathSeparator)+"whales"+string(os.PathSeparator)) {
				whales += st.Size()
			}
		}
		return nil
	})
	var fs unix.Statfs_t
	if e := unix.Statfs(root, &fs); e != nil {
		return e
	}
	available := int64(fs.Bavail) * int64(fs.Bsize)
	// The host metrics job records image/log/backup use outside the data directory.
	var external struct {
		Bytes int64 `json:"bytes"`
	}
	if b, e := os.ReadFile(filepath.Join(root, "external-usage.json")); e == nil {
		_ = json.Unmarshal(b, &external)
		used += external.Bytes
	}
	pressure := used >= int64(budget)<<30 || available < int64(free)<<30
	for _, folder := range []string{"history", "whales"} {
		paths, _ := filepath.Glob(filepath.Join(w.root, folder, "r*", "*.sqlite"))
		sort.Strings(paths)
		for _, path := range paths {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			day, e := time.Parse("2006-01-02", strings.TrimSuffix(filepath.Base(path), ".sqlite"))
			if e != nil {
				continue
			}
			res, _ := strconv.Atoi(strings.TrimPrefix(filepath.Base(filepath.Dir(path)), "r"))
			if day.Add(48 * time.Hour).After(now) {
				continue
			}
			if err := w.cleanPartition(ctx, path, day, res, registry, now, days, pressure); err != nil {
				return err
			}
		}
	}
	_, e := w.db.ExecContext(ctx, "PRAGMA wal_checkpoint(PASSIVE)")
	w.mu.Lock()
	w.status.Bytes = used
	w.status.Free = available
	w.status.WhaleBytes = whales
	w.status.Paused = pressure
	w.status.LastCleanup = now
	if e != nil {
		w.status.Error = e.Error()
	}
	w.mu.Unlock()
	return e
}

func retentionDays(d Dataset, res, days int) int {
	if d.Kind == "wallet" {
		return 1
	}
	if res >= 3600 {
		return days
	}
	if d.Kind == "book" && res <= 60 || d.Kind == "footprint" && res <= 300 {
		return 7
	}
	return 30
}

func (w *Warehouse) cleanPartition(ctx context.Context, path string, day time.Time, res int, registry map[string]Dataset, now time.Time, days int, pressure bool) error {
	w.write.Lock()
	defer w.write.Unlock()
	db, err := openPartition(path)
	if err != nil {
		return err
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, "SELECT DISTINCT dataset FROM records")
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	rows.Close()
	deleted := false
	for _, id := range ids {
		d, ok := registry[id]
		if !ok {
			continue
		}
		keep := retentionDays(d, res, days)
		if pressure && res < 3600 {
			keep = min(keep, 1)
		}
		if day.Add(time.Duration(keep+1) * 24 * time.Hour).After(now) {
			continue
		}
		var pending int
		if res < 3600 {
			if err = w.db.QueryRow("SELECT count(*) FROM dirty WHERE dataset=? AND ts>=? AND ts<?", id, day.Unix(), day.Add(24*time.Hour).Unix()).Scan(&pending); err != nil {
				return err
			}
		}
		if pending > 0 {
			continue
		}
		if _, err = db.ExecContext(ctx, "DELETE FROM records WHERE dataset=?", id); err != nil {
			return err
		}
		deleted = true
	}
	if !deleted {
		return nil
	}
	var count int
	if err = db.QueryRow("SELECT count(*) FROM records").Scan(&count); err != nil {
		return err
	}
	if _, err = db.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		return err
	}
	if count == 0 {
		db.Close()
		for _, suffix := range []string{"", "-wal", "-shm"} {
			if e := os.Remove(path + suffix); e != nil && !os.IsNotExist(e) {
				return e
			}
		}
		return nil
	}
	_, err = db.ExecContext(ctx, "VACUUM")
	return err
}
