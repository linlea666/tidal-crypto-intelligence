package tidal

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"
	_ "modernc.org/sqlite"
)

type Settings struct {
	RetentionDays int `json:"retentionDays"`
	BudgetGB      int `json:"budgetGB"`
	MinFreeGB     int `json:"minFreeGB"`
}
type Store struct {
	mu                           sync.Mutex
	Root                         string
	Meta                         *sql.DB
	dbs                          map[string]*sql.DB
	settings                     Settings
	Paused                       bool
	Bytes, FreeBytes, WhaleBytes int64
	LastCleanup                  time.Time
	LastError                    string
}
type Record struct {
	Kind  string          `json:"kind"`
	Asset string          `json:"asset"`
	TS    int64           `json:"ts"`
	Data  json.RawMessage `json:"data"`
}
type RetentionReport struct {
	UsedBytes   int64     `json:"usedBytes"`
	FreeBytes   int64     `json:"freeBytes"`
	WhaleBytes  int64     `json:"whaleBytes"`
	Paused      bool      `json:"fineHistoryPaused"`
	Settings    Settings  `json:"settings"`
	LastCleanup time.Time `json:"lastCleanup"`
	Error       string    `json:"error,omitempty"`
}

func OpenStore(root string) (*Store, error) {
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	meta, err := openDB(filepath.Join(root, "state.sqlite"))
	if err != nil {
		return nil, err
	}
	_, err = meta.Exec(`CREATE TABLE IF NOT EXISTS kv(key TEXT PRIMARY KEY,value TEXT NOT NULL); CREATE TABLE IF NOT EXISTS sessions(token TEXT PRIMARY KEY, expires INTEGER NOT NULL); CREATE TABLE IF NOT EXISTS annotations(id TEXT PRIMARY KEY, asset TEXT NOT NULL, payload TEXT NOT NULL, updated INTEGER NOT NULL);`)
	if err != nil {
		return nil, err
	}
	s := &Store{Root: root, Meta: meta, dbs: map[string]*sql.DB{}, settings: Settings{90, 20, 8}}
	if v, ok := s.Get("settings"); ok {
		json.Unmarshal([]byte(v), &s.settings)
	}
	return s, nil
}
func openDB(name string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", name)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA busy_timeout=5000; PRAGMA wal_autocheckpoint=256;`)
	if err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}
func (s *Store) Get(key string) (string, bool) {
	var v string
	err := s.Meta.QueryRow("SELECT value FROM kv WHERE key=?", key).Scan(&v)
	return v, err == nil
}
func (s *Store) Put(key string, value any) error {
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = s.Meta.Exec("INSERT INTO kv(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", key, string(b))
	return err
}
func (s *Store) Settings() Settings { s.mu.Lock(); defer s.mu.Unlock(); return s.settings }
func (s *Store) SetSettings(set Settings) error {
	if set.RetentionDays != 30 && set.RetentionDays != 90 {
		return fmt.Errorf("保留期只能为30或90天")
	}
	if set.BudgetGB < 2 || set.BudgetGB > 20 || set.MinFreeGB < 8 || set.MinFreeGB > 20 {
		return fmt.Errorf("总预算2–20GB，最少保留8GB空闲空间")
	}
	s.mu.Lock()
	s.settings = set
	s.mu.Unlock()
	return s.Put("settings", set)
}
func (s *Store) dbLocked(res string, day time.Time) (*sql.DB, error) {
	key := filepath.Join(s.Root, "history", res, day.UTC().Format("2006-01-02")+".sqlite")
	if db := s.dbs[key]; db != nil {
		return db, nil
	}
	if err := os.MkdirAll(filepath.Dir(key), 0700); err != nil {
		return nil, err
	}
	db, err := openDB(key)
	if err != nil {
		return nil, err
	}
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS records(kind TEXT NOT NULL, asset TEXT NOT NULL, ts INTEGER NOT NULL, payload BLOB NOT NULL, PRIMARY KEY(kind,asset,ts)) WITHOUT ROWID;`)
	if err != nil {
		db.Close()
		return nil, err
	}
	s.dbs[key] = db
	return db, nil
}
func encode(value any) ([]byte, error) {
	b, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var out bytes.Buffer
	z, _ := gzip.NewWriterLevel(&out, gzip.BestSpeed)
	if _, err = z.Write(b); err != nil {
		return nil, err
	}
	if err = z.Close(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}
func decode(b []byte) ([]byte, error) {
	z, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	defer z.Close()
	return io.ReadAll(io.LimitReader(z, 32<<20))
}
func (s *Store) Write(res, kind, asset string, t time.Time, value any) error {
	payload, err := encode(value)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if strings.HasPrefix(kind, "whale") && s.WhaleBytes >= 2<<30 {
		return nil
	}
	if s.Paused && res != "15m" {
		return nil
	}
	db, err := s.dbLocked(storageRes(res, kind), t)
	if err != nil {
		return err
	}
	_, err = db.Exec("INSERT INTO records(kind,asset,ts,payload) VALUES(?,?,?,?) ON CONFLICT(kind,asset,ts) DO UPDATE SET payload=excluded.payload", kind, asset, t.Unix(), payload)
	if err != nil {
		s.LastError = err.Error()
	}
	return err
}
func (s *Store) Query(res, kind, asset string, from, to time.Time, maxRows int) ([]Record, error) {
	if maxRows < 1 || maxRows > 20000 {
		maxRows = 20000
	}
	out := []Record{}
	paths, err := filepath.Glob(filepath.Join(s.Root, "history", storageRes(res, kind), "*.sqlite"))
	if err != nil {
		return out, err
	}
	sort.Strings(paths)
	for _, p := range paths {
		day, err := time.Parse("2006-01-02", strings.TrimSuffix(filepath.Base(p), ".sqlite"))
		if err != nil || day.After(to) || day.Add(24*time.Hour).Before(from) {
			continue
		}
		s.mu.Lock()
		db := s.dbs[p]
		own := false
		if db == nil {
			db, err = sql.Open("sqlite", "file:"+p+"?mode=ro&_pragma=busy_timeout(5000)")
			own = true
		}
		if err != nil {
			s.mu.Unlock()
			return out, err
		}
		rows, err := db.Query("SELECT ts,payload FROM records WHERE kind=? AND asset=? AND ts>=? AND ts<=? ORDER BY ts LIMIT ?", kind, asset, from.Unix(), to.Unix(), maxRows-len(out))
		if err != nil {
			if own {
				db.Close()
			}
			s.mu.Unlock()
			return out, err
		}
		for rows.Next() {
			var ts int64
			var b []byte
			if err = rows.Scan(&ts, &b); err != nil {
				break
			}
			raw, er := decode(b)
			if er != nil {
				err = er
				break
			}
			out = append(out, Record{kind, asset, ts, raw})
		}
		rowErr := rows.Err()
		rows.Close()
		if own {
			db.Close()
		}
		s.mu.Unlock()
		if err != nil {
			return out, err
		}
		if rowErr != nil {
			return out, rowErr
		}
		if len(out) >= maxRows {
			break
		}
	}
	return out, nil
}
func (s *Store) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, db := range s.dbs {
		db.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
		db.Close()
	}
	s.Meta.Close()
}
func (s *Store) Report() RetentionReport {
	s.mu.Lock()
	defer s.mu.Unlock()
	return RetentionReport{s.Bytes, s.FreeBytes, s.WhaleBytes, s.Paused, s.settings, s.LastCleanup, s.LastError}
}
func covered(f Frame, price float64, side string) bool {
	for _, c := range f.Coverage {
		if !c.Valid {
			continue
		}
		if side == "bid" && price >= c.BidLow && price <= c.Bid || side == "ask" && price >= c.Ask && price <= c.AskHigh {
			return true
		}
	}
	return false
}
func RollFrames(frames []Frame, at time.Time) Frame {
	if len(frames) == 0 {
		return Frame{At: at, Zones: []Zone{}}
	}
	out := frames[len(frames)-1]
	out.At = at
	type total struct {
		z       Zone
		sum     int64
		count   int64
		sources map[string]int64
	}
	ts := map[string]*total{}
	for _, f := range frames {
		for _, z := range f.Zones {
			k := fmt.Sprintf("%s/%.4f", z.Side, z.Price)
			t := ts[k]
			if t == nil {
				t = &total{z: z, sources: map[string]int64{}}
				ts[k] = t
			}
			t.sum += z.USDCents
			for v, n := range z.Sources {
				t.sources[v] += n
			}
		}
	}
	out.Zones = []Zone{}
	for _, t := range ts {
		for _, f := range frames {
			if coveredRange(f, t.z.Price, t.z.Step, t.z.Side) {
				t.count++
			}
		}
		if t.count == 0 {
			continue
		}
		t.z.USDCents = t.sum / t.count
		t.z.Sources = map[string]int64{}
		for v, n := range t.sources {
			t.z.Sources[v] = n / t.count
		}
		t.z.Samples = t.count
		t.z.Evidence = "区间采样均值"
		out.Zones = append(out.Zones, t.z)
	}
	first := frames[0].Candle
	out.Candle = Candle{at.Unix(), first.Open, first.High, first.Low, out.Candle.Close}
	for _, f := range frames {
		out.Candle.High = math.Max(out.Candle.High, f.Candle.High)
		if f.Candle.Low > 0 && (out.Candle.Low == 0 || f.Candle.Low < out.Candle.Low) {
			out.Candle.Low = f.Candle.Low
		}
	}
	return out
}
func mergeFlows(rows []Flow, ts int64) []Flow {
	m := map[string]*Flow{}
	for _, f := range rows {
		k := f.Venue + f.Market
		v := m[k]
		if v == nil {
			v = &Flow{Venue: f.Venue, Asset: f.Asset, Market: f.Market, Minute: ts, PriceBins: map[int64][2]int64{}}
			m[k] = v
		}
		v.Partial = v.Partial || f.Partial
		v.BuyCents += f.BuyCents
		v.SellCents += f.SellCents
		v.BaseQty += f.BaseQty
		v.USDQty += f.USDQty
		v.Trades += f.Trades
		for p, b := range f.PriceBins {
			x := v.PriceBins[p]
			x[0] += b[0]
			x[1] += b[1]
			v.PriceBins[p] = x
		}
	}
	out := []Flow{}
	for _, v := range m {
		out = append(out, *v)
	}
	return out
}
func (s *Store) Rollup(from time.Time) error {
	end := from.Add(15*time.Minute - time.Second)
	for _, a := range []string{"BTC", "ETH"} {
		rs, err := s.Query("1m", "frame", a, from, end, 15)
		if err != nil {
			return err
		}
		fs := []Frame{}
		for _, r := range rs {
			var f Frame
			if err = json.Unmarshal(r.Data, &f); err != nil {
				return err
			}
			fs = append(fs, f)
		}
		if len(fs) > 0 {
			if err = s.Write("15m", "candle", a, from, RollFrames(fs, from).Candle); err != nil {
				return err
			}
			if err = s.Write("15m", "frame", a, from, RollFrames(fs, from)); err != nil {
				return err
			}
		}
		for _, kind := range []string{"flow:spot", "flow:perp"} {
			rs, err = s.Query("1m", kind, a, from, end, 15)
			if err != nil {
				return err
			}
			all := []Flow{}
			for _, r := range rs {
				var f []Flow
				if err = json.Unmarshal(r.Data, &f); err != nil {
					return err
				}
				all = append(all, f...)
			}
			if len(all) > 0 {
				if err = s.Write("15m", kind, a, from, mergeFlows(all, from.Unix())); err != nil {
					return err
				}
			}
		}
		for _, kind := range []string{"derivatives", "liquidations", "whale-buckets"} {
			rs, err = s.Query("1m", kind, a, from, end, 15)
			if err != nil {
				return err
			}
			if len(rs) > 0 {
				value := any(json.RawMessage(rs[len(rs)-1].Data))
				if kind == "liquidations" {
					all := []Liquidation{}
					for _, r := range rs {
						var ls []Liquidation
						if err = json.Unmarshal(r.Data, &ls); err != nil {
							return err
						}
						all = append(all, ls...)
					}
					value = all
				}
				if err = s.Write("15m", kind, a, from, value); err != nil {
					return err
				}
			}
		}
	}
	return s.Put("rollupThrough", from.Add(15*time.Minute).Unix())
}
func (s *Store) Cleanup(now time.Time) error {
	settings := s.Settings()
	var marker int64
	if raw, ok := s.Get("rollupThrough"); ok {
		json.Unmarshal([]byte(raw), &marker)
	}
	files, err := filepath.Glob(filepath.Join(s.Root, "history", "*", "*.sqlite"))
	if err != nil {
		return err
	}
	sort.Strings(files)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.LastCleanup = now
	for _, p := range files {
		res := strings.TrimPrefix(filepath.Base(filepath.Dir(p)), "whale-")
		day, er := time.Parse("2006-01-02", strings.TrimSuffix(filepath.Base(p), ".sqlite"))
		if er != nil {
			continue
		}
		days := 30
		if res == "5s" {
			days = 1
		}
		if res == "15m" {
			days = settings.RetentionDays
		}
		if day.Add(24 * time.Hour).Before(now.Add(-time.Duration(days) * 24 * time.Hour)) {
			if res == "1m" && settings.RetentionDays == 90 && day.Add(24*time.Hour).Unix() > marker {
				continue
			}
			if db := s.dbs[p]; db != nil {
				db.Close()
				delete(s.dbs, p)
			}
			for _, suffix := range []string{"", "-wal", "-shm"} {
				if er = os.Remove(p + suffix); er != nil && !os.IsNotExist(er) {
					return er
				}
			}
		}
	}
	for p, db := range s.dbs {
		db.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
		if !strings.Contains(p, now.UTC().Format("2006-01-02")) {
			db.Close()
			delete(s.dbs, p)
		}
	}
	s.measureLocked()
	limit := int64(settings.BudgetGB) << 30
	freeMin := int64(settings.MinFreeGB) << 30
	if s.Bytes > limit*9/10 || s.FreeBytes < freeMin {
		for _, res := range []string{"5s", "whale-5m", "1m", "whale-1m"} {
			paths, _ := filepath.Glob(filepath.Join(s.Root, "history", res, "*.sqlite"))
			sort.Strings(paths)
			for _, p := range paths {
				if strings.Contains(p, now.UTC().Format("2006-01-02")) {
					continue
				}
				day, _ := time.Parse("2006-01-02", strings.TrimSuffix(filepath.Base(p), ".sqlite"))
				if (res == "1m" || res == "whale-1m") && day.Add(24*time.Hour).Unix() > marker {
					continue
				}
				if db := s.dbs[p]; db != nil {
					db.Close()
					delete(s.dbs, p)
				}
				for _, suffix := range []string{"", "-wal", "-shm"} {
					os.Remove(p + suffix)
				}
				s.measureLocked()
				if s.Bytes < limit*8/10 && s.FreeBytes >= freeMin {
					break
				}
			}
			if s.Bytes < limit*8/10 && s.FreeBytes >= freeMin {
				break
			}
		}
	}
	// Whale history has its own bounded partitions; remove oldest completed days first.
	if s.WhaleBytes >= 2<<30 {
		for _, res := range []string{"whale-5m", "whale-1m", "whale-15m"} {
			paths, _ := filepath.Glob(filepath.Join(s.Root, "history", res, "*.sqlite"))
			sort.Strings(paths)
			for _, p := range paths {
				day, _ := time.Parse("2006-01-02", strings.TrimSuffix(filepath.Base(p), ".sqlite"))
				if day.Equal(now.UTC().Truncate(24*time.Hour)) || (res == "whale-1m" && day.Add(24*time.Hour).Unix() > marker) {
					continue
				}
				s.removeLocked(p)
				s.measureLocked()
				if s.WhaleBytes < 1800<<20 {
					break
				}
			}
			if s.WhaleBytes < 1800<<20 {
				break
			}
		}
	}
	s.Paused = s.Bytes > limit*9/10 || s.FreeBytes < freeMin
	s.LastError = ""
	if s.Paused {
		s.LastError = "磁盘预算保护：细粒度历史暂停，实时采集继续"
	}
	return nil
}
func (s *Store) measureLocked() {
	var total int64
	filepath.WalkDir(s.Root, func(p string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			if info, e := d.Info(); e == nil {
				total += info.Size()
			}
		}
		return nil
	})
	var external struct {
		Bytes int64 `json:"bytes"`
	}
	if b, err := os.ReadFile(filepath.Join(s.Root, "external-usage.json")); err == nil {
		json.Unmarshal(b, &external)
	}
	s.Bytes = total + external.Bytes
	var stat unix.Statfs_t
	if unix.Statfs(s.Root, &stat) == nil {
		s.FreeBytes = int64(stat.Bavail) * int64(stat.Bsize)
	}
	s.WhaleBytes = 0
	paths, _ := filepath.Glob(filepath.Join(s.Root, "history", "whale-*", "*.sqlite*"))
	for _, p := range paths {
		if st, err := os.Stat(p); err == nil {
			s.WhaleBytes += st.Size()
		}
	}
}

// Initialize runs before any collector goroutine and restores durable observation boundaries.
func (s *Store) Initialize(e *Engine, w *Whales) error {
	e.store = s
	for _, a := range []string{"BTC", "ETH"} {
		for _, market := range []string{"spot", "perp"} {
			rs, err := s.Query("1m", "flow:"+market, a, time.Now().Add(-2*time.Minute), time.Now(), 3)
			if err != nil {
				return err
			}
			for _, r := range rs {
				var fs []Flow
				if err = json.Unmarshal(r.Data, &fs); err != nil {
					return err
				}
				for _, f := range fs {
					cp := f
					cp.Partial = true
					e.flows[fmt.Sprintf("%s/%s/%s/%d", f.Venue, a, market, f.Minute)] = &cp
				}
			}
		}
	}
	if raw, ok := s.Get("startedAt"); ok {
		if err := json.Unmarshal([]byte(raw), &e.Started); err != nil {
			return err
		}
	} else if err := s.Put("startedAt", e.Started); err != nil {
		return err
	}
	if raw, ok := s.Get("whalePins"); ok {
		var pins []string
		json.Unmarshal([]byte(raw), &pins)
		for _, a := range pins {
			w.Add(a, 1e12, true)
		}
	}
	if _, ok := s.Get("rollupThrough"); !ok {
		if err := s.Put("rollupThrough", e.Started.Truncate(15*time.Minute).Unix()); err != nil {
			return err
		}
	}
	return s.Cleanup(time.Now())
}
func (s *Store) Run(ctx context.Context, e *Engine, w *Whales) {
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-tick.C:
			var writeErr error
			write := func(res, kind, a string, t time.Time, v any) {
				if err := s.Write(res, kind, a, t, v); err != nil {
					writeErr = err
				}
			}
			for _, a := range []string{"BTC", "ETH"} {
				f := e.Frame(a)
				if f.Price > 0 {
					lite := f
					lite.Zones = append([]Zone(nil), f.Zones...)
					for i := range lite.Zones {
						lite.Zones[i].Sources = nil
					}
					write("5s", "frame", a, now.UTC().Truncate(5*time.Second), lite)
					write("1m", "frame", a, now.UTC().Truncate(time.Minute), f)
					write("1m", "candle", a, now.UTC().Truncate(time.Minute), f.Candle)
				}
				for _, market := range []string{"spot", "perp"} {
					flows := e.Flows(a, market, now.Add(-2*time.Minute).Truncate(time.Minute))
					for _, t := range []time.Time{now.Truncate(time.Minute), now.Truncate(time.Minute).Add(-time.Minute)} {
						selected := []Flow{}
						for _, fl := range flows {
							if fl.Minute == t.Unix() {
								selected = append(selected, fl)
							}
						}
						write("1m", "flow:"+market, a, t, selected)
					}
				}
				write("1m", "derivatives", a, now.Truncate(time.Minute), e.Derivatives(a))
				for _, t := range []time.Time{now.Truncate(time.Minute), now.Truncate(time.Minute).Add(-time.Minute)} {
					ls := []Liquidation{}
					for _, l := range e.Liquidations(a) {
						if l.At.Truncate(time.Minute).Equal(t) {
							ls = append(ls, l)
						}
					}
					write("1m", "liquidations", a, t, ls)
				}
				ws := e.Whales(a)
				write("5m", "whales", a, now.Truncate(5*time.Minute), ws)
				write("1m", "whale-buckets", a, now.Truncate(time.Minute), WhaleBuckets(ws, Step(a), now))
				write("1m", "health", a, now.Truncate(time.Minute), e.Health())
				if s.Report().Paused {
					slot := now.Truncate(15 * time.Minute)
					f.At = now
					f.Zones = append([]Zone(nil), f.Zones...)
					for n := range f.Zones {
						f.Zones[n].Evidence = "磁盘保护期间的粗采样"
					}
					write("15m", "frame", a, slot, f)
					for _, m := range []string{"spot", "perp"} {
						write("15m", "flow:"+m, a, slot, mergeFlows(e.Flows(a, m, slot), slot.Unix()))
					}
				}

			}
			// A minute of lateness lets trade backfills finish before closing a quarter hour.
			var through int64
			if raw, ok := s.Get("rollupThrough"); ok {
				json.Unmarshal([]byte(raw), &through)
			}
			for n := 0; n < 8 && through > 0 && through+900 <= now.Add(-time.Minute).Unix(); n++ {
				if err := s.Rollup(time.Unix(through, 0)); err != nil {
					e.SetHealth("storage:rollup", false, err.Error())
					break
				}
				through += 900
				e.SetHealth("storage:rollup", true, "15分钟汇总已提交，重启后自动续算")
			}
			if now.Sub(s.Report().LastCleanup) >= 5*time.Minute {
				if err := s.Cleanup(now); err != nil {
					writeErr = err
				}
			}
			rep := s.Report()
			detail := rep.Error
			if writeErr != nil {
				detail = writeErr.Error()
			}
			e.SetHealth("storage", !rep.Paused && writeErr == nil, detail)
		}
	}
}
func storageRes(res, kind string) string {
	if strings.HasPrefix(kind, "whale") {
		return "whale-" + res
	}
	return res
}
func (s *Store) removeLocked(p string) {
	if db := s.dbs[p]; db != nil {
		db.Close()
		delete(s.dbs, p)
	}
	for _, suffix := range []string{"", "-wal", "-shm"} {
		os.Remove(p + suffix)
	}
}
func coveredRange(f Frame, price, step float64, side string) bool {
	for _, z := range f.Zones {
		if z.Price == price && z.Side == side {
			return true
		}
	}
	return covered(f, price, side) && covered(f, price+step-1e-7, side)
}

type WhaleBucket struct {
	Price        float64 `json:"price"`
	Side         string  `json:"side"`
	USDCents     int64   `json:"usdCents"`
	Addresses    int     `json:"addresses"`
	LargestShare float64 `json:"largestShare"`
	Kind         string  `json:"kind"`
}

func WhaleBuckets(ws []Whale, step float64, now time.Time) []WhaleBucket {
	m := map[string]*WhaleBucket{}
	largest := map[string]int64{}
	for _, w := range ws {
		if !w.Valid || now.Sub(w.At) > 90*time.Second {
			continue
		}
		for _, kind := range []string{"entry", "liquidation"} {
			p := number(w.Entry)
			if kind == "liquidation" {
				if w.Liquidation == nil {
					continue
				}
				p = number(*w.Liquidation)
			}
			p *= number(w.Rate)
			if p <= 0 {
				continue
			}
			p = math.Floor(p/step) * step
			k := fmt.Sprintf("%s/%s/%.4f", kind, w.Side, p)
			b := m[k]
			if b == nil {
				b = &WhaleBucket{Price: p, Side: w.Side, Kind: kind}
				m[k] = b
			}
			b.USDCents += w.USDCents
			b.Addresses++
			largest[k] = max(largest[k], w.USDCents)
		}
	}
	out := []WhaleBucket{}
	for k, b := range m {
		b.LargestShare = float64(largest[k]) / float64(max(1, b.USDCents))
		out = append(out, *b)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Price > out[j].Price })
	return out
}
