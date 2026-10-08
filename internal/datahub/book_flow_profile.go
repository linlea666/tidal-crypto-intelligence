package datahub

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

const BookFlowRules = "spot-book-flow-v1"
const BookFlowCollection = "btc-spot-priority-v1"
const bookFlowBudget = 32 << 20

func minuteFootID(venue string) string { return ID("footprint", "BTC", venue, "spot") + ".minute-v1" }
func coreBook(d Dataset) bool {
	return d.Kind == "book" && d.Asset == "BTC" && (d.Venue == "Binance" || d.Venue == "OKX")
}
func minuteFoot(d Dataset) bool {
	return d.Kind == "footprint" && strings.HasSuffix(d.ID, ".minute-v1")
}
func coreFiveFoot(d Dataset) bool {
	return d.Kind == "footprint" && d.Asset == "BTC" && d.Market == "spot" && (d.Venue == "Binance" || d.Venue == "OKX") && !minuteFoot(d)
}

// Successful core requests and clean restarts return to the same minute
// phases; failure backoff and global quota cooldown retain precedence.
func bookFlowNext(d Dataset, now time.Time) (time.Time, bool) {
	if d.Collection != BookFlowCollection || (!coreBook(d) && !minuteFoot(d)) {
		return time.Time{}, false
	}
	seconds := 6
	if d.Venue == "OKX" {
		seconds += 26
	}
	if minuteFoot(d) {
		seconds += 13
	}
	next := now.Truncate(time.Minute).Add(time.Duration(seconds) * time.Second)
	if !next.After(now) {
		next = next.Add(time.Minute)
	}
	return next, true
}

// Keep Registry's old identities and strict freshness limits. A slower polling
// cadence is not permission to feed older evidence into existing strategies.
func bookFlowRegistry(mode string) []Dataset {
	out := Registry()
	if mode == "off" {
		return out
	}
	minutes := []Dataset{}
	for i := range out {
		d := &out[i]
		if d.Source != "coinglass" {
			continue
		}
		d.Collection = BookFlowCollection
		switch d.Kind {
		case "book":
			d.Refresh, d.Priority = 300, 2
			if coreBook(*d) {
				d.Refresh, d.Priority = 60, 0
			}
		case "footprint":
			if coreFiveFoot(*d) {
				m := *d
				m.ID, m.Resolution, m.Refresh, m.SoftDeadline, m.TTL, m.Priority, m.Contract = minuteFootID(d.Venue), 60, 60, 90, 180, 0, true
				m.Params = map[string]string{"exchange": d.Venue, "symbol": d.Symbol, "interval": "1m", "limit": "10"}
				minutes = append(minutes, m)
				// The old source remains available until a verified complete
				// minute-derived five-minute interval is actually committed.
			} else {
				d.Refresh, d.Priority = 1800, 3
			}
		case "large":
			d.Refresh, d.Priority = 900, 2
		case "map", "heatmap":
			d.Refresh, d.Priority = 1800, 3
		case "whales":
			d.Refresh, d.Priority = 900, 3
		case "flow":
			if d.Asset == "BTC" && d.Market == "spot" {
				d.Refresh = 120
			}
		}
		d.SoftDeadline = d.Refresh + d.Refresh/2
	}
	return append(out, minutes...)
}

type minuteContract struct {
	Venue       string     `json:"venue"`
	Attempts    int        `json:"attempts"`
	Consecutive int        `json:"consecutive"`
	LastSource  time.Time  `json:"lastSourceAt"`
	LastFetched time.Time  `json:"lastFetchedAt"`
	VerifiedAt  *time.Time `json:"verifiedAt"`
	Cutover     *time.Time `json:"cutoverAt"`
	ActivatedAt *time.Time `json:"activatedAt"`
	Failure     string     `json:"failure,omitempty"`
}

func (w *Warehouse) bookFlowLoad(ctx context.Context, kind, id string, value any) error {
	var raw []byte
	if e := w.shortDB().QueryRowContext(ctx, "SELECT payload FROM book_flow WHERE kind=? AND id=?", kind, id).Scan(&raw); e != nil {
		return e
	}
	return json.Unmarshal(raw, value)
}
func (w *Warehouse) bookFlowSave(ctx context.Context, kind, id string, at time.Time, value any, immutable bool) error {
	if w.bookFlowError != "" {
		return errors.New(w.bookFlowError)
	}
	if w.Status().ResearchPaused || w.Status().Paused {
		return errors.New("研究容量保护，挂单承接未登记")
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(raw) > 1<<20 {
		return errors.New("挂单承接记录工作集超限")
	}
	query := "INSERT INTO book_flow VALUES(?,?,?,?,?) ON CONFLICT(kind,id) DO UPDATE SET payload=excluded.payload"
	if immutable {
		query = "INSERT OR IGNORE INTO book_flow VALUES(?,?,?,?,?)"
	}
	_, err = boundedExec(ctx, w.shortDB(), 1500, query, kind, id, "", at.Unix(), raw)
	return err
}

func (w *Warehouse) initBookFlow() error {
	_, err := w.research.Exec(`CREATE TABLE IF NOT EXISTS book_flow(kind TEXT,id TEXT,parent TEXT,at INTEGER,payload BLOB,PRIMARY KEY(kind,id)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS book_flow_time ON book_flow(kind,at);
CREATE TABLE IF NOT EXISTS book_flow_budget(id INTEGER PRIMARY KEY,used INTEGER NOT NULL);
INSERT OR IGNORE INTO book_flow_budget VALUES(1,0);
CREATE TRIGGER IF NOT EXISTS book_flow_insert BEFORE INSERT ON book_flow WHEN NOT EXISTS(SELECT 1 FROM book_flow WHERE kind=NEW.kind AND id=NEW.id) AND (SELECT used FROM book_flow_budget WHERE id=1)+length(NEW.payload)+256>33554432 BEGIN SELECT RAISE(ABORT,'book flow sub-budget full'); END;
CREATE TRIGGER IF NOT EXISTS book_flow_update BEFORE UPDATE OF payload ON book_flow WHEN (SELECT used FROM book_flow_budget WHERE id=1)+length(NEW.payload)-length(OLD.payload)>33554432 BEGIN SELECT RAISE(ABORT,'book flow sub-budget full'); END;
CREATE TRIGGER IF NOT EXISTS book_flow_added AFTER INSERT ON book_flow BEGIN UPDATE book_flow_budget SET used=used+length(NEW.payload)+256 WHERE id=1; END;
CREATE TRIGGER IF NOT EXISTS book_flow_changed AFTER UPDATE OF payload ON book_flow BEGIN UPDATE book_flow_budget SET used=used+length(NEW.payload)-length(OLD.payload) WHERE id=1; END;
CREATE TRIGGER IF NOT EXISTS book_flow_removed AFTER DELETE ON book_flow BEGIN UPDATE book_flow_budget SET used=used-length(OLD.payload)-256 WHERE id=1; END;`)
	return err
}

// Probe through the same quota gate. Three independent advancing responses and
// at least five contiguous closed minutes are required; a repeated response is
// not another successful live confirmation. Ten failed/nonadvancing attempts
// terminate the probe without disabling the old five-minute source.
func (s *Scheduler) processMinuteContract(ctx context.Context, d Dataset, observations []Observation, fetched time.Time, fetchErr error) (minuteContract, error) {
	c := minuteContract{Venue: d.Venue}
	err := s.store.bookFlowLoad(ctx, "contract", d.ID, &c)
	if err != nil && err != sql.ErrNoRows {
		return c, err
	}
	c.Attempts++
	valid := map[int64]Observation{}
	var latest time.Time
	for _, o := range observations {
		if o.Quality != "valid" || o.Resolution != 60 || o.Time().Unix()%60 != 0 || o.Time().Add(time.Minute).After(fetched) || o.Time().Before(fetched.Add(-11*time.Minute)) {
			continue
		}
		if !validMinuteFoot(o) {
			continue
		}
		o.Revision = digest(o)
		valid[o.Time().Unix()] = o
		latest = maxTime(latest, o.Time())
	}
	complete := !latest.IsZero() && fetched.Sub(latest.Add(time.Minute)) <= 3*time.Minute
	for i := 0; i < 5; i++ {
		if _, ok := valid[latest.Add(-time.Duration(i)*time.Minute).Unix()]; !ok {
			complete = false
		}
	}
	if fetchErr != nil || !complete {
		c.Consecutive = 0
		c.Failure = "一分钟足迹契约或完整窗口未通过"
	} else if latest.After(c.LastSource) && fetched.After(c.LastFetched) {
		c.Consecutive++
		c.LastSource, c.LastFetched = latest, fetched
	}
	if c.VerifiedAt == nil && c.Consecutive >= 3 {
		c.VerifiedAt = &fetched
		cut := fetched.Truncate(5 * time.Minute).Add(5 * time.Minute)
		c.Cutover = &cut
		c.Failure = ""
	}
	if c.VerifiedAt != nil && c.Cutover != nil && fetchErr == nil {
		legacy, _ := FindDataset(ID("footprint", "BTC", d.Venue, "spot"))
		for start := latest.Truncate(5 * time.Minute).Add(-5 * time.Minute); !start.After(latest.Truncate(5 * time.Minute)); start = start.Add(5 * time.Minute) {
			if start.Before(*c.Cutover) {
				continue
			}
			parts := []Observation{}
			for i := 0; i < 5; i++ {
				if o, ok := valid[start.Add(time.Duration(i)*time.Minute).Unix()]; ok {
					parts = append(parts, o)
				}
			}
			if len(parts) != 5 {
				continue
			}
			o := aggregateMinuteFoot(legacy, start, parts, fetched)
			if _, err = s.store.Ingest(legacy, o); err != nil {
				return c, err
			}
			if c.ActivatedAt == nil {
				c.ActivatedAt = &fetched
			}
		}
	}
	err = s.store.bookFlowSave(ctx, "contract", d.ID, fetched, c, false)
	return c, err
}

func validMinuteFoot(o Observation) bool {
	if len(o.Payload.Foot) == 0 {
		return false
	}
	for _, f := range o.Payload.Foot {
		if !dec(f.High).GreaterThan(dec(f.Low)) || dec(f.Low).IsNegative() || f.BuyCount < 0 || f.SellCount < 0 {
			return false
		}
		for _, v := range []string{f.BuyBase, f.SellBase, f.BuyQuote, f.SellQuote, f.BuyUSDT, f.SellUSDT} {
			if dec(v).IsNegative() {
				return false
			}
		}
		for _, p := range [][2]string{{f.BuyBase, f.BuyQuote}, {f.SellBase, f.SellQuote}} {
			q, amount := dec(p[0]), dec(p[1])
			// Spot quote notional must lie within its price cell, allowing
			// only tiny decimal rounding. USDT fields remain a separate unit.
			if amount.LessThan(q.Mul(dec(f.Low)).Mul(dec("0.999999"))) || amount.GreaterThan(q.Mul(dec(f.High)).Mul(dec("1.000001"))) {
				return false
			}
		}
	}
	return true
}

func aggregateMinuteFoot(d Dataset, at time.Time, parts []Observation, fetched time.Time) Observation {
	o := Aggregate(d, parts, at, at.Add(5*time.Minute), 300)
	o.Dataset, d.Source = d.ID, "derived/coinglass-minute-v1"
	o.Source = d.Source
	o.FetchedAt = fetched
	o.Quality = "valid"
	o.Samples = 5
	o.ExpectedSamples = 5
	o.Dependencies = map[string]string{"collection": BookFlowCollection}
	for _, p := range parts {
		o.Dependencies[p.Time().Format(time.RFC3339)] = p.Revision
	}
	return o
}
