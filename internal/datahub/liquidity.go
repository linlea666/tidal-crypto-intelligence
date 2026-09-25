package datahub

// Ordinary-book observations deliberately have no dependency on large orders.
// Comparison buckets are fixed in each market's native quote currency: changing
// FX cannot create an apparent withdrawal. USD bounds are display coordinates.
import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"time"
)

type liquidityBucket struct {
	Side                  string `json:"side"`
	Low, Step             float64
	Quantity, QuoteAmount string
}
type liquiditySnapshot struct {
	At                  *time.Time
	Fetched             time.Time
	Revision            string
	Rate                string
	Low, High, Bid, Ask float64
	Buckets             map[string]liquidityBucket
}
type LiquidityPolicy struct{ Near, Decrease float64 }

type LiquidityEvent struct {
	Distance                float64    `json:"referenceDistancePercent"`
	Key                     string     `json:"key"`
	Dataset                 string     `json:"dataset"`
	Venue                   string     `json:"venue"`
	Quote                   string     `json:"quote"`
	Side                    string     `json:"side"`
	Low                     float64    `json:"quoteLow"`
	Step                    float64    `json:"step"`
	DisplayLow              float64    `json:"displayLow"`
	DisplayHigh             float64    `json:"displayHigh"`
	From                    *time.Time `json:"from"`
	At                      time.Time  `json:"at"`
	KnownAt                 time.Time  `json:"knownAt"`
	Kind                    string     `json:"kind"`
	Before, After           *string
	BeforeQuote, AfterQuote *string
	Decrease                *float64   `json:"decreasePercent"`
	Near                    bool       `json:"nearReference"`
	Note                    string     `json:"note"`
	TradeNote               string     `json:"tradeNote,omitempty"`
	TradeKnownAt            *time.Time `json:"tradeKnownAt,omitempty"`
}

func (w *Warehouse) initLiquidity() error {
	_, e := w.db.Exec(`CREATE TABLE IF NOT EXISTS liquidity_events(k TEXT PRIMARY KEY,asset TEXT,dataset TEXT,ts INTEGER,payload BLOB);
CREATE INDEX IF NOT EXISTS liquidity_window ON liquidity_events(asset,ts);
CREATE INDEX IF NOT EXISTS liquidity_source ON liquidity_events(dataset,ts);
CREATE TRIGGER IF NOT EXISTS liquidity_size_i AFTER INSERT ON liquidity_events BEGIN UPDATE order_storage SET bytes=bytes+length(NEW.payload)+length(NEW.k)+128 WHERE id=1; END;
CREATE TRIGGER IF NOT EXISTS liquidity_size_u AFTER UPDATE ON liquidity_events BEGIN UPDATE order_storage SET bytes=bytes+length(NEW.payload)-length(OLD.payload) WHERE id=1; END;
CREATE TRIGGER IF NOT EXISTS liquidity_size_d AFTER DELETE ON liquidity_events BEGIN UPDATE order_storage SET bytes=bytes-length(OLD.payload)-length(OLD.k)-128 WHERE id=1; END;`)
	return e
}

func nativeLiquidity(o Observation, d Dataset, rate string) liquiditySnapshot {
	s := liquiditySnapshot{At: o.ObservedAt, Fetched: o.FetchedAt, Revision: o.Revision, Rate: rate, Buckets: map[string]liquidityBucket{}}
	b := o.Payload.Book
	if b == nil {
		return s
	}
	s.Low, s.High = b.Low, b.High
	s.Bid, s.Ask = best(b)
	for side, levels := range map[string][]Level{"bid": b.Bids, "ask": b.Asks} {
		for _, l := range levels {
			for _, step := range steps(d.Asset) {
				low := math.Floor(num(l.Price)/step) * step
				if low <= 0 {
					continue
				}
				key := fmt.Sprintf("%s/%.0f/%.0f", side, step, low)
				v := s.Buckets[key]
				v.Side, v.Low, v.Step = side, low, step
				v.Quantity = dec(v.Quantity).Add(dec(l.Quantity)).String()
				v.QuoteAmount = dec(v.QuoteAmount).Add(dec(l.Price).Mul(dec(l.Quantity))).String()
				s.Buckets[key] = v
			}
		}
	}
	return s
}

func compareLiquidity(d Dataset, old, next liquiditySnapshot, reference float64, referenceOK bool) []LiquidityEvent {
	out := []LiquidityEvent{}
	for key, b := range old.Buckets {
		if next.Rate == "" || !referenceOK || reference <= 0 {
			continue
		}
		low, high := b.Low*num(next.Rate), (b.Low+b.Step)*num(next.Rate)
		distance := math.Min(math.Abs(low/reference-1), math.Abs(high/reference-1)) * 100
		if low <= reference && high >= reference {
			distance = 0
		}
		if distance > 10 {
			continue
		}
		e := LiquidityEvent{Key: d.ID + "/" + old.Revision + "/" + next.Revision + "/" + key, Dataset: d.ID, Venue: d.Venue, Quote: d.Quote, Side: b.Side, Low: b.Low, Step: b.Step, DisplayLow: low, DisplayHigh: high, From: old.At, At: next.Fetched, KnownAt: next.Fetched, Near: distance <= .3, Distance: distance, Kind: "uncomparable", Note: "来源时间或覆盖不可比，暂停减量判断"}
		if next.At != nil {
			e.At = *next.At
		}
		comparable := old.At != nil && next.At != nil && next.At.After(*old.At) && next.At.Sub(*old.At) <= 5*time.Minute
		within := (b.Side == "bid" && b.Low >= next.Low && b.Low+b.Step <= next.Bid+b.Step) || (b.Side == "ask" && b.Low+b.Step <= next.High+b.Step && b.Low >= next.Ask-b.Step)
		if comparable && within {
			v, exists := next.Buckets[key]
			if !exists {
				e.Kind = "unreturned"
				e.Note = "本次未再返回该原始报价区间；原因待确认，不补零"
			} else {
				e.Before, e.After = &b.Quantity, &v.Quantity
				e.BeforeQuote, e.AfterQuote = &b.QuoteAmount, &v.QuoteAmount
				if dec(b.Quantity).IsPositive() {
					n := dec(b.Quantity).Sub(dec(v.Quantity)).Div(dec(b.Quantity)).Mul(dec("100")).InexactFloat64()
					e.Decrease = &n
				}
				e.Kind = "observed"
				e.Note = "两端采样均有挂单，不能证明采样间一直存在"
				if e.Decrease != nil && *e.Decrease > 0 {
					e.Kind = "decrease"
					e.Note = "采样间原币数量减少；成交、改单、补量及撤销无法由快照区分"
				}
			}
		}
		// Keep nearby observations and substantial changes; this is display-only.
		if distance <= 1 || (e.Decrease != nil && *e.Decrease >= 25) {
			out = append(out, e)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

// Called by maintenance, never by a GET. Only changed source facts are compared.
func (h *Hub) SampleLiquidity(ctx context.Context, now time.Time) error {
	for _, d := range Registry() {
		if d.Kind != "book" {
			continue
		}
		o, ok := h.Store.Latest(d.ID)
		if !ok || !o.Fresh(d, now) || o.Payload.Book == nil || o.Quality != "valid" {
			continue
		}
		var old liquiditySnapshot
		stateKey := "liquidity.previous/" + d.ID
		h.Store.LoadState(stateKey, &old)
		if old.Revision == o.Revision {
			continue
		}
		if old.At != nil && o.ObservedAt != nil && o.ObservedAt.Before(*old.At) {
			continue
		}
		rate, _, rateOK := h.Rate(d.Quote, now)
		if !rateOK {
			rate = ""
		}
		next := nativeLiquidity(o, d, rate)
		price, _, priceOK := h.CurrentPrice(d.Asset, now)
		events := compareLiquidity(d, old, next, price, priceOK)
		h.Store.write.Lock()
		err := func() error {
			tx, e := h.Store.db.BeginTx(ctx, nil)
			if e != nil {
				return e
			}
			defer tx.Rollback()
			if old.At != nil && next.At != nil && old.At.Equal(*next.At) {
				// Same-time correction invalidates the former change; it is not a
				// new independent reduction observation.
				if _, e = tx.ExecContext(ctx, "DELETE FROM liquidity_events WHERE dataset=? AND ts=?", d.ID, next.At.Unix()); e != nil {
					return e
				}
				events = nil
			}
			var used int64
			if e = tx.QueryRowContext(ctx, "SELECT bytes FROM order_storage WHERE id=1").Scan(&used); e != nil {
				return e
			}
			paused := h.Store.Status().Paused || used >= OrderHistoryLimit
			for _, event := range events {
				if paused {
					break
				}
				b, e := json.Marshal(event)
				if e != nil {
					return e
				}
				if used+int64(len(b)+len(event.Key)+128) > OrderHistoryLimit {
					paused = true
					break
				}
				r, e := tx.ExecContext(ctx, "INSERT OR IGNORE INTO liquidity_events VALUES(?,?,?,?,?)", event.Key, d.Asset, d.ID, event.At.Unix(), b)
				if e != nil {
					return e
				}
				n, _ := r.RowsAffected()
				used += n * int64(len(b)+len(event.Key)+128)
			}
			if paused {
				b, _ := json.Marshal(now)
				if _, e = tx.ExecContext(ctx, "INSERT INTO state VALUES('liquidityGapAt',?) ON CONFLICT(key) DO UPDATE SET payload=excluded.payload", b); e != nil {
					return e
				}
			}
			b, e := json.Marshal(next)
			if e != nil {
				return e
			}
			if _, e = tx.ExecContext(ctx, "INSERT INTO state VALUES(?,?) ON CONFLICT(key) DO UPDATE SET payload=excluded.payload", stateKey, b); e != nil {
				return e
			}
			if len(events) > 0 {
				if _, e = tx.ExecContext(ctx, "DELETE FROM state WHERE key=?", "liquidity.foot/"+d.Asset); e != nil {
					return e
				}
			}
			return tx.Commit()
		}()
		h.Store.write.Unlock()
		if err != nil {
			return err
		}
	}
	return h.refreshLiquidityEvidence(ctx, now)
}

func (h *Hub) LiquidityEvents(ctx context.Context, asset string, step float64, from, to time.Time, limit int) ([]LiquidityEvent, error) {
	rows, e := h.Store.db.QueryContext(ctx, `SELECT payload FROM liquidity_events WHERE asset=? AND ts>=? AND ts<? AND json_extract(payload,'$.step')=? ORDER BY ts DESC,k LIMIT ?`, asset, from.Unix(), to.Unix(), step, limit)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []LiquidityEvent{}
	for rows.Next() {
		var b []byte
		var event LiquidityEvent
		if e = rows.Scan(&b); e != nil {
			return nil, e
		}
		if e = json.Unmarshal(b, &event); e != nil {
			return nil, e
		}
		out = append(out, event)
	}
	return out, rows.Err()
}

func (h *Hub) refreshLiquidityEvidence(ctx context.Context, now time.Time) error {
	for _, a := range Assets() {
		signature := ""
		feet := map[string][]Observation{}
		for _, venue := range []string{"Binance", "OKX"} {
			d, _ := h.Dataset(ID("footprint", a, venue, "spot"))
			if o, ok := h.Store.Latest(d.ID); ok {
				signature += d.ID + o.Revision
			}
		}
		var last string
		stateKey := "liquidity.foot/" + a
		if h.Store.LoadState(stateKey, &last) && last == signature {
			continue
		}
		for _, venue := range []string{"Binance", "OKX"} {
			d, _ := h.Dataset(ID("footprint", a, venue, "spot"))
			if err := h.Store.Visit(ctx, d, 300, now.Add(-40*time.Minute), now, func(o Observation) error { feet[venue] = append(feet[venue], o); return nil }); err != nil {
				return err
			}
		}
		for _, step := range steps(a) {
			events, err := h.LiquidityEvents(ctx, a, step, now.Add(-30*time.Minute), now, 300)
			if err != nil {
				return err
			}
			for _, event := range events {
				if event.From == nil || event.Kind != "decrease" {
					continue
				}
				event.TradeNote = ""
				event.TradeKnownAt = nil
				for _, o := range feet[event.Venue] {
					if o.ObservedAt == nil || !o.ObservedAt.Before(event.At) || !o.ObservedAt.Add(5*time.Minute).After(*event.From) {
						continue
					}
					for _, f := range o.Payload.Foot {
						if num(f.High) <= event.Low || num(f.Low) >= event.Low+event.Step {
							continue
						}
						qty := f.BuyBase
						if event.Side == "bid" {
							qty = f.SellBase
						}
						if !dec(qty).IsPositive() {
							continue
						}
						event.TradeNote = "重叠的5分钟足迹有反向主动成交；时间／价位仅部分重叠，不能归因全部减量"
						if o.FetchedAt.After(event.KnownAt) {
							event.TradeNote = "此后补到：" + event.TradeNote
						}
						at := o.FetchedAt
						event.TradeKnownAt = &at
						break
					}
				}
				b, err := json.Marshal(event)
				if err != nil {
					return err
				}
				r, err := h.Store.db.ExecContext(ctx, `UPDATE liquidity_events SET payload=? WHERE k=? AND
(SELECT bytes FROM order_storage WHERE id=1)+?-length(payload)<=?`, b, event.Key, len(b), OrderHistoryLimit)
				if err != nil {
					return err
				}
				if n, _ := r.RowsAffected(); n == 0 {
					if err := h.Store.SaveState("liquidityGapAt", now); err != nil {
						return err
					}
				}
			}
		}
		if err := h.Store.SaveState(stateKey, signature); err != nil {
			return err
		}
	}
	return nil
}
