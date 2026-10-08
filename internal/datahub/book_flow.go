package datahub

import (
	"fmt"
	"sort"
	"time"

	"github.com/shopspring/decimal"
)

type bookZone struct {
	Side        string              `json:"side"`
	Low         string              `json:"low"`
	High        string              `json:"high"`
	Quantity    *string             `json:"quantity"`
	Notional    *string             `json:"notionalQuote"`
	Levels      []Level             `json:"levels,omitempty"`
	Median      *string             `json:"baselineMedian"`
	Multiple    *string             `json:"multiple"`
	IncreaseUSD *string             `json:"increaseUsd"`
	Coverage    float64             `json:"baselineCoverage"`
	Enhanced    bool                `json:"enhanced"`
	Baseline    []bookBaselinePoint `json:"baseline,omitempty"`
}
type bookBaselinePoint struct {
	At       time.Time `json:"at"`
	Fetched  time.Time `json:"firstFetchedAt"`
	Revision string    `json:"revision"`
	Quantity string    `json:"quantity"`
}
type bookPoint struct {
	At        time.Time  `json:"sourceAt"`
	Fetched   time.Time  `json:"firstFetchedAt"`
	Known     time.Time  `json:"firstKnownAt"`
	Revision  string     `json:"revision"`
	Reference *string    `json:"referenceQuote"`
	FX        *string    `json:"fxUsd"`
	Zones     []bookZone `json:"zones"`
	Bands     []bookBand `json:"bands"`
}
type bookBand struct {
	Percent   string  `json:"percent"`
	BidUSD    *string `json:"bidUsd"`
	AskUSD    *string `json:"askUsd"`
	Imbalance *string `json:"imbalance"`
}
type bookEvidence struct {
	FX           *string            `json:"fxUsd"`
	BeforeLevels []Level            `json:"beforeLevels,omitempty"`
	AfterLevels  []Level            `json:"afterLevels,omitempty"`
	From         time.Time          `json:"from"`
	To           time.Time          `json:"to"`
	Acquired     time.Time          `json:"acquiredAt"`
	Buy          string             `json:"certainBuyQuote"`
	Sell         string             `json:"certainSellQuote"`
	BoundaryBuy  string             `json:"boundaryBuyQuote"`
	BoundarySell string             `json:"boundarySellQuote"`
	Retention    *string            `json:"depthRetention"`
	Absorption   bool               `json:"absorption"`
	Following    bool               `json:"following"`
	Before       *bookBaselinePoint `json:"before"`
	After        *bookBaselinePoint `json:"after"`
	Minutes      []Observation      `json:"minutes"`
}
type bookUpdate struct {
	Kind     string        `json:"kind"`
	At       time.Time     `json:"publishedAt"`
	SourceAt time.Time     `json:"sourceAt"`
	Evidence *bookEvidence `json:"evidence,omitempty"`
}
type bookEvent struct {
	ID           string       `json:"id"`
	Rules        string       `json:"rulesVersion"`
	Collection   string       `json:"collectionVersion"`
	Venue        string       `json:"venue"`
	Direction    string       `json:"direction"`
	At           time.Time    `json:"publishedAt"`
	Computed     time.Time    `json:"computedAt"`
	Expires      time.Time    `json:"expiresAt"`
	First        bookPoint    `json:"first"`
	Zone         bookZone     `json:"zone"`
	Updates      []bookUpdate `json:"updates"`
	Ended        bool         `json:"ended"`
	CompletePath bool         `json:"completePath"`
	LastSeen     time.Time    `json:"lastSeenAt"`
	LastFetched  time.Time    `json:"lastFetchedAt"`
	BadPrice     int          `json:"adverseSnapshots"`
	ClearSince   *time.Time   `json:"clearSince"`
	Rearmed      bool         `json:"rearmed"`
}

func zoneKey(z bookZone) string { return z.Side + "/" + z.Low }
func overlaps(a, b bookZone) bool {
	return a.Side == b.Side && dec(a.Low).LessThan(dec(b.High)) && dec(b.Low).LessThan(dec(a.High))
}
func zoneIn(p bookPoint, z bookZone) *bookZone {
	for i := range p.Zones {
		if zoneKey(p.Zones[i]) == zoneKey(z) {
			return &p.Zones[i]
		}
	}
	return nil
}

// A sparse returned range is not the complete exchange book. Only compare a
// region enclosed by the returned side's price range; an absent region is null.
func rawBookZone(levels []Level, side string, low decimal.Decimal) bookZone {
	z := bookZone{Side: side, Low: low.String(), High: low.Add(dec("200")).String()}
	if len(levels) == 0 {
		return z
	}
	lo, hi := dec(levels[0].Price), dec(levels[0].Price)
	qty, notional := decimal.Zero, decimal.Zero
	for _, l := range levels {
		p, q := dec(l.Price), dec(l.Quantity)
		lo = decimal.Min(lo, p)
		hi = decimal.Max(hi, p)
		if !p.LessThan(low) && p.LessThan(dec(z.High)) {
			qty = qty.Add(q)
			notional = notional.Add(p.Mul(q))
			z.Levels = append(z.Levels, l)
		}
	}
	if lo.GreaterThan(low) || hi.LessThan(dec(z.High)) || !qty.IsPositive() {
		return z
	}
	z.Quantity, z.Notional = flowPtr(qty.String()), flowPtr(notional.String())
	return z
}
func bookReference(b *Book) *string {
	if b == nil || len(b.Bids) == 0 || len(b.Asks) == 0 {
		return nil
	}
	bid, ask := decimal.Zero, dec(b.Asks[0].Price)
	for _, l := range b.Bids {
		bid = decimal.Max(bid, dec(l.Price))
	}
	for _, l := range b.Asks {
		ask = decimal.Min(ask, dec(l.Price))
	}
	if !bid.IsPositive() || !ask.GreaterThan(bid) || ask.Sub(bid).Div(bid).GreaterThan(dec("0.01")) {
		return nil
	}
	return flowPtr(bid.Add(ask).Div(dec("2")).String())
}
func buildBookPoint(o Observation, rate *string, now time.Time) bookPoint {
	p := bookPoint{At: o.Time(), Fetched: o.FetchedAt, Known: now, Revision: o.Revision, Reference: bookReference(o.Payload.Book), FX: rate, Zones: []bookZone{}, Bands: []bookBand{}}
	if p.Reference == nil || o.Quality != "valid" || o.Payload.Book == nil {
		return p
	}
	ref := dec(*p.Reference)
	start := ref.Mul(dec("0.99")).Div(dec("100")).Floor().Mul(dec("100"))
	end := ref.Mul(dec("1.01"))
	for low, n := start, 0; low.LessThan(end) && n < 64; low, n = low.Add(dec("100")), n+1 {
		p.Zones = append(p.Zones, rawBookZone(o.Payload.Book.Bids, "buy", low), rawBookZone(o.Payload.Book.Asks, "sell", low))
	}
	for _, pct := range []string{"0.25", "0.5", "1"} {
		band := bookBand{Percent: pct}
		if rate != nil {
			lo, hi := ref.Mul(decimal.NewFromInt(1).Sub(dec(pct).Div(dec("100")))), ref.Mul(decimal.NewFromInt(1).Add(dec(pct).Div(dec("100"))))
			b, a := decimal.Zero, decimal.Zero
			minBid, maxAsk := ref, ref
			for _, l := range o.Payload.Book.Bids {
				v := dec(l.Price)
				minBid = decimal.Min(minBid, v)
				if !v.LessThan(lo) {
					b = b.Add(v.Mul(dec(l.Quantity)))
				}
			}
			for _, l := range o.Payload.Book.Asks {
				v := dec(l.Price)
				maxAsk = decimal.Max(maxAsk, v)
				if !v.GreaterThan(hi) {
					a = a.Add(v.Mul(dec(l.Quantity)))
				}
			}
			if !minBid.GreaterThan(lo) && !maxAsk.LessThan(hi) {
				band.BidUSD, band.AskUSD = flowPtr(b.Mul(dec(*rate)).String()), flowPtr(a.Mul(dec(*rate)).String())
				if b.Add(a).IsPositive() {
					band.Imbalance = flowPtr(b.Sub(a).Div(b.Add(a)).String())
				}
			}
		}
		p.Bands = append(p.Bands, band)
	}
	return p
}
func evaluateBookZone(z bookZone, p bookPoint, prior []bookPoint, origin time.Time) bookZone {
	z.Baseline = nil
	z.Enhanced = false
	if z.Quantity == nil || z.Notional == nil || p.FX == nil || p.At.Before(origin.Add(30*time.Minute)) {
		return z
	}
	seen := map[int64]bool{}
	quantities := []decimal.Decimal{}
	for _, old := range prior {
		if old.At.Before(p.At.Add(-30*time.Minute)) || !old.At.Before(p.At) || old.At.Before(origin) || old.Known.After(p.Known) || old.Fetched.After(p.Known) {
			continue
		}
		minute := old.At.Truncate(time.Minute).Unix()
		if seen[minute] {
			continue
		}
		v := zoneIn(old, z)
		if v == nil || v.Quantity == nil {
			continue
		}
		seen[minute] = true
		quantities = append(quantities, dec(*v.Quantity))
		z.Baseline = append(z.Baseline, bookBaselinePoint{old.At, old.Fetched, old.Revision, *v.Quantity})
	}
	z.Coverage = float64(len(quantities)) / 30
	if z.Coverage < .95 {
		return z
	}
	sort.Slice(quantities, func(i, j int) bool { return quantities[i].LessThan(quantities[j]) })
	median := quantities[len(quantities)/2]
	if len(quantities)%2 == 0 {
		median = median.Add(quantities[len(quantities)/2-1]).Div(dec("2"))
	}
	if !median.IsPositive() {
		return z
	}
	q := dec(*z.Quantity)
	z.Median = flowPtr(median.String())
	z.Multiple = flowPtr(q.Div(median).String())
	// Value the quantity increase at this snapshot's regional VWAP and FX;
	// changing FX or approaching an unchanged wall cannot create an increase.
	increase := q.Sub(median).Mul(dec(*z.Notional).Div(q)).Mul(dec(*p.FX))
	z.IncreaseUSD = flowPtr(increase.String())
	z.Enhanced = q.GreaterThanOrEqual(median.Mul(dec("2"))) && increase.GreaterThanOrEqual(dec("1000000"))
	return z
}
func newBookEvent(venue string, p bookPoint, z bookZone, now time.Time) bookEvent {
	id := fmt.Sprintf("%s/%s/%s/%s/%d", BookFlowRules, venue, z.Side, z.Low, p.At.Unix())
	first := p
	first.Zones = nil // Only this region's raw evidence belongs to this event.
	return bookEvent{ID: id, Rules: BookFlowRules, Collection: BookFlowCollection, Venue: venue, Direction: z.Side, At: now, Computed: now, Expires: now.Add(30 * time.Minute), First: first, Zone: z, Updates: []bookUpdate{}, CompletePath: true, LastSeen: p.At, LastFetched: p.Fetched}
}
func hasBookUpdate(e bookEvent, kind string) bool {
	for _, u := range e.Updates {
		if u.Kind == kind {
			return true
		}
	}
	return false
}
func appendBookUpdate(e *bookEvent, kind string, now, source time.Time, evidence *bookEvidence) {
	if hasBookUpdate(*e, kind) {
		return
	}
	e.Updates = append(e.Updates, bookUpdate{kind, now, source, evidence})
}

func evaluateBookFoot(e bookEvent, rows []Observation, points []bookPoint, rate *string, now time.Time) *bookEvidence {
	if len(rows) != 5 {
		return nil
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Time().Before(rows[j].Time()) })
	from, to := rows[0].Time(), rows[4].Time().Add(time.Minute)
	if to.After(now) || now.Sub(to) > 3*time.Minute || !to.After(e.At) {
		return nil
	}
	v := bookEvidence{FX: rate, From: from, To: to, Buy: "0", Sell: "0", BoundaryBuy: "0", BoundarySell: "0", Minutes: []Observation{}}
	for i, o := range rows {
		if o.Dataset != minuteFootID(e.Venue) || o.Resolution != 60 || o.Quality != "valid" || !o.Time().Equal(from.Add(time.Duration(i)*time.Minute)) || o.FetchedAt.After(now) || o.FetchedAt.Before(o.Time().Add(time.Minute)) {
			return nil
		}
		v.Acquired = maxTime(v.Acquired, o.FetchedAt)
		slim := o
		slim.Payload = Payload{}
		for _, f := range o.Payload.Foot {
			if !dec(f.High).GreaterThan(dec(e.Zone.Low)) || !dec(f.Low).LessThan(dec(e.Zone.High)) {
				continue
			}
			slim.Payload.Foot = append(slim.Payload.Foot, f)
			if !dec(f.Low).LessThan(dec(e.Zone.Low)) && !dec(f.High).GreaterThan(dec(e.Zone.High)) {
				v.Buy = dec(v.Buy).Add(dec(f.BuyQuote)).String()
				v.Sell = dec(v.Sell).Add(dec(f.SellQuote)).String()
			} else {
				v.BoundaryBuy = dec(v.BoundaryBuy).Add(dec(f.BuyQuote)).String()
				v.BoundarySell = dec(v.BoundarySell).Add(dec(f.SellQuote)).String()
			}
		}
		v.Minutes = append(v.Minutes, slim)
	}
	for _, p := range points {
		if p.Known.After(now) || p.Fetched.After(now) {
			continue
		}
		z := zoneIn(p, e.Zone)
		if z == nil || z.Quantity == nil {
			continue
		}
		point := bookBaselinePoint{p.At, p.Fetched, p.Revision, *z.Quantity}
		if !p.At.After(from) && from.Sub(p.At) <= 90*time.Second && (v.Before == nil || p.At.After(v.Before.At)) {
			v.Before = &point
		}
		if !p.At.Before(to) && p.At.Sub(to) <= 90*time.Second && (v.After == nil || p.At.Before(v.After.At)) {
			v.After = &point
		}
	}
	if rate == nil {
		return &v
	}
	buy, sell, uncertain := dec(v.Buy), dec(v.Sell), dec(v.BoundaryBuy).Add(dec(v.BoundarySell))
	total := buy.Add(sell)
	if total.Mul(dec(*rate)).LessThan(dec("1000000")) || !total.IsPositive() {
		return &v
	}
	against, follow := sell, buy
	if e.Direction == "sell" {
		against, follow = buy, sell
	}
	// Count all uncertain boundary amount against each candidate proportion.
	v.Following = follow.Div(total.Add(uncertain)).GreaterThanOrEqual(dec("0.6"))
	if v.Before != nil && v.After != nil && dec(v.Before.Quantity).IsPositive() {
		r := dec(v.After.Quantity).Div(dec(v.Before.Quantity))
		v.Retention = flowPtr(r.String())
		v.Absorption = r.GreaterThanOrEqual(dec("0.7")) && against.Div(total.Add(uncertain)).GreaterThanOrEqual(dec("0.6"))
	}
	return &v
}
