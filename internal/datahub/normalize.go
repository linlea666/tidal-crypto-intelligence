package datahub

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/shopspring/decimal"
)

var ErrNoData = errors.New("上游未返回可用数据")

func object(v any) map[string]any { m, _ := v.(map[string]any); return m }
func array(v any) []any           { a, _ := v.([]any); return a }
func str(v any) string {
	if v == nil {
		return ""
	}
	return fmt.Sprint(v)
}
func num(v any) float64 {
	f, _ := strconv.ParseFloat(str(v), 64)
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return 0
	}
	return f
}
func dec(s string) decimal.Decimal { d, _ := decimal.NewFromString(s); return d }
func money(s string) int64 {
	d := dec(s).Mul(decimal.NewFromInt(100)).Round(0)
	if d.Abs().GreaterThan(decimal.NewFromInt(9_000_000_000_000_000)) {
		return 0
	}
	return d.IntPart()
}
func multiply(a, b string) string { return dec(a).Mul(dec(b)).String() }
func validNumber(v any, negative bool) (string, error) {
	d, e := decimal.NewFromString(str(v))
	if e != nil || (!negative && d.IsNegative()) || d.Abs().GreaterThan(decimal.NewFromInt(1_000_000_000_000_000)) {
		return "", errors.New("invalid numeric field")
	}
	return d.String(), nil
}
func timestamp(v any) *time.Time {
	if v == nil {
		return nil
	}
	s := str(v)
	// Preserve exact millisecond history boundaries (float seconds lose precision).
	if n, err := strconv.ParseInt(s, 10, 64); err == nil && n >= 1_000_000_000 {
		var t time.Time
		switch {
		case n > 1e14:
			t = time.UnixMicro(n)
		case n > 1e11:
			t = time.UnixMilli(n)
		default:
			t = time.Unix(n, 0)
		}
		t = t.UTC()
		return &t
	}
	f, e := strconv.ParseFloat(s, 64)
	if e != nil {
		t, e := time.Parse(time.RFC3339Nano, s)
		if e == nil {
			return &t
		}
		return nil
	}
	if f < 1_000_000_000 || math.IsNaN(f) || math.IsInf(f, 0) {
		return nil
	}
	if f > 1e14 {
		f /= 1000
	}
	if f > 1e11 {
		f /= 1000
	}
	t := time.Unix(int64(f), int64((f-math.Floor(f))*1e9)).UTC()
	return &t
}
func Normalize(d Dataset, raw []byte, fetched time.Time) ([]Observation, error) {
	var root map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&root); err != nil {
		return nil, errors.New("invalid upstream JSON")
	}
	if str(root["code"]) != "0" {
		return nil, fmt.Errorf("upstream business error %s", str(root["code"]))
	}
	data := root["data"]
	if data == nil {
		return nil, ErrNoData
	}
	if d.Contract {
		return normalizeObservers(d, data, fetched)
	}
	obs := func(at *time.Time, p Payload) Observation {
		basis := "source"
		if at == nil {
			basis = "retrieval"
		}
		return Observation{Dataset: d.ID, Source: d.Source, ObservedAt: at, FetchedAt: fetched, TimeBasis: basis, Resolution: d.Resolution, Quality: "valid", Payload: p}
	}
	var out []Observation
	switch d.Kind {
	case "book", "footprint":
		for _, v := range array(data) {
			r := array(v)
			if len(r) < 2 {
				return nil, errors.New("invalid price array")
			}
			at := timestamp(r[0])
			if at == nil {
				return nil, errors.New("missing source timestamp")
			}
			if d.Kind == "book" {
				if len(r) < 3 {
					return nil, ErrNoData
				}
				b := Book{Low: math.Inf(1)}
				for side, rawSide := range []any{r[1], r[2]} {
					for _, x := range array(rawSide) {
						a := array(x)
						if len(a) < 2 {
							return nil, errors.New("invalid book row")
						}
						p, e := validNumber(a[0], false)
						if e != nil || dec(p).IsZero() {
							return nil, errors.New("invalid book price")
						}
						q, e := validNumber(a[1], false)
						if e != nil {
							return nil, e
						}
						if dec(q).IsZero() {
							continue
						}
						l := Level{p, q}
						if side == 0 {
							b.Bids = append(b.Bids, l)
							b.Low = math.Min(b.Low, num(p))
						} else {
							b.Asks = append(b.Asks, l)
							b.High = math.Max(b.High, num(p))
						}
					}
				}
				if len(b.Bids) == 0 || len(b.Asks) == 0 {
					return nil, ErrNoData
				}
				out = append(out, obs(at, Payload{Book: &b}))
			} else {
				// Only closed footprint intervals can be evidence; forming bars are retried.
				if at.Add(time.Duration(d.Resolution) * time.Second).After(fetched) {
					continue
				}
				o := obs(at, Payload{})
				if r[1] == nil {
					o.Quality = "missing"
					o.Reason = "该周期足迹为空"
					out = append(out, o)
					continue
				}
				for _, x := range array(r[1]) {
					a := array(x)
					if len(a) < 10 {
						return nil, errors.New("invalid footprint width")
					}
					v := make([]string, 8)
					for i := range v {
						n, e := validNumber(a[i], false)
						if e != nil {
							return nil, e
						}
						v[i] = n
					}
					if dec(v[1]).LessThanOrEqual(dec(v[0])) {
						return nil, errors.New("invalid footprint bounds")
					}
					o.Payload.Foot = append(o.Payload.Foot, Foot{v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7], int64(num(a[8])), int64(num(a[9]))})
				}
				if len(o.Payload.Foot) == 0 {
					o.Quality = "missing"
					o.Reason = "无可用价位足迹"
				}
				out = append(out, o)
			}
		}
	case "flow", "liquidations":
		for _, v := range array(data) {
			r := object(v)
			at := timestamp(r["time"])
			if at == nil {
				return nil, errors.New("missing flow timestamp")
			}
			keys := []string{"aggregated_buy_volume_usd", "aggregated_sell_volume_usd"}
			if d.Kind == "liquidations" {
				keys = []string{"aggregated_long_liquidation_usd", "aggregated_short_liquidation_usd"}
			}
			a, e := validNumber(r[keys[0]], false)
			if e != nil {
				return nil, e
			}
			b, e := validNumber(r[keys[1]], false)
			if e != nil {
				return nil, e
			}
			p := Payload{}
			if d.Kind == "flow" {
				p.Flow = &Flow{a, b}
			} else {
				p.Liquidation = &Liquidation{a, b}
			}
			out = append(out, obs(at, p))
		}
	case "oi":
		p := Payload{}
		for _, v := range array(data) {
			r := object(v)
			name := str(r["exchange"])
			if name == "" {
				return nil, errors.New("missing OI venue")
			}
			usd, e := validNumber(r["open_interest_usd"], false)
			if e != nil {
				return nil, e
			}
			base, e := validNumber(r["open_interest_quantity"], false)
			if e != nil {
				return nil, e
			}
			p.OI = append(p.OI, Interest{name, usd, base})
		}
		if len(p.OI) > 0 {
			out = append(out, obs(nil, p))
		}
	case "funding":
		p := Payload{}
		for _, v := range array(data) {
			r := object(v)
			a := str(r["symbol"])
			if !ValidAsset(a) {
				continue
			}
			for _, margin := range []string{"stablecoin", "token"} {
				for _, x := range array(r[margin+"_margin_list"]) {
					m := object(x)
					rate, e := validNumber(m["funding_rate"], true)
					if e != nil {
						continue // unsupported venue rows can contain only an exchange name; never synthesize zero.
					}
					var hours *float64
					if h := num(m["funding_rate_interval"]); h > 0 {
						hours = &h
					}
					p.Funding = append(p.Funding, Funding{a, str(m["exchange"]), rate, hours, margin})
				}
			}
		}
		if len(p.Funding) > 0 {
			out = append(out, obs(nil, p))
		}
	case "whales":
		p := Payload{Whales: []Whale{}}
		seen := map[string]bool{}
		for _, v := range array(data) {
			r := object(v)
			a := str(r["symbol"])
			if !ValidAsset(a) {
				continue
			}
			address := strings.ToLower(str(r["user"]))
			if address == "" {
				return nil, errors.New("missing public address")
			}
			key := address + ":" + a
			if seen[key] {
				return nil, errors.New("duplicate address/asset")
			}
			seen[key] = true
			at := timestamp(r["update_time"])
			if at == nil {
				return nil, errors.New("missing position timestamp")
			}
			var liq *string
			if n, e := validNumber(r["liq_price"], false); e == nil && dec(n).IsPositive() {
				liq = &n
			}
			fields := []string{"position_size", "entry_price", "mark_price", "position_value_usd", "leverage", "unrealized_pnl", "margin_balance", "funding_fee"}
			values := make([]string, len(fields))
			for i, k := range fields {
				n, e := validNumber(r[k], i == 0 || i == 5 || i == 6 || i == 7)
				if e != nil {
					return nil, e
				}
				values[i] = n
			}
			margin := str(r["margin_mode"])
			p.Whales = append(p.Whales, Whale{address, a, values[0], values[1], values[2], values[3], values[4], margin, liq, values[5], values[6], values[7], *at, timestamp(r["create_time"])})
		}
		out = append(out, obs(nil, p)) // retrieval time describes list completeness; each position keeps its own source time.
	case "large", "large-history":
		p := Payload{Large: []LargeOrder{}}
		for _, v := range array(data) {
			r := object(v)
			price := r["limit_price"]
			if price == nil {
				price = r["price"]
			}
			pv, e := validNumber(price, false)
			if e != nil {
				return nil, e
			}
			q, e := validNumber(r["current_quantity"], false)
			if e != nil {
				return nil, e
			}
			usd, e := validNumber(r["current_usd_value"], false)
			if e != nil {
				return nil, e
			}
			executed, e := validNumber(r["executed_usd_value"], false)
			if e != nil {
				return nil, e
			}
			side := "unknown"
			if num(r["order_side"]) == 1 {
				side = "ask"
			}
			if num(r["order_side"]) == 2 {
				side = "bid"
			}
			state := "未知"
			if num(r["order_state"]) == 1 {
				state = "上游当前列表"
			} else if num(r["order_state"]) == 2 {
				state = "上游记录已结束"
			} else if num(r["order_state"]) == 3 {
				state = "上游标记撤销"
			}
			id := str(r["id"])
			if id == "" || side == "unknown" {
				return nil, errors.New("invalid large order identity/side")
			}
			optional := func(key string) *string {
				n, err := validNumber(r[key], false)
				if err != nil {
					return nil
				}
				return &n
			}
			p.Large = append(p.Large, LargeOrder{ID: id, Side: side, Price: pv, Quantity: q, ReportedUSD: usd, ExecutedUSD: executed, State: state, Start: timestamp(r["start_time"]), Changed: timestamp(r["current_time"]), Trades: int64(num(r["trade_count"])), InitialQuantity: optional("start_quantity"), InitialUSD: optional("start_usd_value"), ExecutedQuantity: optional("executed_volume"), RawState: int(num(r["order_state"])), End: timestamp(r["order_end_time"])})
		}
		out = append(out, obs(nil, p))
	case "map":
		r := object(data)
		model := Model{Bins: []ModelBin{}, ReferencePrice: num(r["last_price"]), Unit: "relative", Model: "CoinGlass aggregated-map", Range: d.Params["range"]}
		for _, v := range array(r["data"]) {
			m := object(v)
			instrument := object(m["instrument"])
			venue := str(instrument["exName"])
			if venue == "" {
				venue = str(instrument["exchange_name"])
			}
			if venue == "" {
				venue = str(instrument["exchange"])
			}
			for _, entries := range object(m["liqMapV2"]) {
				for _, item := range array(entries) {
					a := array(item)
					if len(a) < 2 {
						return nil, errors.New("invalid map row")
					}
					price, strength := num(a[0]), num(a[1])
					if price > 0 && strength >= 0 {
						model.Bins = append(model.Bins, ModelBin{price, strength, venue})
					}
				}
			}
		}
		if len(model.Bins) == 0 {
			return nil, ErrNoData
		}
		sort.Slice(model.Bins, func(i, j int) bool {
			if model.Bins[i].Price == model.Bins[j].Price {
				return model.Bins[i].Venue < model.Bins[j].Venue
			}
			return model.Bins[i].Price < model.Bins[j].Price
		})
		out = append(out, obs(nil, Payload{Model: &model}))
	case "heatmap":
		r := object(data)
		model := Model{Bins: []ModelBin{}, Unit: "relative", Model: "CoinGlass model1", Range: d.Params["range"]}
		for _, v := range array(r["y_axis"]) {
			model.Prices = append(model.Prices, num(v))
		}
		for _, v := range array(r["price_candlesticks"]) {
			a := array(v)
			if len(a) > 0 {
				t := timestamp(a[0])
				if t != nil {
					model.Times = append(model.Times, t.Unix())
				}
			}
		}
		maxX := -1
		for _, v := range array(r["liquidation_leverage_data"]) {
			a := array(v)
			if len(a) < 3 {
				return nil, errors.New("invalid heat cell")
			}
			x, y, z := int(num(a[0])), int(num(a[1])), num(a[2])
			if x < 0 || y < 0 || x >= len(model.Times) || y >= len(model.Prices) || z < 0 {
				return nil, errors.New("heat index out of bounds")
			}
			model.Cells = append(model.Cells, [3]float64{float64(x), float64(y), z})
			if x > maxX {
				maxX = x
			}
		}
		// Only the latest time slice is a current distribution. Never sum time columns.
		for _, c := range model.Cells {
			if int(c[0]) == maxX {
				model.Bins = append(model.Bins, ModelBin{Price: model.Prices[int(c[1])], Strength: c[2]})
			}
		}
		at := timestamp(r["update_time"])
		if at == nil || len(model.Cells) == 0 {
			return nil, ErrNoData
		}
		out = append(out, obs(at, Payload{Model: &model}))
	case "wallet":
		// Account summaries are not recomputed from BTC/ETH-only positions.
		r := object(data)
		filtered := map[string]any{}
		for _, k := range []string{"margin_summary", "cross_margin_summary", "cross_maintenance_margin_used", "withdrawable", "update_time"} {
			if v, ok := r[k]; ok {
				filtered[k] = v
			}
		}
		var positions []any
		for _, x := range array(r["asset_positions"]) {
			m := object(x)
			pos := object(m["position"])
			if ValidAsset(str(pos["coin"])) || ValidAsset(str(pos["symbol"])) {
				positions = append(positions, x)
			}
		}
		filtered["asset_positions"] = positions
		raw, _ := json.Marshal(filtered)
		out = append(out, obs(timestamp(r["update_time"]), Payload{Wallet: raw}))
	default:
		return nil, errors.New("unsupported dataset kind")
	}
	if len(out) == 0 {
		return nil, ErrNoData
	}
	return out, nil
}
