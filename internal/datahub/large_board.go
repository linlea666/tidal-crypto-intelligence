package datahub

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"sort"
	"strings"
	"time"
)

// Snapshots share the bounded view cache, but retain their own immutable version
// across page changes. A cache eviction requires explicitly starting a new view.
func (h *Hub) LargeBoard(ctx context.Context, a string, q url.Values) (any, error) {
	if q.Get("events") == "1" {
		hours := parseInt(q, "hours", 1, 1, 24)
		events, more, e := h.Store.OrderEvents(ctx, a, time.Now().Add(-time.Duration(hours)*time.Hour), time.Now(), 50, parseInt(q, "offset", 0, 0, 10000))
		return map[string]any{"items": events, "hasMore": more, "historyStatus": h.orderHistoryStatus(a)}, e
	}
	if q.Get("history") == "1" {
		return h.LargeOrdersPage(ctx, a, true, parseInt(q, "limit", 50, 1, 100), parseInt(q, "offset", 0, 0, 10000))
	}
	now := time.Now().UTC()
	version := q.Get("version")
	var v map[string]any
	if version != "" {
		h.viewMu.Lock()
		cached, exists := h.views["order-snapshot/"+a+"/"+version]
		h.viewMu.Unlock()
		if !exists || now.After(cached.Until) {
			return map[string]any{"snapshotExpired": true, "items": []any{}, "note": "快照已更新，请刷新列表；筛选条件保留"}, nil
		}
		if err := json.Unmarshal(cached.Raw, &v); err != nil {
			return nil, err
		}
	} else {
		v = h.LargeView(a, false).(map[string]any)
		rows := v["items"].([]map[string]any)
		price, _, priceOK := h.CurrentPrice(a, now)
		for _, r := range rows {
			end := r["fetchedAt"].(time.Time)
			if observed, ok := r["observedAt"].(*time.Time); ok && observed != nil {
				end = *observed
			}
			start, _ := r["startAt"].(*time.Time)
			basis := "source_created"
			if start == nil {
				var t TrackedOrder
				var raw []byte
				if h.Store.db.QueryRowContext(ctx, "SELECT payload FROM tracked_orders WHERE k=?", r["key"]).Scan(&raw) == nil && json.Unmarshal(raw, &t) == nil && !t.FirstSeen.IsZero() {
					start = &t.FirstSeen
				}
				basis = "local_observed"
			}
			r["durationSeconds"], r["distancePercent"] = nil, nil
			if start != nil && !start.After(end) {
				r["durationSeconds"] = end.Sub(*start).Seconds()
			}
			r["durationBasis"], r["durationThrough"] = basis, end
			if priceOK && price > 0 && r["priceUsd"] != nil {
				r["distancePercent"] = (num(r["priceUsd"])/price - 1) * 100
			}
		}
		v["at"] = now
		v["sourceVersion"] = h.largeSourceVersion(a)
		b, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		sum := sha256.Sum256(b)
		version = fmt.Sprintf("%x", sum[:12])
		h.viewMu.Lock()
		for k, c := range h.views {
			if now.After(c.Until) {
				h.viewBytes -= len(c.Raw)
				delete(h.views, k)
			}
		}
		if len(b) > ResultLimit {
			h.viewMu.Unlock()
			return nil, errors.New("大单快照超过响应预算")
		}
		if h.viewBytes+len(b) > 32<<20 {
			h.views = map[string]cachedView{}
			h.viewBytes = 0
		}
		h.views["order-snapshot/"+a+"/"+version] = cachedView{Raw: b, Until: now.Add(12 * time.Minute)}
		h.viewBytes += len(b)
		h.viewMu.Unlock()
		// Decode the same wire representation for first and subsequent pages.
		if err = json.Unmarshal(b, &v); err != nil {
			return nil, err
		}
	}
	v["newSnapshotAvailable"] = v["sourceVersion"] != h.largeSourceVersion(a)
	all := v["items"].([]any)
	if err := h.reconcileOrderRows(ctx, a, all, now); err != nil {
		return nil, err
	}
	v["version"] = version
	v["note"] = "独立大单快照；金额及排序固定于所选快照，状态另按最新本地事实核对。只统计已获取样本，不额外计入普通买卖墙或BTC信号。"
	if q.Get("layout") == "split" {
		scale := 0.0
		for _, side := range []string{"bid", "ask"} {
			params := url.Values{"side": {side}}
			for _, key := range []string{"venue", "sort", "minUsd", "distance", "offset", "limit"} {
				params.Set(key, q.Get(side+"_"+key))
			}
			col, err := filterOrderRows(all, params)
			if err != nil {
				return nil, err
			}
			v[side] = col
			scale = math.Max(scale, num(col["scaleMaxCents"]))
		}
		// The full immutable snapshot is cached once; a split reply only contains
		// the two requested pages. Both use the union's scale across all pages.
		delete(v, "items")
		v["scaleMaxCents"] = scale
		return v, nil
	}
	col, err := filterOrderRows(all, q)
	if err != nil {
		return nil, err
	}
	for k, value := range col {
		v[k] = value
	}
	return v, nil
}

func filterOrderRows(all []any, q url.Values) (map[string]any, error) {
	side, venue, order := q.Get("side"), q.Get("venue"), q.Get("sort")
	if side != "" && side != "all" && side != "bid" && side != "ask" {
		return nil, errors.New("无效买卖方向")
	}
	if order == "" {
		order = "amount_desc"
	}
	allowed := map[string]string{"amount_desc": "usdCents", "amount_asc": "usdCents", "price_asc": "priceUsd", "price_desc": "priceUsd", "distance_asc": "distancePercent", "duration_desc": "durationSeconds"}
	field, ok := allowed[order]
	if !ok {
		return nil, errors.New("无效排序")
	}
	minimum := parseFloat(q, "minUsd", 0, 0, 1e12) * 100
	distance := parseFloat(q, "distance", 0, 0, 10000)
	rows := []map[string]any{}
	var buy, sell, total int64
	valid, fetched, excludedFX, excludedDistance := 0, 0, 0, 0
	maxAmount := 0.0
	for _, raw := range all {
		r := raw.(map[string]any)
		if side != "" && side != "all" && r["side"] != side {
			continue
		}
		fetched++
		if venue != "" && venue != "all" && !strings.EqualFold(str(r["venue"]), venue) {
			continue
		}
		if minimum > 0 && r["usdCents"] == nil {
			excludedFX++
			continue
		}
		if minimum > 0 && num(r["usdCents"]) < minimum {
			continue
		}
		if distance > 0 {
			if r["distancePercent"] == nil {
				excludedDistance++
				continue
			}
			if math.Abs(num(r["distancePercent"])) > distance {
				continue
			}
		}
		if r["usdCents"] != nil {
			total += int64(num(r["usdCents"]))
		}
		if r["valid"] == true {
			valid++
			if r["side"] == "bid" {
				buy += int64(num(r["usdCents"]))
			} else if r["side"] == "ask" {
				sell += int64(num(r["usdCents"]))
			}
		}
		maxAmount = math.Max(maxAmount, num(r["usdCents"]))
		rows = append(rows, r)
	}
	sort.Slice(rows, func(i, j int) bool {
		x, y := rows[i][field], rows[j][field]
		if (x == nil) != (y == nil) {
			return x != nil
		}
		xn, yn := num(x), num(y)
		if field == "distancePercent" {
			xn, yn = math.Abs(xn), math.Abs(yn)
		}
		if xn == yn {
			return str(rows[i]["id"]) < str(rows[j]["id"])
		}
		if strings.HasSuffix(order, "_asc") {
			return xn < yn
		}
		return xn > yn
	})
	offset, limit := parseInt(q, "offset", 0, 0, 10000), parseInt(q, "limit", 50, 1, 100)
	end := min(len(rows), offset+limit)
	v := map[string]any{"items": rows[min(offset, len(rows)):end], "total": len(rows), "fetchedCount": fetched, "validCount": valid, "bidCents": buy, "askCents": sell, "snapshotCents": total, "validCents": buy + sell, "scaleMaxCents": maxAmount, "offset": offset, "limit": limit, "hasMore": end < len(rows), "excludedFX": excludedFX, "excludedDistance": excludedDistance}
	if fetched == 0 || (len(rows) > 0 && valid == 0) {
		v["bidCents"], v["askCents"], v["validCents"] = nil, nil, nil
	}
	known := false
	for _, r := range rows {
		if r["usdCents"] != nil {
			known = true
			break
		}
	}
	if !known {
		v["snapshotCents"] = nil
	}
	return v, nil
}

// GET only reads facts. Newer lifecycle facts may invalidate a pinned row but
// never replace its quantity, amount, FX, distance or position within that page.
func (h *Hub) reconcileOrderRows(ctx context.Context, a string, all []any, now time.Time) error {
	latest := map[string]Observation{}
	identities := map[string]map[string]LargeOrder{}
	for _, d := range Registry() {
		if d.Asset == a && d.Kind == "large" {
			if o, ok := h.Store.Latest(d.ID); ok {
				latest[d.ID] = o
				ids := map[string]LargeOrder{}
				for _, r := range o.Payload.Large {
					ids[d.ID+":"+r.ID] = r
				}
				identities[d.ID] = ids
			}
		}
	}
	for _, raw := range all {
		r := raw.(map[string]any)
		expires, _ := time.Parse(time.RFC3339Nano, str(r["expiresAt"]))
		r["presenceState"], r["presenceNote"] = "observed", "截至所选来源快照仍有记录"
		if expires.IsZero() || now.After(expires) {
			r["valid"] = false
			r["presenceState"], r["presenceNote"] = "stale", "快照或换算已过期，当前状态待更新"
		}
		parts := strings.SplitN(str(r["id"]), ":", 2)
		if len(parts) != 2 {
			continue
		}
		o, ok := latest[parts[0]]
		fetched, _ := time.Parse(time.RFC3339Nano, str(r["fetchedAt"]))
		if ok && o.FetchedAt.After(fetched) {
			r["checkedAt"] = o.FetchedAt
			if n, found := identities[parts[0]][str(r["id"])]; found {
				r["lastReturnedAt"] = o.FetchedAt
				if n.Quantity != str(r["quantity"]) || n.Price != str(r["price"]) {
					r["valid"] = false
					r["presenceState"], r["presenceNote"] = "changed", "本地已有更新余量或价格，请刷新快照"
				}
			} else {
				r["valid"] = false
				r["presenceState"], r["presenceNote"] = "unreturned", "本次列表未再返回，原因待确认；可能低于筛选门槛或上游覆盖变化"
				if o.Quality != "valid" {
					r["presenceState"], r["presenceNote"] = "coverage_unknown", "最新列表覆盖不完整，当前是否存在待确认"
				}
			}
		}
		var tracked TrackedOrder
		var b []byte
		if h.Store.db.QueryRowContext(ctx, "SELECT payload FROM tracked_orders WHERE k=?", r["key"]).Scan(&b) == nil && json.Unmarshal(b, &tracked) == nil && tracked.Order.RawState > 1 {
			r["valid"] = false
			r["presenceState"] = "ended"
			r["presenceNote"] = "上游记录已结束，结束不等于撤销"
			if tracked.Order.RawState == 3 {
				r["presenceState"], r["presenceNote"] = "revoked", "上游标记撤销"
			}
			r["latestRawState"], r["latestEndAt"] = tracked.Order.RawState, tracked.Order.End
		}
		// A reference-price prompt is not a venue-level touch or execution claim.
		r["nearReference"] = r["distancePercent"] != nil && math.Abs(num(r["distancePercent"])) <= .3
	}
	return ctx.Err()
}

// Dataset revisions and retrieval times, independent of page visits or FX ticks.
func (h *Hub) largeSourceVersion(a string) string {
	parts := []string{}
	for _, d := range h.datasets() {
		if d.Asset == a && d.Kind == "large" {
			if o, ok := h.Store.Latest(d.ID); ok {
				parts = append(parts, fmt.Sprintf("%s/%s/%d", d.ID, o.Revision, o.FetchedAt.UnixNano()))
			}
		}
	}
	sort.Strings(parts)
	return strings.Join(parts, ";")
}
