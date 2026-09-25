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
	all := v["items"].([]any)
	rows := []map[string]any{}
	minimum := parseFloat(q, "minUsd", 0, 0, 1e12) * 100
	var buy, sell int64
	valid := 0
	maxAmount := 0.0
	for _, raw := range all {
		r := raw.(map[string]any)
		if side != "" && side != "all" && r["side"] != side || venue != "" && venue != "all" && !strings.EqualFold(str(r["venue"]), venue) {
			continue
		}
		if minimum > 0 && (r["usdCents"] == nil || num(r["usdCents"]) < minimum) {
			continue
		}
		// Freshness changes while browsing; source timestamps and amounts do not.
		expires, _ := time.Parse(time.RFC3339Nano, str(r["expiresAt"]))
		if expires.IsZero() || now.After(expires) {
			r["valid"] = false
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
	v["items"], v["total"], v["fetchedCount"], v["validCount"] = rows[min(offset, len(rows)):end], len(rows), len(all), valid
	v["version"], v["offset"], v["limit"], v["hasMore"] = version, offset, limit, end < len(rows)
	v["bidCents"], v["askCents"], v["scaleMaxCents"] = buy, sell, maxAmount
	v["note"] = "独立大单快照；只统计已获取样本，不额外计入普通买卖墙或BTC信号。时长截至快照，不承诺采样间一直存在。"
	return v, nil
}
