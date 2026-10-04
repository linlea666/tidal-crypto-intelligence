package datahub

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/url"
	"sort"
	"strconv"
	"time"
)

func itoa(n int) string { return strconv.Itoa(n) }
func costJSON(v any) (json.RawMessage, error) {
	if m, ok := v.(map[string]any); ok {
		m["source"] = onchainSource
		m["rulesVersion"] = OnchainRules
		m["asOf"] = time.Now().UTC()
	}
	b, e := json.Marshal(v)
	if len(b) > 2<<20 {
		return nil, errors.New("链上查询超过2MiB，请缩小窗口或分页")
	}
	return b, e
}
func costPageParam(q url.Values, k string, defaultN, maxN int) (int, error) {
	s := q.Get(k)
	if s == "" {
		return defaultN, nil
	}
	n, e := strconv.Atoi(s)
	if e != nil || n < 0 || n > maxN {
		return 0, errors.New("无效分页参数")
	}
	return n, nil
}
func (h *Hub) costRead(ctx context.Context, path string, q url.Values) (json.RawMessage, error) {
	if a := q.Get("asset"); a != "" && a != "BTC" {
		return nil, errors.New("链上筹码仅支持BTC")
	}
	now := time.Now().UTC()
	s := h.Store.onchain
	if path == "onchain-cost/settings" {
		settings, e := h.CostSettings(ctx)
		if e != nil {
			return nil, e
		}
		return costJSON(map[string]any{"emailEnabled": settings.EmailEnabled, "mailConfigured": h.mail != nil})
	}
	if e := s.available(); e != nil {
		if path == "onchain-cost" {
			return costJSON(map[string]any{"health": h.onchainHealth(), "frame": nil, "dates": []string{}, "source": onchainSource, "rulesVersion": OnchainRules})
		}
		return nil, e
	}
	return h.cached(ctx, path+"?"+q.Encode()+"&localVersion="+strconv.FormatUint(s.epoch.Load(), 10), time.Minute, func() (any, error) {
		b, e := h.costReadLocal(ctx, path, q, now)
		if e != nil {
			return nil, e
		}
		return json.RawMessage(b), nil
	})
}
func (h *Hub) costReadLocal(ctx context.Context, path string, q url.Values, now time.Time) (json.RawMessage, error) {
	s := h.Store.onchain
	offset, e := costPageParam(q, "offset", 0, 20000)
	if e != nil {
		return nil, e
	}
	limit, e := costPageParam(q, "limit", 100, 400)
	if e != nil || limit < 1 {
		return nil, errors.New("无效分页大小")
	}
	if path == "onchain-cost/events" {
		rows, e := s.db.QueryContext(ctx, "SELECT payload FROM events ORDER BY detected DESC,id LIMIT ? OFFSET ?", limit+1, offset)
		if e != nil {
			return nil, e
		}
		out := []CostEvent{}
		for rows.Next() {
			var b []byte
			var v CostEvent
			if e = rows.Scan(&b); e != nil {
				rows.Close()
				return nil, e
			}
			if e = json.Unmarshal(b, &v); e != nil {
				rows.Close()
				return nil, e
			}
			out = append(out, v)
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			return nil, e
		}
		more := len(out) > limit
		if more {
			out = out[:limit]
		}
		for i := range out {
			var status string
			if e = h.Store.research.QueryRowContext(ctx, "SELECT status FROM notices WHERE id=?", "cost-"+out[i].ID).Scan(&status); e == nil {
				out[i].NoticeStatus = status
			} else if e == sql.ErrNoRows {
				if e = s.db.QueryRowContext(ctx, "SELECT status FROM outbox WHERE id=?", out[i].ID).Scan(&status); e == nil {
					out[i].NoticeStatus = status
				} else if e != sql.ErrNoRows {
					return nil, e
				}
			} else {
				return nil, e
			}
		}
		return costJSON(map[string]any{"items": out, "offset": offset, "hasMore": more})
	}
	if path == "onchain-cost/research" {
		mode := q.Get("mode")
		if mode != "" && mode != "forward" && mode != "historical" {
			return nil, errors.New("无效研究模式")
		}
		v, e := h.costResearchView(ctx, now, mode, offset, limit)
		if e != nil {
			return nil, e
		}
		return costJSON(v)
	}
	if path != "onchain-cost" && path != "onchain-cost/history" {
		return nil, errors.New("未知链上接口")
	}
	date := q.Get("date")
	if date != "" {
		if _, e = costDay(date); e != nil {
			return nil, e
		}
	}
	f, e := s.frame(ctx, date, now)
	if e != nil {
		return nil, e
	}
	summaries, e := s.summaries(ctx, now)
	if e != nil {
		return nil, e
	}
	dates := []string{}
	for _, sm := range summaries {
		dates = append(dates, sm.Date)
	}
	if f == nil {
		return costJSON(map[string]any{"frame": nil, "dates": dates, "health": h.onchainHealth(), "source": onchainSource, "rulesVersion": OnchainRules, "methodNote": onchainMethodNote})
	}
	prices, e := s.prices(ctx, now)
	if e != nil {
		return nil, e
	}
	m := costMetricsFromSummaries(*f, summaries, prices)
	low, high := q.Get("low"), q.Get("high")
	if low == "" && high == "" {
		low = dec(f.Price).Div(dec("1000")).Floor().Mul(dec("1000")).String()
		high = dec(low).Add(dec("1000")).String()
	}
	l, e := costNumber(low, false)
	if e != nil {
		return nil, e
	}
	r, e := costNumber(high, true)
	if e != nil || !r.GreaterThan(l) {
		return nil, errors.New("无效固定区间")
	}
	if path == "onchain-cost/history" {
		return h.costHistory(ctx, q, *f, summaries, prices, l.String(), r.String(), offset, limit, now)
	}
	step := q.Get("step")
	if step == "" {
		step = "1000"
	}
	if step != "500" && step != "1000" && step != "2000" {
		return nil, errors.New("展示桶宽仅支持500/1000/2000")
	}
	bins, actual, e := costBins(*f, step)
	if e != nil {
		return nil, e
	}
	span := q.Get("range")
	if span == "" {
		span = "20"
	}
	if span != "10" && span != "20" && span != "all" {
		return nil, errors.New("范围仅支持10/20/all")
	}
	filtered := []CostBin{}
	scale := "0"
	for _, b := range bins {
		if span != "all" {
			w := dec(span).Div(dec("100"))
			lo, hi := dec(f.Price).Mul(dec("1").Sub(w)), dec(f.Price).Mul(dec("1").Add(w))
			if dec(b.High).LessThanOrEqual(lo) || dec(b.Low).GreaterThanOrEqual(hi) {
				continue
			}
		}
		filtered = append(filtered, b)
		if dec(b.Total).GreaterThan(dec(scale)) {
			scale = b.Total
		}
	}
	// Daily UI never materializes tens of thousands of native buckets on one page.
	if len(filtered) > 400 {
		return nil, errors.New("原生桶超过400档，请选择更粗精度或较小范围")
	}
	supply := costIntervalSupply(*f, l, r)
	changes := []any{}
	day, _ := costDay(f.Date)
	compare := q.Get("compare")
	compareDates := []string{costDate(day.AddDate(0, 0, -1)), costDate(day.AddDate(0, 0, -7))}
	if compare != "" {
		if _, e = costDay(compare); e != nil {
			return nil, e
		}
		if compare >= f.Date {
			return nil, errors.New("比较日期必须早于所选日期")
		}
		compareDates = append(compareDates, compare)
	}
	for _, d := range compareDates {
		past, e := s.frame(ctx, d, now)
		if e != nil {
			return nil, e
		}
		var delta any
		status := "missing"
		if past != nil && past.Method == f.Method {
			delta = costDifference(supply, costIntervalSupply(*past, l, r))
			status = "available"
		}
		changes = append(changes, map[string]any{"from": d, "to": f.Date, "status": status, "change": delta})
	}
	asOf := now
	if date != "" {
		asOf = day.AddDate(0, 0, 1).Add(6 * time.Hour)
		if asOf.After(now) {
			asOf = now
		}
	}
	evidence, e := h.costFrozenEvidence(ctx, f.Date, asOf)
	if e != nil {
		return nil, e
	}
	var observation costState
	if date == "" {
		if e = costLoad(ctx, s.db, "observation", &observation); e != nil {
			return nil, e
		}
		if observation.LastDate != f.Date {
			observation = costState{}
		}
	}
	settings, e := h.CostSettings(ctx)
	if e != nil {
		return nil, e
	}
	meta := *f
	meta.STH.Values = nil
	meta.LTH.Values = nil
	return costJSON(map[string]any{"frame": meta, "dates": dates, "health": h.onchainHealth(), "source": onchainSource, "sourceURL": "https://charts.blockhorizon.io/charts/cost-basis-distribution", "rulesVersion": OnchainRules, "methodNote": onchainMethodNote, "historical": date != "", "metrics": m, "bins": filtered, "requestedStep": step, "actualStep": actual, "scaleMaxBTC": scale, "selected": map[string]any{"low": l.String(), "high": r.String(), "supply": supply, "changes": changes}, "evidence": evidence, "observation": observation, "settings": settings, "mailConfigured": h.mail != nil, "note": "成本分布与集中度为来源最新修订；历史跨来源证据限制在所选日期次日06:00 UTC前实际可得的数据。冻结事件保留当时版本。"})
}
func (h *Hub) costHistory(ctx context.Context, q url.Values, f CostFrame, summaries []costSummary, prices map[string]string, low, high string, offset, limit int, now time.Time) (json.RawMessage, error) {
	end, _ := costDay(f.Date)
	from := end.AddDate(-1, 0, 0)
	switch q.Get("period") {
	case "", "1y":
	case "3m":
		from = end.AddDate(0, -3, 0)
	case "4y":
		from = end.AddDate(-4, 0, 0)
	case "all":
		from, _ = costDay("2010-07-18")
	default:
		return nil, errors.New("无效历史范围")
	}
	mode := q.Get("mode")
	if mode != "" && mode != "heatmap" {
		return nil, errors.New("无效历史图层")
	}
	selected := []costSummary{}
	for _, sm := range summaries {
		if sm.Date >= costDate(from) && sm.Date <= f.Date {
			selected = append(selected, sm)
		}
	}
	// Latest page first, kept in ascending date order for a truthful time axis.
	total := len(selected)
	finish := max(0, total-offset)
	start := max(0, finish-limit)
	selected = selected[start:finish]
	out := []any{}
	cells := []any{}
	for _, sm := range selected {
		frame, e := h.Store.onchain.frame(ctx, sm.Date, now)
		if e != nil {
			return nil, e
		}
		if frame == nil {
			continue
		}
		supply := costIntervalSupply(*frame, dec(low), dec(high))
		out = append(out, map[string]any{"date": sm.Date, "concentration": sm.Concentration["5"], "supply": supply, "origin": sm.Origin, "revision": sm.Revision})
		if mode == "heatmap" {
			bins, actual, e := costBins(*frame, "1000")
			if e != nil || actual != "1000" {
				continue
			}
			byLow := map[string]string{}
			aligned := true
			for _, b := range bins {
				if !dec(b.Low).Mod(dec("1000")).IsZero() {
					aligned = false
					break
				}
				byLow[b.Low] = b.Total
			}
			if !aligned {
				continue
			}
			first := dec(f.Price).Mul(dec("0.8")).Div(dec("1000")).Floor().Mul(dec("1000"))
			last := dec(f.Price).Mul(dec("1.2"))
			if last.Sub(first).Div(dec("1000")).GreaterThan(dec("400")) {
				return nil, errors.New("热力图价格桶超限，请使用精确历史表")
			}
			for low := first; low.LessThan(last); low = low.Add(dec("1000")) {
				// A validated frame includes both full cohort totals. Empty bins
				// are observed zero supply; absent dates remain entirely blank.
				value, ok := byLow[low.String()]
				if !ok {
					value = "0"
				}
				cells = append(cells, []any{sm.Date, low.String(), value})
				if len(cells) > 25000 {
					return nil, errors.New("热力图单页超限，请缩小时间范围或分页")
				}
			}
		}
	}
	p := []CostPrice{}
	for d, v := range prices {
		if d >= costDate(from) && d <= f.Date {
			p = append(p, CostPrice{Date: d, Value: v})
		}
	}
	sort.Slice(p, func(i, j int) bool { return p[i].Date < p[j].Date })
	// Mark first discovery on its true calendar date, never on the prior signal day.
	eventRows, e := h.Store.onchain.db.QueryContext(ctx, "SELECT payload FROM events WHERE detected>=? AND detected<? ORDER BY detected LIMIT 1000", from.UnixNano(), end.AddDate(0, 0, 1).UnixNano())
	if e != nil {
		return nil, e
	}
	markers := []any{}
	for eventRows.Next() {
		var b []byte
		var event CostEvent
		if e = eventRows.Scan(&b); e != nil {
			eventRows.Close()
			return nil, e
		}
		if e = json.Unmarshal(b, &event); e != nil {
			eventRows.Close()
			return nil, e
		}
		markers = append(markers, map[string]any{"date": costDate(event.DetectedAt), "id": event.ID, "kind": event.Kind, "price": event.Price, "detectedAt": event.DetectedAt})
	}
	e = eventRows.Err()
	eventRows.Close()
	if e != nil {
		return nil, e
	}
	return costJSON(map[string]any{"from": costDate(from), "to": f.Date, "points": out, "prices": p, "cells": cells, "events": markers, "total": total, "offset": offset, "hasMore": start > 0, "low": low, "high": high, "note": "缺日留空；价格日线与成本快照的实际频率不同。历史均为来源目前可得修订，非当时预测。事件标线为实际发现日期；今天发现的事件在今日收盘纳入历史后显示。"})
}
