package datahub

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/url"
	"strconv"
	"strings"
	"time"
)

func (h *Hub) radarRead(ctx context.Context, path string, q url.Values) (json.RawMessage, error) {
	if h.Store.radar == nil {
		return json.Marshal(map[string]any{"available": false, "error": h.radarInitError, "events": []RadarEvent{}})
	}
	var result any
	var err error
	switch path {
	case "hl-radar/settings":
		result, err = h.radarSettingsView(ctx)
	case "hl-radar/status":
		result = h.radarStatus()
	case "hl-radar/wallet":
		a := radarAddress(q.Get("address"))
		if !publicAddress.MatchString(a) {
			return nil, errors.New("无效公开地址")
		}
		var w RadarWallet
		err = radarLoad(ctx, h.Store.radar.db, "wallet", a, &w)
		if err == sql.ErrNoRows {
			return json.Marshal(map[string]any{"wallet": nil, "note": "尚未达到后台核验条件；GET不触发上游请求"})
		}
		if err != nil {
			return nil, err
		}
		rows, e := h.Store.radar.db.QueryContext(ctx, "SELECT payload FROM fills WHERE address=? ORDER BY ts DESC LIMIT 100", a)
		if e != nil {
			return nil, e
		}
		fills := []radarFill{}
		for rows.Next() {
			var b []byte
			if err = rows.Scan(&b); err != nil {
				break
			}
			var f radarFill
			if err = json.Unmarshal(b, &f); err != nil {
				break
			}
			fills = append(fills, f)
		}
		if err == nil {
			err = rows.Err()
		}
		rows.Close()
		result = map[string]any{"wallet": w, "fills": fills, "scope": "最近100条本地已核验BTC/ETH成交；不是全部历史"}
	case "hl-radar", "hl-radar/events":
		result, err = h.radarEventsView(ctx, q)
	case "hl-radar/study":
		result, err = h.radarStudyView(ctx)
	default:
		return nil, errors.New("未知雷达接口")
	}
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(result)
	if len(b) > ResultLimit {
		return nil, errors.New("雷达结果超过查询上限")
	}
	return b, err
}
func (h *Hub) radarStatus() any {
	if h.Store.radar == nil {
		return map[string]any{"available": false, "error": h.radarInitError}
	}
	state := radarHealth{}
	if h.radar != nil {
		h.radar.mu.Lock()
		h.radar.weightLocked(time.Now())
		state = h.radar.health
		state.Candidates = len(h.radar.candidates)
		state.Queued = len(h.radar.queue)
		h.radar.mu.Unlock()
	}
	return map[string]any{"available": h.Store.radar != nil, "health": state, "offline": h.offline, "rulesVersion": RadarRules, "storageBytes": h.Store.radar.size(), "storageBudget": radarBudget, "storagePaused": h.Store.radar.size() >= radarBudget || h.Store.Status().Paused, "source": "Hyperliquid公开原生BTC/ETH永续", "note": "部分覆盖；断线期间未知地址的短暂开平仓可能遗漏，历史不足不等于没有活动", "targetSubmissionSeconds": 60, "restWeightLimit": 600, "queueLimit": 128, "candidateLimit": 5000}
}
func (h *Hub) radarEventsView(ctx context.Context, q url.Values) (any, error) {
	asset := q.Get("asset")
	if asset != "" && !ValidAsset(asset) {
		return nil, errors.New("仅支持BTC/ETH")
	}
	side := q.Get("side")
	if side != "" && side != "long" && side != "short" {
		return nil, errors.New("无效方向")
	}
	address := radarAddress(q.Get("address"))
	if address != "" && !publicAddress.MatchString(address) {
		return nil, errors.New("无效地址")
	}
	query := "SELECT payload FROM events WHERE 1=1"
	args := []any{}
	for _, p := range []struct{ k, v string }{{"asset", asset}, {"side", side}, {"address", address}} {
		if p.v != "" {
			query += " AND " + p.k + "=?"
			args = append(args, p.v)
		}
	}
	if cur := q.Get("before"); cur != "" {
		parts := strings.Split(cur, "|")
		if len(parts) != 2 || len(cur) > 100 {
			return nil, errors.New("无效分页游标")
		}
		ts, e := strconv.ParseInt(parts[0], 10, 64)
		if e != nil {
			return nil, e
		}
		query += " AND (opened<? OR (opened=? AND id<?))"
		args = append(args, ts, ts, parts[1])
	}
	limit := parseInt(q, "limit", 25, 1, 50)
	query += " ORDER BY opened DESC,id DESC LIMIT ?"
	args = append(args, limit+1)
	rows, e := h.Store.radar.db.QueryContext(ctx, query, args...)
	if e != nil {
		return nil, e
	}
	items := []RadarEvent{}
	for rows.Next() {
		var b []byte
		if e = rows.Scan(&b); e != nil {
			break
		}
		var v RadarEvent
		if e = json.Unmarshal(b, &v); e != nil {
			break
		}
		items = append(items, v)
	}
	if e == nil {
		e = rows.Err()
	}
	rows.Close()
	if e != nil {
		return nil, e
	}
	next := ""
	if len(items) > limit {
		items = items[:limit]
		v := items[len(items)-1]
		next = strconv.FormatInt(v.Opened.UnixMilli(), 10) + "|" + v.ID
	}
	mails := []map[string]any{}
	for _, v := range items {
		r, e := h.Store.research.QueryContext(ctx, `SELECT n.id,n.kind,n.status,n.attempted,r.completed,r.error FROM notices n LEFT JOIN mail_batch_items i ON i.notice_id=n.id LEFT JOIN mail_results r ON r.batch_id=i.batch_id WHERE n.signal_id=? AND n.kind LIKE 'hl-radar:%' ORDER BY n.created LIMIT 2`, v.ID)
		if e != nil {
			return nil, e
		}
		for r.Next() {
			var id, kind, status string
			var at int64
			var completed sql.NullInt64
			var failure sql.NullString
			if e = r.Scan(&id, &kind, &status, &at, &completed, &failure); e != nil {
				break
			}
			var done *time.Time
			if completed.Valid {
				t := time.Unix(0, completed.Int64).UTC()
				done = &t
			}
			mails = append(mails, map[string]any{"id": id, "eventId": v.ID, "kind": kind, "status": status, "attemptedAt": unixTimeOrNil(at), "completedAt": done, "error": failure.String})
		}
		if e == nil {
			e = r.Err()
		}
		r.Close()
		if e != nil {
			return nil, e
		}
	}
	settings, e := h.radarSettingsView(ctx)
	return map[string]any{"events": items, "next": next, "notifications": mails, "settings": settings, "status": h.radarStatus()}, e
}
