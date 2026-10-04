package datahub

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type CostEvent struct {
	ID           string         `json:"id"`
	Kind         string         `json:"kind"`
	Date         string         `json:"date"`
	DetectedAt   time.Time      `json:"detectedAt"`
	Rules        string         `json:"rulesVersion"`
	Revision     string         `json:"revision"`
	Price        string         `json:"price"`
	Cycle        string         `json:"cycle"`
	Zone         *CostZone      `json:"zone"`
	Metrics      *CostMetrics   `json:"metrics"`
	Evidence     []CostEvidence `json:"evidence"`
	Note         string         `json:"note"`
	NoticeStatus string         `json:"noticeStatus,omitempty"`
}
type costWatch struct {
	Zone  CostZone `json:"zone"`
	State string   `json:"state"`
	Count int      `json:"count"`
}
type costState struct {
	Rules       string      `json:"rulesVersion"`
	LastDate    string      `json:"lastDate"`
	Cycle       string      `json:"cycle"`
	FrozenAt    time.Time   `json:"frozenAt"`
	From        string      `json:"from"`
	Through     string      `json:"through"`
	Revision    string      `json:"revision"`
	Watches     []costWatch `json:"watches"`
	Compression bool        `json:"compression"`
	Meet        int         `json:"meet"`
	Clear       int         `json:"clear"`
	Initial     bool        `json:"initial"`
}

func costInsertEvent(ctx context.Context, tx *sql.Tx, event CostEvent, mail bool) error {
	b, e := json.Marshal(event)
	if e != nil {
		return e
	}
	r, e := tx.ExecContext(ctx, "INSERT OR IGNORE INTO events VALUES(?,?,?,?)", event.ID, event.DetectedAt.UnixNano(), event.Kind, b)
	if e != nil {
		return e
	}
	n, e := r.RowsAffected()
	if e != nil {
		return e
	}
	if mail && n > 0 {
		_, e = tx.ExecContext(ctx, "INSERT OR IGNORE INTO outbox VALUES(?,?,?,?)", event.ID, event.DetectedAt.UnixNano(), b, "pending")
	}
	return e
}
func costStartCycle(s *costState, f CostFrame, m CostMetrics, now time.Time) {
	s.Rules = OnchainRules
	s.Cycle = costHash([]string{OnchainRules, now.Format(time.RFC3339Nano), f.Revision})
	s.FrozenAt = now
	s.From = costDate(now.UTC().Truncate(24*time.Hour).AddDate(0, 0, 1))
	from, _ := costDay(s.From)
	s.Through = costDate(from.AddDate(0, 0, 6))
	s.Revision = f.Revision
	s.Watches = []costWatch{}
	for _, z := range m.Zones {
		s.Watches = append(s.Watches, costWatch{Zone: z, State: "watching"})
	}
}
func costCompressed(m CostMetrics) *bool {
	if m.ConcentrationRank == nil || m.VolatilityRank == nil {
		return nil
	}
	if *m.VolatilityRank <= 20 && dec(m.ConcentrationRank.Lower).LessThan(dec("80")) && dec(m.ConcentrationRank.Upper).GreaterThanOrEqual(dec("80")) {
		return nil // Boundary uncertainty cannot clear or rearm a known condition.
	}
	b := dec(m.ConcentrationRank.Lower).GreaterThanOrEqual(dec("80")) && *m.VolatilityRank <= 20
	return &b
}

// Pure transition: no current source revision can alter a frozen watch boundary.
func costAdvance(s *costState, f CostFrame, m CostMetrics, now time.Time, initial bool) []CostEvent {
	events := []CostEvent{}
	makeEvent := func(kind string, z *CostZone, note string) {
		id := costHash([]string{OnchainRules, s.Cycle, kind, f.Date, func() string {
			if z != nil {
				return z.Side
			}
			return ""
		}()})
		events = append(events, CostEvent{ID: id, Kind: kind, Date: f.Date, DetectedAt: now, Rules: OnchainRules, Revision: f.Revision, Price: f.Price, Cycle: s.Cycle, Zone: z, Metrics: &m, Note: note})
	}
	if s.LastDate == "" || initial || s.Rules != "" && s.Rules != OnchainRules {
		s.Meet, s.Clear, s.Compression = 0, 0, false
		costStartCycle(s, f, m, now)
		s.LastDate = f.Date
		s.Initial = true
		if b := costCompressed(m); b != nil && *b {
			s.Compression = true
		}
		makeEvent("initialized", nil, "初始状态已记录；冻结后下一个完整UTC日起观察，不补发历史提醒")
		return events
	}
	if f.Date <= s.LastDate {
		return events
	}
	last, _ := costDay(s.LastDate)
	consecutive := costDate(last.AddDate(0, 0, 1)) == f.Date
	if !consecutive {
		s.Meet = 0
		s.Clear = 0
		for i := range s.Watches {
			if s.Watches[i].State == "pending" {
				s.Watches[i].State = "watching"
				s.Watches[i].Count = 0
			}
		}
		makeEvent("gap", nil, "缺少连续日快照，连续确认计数中断")
	}
	if s.Through != "" && f.Date > s.Through {
		makeEvent("expired", nil, "七个完整UTC日观察轮次结束；原事件保留")
		costStartCycle(s, f, m, now)
	}
	if f.Date >= s.From && f.Date <= s.Through {
		for i := range s.Watches {
			w := &s.Watches[i]
			beyond := dec(f.Price).GreaterThan(dec(w.Zone.High))
			if w.Zone.Side == "below" {
				beyond = dec(f.Price).LessThan(dec(w.Zone.Low))
			}
			z := w.Zone
			switch w.State {
			case "watching", "pending":
				if beyond {
					w.Count++
					if w.Count >= 2 && consecutive {
						w.State = "confirmed"
						makeEvent("confirmed", &z, "连续两个完整UTC日收盘越过冻结边界；需独立查看成交与杠杆证据")
					} else {
						w.State = "pending"
						makeEvent("pending", &z, "首个完成日越过边界，等待下一日确认")
					}
				} else {
					if w.State == "pending" {
						makeEvent("unconfirmed", &z, "收盘返回边界以内，待确认条件未延续")
					}
					w.Count = 0
					w.State = "watching"
				}
			case "confirmed":
				if !beyond {
					w.State = "invalidated"
					makeEvent("invalidated", &z, "完成日收盘返回被突破边界以内，原确认失效")
				}
			}
		}
	}
	if b := costCompressed(m); b != nil {
		if *b {
			s.Clear = 0
			s.Meet++
			if s.Meet >= 2 && !s.Compression && consecutive {
				s.Compression = true
				makeEvent("concentrated", nil, "集中度周频参考P80及以上且收盘波动P20及以下连续两日；方向未确认")
			}
		} else {
			s.Meet = 0
			s.Clear++
			if s.Clear >= 2 {
				s.Compression = false
			}
		}
	} else {
		s.Meet = 0
		s.Clear = 0
	}
	s.LastDate = f.Date
	s.Initial = false
	return events
}
func (h *Hub) evaluateCostDay(ctx context.Context, now time.Time, initial bool) error {
	s := h.Store.onchain
	if e := s.writable(); e != nil {
		return e
	}
	f, e := s.frame(ctx, "", now)
	if e != nil || f == nil {
		return e
	}
	if f.Date != costDate(now.AddDate(0, 0, -1)) {
		return nil
	}
	var state costState
	if e = costLoad(ctx, s.db, "observation", &state); e != nil {
		return e
	}
	if state.LastDate >= f.Date {
		return nil
	}
	// A date retrieved after its live day is history, never a new live event.
	if f.Origin != "forward" && state.LastDate != "" {
		return nil
	}
	metrics, e := s.metrics(ctx, *f, now)
	if e != nil {
		return e
	}
	events := costAdvance(&state, *f, metrics, now, initial)
	evidence := h.costEvidence(ctx, f.Date, now)
	for i := range events {
		events[i].Evidence = evidence
	}
	var settings CostSettings
	if e = costLoad(ctx, s.db, "settings", &settings); e != nil {
		return e
	}
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	if e = costSave(ctx, tx, "observation", state); e != nil {
		return e
	}
	raw, e := json.Marshal(evidence)
	if e != nil {
		return e
	}
	if _, e = tx.ExecContext(ctx, "INSERT OR IGNORE INTO evidence VALUES(?,?,?)", f.Date, now.UnixNano(), raw); e != nil {
		return e
	}
	for _, event := range events {
		send := settings.EmailEnabled && (event.Kind == "confirmed" || event.Kind == "invalidated" || event.Kind == "concentrated")
		if e = costInsertEvent(ctx, tx, event, send); e != nil {
			return e
		}
	}
	return s.commit(ctx, tx)
}
func (h *Hub) CostSettings(ctx context.Context) (CostSettings, error) {
	var s CostSettings
	if e := h.Store.onchain.available(); e != nil {
		return s, e
	}
	e := costLoad(ctx, h.Store.onchain.db, "settings", &s)
	return s, e
}
func (h *Hub) SetCostSettings(ctx context.Context, s CostSettings, now time.Time) (CostSettings, error) {
	if s.EmailEnabled && h.mail == nil {
		return s, errors.New("SMTP尚未配置")
	}
	st := h.Store.onchain
	if e := st.available(); e != nil {
		return s, e
	}
	if s.EmailEnabled {
		if e := st.writable(); e != nil {
			return s, e
		}
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	h.noticeMu.Lock()
	defer h.noticeMu.Unlock()
	tx, e := st.db.BeginTx(ctx, nil)
	if e != nil {
		return s, e
	}
	defer tx.Rollback()
	if e = costSave(ctx, tx, "settings", s); e != nil {
		return s, e
	}
	// Toggle never replays earlier observations, including queued handoff intents.
	if _, e = tx.ExecContext(ctx, "UPDATE outbox SET status='suppressed_setting' WHERE status='pending'"); e != nil {
		return s, e
	}
	if e = tx.Commit(); e != nil {
		return s, e
	}
	st.epoch.Add(1) // In-flight old reads retain their old cache identity.
	_, e = h.Store.research.ExecContext(ctx, "UPDATE notices SET status='suppressed_setting' WHERE kind LIKE 'onchain-cost:%' AND status='pending'")
	return s, e
}
func (h *Hub) processCostNotices(ctx context.Context, now time.Time) (result error) {
	st := h.Store.onchain
	if st == nil || st.available() != nil {
		return nil
	}
	h.noticeMu.Lock()
	defer h.noticeMu.Unlock()
	defer func() {
		if e := h.archiveCostNoticeResults(ctx); result == nil {
			result = e
		}
	}()
	// The shared queue is authoritative after handoff; restart suppresses both sides.
	_, e := h.Store.research.ExecContext(ctx, "UPDATE notices SET status='unknown_after_restart' WHERE kind LIKE 'onchain-cost:%' AND status='sending' AND attempted<=?", h.boot.Unix())
	if e != nil {
		return e
	}
	if _, e = st.db.ExecContext(ctx, "UPDATE outbox SET status='suppressed_restart' WHERE status='pending' AND created<=?", h.boot.UnixNano()); e != nil {
		return e
	}
	settings, e := h.CostSettings(ctx)
	if e != nil {
		return e
	}
	feed, e := h.costFeed(ctx)
	if e != nil {
		return e
	}
	status, _ := costFeedStatus(feed, now)
	if st.writable() != nil {
		status = "capacity"
	}
	rows, e := st.db.QueryContext(ctx, "SELECT id,payload FROM outbox WHERE status='pending' ORDER BY created,id LIMIT 50")
	if e != nil {
		return e
	}
	events := []CostEvent{}
	for rows.Next() {
		var id string
		var b []byte
		if e = rows.Scan(&id, &b); e != nil {
			rows.Close()
			return e
		}
		var event CostEvent
		if e = json.Unmarshal(b, &event); e != nil {
			rows.Close()
			return e
		}
		events = append(events, event)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	for _, event := range events {
		target := "pending"
		if !settings.EmailEnabled || h.onchainDisabled || h.onchainEventsDisabled {
			target = "suppressed_setting"
		} else if status != "fresh" || now.Sub(event.DetectedAt) > 6*time.Hour {
			target = "suppressed_stale"
		}
		raw, _ := json.Marshal(event)
		if _, e = h.Store.research.ExecContext(ctx, "INSERT OR IGNORE INTO notices(id,signal_id,kind,created,status,payload) VALUES(?,?,?,?,?,?)", "cost-"+event.ID, event.ID, "onchain-cost:"+event.Kind, event.DetectedAt.Unix(), target, raw); e != nil {
			return e
		}
		if _, e = st.db.ExecContext(ctx, "UPDATE outbox SET status='handed_off' WHERE id=? AND status='pending'", event.ID); e != nil {
			return e
		}
	}
	rows, e = h.Store.research.QueryContext(ctx, "SELECT id,payload FROM notices WHERE kind LIKE 'onchain-cost:%' AND status='pending' ORDER BY created,id LIMIT 50")
	if e != nil {
		return e
	}
	ids := []string{}
	bodies := ""
	suppressed := map[string]string{}
	for rows.Next() {
		var id string
		var raw []byte
		if e = rows.Scan(&id, &raw); e != nil {
			rows.Close()
			return e
		}
		var event CostEvent
		if e = json.Unmarshal(raw, &event); e != nil {
			rows.Close()
			return e
		}
		if !event.DetectedAt.After(h.boot) {
			suppressed[id] = "suppressed_restart"
			continue
		}
		if !settings.EmailEnabled || h.onchainDisabled || h.onchainEventsDisabled || status != "fresh" || now.Sub(event.DetectedAt) > 6*time.Hour {
			suppressed[id] = "suppressed_stale_or_setting"
			continue
		}
		ids = append(ids, id)
		bodies += fmt.Sprintf("%s · %s\r\n同日收盘：$%s\r\n发现：%s\r\n规则：%s\r\n%s\r\n\r\n", costEventName(event.Kind), event.Date, event.Price, event.DetectedAt.UTC().Format(time.RFC3339), event.Rules, event.Note)
		if event.Zone != nil {
			bodies += fmt.Sprintf("冻结观察区：$%s–$%s（%s）；收盘返回外侧边界以内则原确认失效。\r\n", event.Zone.Low, event.Zone.High, event.Zone.Side)
		}
		for _, evidence := range event.Evidence {
			bodies += fmt.Sprintf("%s：%s；%s UTC日；%s\r\n", evidence.Title, evidence.Status, evidence.From, evidence.Note)
		}
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	for id, reason := range suppressed {
		if _, e = h.Store.research.ExecContext(ctx, "UPDATE notices SET status=? WHERE id=? AND status='pending'", reason, id); e != nil {
			return e
		}
	}
	if len(ids) == 0 {
		return nil
	}
	bodies += "链上最后移动成本观察，不是交易成本、确定性方向或交易建议。"
	if h.mail != nil && h.mail.DashboardURL != "" {
		bodies += "\r\n" + h.mail.DashboardURL + "#onchain-cost"
	}
	_, e = h.deliverNoticeBatch(ctx, now, ids, "TIDAL BTC 链上筹码观察", bodies)
	return e
}

// Preserve delivery outcomes in the long-lived audit store before the shared
// short-history queue eventually expires. Repeated cross-database copies are safe.
func (h *Hub) archiveCostNoticeResults(ctx context.Context) error {
	st := h.Store.onchain
	rows, e := st.db.QueryContext(ctx, "SELECT id FROM outbox WHERE status='handed_off' ORDER BY created LIMIT 100")
	if e != nil {
		return e
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if e = rows.Scan(&id); e != nil {
			rows.Close()
			return e
		}
		ids = append(ids, id)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	for _, id := range ids {
		var status string
		e = h.Store.research.QueryRowContext(ctx, "SELECT status FROM notices WHERE id=?", "cost-"+id).Scan(&status)
		if e == sql.ErrNoRows {
			status = "unknown_delivery_record_missing"
		} else if e != nil {
			return e
		}
		if status == "pending" || status == "sending" {
			continue
		}
		if _, e = st.db.ExecContext(ctx, "UPDATE outbox SET status=? WHERE id=? AND status='handed_off'", status, id); e != nil {
			return e
		}
	}
	return nil
}
func costEventName(kind string) string {
	if s, ok := map[string]string{"initialized": "开始观察", "pending": "突破待确认", "confirmed": "价格突破确认", "invalidated": "确认失效", "unconfirmed": "未延续确认", "concentrated": "集中且波动偏低", "expired": "观察轮次到期", "gap": "日快照缺口", "revision": "来源修订"}[kind]; ok {
		return s
	}
	return kind
}
