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
	MarketPriceAt         *time.Time     `json:"marketPriceAt"`
	DiscoveredMarketPrice *string        `json:"discoveredMarketPrice"`
	PriceRevision         string         `json:"priceRevision,omitempty"`
	FirstConditionPrice   string         `json:"firstConditionPrice,omitempty"`
	ID                    string         `json:"id"`
	Kind                  string         `json:"kind"`
	Date                  string         `json:"date"`
	DetectedAt            time.Time      `json:"detectedAt"`
	Rules                 string         `json:"rulesVersion"`
	Revision              string         `json:"revision"`
	Price                 string         `json:"price"`
	Cycle                 string         `json:"cycle"`
	Zone                  *CostZone      `json:"zone"`
	Metrics               *CostMetrics   `json:"metrics"`
	Evidence              []CostEvidence `json:"evidence"`
	Note                  string         `json:"note"`
	NoticeStatus          string         `json:"noticeStatus,omitempty"`
	CaseID                string         `json:"caseId,omitempty"`
	EpisodeID             string         `json:"episodeId,omitempty"`
	Direction             string         `json:"direction,omitempty"`
	Group                 string         `json:"group,omitempty"`
	OccurredAt            *time.Time     `json:"occurredAt"`
	FirstSeen             *time.Time     `json:"firstSeen"`
	ValidatedAt           *time.Time     `json:"validatedAt"`
	PriceSource           string         `json:"priceSource,omitempty"`
	Shadow                bool           `json:"shadow"`
	FrozenAt              *time.Time     `json:"frozenAt"`
	SubmittedAt           *time.Time     `json:"submittedAt"`
	AttemptedAt           *time.Time     `json:"attemptedAt"`
	MarketPrice           *string        `json:"marketPrice"`
	BoundaryDistance      *string        `json:"boundaryDistancePercent"`
	DecisionDistance      *string        `json:"decisionDistancePercent"`
	DiscoveryDistance     *string        `json:"discoveryDistancePercent"`
}
type costWatch struct {
	Zone  CostZone `json:"zone"`
	State string   `json:"state"`
	Count int      `json:"count"`
}
type costState struct {
	InputKey          string      `json:"inputKey"`
	Rules             string      `json:"rulesVersion"`
	LastDate          string      `json:"lastDate"`
	Cycle             string      `json:"cycle"`
	FrozenAt          time.Time   `json:"frozenAt"`
	From              string      `json:"from"`
	Through           string      `json:"through"`
	Revision          string      `json:"revision"`
	Watches           []costWatch `json:"watches"`
	Compression       bool        `json:"compression"`
	Meet              int         `json:"meet"`
	Clear             int         `json:"clear"`
	Initial           bool        `json:"initial"`
	Cases             []CostCase  `json:"cases"`
	EpisodeID         string      `json:"episodeId"`
	StructureLastDate string      `json:"structureLastDate"`
	BaselineThrough   string      `json:"baselineThrough"`
	LowMeet           int         `json:"lowMeet"`
	LowClear          int         `json:"lowClear"`
	LowActive         bool        `json:"lowActive"`
	LowEpisode        string      `json:"lowEpisode"`
	LowLastDate       string      `json:"lowLastDate"`
	EnabledAt         time.Time   `json:"enabledAt"`
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
	s.Rules = onchainLegacyRules
	s.Cycle = costHash([]string{onchainLegacyRules, now.Format(time.RFC3339Nano), f.Revision})
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
		id := costHash([]string{onchainLegacyRules, s.Cycle, kind, f.Date, func() string {
			if z != nil {
				return z.Side
			}
			return ""
		}()})
		events = append(events, CostEvent{ID: id, Kind: kind, Date: f.Date, DetectedAt: now, Rules: onchainLegacyRules, Revision: f.Revision, Price: f.Price, Cycle: s.Cycle, Zone: z, Metrics: &m, Note: note})
	}
	if s.LastDate == "" || initial || s.Rules != "" && s.Rules != onchainLegacyRules {
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
	if e := st.controlReady(); e != nil {
		return s, e
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	h.noticeMu.Lock()
	defer h.noticeMu.Unlock()
	var previous CostSettings
	if e := costLoad(ctx, st.db, "settings", &previous); e != nil {
		return s, e
	}
	if previous.EmailEnabled == s.EmailEnabled {
		return s, nil
	}
	tx, e := st.db.BeginTx(ctx, nil)
	if e != nil {
		return s, e
	}
	defer tx.Rollback()
	if e = costSave(ctx, tx, "settings", s); e != nil {
		return s, e
	}
	if e = costSave(ctx, tx, "settings-boundary", map[string]any{"at": now}); e != nil {
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
	if e := st.controlReady(); e != nil {
		return e
	}
	_, e := h.Store.research.ExecContext(ctx, "UPDATE notices SET status='unknown_after_restart' WHERE kind LIKE 'onchain-cost:%' AND status='sending' AND attempted<=?", h.boot.Unix())
	if e != nil {
		return e
	}
	settings, e := h.CostSettings(ctx)
	if e != nil {
		return e
	}
	rows, e := st.db.QueryContext(ctx, "SELECT payload FROM outbox WHERE status='pending' ORDER BY CASE json_extract(payload,'$.kind') WHEN 'invalidated' THEN 0 WHEN 'confirmed' THEN 1 ELSE 2 END,created,id LIMIT 50")
	if e != nil {
		return e
	}
	events := []CostEvent{}
	for rows.Next() {
		var b []byte
		var event CostEvent
		if e = rows.Scan(&b); e != nil {
			break
		}
		if e = json.Unmarshal(b, &event); e != nil {
			break
		}
		events = append(events, event)
	}
	re := rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	if re != nil {
		return re
	}
	for _, event := range events {
		target, e := h.costNoticeEligibility(ctx, event, settings, now)
		if e != nil {
			return e
		}
		if target == "waiting_price" {
			continue
		}
		raw, _ := json.Marshal(event)
		if _, e = h.Store.research.ExecContext(ctx, "INSERT OR IGNORE INTO notices(id,signal_id,kind,created,status,payload) VALUES(?,?,?,?,?,?)", "cost-"+event.ID, event.ID, "onchain-cost:"+event.Kind, event.DetectedAt.Unix(), target, raw); e != nil {
			return e
		}
		if e = st.controlExec(ctx, "UPDATE outbox SET status='handed_off' WHERE id=? AND status='pending'", event.ID); e != nil {
			return e
		}
	}
	rows, e = h.Store.research.QueryContext(ctx, "SELECT id,payload,attempted FROM notices WHERE kind LIKE 'onchain-cost:%' AND status='pending' ORDER BY CASE kind WHEN 'onchain-cost:invalidated' THEN 0 WHEN 'onchain-cost:confirmed' THEN 1 ELSE 2 END,created,id LIMIT 50")
	if e != nil {
		return e
	}
	type queued struct {
		id        string
		event     CostEvent
		attempted int64
	}
	queue := []queued{}
	for rows.Next() {
		var q queued
		var raw []byte
		if e = rows.Scan(&q.id, &raw, &q.attempted); e != nil {
			break
		}
		if e = json.Unmarshal(raw, &q.event); e != nil {
			break
		}
		queue = append(queue, q)
	}
	re = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	if re != nil {
		return re
	}
	ids := []string{}
	bodies := ""
	for _, q := range queue {
		target, e := h.costNoticeEligibility(ctx, q.event, settings, now)
		if e != nil {
			return e
		}
		if q.attempted != 0 {
			target = "unknown_previous_attempt"
		}
		if target == "waiting_price" {
			continue
		}
		if target != "pending" {
			if _, e = h.Store.research.ExecContext(ctx, "UPDATE notices SET status=? WHERE id=? AND status='pending'", target, q.id); e != nil {
				return e
			}
			continue
		}
		event := q.event
		h.decorateCostEvent(&event, now)
		ids = append(ids, q.id)
		bodies += fmt.Sprintf("%s · %s · %s\r\n已完成日收盘：$%s（%s）\r\n判定：%s\r\n规则：%s\r\n%s\r\n", costEventName(event.Kind), event.Direction, event.Date, event.Price, event.PriceSource, event.DetectedAt.UTC().Format(time.RFC3339), event.Rules, event.Note)
		if event.Zone != nil {
			bodies += fmt.Sprintf("冻结成本区：$%s–$%s；完成日收盘返回边界以内或等于边界时失效。\r\n", event.Zone.Low, event.Zone.High)
		}
		if event.MarketPrice != nil {
			bodies += "当前美元折算参考价：$" + *event.MarketPrice + "\r\n"
		}
		if event.BoundaryDistance != nil {
			bodies += "距冻结区边界：" + *event.BoundaryDistance + "%\r\n"
		}
		if event.DiscoveryDistance != nil {
			bodies += "距首次条件价格：" + *event.DiscoveryDistance + "%\r\n"
		}
		for _, ev := range event.Evidence {
			bodies += fmt.Sprintf("%s：%s；%s UTC日；%s\r\n", ev.Title, ev.Status, ev.From, ev.Note)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	bodies += "价格条件不等于趋势或交易有效性；辅助证据缺失不代表零。SMTP接受不等于收件人收到。"
	if h.mail != nil && h.mail.DashboardURL != "" {
		bodies += "\r\n" + h.mail.DashboardURL + "#onchain-cost"
	}
	_, e = h.deliverNoticeBatch(ctx, now, ids, "TIDAL BTC 链上成本条件观察", bodies)
	return e
}
func (h *Hub) costNoticeEligibility(ctx context.Context, event CostEvent, settings CostSettings, now time.Time) (string, error) {
	if !settings.EmailEnabled || h.onchainDisabled || h.onchainEventsDisabled {
		return "suppressed_setting", nil
	}
	if event.Shadow {
		return "suppressed_shadow", nil
	}
	if now.Sub(event.DetectedAt) > 6*time.Hour || event.DetectedAt.After(now) {
		return "suppressed_expired", nil
	}
	s := h.Store.onchain
	var recovered struct {
		At time.Time `json:"at"`
	}
	if e := costLoad(ctx, s.db, "restore-boundary", &recovered); e != nil {
		return "", e
	}
	if !recovered.At.IsZero() && !event.DetectedAt.After(recovered.At) {
		return "suppressed_restore", nil
	}
	var changed struct {
		At time.Time `json:"at"`
	}
	if e := costLoad(ctx, s.db, "settings-boundary", &changed); e != nil {
		return "", e
	}
	if !changed.At.IsZero() && !event.DetectedAt.After(changed.At) {
		return "suppressed_setting", nil
	}
	if event.Rules != OnchainRules && event.CaseID == "" {
		return "suppressed_legacy_upgrade", nil
	}
	if s.writable() != nil {
		return "waiting_price", nil
	}
	if event.Kind == "concentrated" {
		feed, e := h.costFeed(ctx)
		if e != nil {
			return "", e
		}
		status, _ := costFeedStatus(feed, now)
		var state costState
		if e = costLoad(ctx, s.db, "observation", &state); e != nil {
			return "", e
		}
		if !state.Compression || state.EpisodeID != event.EpisodeID {
			return "suppressed_superseded", nil
		}
		if status != "fresh" {
			return "waiting_price", nil
		}
		return "pending", nil
	}
	if event.Kind != "confirmed" && event.Kind != "invalidated" {
		return "suppressed_nonmail", nil
	}
	c, e := s.caseByID(ctx, event.CaseID)
	if e != nil {
		return "", e
	}
	if c == nil {
		return "suppressed_missing_case", nil
	}
	if c.State != event.Kind {
		return "suppressed_superseded", nil
	}
	price, e := s.price(ctx, costDate(now.AddDate(0, 0, -1)), now)
	if e != nil {
		return "", e
	}
	if price == nil {
		return "waiting_price", nil
	}
	if event.Kind == "confirmed" && !costBeyond(*c, price.Value) {
		return "suppressed_superseded", nil
	}
	return "pending", nil
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
		var attempted int64
		var completed sql.NullInt64
		e = h.Store.research.QueryRowContext(ctx, `SELECT n.status,n.attempted,r.completed FROM notices n LEFT JOIN mail_batch_items i ON i.notice_id=n.id LEFT JOIN mail_results r ON r.batch_id=i.batch_id WHERE n.id=?`, "cost-"+id).Scan(&status, &attempted, &completed)
		if e == sql.ErrNoRows {
			status = "unknown_delivery_record_missing"
		} else if e != nil {
			return e
		}
		if status == "pending" || status == "sending" {
			continue
		}
		var raw []byte
		var event CostEvent
		if e = st.db.QueryRowContext(ctx, "SELECT payload FROM outbox WHERE id=?", id).Scan(&raw); e != nil {
			return e
		}
		if e = json.Unmarshal(raw, &event); e != nil {
			return e
		}
		if attempted > 0 {
			at := time.Unix(attempted, 0).UTC()
			event.AttemptedAt = &at
		}
		if status == "sent" && completed.Valid {
			at := time.Unix(0, completed.Int64).UTC()
			event.SubmittedAt = &at
		}
		event.NoticeStatus = status
		raw, _ = json.Marshal(event)
		if e = st.controlExec(ctx, "UPDATE outbox SET status=?,payload=? WHERE id=? AND status='handed_off'", status, raw, id); e != nil {
			return e
		}
	}

	return nil
}
func costEventName(kind string) string {
	if s, ok := map[string]string{"initialized": "开始观察", "pending": "收盘越界待确认", "confirmed": "连续两日收于成本区外", "invalidated": "确认失效", "unconfirmed": "未延续确认", "concentrated": "集中且波动偏低", "expired": "观察轮次到期", "gap": "日快照缺口", "revision": "来源修订", "discovery_expired": "发现窗口到期", "pending_expired": "待确认到期", "tracking_expired": "确认跟踪到期", "attention_near": "4小时接近边界", "attention_outside": "4小时收于边界之外", "attention_returned": "4小时参考价回到区间"}[kind]; ok {
		return s
	}
	return kind
}
