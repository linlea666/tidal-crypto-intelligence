package datahub

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const BookFlowMailTemplate = "book-flow-two-stage-v1"

type BookFlowSettings struct {
	EmailEnabled bool       `json:"emailEnabled"`
	EnabledAt    *time.Time `json:"enabledAt"`
}
type bookNotice struct {
	ID          string     `json:"id"`
	EventID     string     `json:"eventId"`
	Stage       string     `json:"stage"`
	PublishedAt time.Time  `json:"publishedAt"`
	EnabledAt   *time.Time `json:"enabledAt"`
	Template    string     `json:"templateVersion"`
	Event       bookEvent  `json:"event"`
	Update      bookUpdate `json:"update"`
}

func (h *Hub) bookSettings(ctx context.Context) (BookFlowSettings, error) {
	var s BookFlowSettings
	e := h.Store.bookFlowLoad(ctx, "settings", BookFlowRules, &s)
	if e == sql.ErrNoRows {
		e = nil
	}
	return s, e
}
func (h *Hub) bookReceipt(ctx context.Context) bool {
	var r radarReceiptRecord
	return h.mail != nil && h.Store.bookFlowLoad(ctx, "receipt", BookFlowMailTemplate+"/verified", &r) == nil && r.Recipient == h.radarRecipient()
}
func (h *Hub) bookSettingsView(ctx context.Context) (any, error) {
	s, e := h.bookSettings(ctx)
	if e != nil {
		return nil, e
	}
	return map[string]any{"emailEnabled": s.EmailEnabled, "enabledAt": s.EnabledAt, "configured": h.mail != nil, "receiptVerified": h.bookReceipt(ctx), "limitPerHour": 2, "totalLimitPerHour": 6, "templateVersion": BookFlowMailTemplate, "mode": h.bookFlowMode}, nil
}
func (h *Hub) SetBookFlowSettings(ctx context.Context, enabled bool, now time.Time) (any, error) {
	h.noticeMu.Lock()
	defer h.noticeMu.Unlock()
	if enabled && (!h.bookReceipt(ctx) || h.bookFlowMode != "run") {
		return nil, errors.New("须运行前向研究并完成本版模板收件验收后开启邮件")
	}
	s, err := h.bookSettings(ctx)
	if err != nil {
		return nil, err
	}
	if s.EmailEnabled != enabled {
		s.EmailEnabled = enabled
		if enabled {
			s.EnabledAt = &now
		}
		if err = h.Store.bookFlowSave(ctx, "settings", BookFlowRules, now, s, false); err != nil {
			return nil, err
		}
		if _, err = h.Store.research.ExecContext(ctx, "UPDATE notices SET status='disabled' WHERE kind LIKE 'book-flow:%' AND status='pending'"); err != nil {
			return nil, err
		}
	}
	return h.bookSettingsView(ctx)
}

// Called inside the publication transaction. A stage and its outbox entry have
// one durable identity; changing settings cannot resurrect a suppressed stage.
func queueBookNotice(ctx context.Context, tx *researchTx, s BookFlowSettings, e bookEvent, u bookUpdate, published time.Time) error {
	if u.Kind != "persistent" && u.Kind != "absorption" {
		return nil
	}
	n := bookNotice{ID: e.ID + "/mail/" + u.Kind, EventID: e.ID, Stage: u.Kind, PublishedAt: published, EnabledAt: s.EnabledAt, Template: BookFlowMailTemplate, Event: e, Update: u}
	status := "pending"
	if !s.EmailEnabled || s.EnabledAt == nil {
		status = "disabled"
	} else if !published.After(*s.EnabledAt) {
		status = "suppressed_before_enable"
	}
	b, err := json.Marshal(n)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "INSERT OR IGNORE INTO notices(id,signal_id,kind,created,status,payload) VALUES(?,?,?,?,?,?)", n.ID, e.ID, "book-flow:"+u.Kind, published.Unix(), status, b)
	return err
}
func (h *Hub) processBookNotices(ctx context.Context, now time.Time) error {
	h.noticeMu.Lock()
	defer h.noticeMu.Unlock()
	if _, err := h.Store.research.ExecContext(ctx, "UPDATE notices SET status='unknown_after_restart' WHERE kind LIKE 'book-flow:%' AND status='sending' AND attempted<=?", h.boot.Unix()); err != nil {
		return err
	}
	notices, err := paperRows[bookNotice](ctx, h.Store.research, "SELECT payload FROM notices WHERE kind IN ('book-flow:persistent','book-flow:absorption') AND status='pending' ORDER BY created,id LIMIT 64")
	if err != nil {
		return err
	}
	if len(notices) == 0 {
		return nil
	}
	s, err := h.bookSettings(ctx)
	if err != nil {
		return err
	}
	blocked, err := h.bookInputsHealthy(ctx, now)
	if err != nil {
		return err
	}
	var pending int
	if err = h.Store.research.QueryRowContext(ctx, "SELECT count(*) FROM notices WHERE status='pending' AND (kind IS NULL OR kind NOT LIKE 'book-flow:%')").Scan(&pending); err != nil {
		return err
	}
	allowed, err := h.mailBudgetAllowed(ctx, now, "book-flow")
	if err != nil {
		return err
	}
	ids := []string{}
	body := []string{"市场观察提醒，效果验证中。以下线索不构成多空开仓指令。"}
	for _, n := range notices {
		status := ""
		var event bookEvent
		if err = h.Store.bookFlowLoad(ctx, "event", n.EventID, &event); err != nil {
			return err
		}
		switch {
		case !s.EmailEnabled || s.EnabledAt == nil:
			status = "disabled"
		case n.EnabledAt == nil || !n.EnabledAt.Equal(*s.EnabledAt) || !n.PublishedAt.After(*s.EnabledAt):
			status = "suppressed_before_enable"
		case n.PublishedAt.Before(h.boot):
			status = "suppressed_restart"
		case now.Before(n.PublishedAt) || now.Sub(n.PublishedAt) > 3*time.Minute:
			status = "suppressed_stale"
		case h.bookFlowMode != "run" || len(blocked) > 0:
			status = "suppressed_inputs"
		case !event.CompletePath:
			status = "suppressed_evidence"
		case n.Stage == "absorption" && (n.Update.Evidence == nil || !n.Update.Evidence.Absorption || len(n.Update.Evidence.Minutes) != 5 || len(n.Update.Evidence.BeforeLevels) == 0 || len(n.Update.Evidence.AfterLevels) == 0):
			status = "suppressed_evidence"
		case event.Ended || !now.Before(event.Expires):
			status = "suppressed_region"
		case h.mail == nil || !h.bookReceipt(ctx):
			status = "unconfigured"
		case pending > 0:
			status = "suppressed_existing_priority"
		case !allowed:
			status = "suppressed_rate_limit"
		}
		// Recheck the live fixed region at send time; a worker tick can lag a
		// new snapshot that already faded or moved through the price boundary.
		if status == "" {
			o, _ := h.Store.Latest(ID("book", "BTC", event.Venue, "spot"))
			p := buildBookPoint(o, h.bookRate(now), now)
			z := zoneIn(p, event.Zone)
			if z == nil || z.Quantity == nil || event.Zone.Quantity == nil || dec(*z.Quantity).LessThanOrEqual(dec(*event.Zone.Quantity).Mul(dec("0.5"))) {
				status = "suppressed_region"
			}
			if p.Reference == nil || event.Direction == "buy" && dec(*p.Reference).LessThan(dec(event.Zone.Low)) || event.Direction == "sell" && dec(*p.Reference).GreaterThanOrEqual(dec(event.Zone.High)) {
				status = "suppressed_region"
			}
		}
		if status != "" {
			if _, err = h.Store.research.ExecContext(ctx, "UPDATE notices SET status=? WHERE id=? AND status='pending'", status, n.ID); err != nil {
				return err
			}
			continue
		}
		ids = append(ids, n.ID)
		body = append(body, bookNoticeText(n))
	}
	if len(ids) == 0 {
		return nil
	}
	_, err = h.deliverTopicNoticeBatch(ctx, now, ids, "【TIDAL】挂单与承接 · 市场观察提醒，效果验证中", strings.Join(body, "\r\n\r\n"), "book-flow")
	return err
}
func bookNoticeText(n bookNotice) string {
	e := n.Event
	stage := "挂单持续增强（尚未证明成交承接）"
	if n.Stage == "absorption" {
		stage = "承接 / 抛压升级（观察线索）"
	}
	value := func(p *string) string {
		if p == nil {
			return "未知"
		}
		return *p
	}
	body := fmt.Sprintf("%s · %s · %s\r\n固定价区 %s—%s USDT，增量 %s USD，倍数 %s\r\n源时间 %s；阶段发布时间 %s\r\n区域状态：有效；输入证据完整。", stage, e.Venue, e.Direction, e.Zone.Low, e.Zone.High, value(e.Zone.IncreaseUSD), value(e.Zone.Multiple), n.Update.SourceAt.Format(time.RFC3339), n.PublishedAt.Format(time.RFC3339))
	if n.Update.Evidence != nil {
		v := n.Update.Evidence
		body += fmt.Sprintf("\r\n成交窗口 %s—%s；确定主动买入 %s / 卖出 %s USDT；深度保持 %s；证据取得 %s", v.From.Format(time.RFC3339), v.To.Format(time.RFC3339), v.Buy, v.Sell, value(v.Retention), v.Acquired.Format(time.RFC3339))
	}
	return body + "\r\n模板 " + n.Template + "；事件 " + n.EventID
}
func bookMailTrial(ctx context.Context, tx *researchTx, id string, accepted time.Time) error {
	var raw []byte
	if err := tx.QueryRowContext(ctx, "SELECT payload FROM notices WHERE id=?", id).Scan(&raw); err != nil {
		return err
	}
	var n bookNotice
	if err := json.Unmarshal(raw, &n); err != nil {
		return err
	}
	if n.Stage != "persistent" && n.Stage != "absorption" {
		return nil
	}
	key := n.EventID + "/mail-" + n.Stage
	t := newShortTrial(key, BookFlowRules+"/mail-"+n.Stage, n.Event.Direction, accepted, n.Update.SourceAt, nil)
	t.Outcomes = append([]ShortOutcome{{Minutes: 5, State: "pending"}}, t.Outcomes...)
	b, err := json.Marshal(t)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "INSERT OR IGNORE INTO book_flow VALUES('trial',?,?,?,?)", key, n.EventID, accepted.Unix(), b)
	return err
}

func (h *Hub) bookNoticeViews(ctx context.Context, event string) ([]any, error) {
	rows, err := h.Store.research.QueryContext(ctx, `SELECT n.id,n.signal_id,n.kind,n.created,n.status,n.attempted,json_extract(n.payload,'$.templateVersion'),json_extract(n.payload,'$.enabledAt'),r.completed,r.status
 FROM notices n LEFT JOIN mail_batch_items i ON i.notice_id=n.id LEFT JOIN mail_results r ON r.batch_id=i.batch_id
 WHERE n.kind LIKE 'book-flow:%' AND (?='' OR n.signal_id=?) ORDER BY n.created DESC,n.id LIMIT 20`, event, event)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []any{}
	for rows.Next() {
		var id, parent, kind, status string
		var created, attempted int64
		var template, enabled, result sql.NullString
		var completed sql.NullInt64
		if err = rows.Scan(&id, &parent, &kind, &created, &status, &attempted, &template, &enabled, &completed, &result); err != nil {
			return nil, err
		}
		var attempt, done *time.Time
		if attempted > 0 {
			t := time.Unix(attempted, 0).UTC()
			attempt = &t
		}
		if completed.Valid {
			t := time.Unix(0, completed.Int64).UTC()
			done = &t
		}
		out = append(out, map[string]any{"id": id, "eventId": parent, "kind": kind, "createdAt": time.Unix(created, 0).UTC(), "status": status, "attemptedAt": attempt, "completedAt": done, "templateVersion": template.String, "enabledAt": enabled.String, "deliveryResult": result.String})
	}
	return out, rows.Err()
}
