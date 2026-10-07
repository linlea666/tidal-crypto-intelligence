package datahub

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"
)

// Existing topics retain their shared six attempts. Radar owns the other six.
// Legacy rows have no topic, and always remain in the existing pool.
func (h *Hub) mailAttempts(ctx context.Context, now time.Time) (int, error) {
	total, e := h.totalMailAttempts(ctx, now)
	if e != nil {
		return 0, e
	}
	var radar int
	e = h.Store.research.QueryRowContext(ctx, "SELECT count(*) FROM mail_batches b JOIN mail_batch_topics t ON t.batch_id=b.id WHERE b.attempted>? AND t.topic='hl-radar'", now.Add(-time.Hour).UnixNano()).Scan(&radar)
	return total - radar, e
}
func (h *Hub) mailBudgetAllowed(ctx context.Context, now time.Time, topic string) (bool, error) {
	total, e := h.totalMailAttempts(ctx, now)
	if e != nil {
		return false, e
	}
	old, e := h.mailAttempts(ctx, now)
	if e != nil {
		return false, e
	}
	if topic == "hl-radar" {
		return total < 12 && total-old < 6, nil
	}
	if topic != "existing" {
		return false, errors.New("未知邮件额度主题")
	}
	return total < 12 && old < 6, nil
}
func (h *Hub) radarReceipt(now time.Time) bool {
	return h.mail != nil && h.mail.RadarReceiptVerifiedAt != nil && !h.mail.RadarReceiptVerifiedAt.IsZero() && !h.mail.RadarReceiptVerifiedAt.After(now)
}
func (h *Hub) radarSettings(ctx context.Context) (RadarSettings, error) {
	var s RadarSettings
	e := radarLoad(ctx, h.Store.radar.db, "settings", "mail", &s)
	if e == sql.ErrNoRows {
		e = nil
	}
	return s, e
}
func (h *Hub) SetRadarSettings(ctx context.Context, s RadarSettings, now time.Time) (any, error) {
	if h.Store.radar == nil {
		return nil, errors.New("雷达存储不可用")
	}
	if s.EmailEnabled && !h.radarReceipt(now) {
		return nil, errors.New("需先完成真实收件验收并记录SMTP配置中的radarReceiptVerifiedAt")
	}
	h.noticeMu.Lock()
	defer h.noticeMu.Unlock()
	if e := h.Store.radar.put(ctx, "settings", "mail", now, s, false); e != nil {
		return nil, e
	}
	if !s.EmailEnabled {
		if _, e := h.Store.research.ExecContext(ctx, "UPDATE notices SET status='disabled' WHERE kind LIKE 'hl-radar:%' AND status='pending'"); e != nil {
			return nil, e
		}
	}
	return h.radarSettingsView(ctx)
}
func (h *Hub) radarSettingsView(ctx context.Context) (any, error) {
	s, e := h.radarSettings(ctx)
	if e != nil {
		return nil, e
	}
	return map[string]any{"emailEnabled": s.EmailEnabled, "configured": h.mail != nil, "receiptVerified": h.radarReceipt(time.Now()), "offline": h.offline, "limitPerHour": 6, "existingLimitPerHour": 6, "totalLimitPerHour": 12}, nil
}
func (h *Hub) radarExport(ctx context.Context, now time.Time) error {
	rows, e := h.Store.radar.db.QueryContext(ctx, "SELECT id,payload FROM outbox WHERE exported=0 ORDER BY at,id LIMIT 32")
	if e != nil {
		return e
	}
	items := []radarNotice{}
	for rows.Next() {
		var id string
		var b []byte
		if e = rows.Scan(&id, &b); e != nil {
			break
		}
		var n radarNotice
		if e = json.Unmarshal(b, &n); e != nil {
			break
		}
		if n.ID != id {
			e = errors.New("雷达outbox身份不一致")
			break
		}
		items = append(items, n)
	}
	if e == nil {
		e = rows.Err()
	}
	rows.Close()
	if e != nil {
		return e
	}
	s, e := h.radarSettings(ctx)
	if e != nil {
		return e
	}
	var enabledAt int64
	if e = h.Store.radar.db.QueryRowContext(ctx, "SELECT at FROM records WHERE kind='settings' AND id='mail'").Scan(&enabledAt); e != nil && e != sql.ErrNoRows {
		return e
	}
	for _, n := range items {
		status := "pending"
		switch {
		case !n.At.After(h.boot):
			status = "suppressed_restart"
		case !now.Before(n.Expires):
			status = "suppressed_expired"
		case !s.EmailEnabled || n.At.UnixMilli() < enabledAt:
			status = "disabled"
		case !h.radarReceipt(now):
			status = "unconfigured"
		}
		b, _ := json.Marshal(n)
		if _, e = h.Store.research.ExecContext(ctx, "INSERT OR IGNORE INTO notices(id,signal_id,kind,created,status,payload) VALUES(?,?,?,?,?,?)", n.ID, n.Event.ID, "hl-radar:"+n.Kind, n.At.Unix(), status, b); e != nil {
			return e
		}
		if _, e = h.Store.radar.db.ExecContext(ctx, "UPDATE outbox SET exported=1 WHERE id=?", n.ID); e != nil {
			return e
		}
	}
	return nil
}
func (h *Hub) radarProcessNotices(ctx context.Context, now time.Time) error {
	h.noticeMu.Lock()
	defer h.noticeMu.Unlock()
	for _, pair := range [][2]string{{"sending", "unknown_after_restart"}, {"pending", "suppressed_restart"}} {
		if _, e := h.Store.research.ExecContext(ctx, "UPDATE notices SET status=? WHERE kind LIKE 'hl-radar:%' AND status=? AND created<=?", pair[1], pair[0], h.boot.Unix()); e != nil {
			return e
		}
	}
	if err := h.radarExport(ctx, now); err != nil {
		return err
	}
	rows, e := h.Store.research.QueryContext(ctx, "SELECT payload FROM notices WHERE kind LIKE 'hl-radar:%' AND status='pending' ORDER BY created,id LIMIT 32")
	if e != nil {
		return e
	}
	items := []radarNotice{}
	for rows.Next() {
		var b []byte
		if e = rows.Scan(&b); e != nil {
			break
		}
		var n radarNotice
		if e = json.Unmarshal(b, &n); e != nil {
			break
		}
		items = append(items, n)
	}
	if e == nil {
		e = rows.Err()
	}
	rows.Close()
	if e != nil {
		return e
	}
	s, e := h.radarSettings(ctx)
	if e != nil {
		return e
	}
	ids := []string{}
	bodies := []string{}
	groupRendered := map[string]bool{}
	eventRendered := map[string]bool{}
	// A group upgrade represents pending member openings in this SMTP batch.
	// Link every consumed notice to the batch, but render the group only once.
	sort.SliceStable(items, func(i, j int) bool { return items[i].Kind == "priority" && items[j].Kind != "priority" })
	title := "新发现地址百万级开仓"
	for _, n := range items {
		status := ""
		v, err := radarReadEvent(ctx, h.Store.radar.db, n.Event.ID)
		_, _, fx := h.Rate("USDC", now)
		switch {
		case !s.EmailEnabled:
			status = "disabled"
		case !h.radarReceipt(now):
			status = "unconfigured"
		case !now.Before(n.Expires):
			status = "suppressed_expired"
		case err != nil:
			return err
		case v.Closed != nil:
			status = "suppressed_closed"
		case !v.Verified || now.Sub(v.Through) > 90*time.Second || !fx:
			continue
		}
		if status != "" {
			if _, e = h.Store.research.ExecContext(ctx, "UPDATE notices SET status=? WHERE id=? AND status='pending'", status, n.ID); e != nil {
				return e
			}
			continue
		}
		ids = append(ids, n.ID)
		if eventRendered[v.ID] {
			continue
		}
		eventRendered[v.ID] = true
		if v.Group != "" && groupRendered[v.Group] {
			continue
		}
		if v.Group != "" {
			groupRendered[v.Group] = true
			n.Event.Group = v.Group
			n.Event.Members = v.Members
			title = "多地址同步建仓"
		} else if n.Kind == "priority" && title != "多地址同步建仓" {
			title = "快速入金与敞口集中"
		}
		body := radarMailBody(n, h.mail.DashboardURL)
		body += "\r\n计划提交时间：" + now.Format(time.RFC3339)
		if n.Event.Threshold != nil {
			body += fmt.Sprintf("；距门槛 %.1f 秒（排队与限流计入）", now.Sub(*n.Event.Threshold).Seconds())
		}
		bodies = append(bodies, body)
	}
	if len(ids) == 0 {
		return nil
	}
	_, e = h.deliverTopicNoticeBatch(ctx, now, ids, fmt.Sprintf("【TIDAL·%s】%d组事实", title, len(bodies)), strings.Join(bodies, "\r\n\r\n"), "hl-radar")
	return e
}
func radarMailBody(n radarNotice, dashboard string) string {
	e := n.Event
	label := "新发现地址百万级开仓"
	if n.Kind == "priority" {
		label = "快速入金与敞口集中"
	}
	if e.Group != "" {
		label = "多地址同步建仓（金额与账户字段属于下列代表地址；同步成员另列）"
	}
	value := "未知"
	if e.USDCents != nil {
		value = fmt.Sprintf("%.2f 美元", float64(*e.USDCents)/100)
	}
	body := fmt.Sprintf("%s\r\n地址：%s\r\n%s %s · 本轮建仓／加仓\r\n当前名义仓位：%s\r\n本轮已核验开仓成交：%s USDC\r\n均价：%s USDC\r\n钱包历史：%s，30天范围完整=%t（不是全链创建时间）\r\n设置杠杆：%d；该永续账户总名义／权益：%s\r\n外部入金：%s；敞口集中=%t\r\n触发证据：%s\r\n市场背景：%s\r\n同步地址：%s\r\n开仓：%s\r\n门槛时间：%s\r\n发现：%s\r\n核验：%s\r\n数据截止：%s\r\n公开行为观察，不认定内幕交易；其他平台对冲及跨链原始资金来源未知。", label, e.Address, e.Asset, e.Side, value, e.OpenedNative, e.Entry, e.Age, e.HistoryComplete, e.ConfiguredLeverage, radarString(e.EffectiveLeverage), radarCents(e.DepositCents), e.Concentrated, strings.Join(e.Reasons, "；"), strings.Join(e.Context, "；"), strings.Join(e.Members, ", "), e.Opened.Format(time.RFC3339), radarStamp(e.Threshold), e.Detected.Format(time.RFC3339), e.Updated.Format(time.RFC3339), e.Through.Format(time.RFC3339))
	if u, err := url.Parse(dashboard); err == nil && u.Scheme == "https" && u.Host != "" {
		u.Fragment = "hl-radar"
		body += "\r\n看板：" + u.String()
	}
	return body
}
func radarString(p *string) string {
	if p == nil {
		return "未知"
	}
	return *p
}
func radarCents(p *int64) string {
	if p == nil {
		return "未知"
	}
	return fmt.Sprintf("%.2f 美元", float64(*p)/100)
}
func radarStamp(p *time.Time) string {
	if p == nil {
		return "未知"
	}
	return p.Format(time.RFC3339)
}
func (h *Hub) radarBackground(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	lastMaintenance := time.Time{}
	lastPrune := time.Time{}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now().UTC()
			step, cancel := context.WithTimeout(ctx, 8*time.Second)
			e := h.radarGroups(step, now)
			cancel()
			h.radarError(e, false)
			step, cancel = context.WithTimeout(ctx, 35*time.Second)
			e = h.radarProcessNotices(step, time.Now().UTC())
			cancel()
			h.radarError(e, false)
			if now.Sub(lastMaintenance) >= time.Minute {
				step, cancel = context.WithTimeout(ctx, 5*time.Second)
				e = h.radarStudy(step, now)
				cancel()
				h.radarError(e, false)
				lastMaintenance = now
			}
			if now.Sub(lastPrune) >= 10*time.Minute {
				lastPrune = now
				step, cancel = context.WithTimeout(ctx, 2*time.Second)
				e = h.Store.radar.maintain(step, now)
				if e == nil {
					_, e = h.Store.research.ExecContext(step, "DELETE FROM notices WHERE kind LIKE 'hl-radar:%' AND created<?", now.Add(-90*24*time.Hour).Unix())
				}
				if e == nil {
					h.radar.mu.Lock()
					saved := h.radar.health
					h.radar.mu.Unlock()
					e = h.Store.radar.put(step, "health", "last", now, saved, false)
				}
				cancel()
				h.radarError(e, false)
			}
		}
	}
}
