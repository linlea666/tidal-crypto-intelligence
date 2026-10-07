package datahub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/google/uuid"
)

// Legacy notices used one second-resolution attempted timestamp per batch.
// New batches have distinct IDs, so simultaneous BTC/VIX sends count separately.
func (h *Hub) totalMailAttempts(ctx context.Context, now time.Time) (int, error) {
	var n int
	err := h.Store.research.QueryRowContext(ctx, `SELECT
 (SELECT count(*) FROM mail_batches WHERE attempted>?) +
 (SELECT count(DISTINCT attempted) FROM notices n WHERE attempted>? AND NOT EXISTS (SELECT 1 FROM mail_batch_items i WHERE i.notice_id=n.id))`, now.Add(-time.Hour).UnixNano(), now.Add(-time.Hour).Unix()).Scan(&n)
	return n, err
}

// Caller holds noticeMu. Reserve the attempt and all its members atomically
// before SMTP; a crash can never make an ambiguous send eligible for retry.
func (h *Hub) deliverNoticeBatch(ctx context.Context, now time.Time, ids []string, title, body string) (bool, error) {
	return h.deliverTopicNoticeBatch(ctx, now, ids, title, body, "existing")
}
func (h *Hub) deliverTopicNoticeBatch(ctx context.Context, now time.Time, ids []string, title, body, topic string) (bool, error) {
	if h.offline || h.mail == nil || len(ids) == 0 {
		return false, nil
	}
	allowed, err := h.mailBudgetAllowed(ctx, now, topic)
	if err != nil || !allowed {
		return false, err
	}
	tx, err := h.Store.research.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	batch := uuid.NewString()
	if _, err = tx.ExecContext(ctx, "INSERT INTO mail_batches VALUES(?,?)", batch, now.UnixNano()); err != nil {
		return false, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO mail_batch_topics VALUES(?,?)", batch, topic); err != nil {
		return false, err
	}
	for _, id := range ids {
		r, e := tx.ExecContext(ctx, "UPDATE notices SET status='sending',attempted=? WHERE id=? AND status='pending'", now.Unix(), id)
		if e != nil {
			return false, e
		}
		n, e := r.RowsAffected()
		if e != nil {
			return false, e
		}
		if n != 1 {
			return false, nil
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO mail_batch_items VALUES(?,?)", id, batch); err != nil {
			return false, err
		}
	}
	if err = tx.Commit(); err != nil {
		return false, err
	}
	err = h.mailSend(ctx, *h.mail, title, body)
	status := "sent"
	if err != nil {
		status = "delivery_unknown"
		var rejected *mailSubmissionError
		if errors.As(err, &rejected) {
			status = rejected.status
		}
	}
	// The SMTP deadline must not prevent recording its outcome.
	persist, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resultTx, e := h.Store.research.BeginTx(persist, nil)
	if e != nil {
		return true, e
	}
	defer resultTx.Rollback()
	for _, id := range ids {
		if _, e := resultTx.ExecContext(persist, "UPDATE notices SET status=? WHERE id=? AND status='sending'", status, id); e != nil {
			return true, e
		}
	}
	message := ""
	if err != nil {
		message = err.Error()
	}
	if _, e = resultTx.ExecContext(persist, "INSERT INTO mail_results VALUES(?,?,?,?)", batch, time.Now().UTC().UnixNano(), status, message); e != nil {
		return true, e
	}
	if e = resultTx.Commit(); e != nil {
		return true, e
	}
	return true, err
}
func (h *Hub) wakeVIXMail() {
	select {
	case h.vixWake <- struct{}{}:
	default:
	}
}
func (h *Hub) vixMailWorker(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-h.vixWake:
		case <-ticker.C:
		}
		step, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := h.processVIXNotices(step, time.Now().UTC())
		cancel()
		if err != nil {
			_ = h.Store.SaveState("vix/mail-error", map[string]any{"at": time.Now().UTC(), "error": err.Error()})
		}
	}
}
func (h *Hub) processVIXNotices(ctx context.Context, now time.Time) error {
	h.noticeMu.Lock()
	defer h.noticeMu.Unlock()
	rows, err := h.Store.research.QueryContext(ctx, "SELECT id,payload FROM notices WHERE kind LIKE 'vix:%' AND status='pending' ORDER BY created,id LIMIT 100")
	if err != nil {
		return err
	}
	items := []noticePayload{}
	for rows.Next() {
		var id string
		var b []byte
		if err = rows.Scan(&id, &b); err != nil {
			break
		}
		var n noticePayload
		if err = json.Unmarshal(b, &n); err != nil {
			break
		}
		if n.Topic != "vix" || n.VIX == nil || n.ID != id {
			err = fmt.Errorf("VIX通知快照无效")
			break
		}
		items = append(items, n)
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return err
	}
	for _, n := range items {
		var state vixState
		if err = vixLoad(ctx, h.Store.research, "SELECT payload FROM vix_state WHERE id=1", &state); err != nil {
			return err
		}
		f, e := h.vixFeed(ctx, "sina")
		if e != nil {
			return e
		}
		quality, _ := vixQuality(f, now)
		status := ""
		var recordedValue string
		rowErr := h.Store.research.QueryRowContext(ctx, "SELECT value FROM vix_minute WHERE ts=?", n.VIX.ObservedAt.Unix()).Scan(&recordedValue)
		if !state.EmailEnabled {
			status = "disabled"
		} else if rowErr != nil || recordedValue != n.VIX.Value {
			status = "superseded"
		} else if !now.Before(n.Expires) || quality != "delayed" || state.CycleID != n.VIX.CycleID {
			status = "suppressed_stale"
		} else {
			level := vixLevel(f.Latest.Value)
			if level == "low" || level == "elevated" || n.VIX.Level == "priority" && level != "priority" {
				status = "suppressed_recovered"
			}
		}
		if status != "" {
			if _, err = h.Store.research.ExecContext(ctx, "UPDATE notices SET status=? WHERE id=? AND status='pending'", status, n.ID); err != nil {
				return err
			}
			continue
		}
		if h.mail == nil {
			if _, err = h.Store.research.ExecContext(ctx, "UPDATE notices SET status='unconfigured' WHERE id=? AND status='pending'", n.ID); err != nil {
				return err
			}
			continue
		}
		title, body := vixNoticeBody(*n.VIX, h.mail.DashboardURL)
		sent, e := h.deliverNoticeBatch(ctx, now, []string{n.ID}, title, body)
		if e != nil {
			return e
		}
		if !sent {
			return nil
		}
		_ = h.Store.SaveState("vix/mail-error", map[string]any{"at": now, "error": ""})
	}
	return nil
}
func vixNoticeBody(e VIXEvent, dashboard string) (string, string) {
	label, condition := "买入观察", "30 ≤ VIX ≤ 40"
	if e.Level == "priority" {
		label, condition = "重点买入观察", "VIX > 40"
	}
	title := fmt.Sprintf("【TIDAL·VIX】%s：%s（新浪延时）", label, e.Value)
	initial := "首次检测到本轮阈值"
	if e.Initial {
		initial = "首次启用时，最新有效行情已经达标"
	}
	body := fmt.Sprintf("美股%s · %s\r\nVIX：%s\r\n规则：%s\r\n交易日：%s\r\n行情时间：%s（北京时间）\r\n发现时间：%s（北京时间）\r\n观察到的数据时间差：%.1f分钟\r\n来源：新浪财经；来源声明行情至少延迟15分钟。\r\n本轮每档提醒一次，低于30连续观察30分钟后重新布防。\r\n用户自定观察规则，不表示底部已确认，也不是BTC/ETH买点；系统不自动交易。", label, initial, e.Value, condition, e.TradingDate, e.ObservedAt.In(vixShanghai).Format("2006-01-02 15:04:05"), e.DetectedAt.In(vixShanghai).Format("2006-01-02 15:04:05"), e.DetectedAt.Sub(e.ObservedAt).Minutes())
	if u, err := url.Parse(dashboard); err == nil && u.Scheme == "https" && u.Host != "" {
		u.Fragment = "vix"
		body += "\r\n查看页面：" + u.String()
	}
	return title, body
}
