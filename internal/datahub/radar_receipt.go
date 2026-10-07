package datahub

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

type radarReceiptRecord struct {
	At        time.Time `json:"at"`
	Recipient string    `json:"recipientHash"`
}
type radarReceiptChallenge struct {
	ID        string    `json:"id"`
	At        time.Time `json:"at"`
	Hash      string    `json:"codeHash"`
	Recipient string    `json:"recipientHash"`
}

func (h *Hub) radarRecipient() string {
	if h.mail == nil {
		return ""
	}
	b := sha256.Sum256([]byte(strings.TrimSpace(strings.ToLower(h.mail.To))))
	return hex.EncodeToString(b[:])
}
func (h *Hub) loadRadarReceipt(ctx context.Context) {
	var record radarReceiptRecord
	if h.radar != nil && radarLoad(ctx, h.Store.radar.db, "receipt", "verified", &record) == nil && record.Recipient == h.radarRecipient() {
		h.radar.receipt.Store(record.At.UnixMilli())
	}
}

// Test messages contain no synthetic market event. Only a code from the actual
// recipient's inbox can acknowledge delivery; HTTP/SMTP success never does so.
func (h *Hub) RadarMailAcceptance(ctx context.Context, action, code string, now time.Time) (any, error) {
	if h.radar == nil || h.Store.radar == nil {
		return nil, errors.New("雷达存储不可用")
	}
	if h.mail == nil || h.offline {
		return nil, errors.New("需已配置SMTP且处于在线模式")
	}
	h.noticeMu.Lock()
	defer h.noticeMu.Unlock()
	switch action {
	case "confirm":
		var challenge radarReceiptChallenge
		if e := radarLoad(ctx, h.Store.radar.db, "receipt", "challenge", &challenge); e != nil {
			return nil, errors.New("尚无可确认的测试邮件")
		}
		if now.Before(challenge.At) || now.Sub(challenge.At) > time.Hour || challenge.Recipient != h.radarRecipient() {
			return nil, errors.New("验收码已过期或收件人已改变")
		}
		code = strings.TrimSpace(code)
		hash := sha256.Sum256([]byte(code))
		if len(code) != 32 || subtle.ConstantTimeCompare([]byte(hex.EncodeToString(hash[:])), []byte(challenge.Hash)) != 1 {
			return nil, errors.New("验收码不匹配，请使用最新测试邮件中的验收码")
		}
		if e := h.Store.radar.put(ctx, "receipt", "verified", now, radarReceiptRecord{now, challenge.Recipient}, false); e != nil {
			return nil, e
		}
		h.radar.receipt.Store(now.UnixMilli())
		return h.radarSettingsView(ctx)
	case "test":
		if code != "" {
			return nil, errors.New("测试请求不接受验收码")
		}
		var previous radarReceiptChallenge
		if e := radarLoad(ctx, h.Store.radar.db, "receipt", "challenge", &previous); e == nil && now.Sub(previous.At) < time.Minute {
			return nil, errors.New("一分钟内仅允许一次测试，请检查收件箱或稍后重试")
		} else if e != nil && e != sql.ErrNoRows {
			return nil, e
		}
		allowed, e := h.mailBudgetAllowed(ctx, now, "hl-radar")
		if e != nil {
			return nil, e
		}
		if !allowed {
			return nil, errors.New("雷达或全站滚动小时发送额度已用完")
		}
		raw := make([]byte, 16)
		if _, e = rand.Read(raw); e != nil {
			return nil, e
		}
		code = hex.EncodeToString(raw)
		sum := sha256.Sum256([]byte(code))
		id := "hl-radar/receipt/" + hex.EncodeToString(sum[:12])
		challenge := radarReceiptChallenge{id, now, hex.EncodeToString(sum[:]), h.radarRecipient()}
		if e = h.Store.radar.put(ctx, "receipt", "challenge", now, challenge, false); e != nil {
			return nil, e
		}
		payload, _ := json.Marshal(map[string]any{"purpose": "mail acceptance only", "at": now})
		if _, e = h.Store.research.ExecContext(ctx, "INSERT INTO notices(id,signal_id,kind,created,status,payload) VALUES(?,?,'hl-radar:acceptance',?,'pending',?)", id, id, now.Unix(), payload); e != nil {
			return nil, e
		}
		body := fmt.Sprintf("这是 TIDAL Hyperliquid 异常建仓雷达的真实收件测试，不是市场提醒，也没有生成任何示例持仓。\r\n\r\n验收码：%s\r\n\r\n请在雷达页面输入此码完成收件确认，随后可独立开启雷达邮件。验收码一小时有效；发送新测试后旧码失效。\r\n测试占用一次雷达/全站发送尝试；SMTP接受不等于收件箱收到。\r\n时间：%s", code, now.Format(time.RFC3339))
		sent, e := h.deliverTopicNoticeBatch(ctx, now, []string{id}, "【TIDAL】异常建仓雷达 · 收件验收", body, "hl-radar")
		if e != nil {
			return nil, fmt.Errorf("测试发送失败或结果不确定，不自动重发：%w", e)
		}
		if !sent {
			return nil, errors.New("测试未提交，请检查配置与额度")
		}
		return map[string]any{"status": "smtp_accepted", "testId": id, "message": "SMTP已接受，请从收件箱取得验收码；尚未认定签收"}, nil
	default:
		return nil, errors.New("仅支持test或confirm操作")
	}
}
