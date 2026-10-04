package datahub

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"net"
	"net/mail"
	"net/smtp"
	"net/textproto"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type CandidateMailApproval struct {
	ValidationID        string    `json:"validationId"`
	StudyID             string    `json:"studyId"`
	Rules               string    `json:"rulesVersion"`
	Evaluation          string    `json:"evaluationVersion"`
	ReviewedAt          time.Time `json:"reviewedAt"`
	ReceiptVerifiedAt   time.Time `json:"receiptVerifiedAt"`
	EqualBudgetReviewed bool      `json:"equalBudgetReviewed"`
	PerformanceAccepted bool      `json:"performanceAccepted"`
}
type MailConfig struct {
	DashboardURL      string                 `json:"dashboardUrl"`
	CandidateApproval *CandidateMailApproval `json:"candidateApproval,omitempty"`
	Host              string                 `json:"host"`
	Port              int                    `json:"port"`
	Username          string                 `json:"username"`
	Password          string                 `json:"password"`
	From              string                 `json:"from"`
	To                string                 `json:"to"`
	TLS               string                 `json:"tls"`
}

func LoadMailConfig(path string) (*MailConfig, error) {
	if path == "" {
		return nil, nil
	}
	b, e := os.ReadFile(path)
	if os.IsNotExist(e) {
		return nil, nil
	}
	if e != nil {
		return nil, errors.New("邮件配置不可读取")
	}
	var c MailConfig
	if json.Unmarshal(b, &c) != nil {
		return nil, errors.New("邮件配置格式错误")
	}
	if c.Host == "" || strings.ContainsAny(c.Host, "\r\n/: ") || c.Port < 1 || c.Port > 65535 || c.Username == "" || c.Password == "" {
		return nil, errors.New("邮件配置缺少服务器/端口/认证")
	}
	for _, a := range []string{c.From, c.To} {
		if _, e := mail.ParseAddress(a); e != nil || strings.ContainsAny(a, "\r\n") {
			return nil, errors.New("邮件地址格式错误")
		}
	}
	if c.TLS != "tls" && c.TLS != "starttls" {
		return nil, errors.New("邮件仅支持TLS或STARTTLS")
	}
	if c.DashboardURL != "" {
		u, e := url.Parse(c.DashboardURL)
		if e != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || strings.ContainsAny(c.DashboardURL, "\r\n") {
			return nil, errors.New("看板入口必须是无认证信息的HTTPS地址")
		}
	}
	return &c, nil
}
func (h *Hub) mailStatus() any {
	var lastError map[string]any
	_ = h.Store.LoadState("mail/error", &lastError)
	var status string
	var attempted int64
	_ = h.Store.research.QueryRow("SELECT status,attempted FROM notices WHERE attempted>0 ORDER BY attempted DESC,rowid DESC LIMIT 1").Scan(&status, &attempted)
	var activeError any = lastError
	var errorAt time.Time
	if raw, ok := lastError["at"].(string); ok {
		errorAt, _ = time.Parse(time.RFC3339Nano, raw)
	}
	// A later successful submission resolves an old SMTP failure, but must
	// not hide a newer worker/persistence error or one with unknown timing.
	if status == "sent" && !errorAt.IsZero() && errorAt.Before(time.Unix(attempted, 0)) {
		activeError = nil
	}
	return map[string]any{"candidateEnabled": h.candidateMailAllowed(context.Background(), time.Now().UTC()), "multifactorEnabled": h.mail != nil, "configured": h.mail != nil, "lastError": activeError, "historicalError": lastError, "latestStatus": status, "lastAttemptAt": unixTimeOrNil(attempted), "limitPerHour": 6, "note": "新双向规则采用早期异动、价格确认两阶段邮件，效果验证中。SMTP已接受不等于用户已收到；不补发过期或重启积压，发送结果不确定时不自动重发。"}
}

// A failure before DATA cannot have submitted the message. Errors after DATA
// remain ambiguous unless SMTP explicitly rejects its final response.
type mailSubmissionError struct{ message, status string }

func (e *mailSubmissionError) Error() string { return e.message }
func mailRejected(message string) error {
	return &mailSubmissionError{message, "failed_before_submission"}
}
func (h *Hub) queueNotice(s Signal, kind string, now time.Time) error {
	if !researchAsset(s.Asset) || s.Rules == CandidateRules && (!h.candidateMailAllowed(context.Background(), now) || s.Level != "strong" || (kind != "strong" && kind != "confirmed")) {
		return nil
	}
	b, e := json.Marshal(noticeSnapshot(s, kind))
	if e != nil {
		return e
	}
	status := "pending"
	if h.mail == nil {
		status = "unconfigured"
	}
	_, e = h.Store.research.Exec("INSERT OR IGNORE INTO notices(id,signal_id,kind,created,status,payload) VALUES(?,?,?,?,?,?)", s.ID+"/"+kind, s.ID, kind, now.Unix(), status, b)
	return e
}
func sendMail(ctx context.Context, c MailConfig, subject, body string) error {
	addr := net.JoinHostPort(c.Host, strconv.Itoa(c.Port))
	tlsConfig := &tls.Config{ServerName: c.Host, MinVersion: tls.VersionTLS12}
	var conn net.Conn
	var e error
	dialer := net.Dialer{Timeout: 12 * time.Second}
	if c.TLS == "tls" {
		d := tls.Dialer{NetDialer: &dialer, Config: tlsConfig}
		conn, e = d.DialContext(ctx, "tcp", addr)
	} else {
		conn, e = dialer.DialContext(ctx, "tcp", addr)
	}
	if e != nil {
		return mailRejected("SMTP连接失败")
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(25 * time.Second))
	client, e := smtp.NewClient(conn, c.Host)
	if e != nil {
		return mailRejected("SMTP握手失败")
	}
	defer client.Close()
	if c.TLS == "starttls" {
		if ok, _ := client.Extension("STARTTLS"); !ok {
			return mailRejected("SMTP不支持STARTTLS")
		}
		if e = client.StartTLS(tlsConfig); e != nil {
			return mailRejected("SMTP TLS握手失败")
		}
	}
	if e = client.Auth(smtp.PlainAuth("", c.Username, c.Password, c.Host)); e != nil {
		return mailRejected("SMTP认证失败")
	}
	from, _ := mail.ParseAddress(c.From)
	to, _ := mail.ParseAddress(c.To)
	if e = client.Mail(from.Address); e != nil {
		return mailRejected("SMTP发件人被拒绝")
	}
	if e = client.Rcpt(to.Address); e != nil {
		return mailRejected("SMTP收件人被拒绝")
	}
	w, e := client.Data()
	if e != nil {
		return mailRejected("SMTP DATA失败")
	}
	message := "From: " + from.String() + "\r\nTo: " + to.String() + "\r\nSubject: " + mime.QEncoding.Encode("UTF-8", subject) + "\r\nMIME-Version: 1.0\r\nContent-Type: text/plain; charset=UTF-8\r\n\r\n" + body
	if _, e = w.Write([]byte(message)); e != nil {
		return errors.New("SMTP发送结果不确定")
	}
	if e = w.Close(); e != nil {
		var rejection *textproto.Error
		if errors.As(e, &rejection) && rejection.Code >= 400 && rejection.Code < 600 {
			return &mailSubmissionError{"SMTP明确拒绝邮件", "rejected"}
		}
		return errors.New("SMTP发送结果不确定")
	}
	_ = client.Quit()
	return nil
}
func (h *Hub) processNotices(ctx context.Context, now time.Time) error {
	h.noticeMu.Lock()
	defer h.noticeMu.Unlock()
	if _, e := h.Store.research.ExecContext(ctx, "UPDATE notices SET status='suppressed_scope' WHERE status='pending' AND signal_id LIKE 'ETH-%'"); e != nil {
		return e
	}
	if h.mail == nil {
		return nil
	}
	// Crash-after-send is intentionally at-most-once: never repeat an ambiguous
	// SMTP delivery. Persisted in-flight notices become unknown on restart.
	if _, e := h.Store.research.ExecContext(ctx, "UPDATE notices SET status='unknown_after_restart' WHERE (kind IS NULL OR (kind NOT LIKE 'vix:%' AND kind NOT LIKE 'onchain-cost:%')) AND status='sending' AND attempted<=?", h.boot.Unix()); e != nil {
		return e
	}
	if _, e := h.Store.research.ExecContext(ctx, "UPDATE notices SET status='suppressed_restart' WHERE (kind IS NULL OR (kind NOT LIKE 'vix:%' AND kind NOT LIKE 'onchain-cost:%')) AND status='pending' AND created<=?", h.boot.Unix()); e != nil {
		return e
	}
	attempts, e := h.mailAttempts(ctx, now)
	if e != nil {
		return e
	}
	if attempts >= 6 {
		return nil
	}
	candidateAllowed := h.candidateMailAllowed(ctx, now)
	var cutover time.Time
	cutoverActive := h.Store.LoadState("signals/multifactor-cutover", &cutover)
	rows, e := h.Store.research.QueryContext(ctx, "SELECT id,payload FROM notices WHERE status='pending' AND (kind IS NULL OR (kind NOT LIKE 'vix:%' AND kind NOT LIKE 'onchain-cost:%')) ORDER BY created LIMIT 100")
	if e != nil {
		return e
	}
	suppressed := []string{}
	ids := []string{}
	bodies := []string{}
	titles := []string{}
	for rows.Next() {
		var id string
		var b []byte
		if e = rows.Scan(&id, &b); e != nil {
			rows.Close()
			return e
		}
		var v noticePayload
		if json.Unmarshal(b, &v) != nil || !researchAsset(v.Asset) {
			continue
		}
		expires := v.Expires
		if expires.IsZero() {
			expires = v.At.Add(4 * time.Hour)
		}
		legacyTail := cutoverActive && v.Rules != MultifactorRules && v.Kind == "confirmed" && !v.At.After(cutover)
		if !now.Before(expires) || now.Sub(v.DataThrough) > 12*time.Minute || v.Rules == CandidateRules && !candidateAllowed && !legacyTail || cutoverActive && v.Rules != MultifactorRules && !legacyTail {
			suppressed = append(suppressed, id)
			continue
		}
		ids = append(ids, id)
		bodies = append(bodies, noticeBody(v, h.mail.DashboardURL))
		titles = append(titles, flowNoticeTitle(v))
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	for _, id := range suppressed {
		if _, e = h.Store.research.ExecContext(ctx, "UPDATE notices SET status='suppressed_expired_or_validation' WHERE id=? AND status='pending'", id); e != nil {
			return e
		}
	}
	if len(ids) == 0 {
		return nil
	}
	title := titles[0]
	if len(ids) > 1 {
		title = fmt.Sprintf("TIDAL 异动摘要（%d项）", len(ids))
	}
	_, e = h.deliverNoticeBatch(ctx, now, ids, title, strings.Join(bodies, "\r\n")+"\r\n\r\n实验性成交描述，不是交易建议或胜率。请查看看板中的支持、冲突和缺失证据。")
	return e
}

type noticePayload struct {
	Topic       string    `json:"topic,omitempty"`
	VIX         *VIXEvent `json:"vix,omitempty"`
	Asset       string    `json:"asset"`
	Direction   string    `json:"direction"`
	Kind        string    `json:"kind"`
	At          time.Time `json:"at"`
	DataThrough time.Time `json:"dataThrough"`
	Expires     time.Time `json:"expiresAt"`
	ID          string    `json:"id"`
	Rules       string    `json:"rulesVersion"`
	Signal      *Signal   `json:"signal,omitempty"`
}

func noticeSnapshot(s Signal, kind string) noticePayload {
	through := s.DataThrough
	if kind == "strong" && s.Upgrade != nil {
		through = s.Upgrade.DataThrough
	}
	if kind == "confirmed" && s.ConfirmedThrough != nil {
		through = *s.ConfirmedThrough
	}
	return noticePayload{Asset: s.Asset, Direction: s.Direction, Kind: kind, At: s.At, DataThrough: through, Expires: s.Expires, ID: s.ID, Rules: s.Rules, Signal: &s}
}
func (h *Hub) candidateMailAllowed(ctx context.Context, now time.Time) bool {
	if h.mail == nil || h.mail.CandidateApproval == nil || h.mail.DashboardURL == "" {
		return false
	}
	a := h.mail.CandidateApproval
	var study Study
	var snapshot StudyValidation
	if a.StudyID == "" || a.ValidationID == "" || h.Store.document(ctx, "study", a.StudyID, &study) != nil || study.Pipeline != studyPipeline || study.ValidationID != a.ValidationID || h.Store.document(ctx, "study-validation", a.ValidationID, &snapshot) != nil || snapshot.StudyID != a.StudyID || snapshot.Rules != CandidateRules || snapshot.Evaluation != EvaluationVersion || snapshot.CalculatedAt.After(a.ReviewedAt) {
		return false
	}
	if a.Rules != CandidateRules || a.Evaluation != EvaluationVersion || !a.EqualBudgetReviewed || !a.PerformanceAccepted || a.ReviewedAt.IsZero() || a.ReceiptVerifiedAt.IsZero() || a.ReviewedAt.After(now) || a.ReceiptVerifiedAt.After(now) {
		return false
	}
	var r ForwardReport
	if h.Store.document(ctx, "forward-report", "BTC", &r) != nil || r.Evaluation != EvaluationVersion || !r.CandidateReady || r.CandidateOrigin == nil || now.Sub(r.To) > 2*time.Hour || r.To.After(now) {
		return false
	}
	return !a.ReviewedAt.Before(r.CandidateOrigin.Add(14 * 24 * time.Hour))
}
func noticeBody(v noticePayload, dashboard string) string {
	if v.Signal != nil && v.Signal.Multifactor != nil {
		return multifactorNoticeBody(v, dashboard)
	}
	body := fmt.Sprintf("%s %s · %s\r\n发现：%s\r\n数据截止：%s", v.Asset, v.Direction, v.Kind, v.At.Format(time.RFC3339), v.DataThrough.Format(time.RFC3339))
	if s := v.Signal; s != nil {
		f := s.Features
		if v.Kind == "strong" && s.Upgrade != nil {
			f = &s.Upgrade.Features
		}
		if f != nil {
			body += fmt.Sprintf("\r\n主动净买入：1小时 %.2f USD / 4小时 %.2f USD\r\n量比 %.2f / 买入占比 %.1f%% / 价格阶段 %s / 位移偏大 %t", float64(f.Net1H)/100, float64(f.Net4H)/100, f.VolumeRatio, f.BuyShare, f.Stage, f.Extended)
			if f.FuturesNet1H != nil {
				body += fmt.Sprintf("\r\n合约1小时净主动买入 %.2f USD", float64(*f.FuturesNet1H)/100)
			}
			if f.FundingAt != nil {
				body += "\r\n资金费率（各自结算周期，采样 " + f.FundingAt.Format(time.RFC3339) + "）："
				for _, rate := range f.Funding {
					body += fmt.Sprintf(" %s %s%%", rate.Venue, rate.RatePercent)
				}
			}
			if f.Liquidation != nil {
				body += fmt.Sprintf("\r\n已发生清算（最近已闭合1分钟）：多单 %s USD / 空单 %s USD；截止 %s", f.Liquidation.Long, f.Liquidation.Short, f.LiquidationAt.Add(time.Minute).Format(time.RFC3339))
			}
			if f.OIChange != nil {
				body += fmt.Sprintf("\r\n美元OI变化 %.2f%%（含价格影响，不等同新增资金）", *f.OIChange)
			}
		} else {
			body += fmt.Sprintf("\r\n15分钟主动净买卖 %.2f USD", float64(s.Net15)/100)
		}
		body += "\r\n支持：" + strings.Join(s.Evidence, "；") + "\r\n冲突：" + strings.Join(s.Conflicts, "；") + "\r\n限制：" + strings.Join(s.Missing, "；")
	}
	if dashboard != "" {
		body += "\r\n看板：" + dashboard
	}
	return body
}
