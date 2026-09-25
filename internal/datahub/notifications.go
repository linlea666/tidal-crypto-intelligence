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
	"os"
	"strconv"
	"strings"
	"time"
)

type MailConfig struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`
	From     string `json:"from"`
	To       string `json:"to"`
	TLS      string `json:"tls"`
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
	return &c, nil
}
func (h *Hub) mailStatus() any {
	var lastError map[string]any
	_ = h.Store.LoadState("mail/error", &lastError)
	return map[string]any{"configured": h.mail != nil, "lastError": lastError, "limitPerHour": 6, "note": "站内记录始终可见；未配置邮件时不会补发旧事件。发送结果不确定时不自动重复发送。"}
}
func (h *Hub) queueNotice(s Signal, kind string, now time.Time) error {
	b, e := json.Marshal(map[string]any{"asset": s.Asset, "direction": s.Direction, "kind": kind, "at": s.At, "dataThrough": s.DataThrough, "id": s.ID})
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
		return errors.New("SMTP连接失败")
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(25 * time.Second))
	client, e := smtp.NewClient(conn, c.Host)
	if e != nil {
		return errors.New("SMTP握手失败")
	}
	defer client.Close()
	if c.TLS == "starttls" {
		if ok, _ := client.Extension("STARTTLS"); !ok {
			return errors.New("SMTP不支持STARTTLS")
		}
		if e = client.StartTLS(tlsConfig); e != nil {
			return errors.New("SMTP TLS握手失败")
		}
	}
	if e = client.Auth(smtp.PlainAuth("", c.Username, c.Password, c.Host)); e != nil {
		return errors.New("SMTP认证失败")
	}
	from, _ := mail.ParseAddress(c.From)
	to, _ := mail.ParseAddress(c.To)
	if e = client.Mail(from.Address); e != nil {
		return errors.New("SMTP发件人被拒绝")
	}
	if e = client.Rcpt(to.Address); e != nil {
		return errors.New("SMTP收件人被拒绝")
	}
	w, e := client.Data()
	if e != nil {
		return errors.New("SMTP DATA失败")
	}
	message := "From: " + from.String() + "\r\nTo: " + to.String() + "\r\nSubject: " + mime.QEncoding.Encode("UTF-8", subject) + "\r\nMIME-Version: 1.0\r\nContent-Type: text/plain; charset=UTF-8\r\n\r\n" + body
	if _, e = w.Write([]byte(message)); e != nil {
		return errors.New("SMTP发送结果不确定")
	}
	if e = w.Close(); e != nil {
		return errors.New("SMTP发送结果不确定")
	}
	_ = client.Quit()
	return nil
}
func (h *Hub) processNotices(ctx context.Context, now time.Time) error {
	if _, e := h.Store.research.ExecContext(ctx, "UPDATE notices SET status='suppressed_scope' WHERE status='pending' AND signal_id LIKE 'ETH-%'"); e != nil {
		return e
	}
	if h.mail == nil {
		return nil
	}
	// Crash-after-send is intentionally at-most-once: never repeat an ambiguous
	// SMTP delivery. Persisted in-flight notices become unknown on restart.
	if _, e := h.Store.research.ExecContext(ctx, "UPDATE notices SET status='unknown_after_restart' WHERE status='sending' AND attempted<?", h.boot.Unix()); e != nil {
		return e
	}
	if _, e := h.Store.research.ExecContext(ctx, "UPDATE notices SET status='suppressed_restart' WHERE status='pending' AND created<?", h.boot.Unix()); e != nil {
		return e
	}
	var attempts int
	if e := h.Store.research.QueryRowContext(ctx, "SELECT count(DISTINCT attempted) FROM notices WHERE attempted>?", now.Add(-time.Hour).Unix()).Scan(&attempts); e != nil {
		return e
	}
	if attempts >= 6 {
		return nil
	}
	rows, e := h.Store.research.QueryContext(ctx, "SELECT id,payload FROM notices WHERE status='pending' ORDER BY created LIMIT 100")
	if e != nil {
		return e
	}
	ids := []string{}
	bodies := []string{}
	for rows.Next() {
		var id string
		var b []byte
		if e = rows.Scan(&id, &b); e != nil {
			rows.Close()
			return e
		}
		var v struct {
			Asset     string
			Direction string
			Kind      string
			At        time.Time
		}
		if json.Unmarshal(b, &v) != nil || !researchAsset(v.Asset) {
			continue
		}
		side := "买方"
		if v.Direction == "sell" {
			side = "卖方"
		}
		phase := "资金异动"
		if v.Kind == "confirmed" {
			phase = "价格确认"
		}
		ids = append(ids, id)
		bodies = append(bodies, fmt.Sprintf("%s %s%s · %s", v.Asset, side, phase, v.At.In(time.FixedZone("CST", 8*3600)).Format("01-02 15:04")))
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	if len(ids) == 0 {
		return nil
	}
	tx, e := h.Store.research.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	for _, id := range ids {
		if _, e = tx.ExecContext(ctx, "UPDATE notices SET status='sending',attempted=? WHERE id=? AND status='pending'", now.Unix(), id); e != nil {
			tx.Rollback()
			return e
		}
	}
	if e = tx.Commit(); e != nil {
		return e
	}
	title := "TIDAL 资金异动提醒"
	if len(ids) > 1 {
		title = fmt.Sprintf("TIDAL 异动摘要（%d项）", len(ids))
	}
	e = sendMail(ctx, *h.mail, title, strings.Join(bodies, "\r\n")+"\r\n\r\n实验性成交描述，不是交易建议或胜率。请查看看板中的支持、冲突和缺失证据。")
	status := "sent"
	if e != nil {
		status = "delivery_unknown"
	}
	for _, id := range ids {
		if _, err := h.Store.research.ExecContext(ctx, "UPDATE notices SET status=? WHERE id=?", status, id); err != nil {
			return err
		}
	}
	return e
}
