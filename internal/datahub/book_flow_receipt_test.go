package datahub

import (
	"context"
	"encoding/json"
	"regexp"
	"testing"
	"time"
)

func TestBookRealReceiptGateQuotaAndRecovery(t *testing.T) {
	h, now := radarTestHub(t)
	h.bookFlowMode = "run"
	ctx := context.Background()
	h.offline = false
	h.mail = &MailConfig{To: "receipt-test@example.invalid"}
	var body string
	calls := 0
	h.mailSend = func(_ context.Context, _ MailConfig, _, b string) error { calls++; body = b; return nil }
	result, e := h.BookFlowMailAcceptance(ctx, "test", "", now)
	if e != nil {
		t.Fatal(e)
	}
	if h.bookReceipt(ctx) {
		t.Fatal("SMTP acceptance incorrectly proved receipt")
	}
	match := regexp.MustCompile(`[0-9a-f]{32}`).FindString(body)
	if match == "" {
		t.Fatal("no inbox code")
	}
	raw, _ := json.Marshal(result)
	if string(raw) == "" {
		t.Fatal("no submission status")
	}
	if _, e = h.BookFlowMailAcceptance(ctx, "test", "", now.Add(time.Second)); e == nil || calls != 1 {
		t.Fatal("duplicate test attempt")
	}
	if _, e = h.BookFlowMailAcceptance(ctx, "confirm", "bad", now.Add(time.Second)); e == nil {
		t.Fatal("invalid confirmation accepted")
	}
	if _, e = h.SetBookFlowSettings(ctx, true, now); e == nil {
		t.Fatal("enabled without recipient evidence")
	}
	if _, e = h.BookFlowMailAcceptance(ctx, "confirm", match, now.Add(2*time.Second)); e != nil {
		t.Fatal(e)
	}
	if !h.bookReceipt(ctx) {
		t.Fatal("real inbox proof not accepted")
	}
	s, _ := h.bookSettings(ctx)
	if s.EmailEnabled {
		t.Fatal("confirmation silently enabled notifications")
	}
	if !h.bookReceipt(ctx) {
		t.Fatal("receipt lost across reload")
	}
	h.mail = &MailConfig{To: "changed@example.invalid"}
	if h.bookReceipt(ctx) {
		t.Fatal("receipt reused for different recipient")
	}
	total, _ := h.totalMailAttempts(ctx, now.Add(3*time.Second))
	old, _ := h.mailAttempts(ctx, now.Add(3*time.Second))
	if total != 1 || old != 1 {
		t.Fatal("test bypassed radar budget", total, old)
	}
	if e = h.processBookNotices(ctx, now.Add(3*time.Second)); e != nil {
		t.Fatal("test consumed as trading event", e)
	}
	if _, e = h.BookFlowMailAcceptance(ctx, "confirm", match, now.Add(2*time.Hour)); e == nil {
		t.Fatal("expired receipt code accepted")
	}
}
