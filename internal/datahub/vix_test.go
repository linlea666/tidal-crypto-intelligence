package datahub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func vixTestHub(t *testing.T) *Hub {
	t.Helper()
	h, err := Open(Config{Root: t.TempDir(), Offline: true, Mail: &MailConfig{To: "observer@example.invalid", DashboardURL: "https://dashboard.invalid/"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Store.Close() })
	h.offline = false // No workers are run; all SMTP and upstream calls are trapped.
	h.mailSend = func(context.Context, MailConfig, string, string) error { t.Fatal("unexpected mail"); return nil }
	h.vixFetch = func(context.Context, string) ([]byte, error) { t.Fatal("unexpected upstream IO"); return nil, nil }
	return h
}
func vixSample(at time.Time, value string) VIXPoint {
	return VIXPoint{at, value, at.In(vixNewYork).Format("2006-01-02")}
}
func vixTestAt() time.Time { return testTime("2026-09-25T14:00:00Z") }
func vixEnable(t *testing.T, h *Hub, now time.Time) {
	t.Helper()
	if _, e := h.SetVIXSettings(context.Background(), VIXSettings{true}, now); e != nil {
		t.Fatal(e)
	}
}
func vixPush(t *testing.T, h *Hub, at time.Time, value string) {
	t.Helper()
	if e := h.ingestVIX(context.Background(), []VIXPoint{vixSample(at, value)}, at.Add(15*time.Minute)); e != nil {
		t.Fatal(e)
	}
}
func vixCount(t *testing.T, h *Hub, table string) int {
	t.Helper()
	var n int
	if e := h.Store.research.QueryRow("SELECT count(*) FROM " + table).Scan(&n); e != nil {
		t.Fatal(e)
	}
	return n
}
func vixStateForTest(t *testing.T, h *Hub) vixState {
	t.Helper()
	var s vixState
	if e := vixLoad(context.Background(), h.Store.research, "SELECT payload FROM vix_state WHERE id=1", &s); e != nil {
		t.Fatal(e)
	}
	return s
}

func TestVIXSinaContractMidnightAndDST(t *testing.T) {
	body := `{"code":0,"result":{"status":{"code":0},"data":[["15:15","15.6100","0.0000","0","2026-09-25","15.6800"],["23:59","31.0000","0","0"],["00:00","30.9900","0","0"],["04:14","14.9000","0","0"]]}}`
	p, e := parseVIXMinute([]byte(body), testTime("2026-09-26T04:00:00Z"))
	if e != nil {
		t.Fatal(e)
	}
	if len(p) != 4 || p[0].Value != "15.61" || p[3].At != testTime("2026-09-25T20:14:00Z") || p[3].TradingDate != "2026-09-25" {
		t.Fatal(p)
	}
	winter := `{"code":0,"result":{"status":{"code":0},"data":[["16:15","21.10","0","0","2026-01-09","21"],["05:14","21.20","0","0"]]}}`
	// A sparse jump past midnight is not proof of a rollover; require the
	// actual boundary observations rather than inventing missing timestamps.
	if _, e = parseVIXMinute([]byte(winter), testTime("2026-01-10T06:00:00Z")); e == nil {
		t.Fatal("ambiguous midnight accepted")
	}
	winter = strings.Replace(winter, `["05:14"`, `["23:59","21.10","0","0"],["00:00","21.10","0","0"],["05:14"`, 1)
	p, e = parseVIXMinute([]byte(winter), testTime("2026-01-10T06:00:00Z"))
	if e != nil {
		t.Fatal(e)
	}
	if p[0].At != testTime("2026-01-09T08:15:00Z") || p[len(p)-1].At != testTime("2026-01-09T21:14:00Z") {
		t.Fatal(p)
	}
	for _, tc := range []struct {
		at   string
		open bool
	}{
		{"2026-03-06T08:15:00Z", true}, {"2026-03-09T07:15:00Z", true}, {"2026-11-02T08:15:00Z", true},
		{"2026-09-26T14:00:00Z", false}, {"2026-09-25T13:27:00Z", false}, {"2026-09-25T20:16:00Z", false},
	} {
		if vixSession(testTime(tc.at)) != tc.open {
			t.Fatal(tc)
		}
	}
	if vixNextPoll(testTime("2026-09-25T07:10:00Z")) != 5*time.Minute {
		t.Fatal("slow poll misses opening")
	}
}
func TestVIXSinaRejectsBadData(t *testing.T) {
	good := `{"code":0,"result":{"status":{"code":0},"data":[["22:00","30.00","0","0","2026-09-25","20"],["22:01","31.00","0","0"]]}}`
	for _, b := range []string{
		`{}`, `{"code":0,"result":{"status":{"code":0},"data":[]}}`, strings.Replace(good, `"code":0`, `"code":1`, 1),
		strings.Replace(good, `"30.00"`, `"NaN"`, 1), strings.Replace(good, `"30.00"`, `"0"`, 1), strings.Replace(good, `"30.00"`, `"-1"`, 1),
		strings.Replace(good, `"22:01"`, `"21:59"`, 1), strings.Replace(good, `"22:01"`, `"22:00"`, 1),
		strings.Replace(good, `"2026-09-25"`, `"2026-09-26"`, 1), strings.Replace(good, `"2026-09-25"`, `"bad"`, 1),
		strings.Replace(good, `"22:01"`, `"99:99"`, 1), strings.Replace(good, `"30.00"`, `"1e10000"`, 1),
	} {
		if _, e := parseVIXMinute([]byte(b), testTime("2026-09-25T15:00:00Z")); e == nil {
			t.Fatal("accepted", b)
		}
	}
	duplicate := strings.Replace(strings.Replace(good, `"22:01"`, `"22:00"`, 1), `"31.00"`, `"30.00"`, 1)
	if p, e := parseVIXMinute([]byte(duplicate), testTime("2026-09-25T15:00:00Z")); e != nil || len(p) != 1 {
		t.Fatal(p, e)
	}
}
func TestVIXDailyContractAndFreshness(t *testing.T) {
	b := "DATE,OPEN,HIGH,LOW,CLOSE\n09/24/2026,15.83,16.57,15.34,15.67\n09/25/2026,15.61,15.94,14.68,14.87\n"
	legacy := strings.Replace(b, "DATE,OPEN,HIGH,LOW,CLOSE\n", "DATE,OPEN,HIGH,LOW,CLOSE\n02/11/1992,19.24,18.57,17.61,17.70\n", 1)
	if p, e := parseVIXDaily([]byte(legacy), testTime("2026-09-27T00:00:00Z")); e != nil || len(p) != 2 {
		t.Fatal("unused legacy anomaly blocked current window", e)
	}
	p, e := parseVIXDaily([]byte(b), testTime("2026-09-27T00:00:00Z"))
	if e != nil || len(p) != 2 || p[1].Close != "14.87" {
		t.Fatal(p, e)
	}
	for _, bad := range []string{strings.Replace(b, "15.94", "14.50", 1), b + "09/25/2026,15,16,14,15\n", strings.Replace(b, "CLOSE", "PRICE", 1), strings.Replace(b, "14.87", "null", 1)} {
		if _, e := parseVIXDaily([]byte(bad), testTime("2026-09-27T00:00:00Z")); e == nil {
			t.Fatal("bad daily accepted")
		}
	}
	if _, e := parseVIXDaily([]byte(b), testTime("2026-09-25T15:00:00Z")); e == nil {
		t.Fatal("future close")
	}
	at := vixTestAt()
	now := at.Add(15 * time.Minute)
	pnt := vixSample(at, "31")
	f := VIXFeed{Source: "sina", Latest: &pnt, FetchedAt: &now}
	if s, _ := vixQuality(f, now); s != "delayed" {
		t.Fatal(s)
	}
	if s, _ := vixQuality(f, now.Add(3*time.Minute)); s != "stale" {
		t.Fatal("fetch expiry", s)
	}
	later := at.Add(26 * time.Minute)
	f.FetchedAt = &later
	if s, _ := vixQuality(f, later); s != "stale" {
		t.Fatal("HTTP success refreshed old data", s)
	}
	f.Error = "source error"
	if s, _ := vixQuality(f, now); s != "error" {
		t.Fatal(s)
	}
	f.Error = ""
	if s, _ := vixQuality(f, testTime("2026-09-27T10:00:00Z")); s != "closed" {
		t.Fatal(s)
	}
}
func TestVIXThresholdsCyclesAndDelivery(t *testing.T) {
	h := vixTestHub(t)
	at := vixTestAt()
	ctx := context.Background()
	var titles []string
	h.mailSend = func(_ context.Context, _ MailConfig, title, body string) error {
		titles = append(titles, title)
		if !strings.Contains(body, "至少延迟15分钟") || !strings.Contains(body, "#vix") {
			t.Fatal(body)
		}
		return nil
	}
	vixEnable(t, h, at.Add(15*time.Minute))
	for i, v := range []string{"29.99", "30", "40", "40.01", "39", "41"} {
		p := at.Add(time.Duration(i) * time.Minute)
		vixPush(t, h, p, v)
		if e := h.processVIXNotices(ctx, p.Add(15*time.Minute)); e != nil {
			t.Fatal(e)
		}
	}
	if len(titles) != 2 || !strings.Contains(titles[0], "买入观察") || !strings.Contains(titles[1], "重点") {
		t.Fatal(titles)
	}
	if vixCount(t, h, "vix_events") != 2 {
		t.Fatal("duplicate band")
	}
	// Thirty minutes of consecutive source-time observations re-arm; 29 do not.
	for i := 6; i <= 35; i++ {
		vixPush(t, h, at.Add(time.Duration(i)*time.Minute), "29")
	}
	if vixStateForTest(t, h).CycleID == "" {
		t.Fatal("early recovery")
	}
	vixPush(t, h, at.Add(36*time.Minute), "29")
	if vixStateForTest(t, h).CycleID != "" {
		t.Fatal("not rearmed")
	}
	vixPush(t, h, at.Add(37*time.Minute), "31")
	if vixCount(t, h, "vix_events") != 3 {
		t.Fatal("new episode absent")
	}
}
func TestVIXDirectJumpDisabledAndInitialEnable(t *testing.T) {
	h := vixTestHub(t)
	at := vixTestAt()
	ctx := context.Background()
	vixPush(t, h, at, "42") // Observation while disabled is visible, never replayed.
	var first VIXEvent
	var b []byte
	if e := h.Store.research.QueryRow("SELECT payload FROM vix_events").Scan(&b); e != nil {
		t.Fatal(e)
	}
	json.Unmarshal(b, &first)
	var state string
	h.Store.research.QueryRow("SELECT status FROM notices WHERE id=?", first.ID).Scan(&state)
	if state != "disabled" {
		t.Fatal(state)
	}
	vixEnable(t, h, at.Add(15*time.Minute))
	if vixCount(t, h, "vix_events") != 2 {
		t.Fatal("first opt-in current condition missing")
	}
	s := vixStateForTest(t, h)
	if !s.Watch || !s.Priority {
		t.Fatal(s)
	}
	var pending int
	h.Store.research.QueryRow("SELECT count(*) FROM notices WHERE status='pending'").Scan(&pending)
	if pending != 1 {
		t.Fatal(pending)
	}
	if _, e := h.SetVIXSettings(ctx, VIXSettings{false}, at.Add(16*time.Minute)); e != nil {
		t.Fatal(e)
	}
	vixPush(t, h, at.Add(2*time.Minute), "39")
	vixEnable(t, h, at.Add(18*time.Minute))
	vixPush(t, h, at.Add(4*time.Minute), "43")
	h.Store.research.QueryRow("SELECT count(*) FROM notices WHERE status='pending'").Scan(&pending)
	if pending != 0 {
		t.Fatal("re-enable replayed old episode")
	}
}
func TestVIXRecoveryGapsErrorsAndSameTimestamp(t *testing.T) {
	h := vixTestHub(t)
	at := vixTestAt()
	ctx := context.Background()
	vixEnable(t, h, at.Add(15*time.Minute))
	vixPush(t, h, at, "31")
	for i := 1; i <= 20; i++ {
		vixPush(t, h, at.Add(time.Duration(i)*time.Minute), "29")
	}
	vixPush(t, h, at.Add(27*time.Minute), "29")
	if s := vixStateForTest(t, h); s.LowSince == nil || !s.LowSince.Equal(at.Add(27*time.Minute)) {
		t.Fatal("gap counted", s)
	}
	if e := h.vixFailure(ctx, "sina", "test failure", at.Add(43*time.Minute)); e != nil {
		t.Fatal(e)
	}
	if vixStateForTest(t, h).LowSince != nil {
		t.Fatal("error retained continuity")
	}
	vixPush(t, h, at.Add(29*time.Minute), "29")
	vixPush(t, h, at.Add(29*time.Minute), "41") // Correction of a timestamp is not a new trigger.
	if vixStateForTest(t, h).LowSince != nil {
		t.Fatal("correction retained invalid recovery interval")
	}
	if vixCount(t, h, "vix_events") != 1 {
		t.Fatal("same timestamp triggered")
	}
	// Even though fetch succeeded, an old point must not advance/re-arm state.
	if e := h.ingestVIX(ctx, []VIXPoint{vixSample(at.Add(30*time.Minute), "29")}, at.Add(60*time.Minute)); e != nil {
		t.Fatal(e)
	}
	if vixStateForTest(t, h).LowSince != nil {
		t.Fatal("stale point counted")
	}
}

func TestVIXInitialLabelRequiresFreshConditionAtOptIn(t *testing.T) {
	for _, condition := range []string{"fresh", "stale", "missing"} {
		t.Run(condition, func(t *testing.T) {
			h := vixTestHub(t)
			at := vixTestAt()
			if condition != "missing" {
				vixPush(t, h, at, "41")
			}
			enabled := at.Add(15 * time.Minute)
			if condition == "stale" {
				enabled = at.Add(40 * time.Minute)
			}
			vixEnable(t, h, enabled)
			if condition != "fresh" {
				vixPush(t, h, enabled.Add(time.Minute), "41")
			}
			var raw []byte
			cycle := vixStateForTest(t, h).CycleID
			if err := h.Store.research.QueryRow("SELECT payload FROM vix_events WHERE id=?", cycle+"/priority").Scan(&raw); err != nil {
				t.Fatal(err)
			}
			var event VIXEvent
			if err := json.Unmarshal(raw, &event); err != nil {
				t.Fatal(err)
			}
			if event.Initial != (condition == "fresh") {
				t.Fatalf("%s opt-in mislabeled: %+v", condition, event)
			}
		})
	}
}
func TestVIXEventStateOutboxAtomic(t *testing.T) {
	h := vixTestHub(t)
	ctx := context.Background()
	at := vixTestAt()
	vixEnable(t, h, at.Add(15*time.Minute))
	_, e := h.Store.research.Exec("CREATE TRIGGER reject_vix BEFORE INSERT ON notices WHEN NEW.kind LIKE 'vix:%' BEGIN SELECT RAISE(FAIL,'simulated disk failure'); END;")
	if e != nil {
		t.Fatal(e)
	}
	if e = h.ingestVIX(ctx, []VIXPoint{vixSample(at, "31")}, at.Add(15*time.Minute)); e == nil {
		t.Fatal("failure ignored")
	}
	if vixCount(t, h, "vix_events") != 0 || vixCount(t, h, "vix_minute") != 0 || vixStateForTest(t, h).CycleID != "" {
		t.Fatal("partial transaction")
	}
	h.Store.research.Exec("DROP TRIGGER reject_vix")
	vixPush(t, h, at, "31")
	if vixCount(t, h, "vix_events") != 1 || vixCount(t, h, "notices") != 1 {
		t.Fatal("retry lost event")
	}
}

func TestVIXInterveningSpikeInvalidatesRecoveryWithoutBackfillAlert(t *testing.T) {
	h := vixTestHub(t)
	at := vixTestAt()
	vixEnable(t, h, at.Add(15*time.Minute))
	vixPush(t, h, at, "31")
	for i := 1; i <= 29; i++ {
		vixPush(t, h, at.Add(time.Duration(i)*time.Minute), "29")
	}
	points := []VIXPoint{vixSample(at.Add(30*time.Minute), "45"), vixSample(at.Add(31*time.Minute), "29")}
	if e := h.ingestVIX(context.Background(), points, at.Add(46*time.Minute)); e != nil {
		t.Fatal(e)
	}
	s := vixStateForTest(t, h)
	if s.CycleID == "" || s.LowSince == nil || !s.LowSince.Equal(at.Add(31*time.Minute)) || vixCount(t, h, "vix_events") != 1 {
		t.Fatal("intervening high ignored or backfill mailed", s)
	}
}

func TestVIXPendingCorrectionAndOfflineDoNotSend(t *testing.T) {
	h := vixTestHub(t)
	at := vixTestAt()
	now := at.Add(15 * time.Minute)
	ctx := context.Background()
	vixEnable(t, h, now)
	vixPush(t, h, at, "31")
	vixPush(t, h, at, "29")
	if e := h.processVIXNotices(ctx, now); e != nil {
		t.Fatal(e)
	}
	var status string
	h.Store.research.QueryRow("SELECT status FROM notices WHERE kind='vix:watch'").Scan(&status)
	if status != "superseded" {
		t.Fatal("corrected observation still eligible", status)
	}
	h.offline = true
	vixPush(t, h, at.Add(time.Minute), "42")
	if e := h.processVIXNotices(ctx, now.Add(time.Minute)); e != nil {
		t.Fatal(e)
	}
	if n, e := h.mailAttempts(ctx, now.Add(time.Minute)); e != nil || n != 0 {
		t.Fatal("offline reserved a send", n, e)
	}
}
func TestVIXReadOnlyHistoryAndDailyNeverAlerts(t *testing.T) {
	h := vixTestHub(t)
	ctx := context.Background()
	at := vixTestAt()
	vixEnable(t, h, at.Add(15*time.Minute))
	if e := h.ingestVIXDaily(ctx, []VIXDaily{{"2026-09-25", "40", "50", "39", "45"}}, at.Add(24*time.Hour)); e != nil {
		t.Fatal(e)
	}
	if vixCount(t, h, "vix_events") != 0 {
		t.Fatal("daily triggered")
	}
	// An intraday response can contain an earlier spike: only the latest is eligible.
	if e := h.ingestVIX(ctx, []VIXPoint{vixSample(at, "41"), vixSample(at.Add(time.Minute), "22")}, at.Add(16*time.Minute)); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 100; i++ {
		for _, path := range []string{"vix", "vix/history", "vix/settings", "vix/alerts"} {
			if _, e := h.Read(ctx, path, url.Values{}); e != nil {
				t.Fatal(e)
			}
		}
	}
	if h.Scheduler.State()["calls"].(int64) != 0 || vixCount(t, h, "vix_events") != 0 {
		t.Fatal("read performed upstream or alerts")
	}
	if ValidAsset("VIX") || researchAsset("VIX") {
		t.Fatal("VIX leaked into crypto")
	}
}
func TestVIXSharedLimitConcurrentWithLegacy(t *testing.T) {
	h := vixTestHub(t)
	at := vixTestAt()
	now := at.Add(15 * time.Minute)
	h.boot = now.Add(-time.Hour)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		_, e := h.Store.research.Exec("INSERT INTO notices(id,created,status,attempted,payload) VALUES(?,?,'sent',?,'{}')", fmt.Sprint(i), now.Unix(), now.Add(-time.Duration(i+1)*time.Minute).Unix())
		if e != nil {
			t.Fatal(e)
		}
	}
	var sends atomic.Int32
	h.mailSend = func(context.Context, MailConfig, string, string) error { sends.Add(1); return nil }
	vixEnable(t, h, now)
	vixPush(t, h, at, "31")
	if e := h.queueNotice(Signal{ID: "BTC-control", Asset: "BTC", At: now, DataThrough: now, Expires: now.Add(time.Hour), Rules: SignalRules}, "anomaly", now); e != nil {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	for _, fn := range []func(context.Context, time.Time) error{h.processNotices, h.processVIXNotices} {
		wg.Add(1)
		go func(fn func(context.Context, time.Time) error) {
			defer wg.Done()
			if e := fn(ctx, now); e != nil {
				t.Error(e)
			}
		}(fn)
	}
	wg.Wait()
	if sends.Load() != 1 {
		t.Fatal("shared cap", sends.Load())
	}
	if n, e := h.mailAttempts(ctx, now); e != nil || n != 6 {
		t.Fatal(n, e)
	}
}
func TestVIXMailBatchesSameSecondAndUnknown(t *testing.T) {
	h := vixTestHub(t)
	at := vixTestAt()
	now := at.Add(15 * time.Minute)
	ctx := context.Background()
	sends := 0
	h.mailSend = func(context.Context, MailConfig, string, string) error {
		sends++
		return errors.New("ambiguous delivery")
	}
	vixEnable(t, h, now)
	vixPush(t, h, at, "31")
	if e := h.processVIXNotices(ctx, now); e == nil {
		t.Fatal("lost error")
	}
	if e := h.processVIXNotices(ctx, now); e != nil {
		t.Fatal(e)
	}
	if sends != 1 {
		t.Fatal("duplicate SMTP")
	}
	h.mailSend = func(context.Context, MailConfig, string, string) error { sends++; return nil }
	for _, id := range []string{"batch2", "batch3"} {
		h.Store.research.Exec("INSERT INTO notices(id,created,status,payload) VALUES(?,?,'pending','{}')", id, now.Unix())
		h.noticeMu.Lock()
		_, e := h.deliverNoticeBatch(ctx, now, []string{id}, "test", "test")
		h.noticeMu.Unlock()
		if e != nil {
			t.Fatal(e)
		}
	}
	if n, e := h.mailAttempts(ctx, now); e != nil || n != 3 {
		t.Fatal("same-second batches collapsed", n, e)
	}
}
func TestVIXRestartBackupAndRetention(t *testing.T) {
	h := vixTestHub(t)
	at := vixTestAt()
	ctx := context.Background()
	vixEnable(t, h, at.Add(15*time.Minute))
	vixPush(t, h, at, "31")
	root := h.Store.Root()
	state := vixStateForTest(t, h)
	backup := filepath.Join(t.TempDir(), "research.sqlite")
	if e := BackupFile(ctx, filepath.Join(root, "research.sqlite"), backup); e != nil {
		t.Fatal(e)
	}
	db, e := database(backup)
	if e != nil {
		t.Fatal(e)
	}
	var b []byte
	db.QueryRow("SELECT payload FROM vix_state WHERE id=1").Scan(&b)
	db.Close()
	var copied vixState
	if json.Unmarshal(b, &copied) != nil || copied.CycleID != state.CycleID {
		t.Fatal("backup lost cycle")
	}
	h.Store.Close()
	restarted, e := Open(Config{Root: root, Offline: true, Mail: h.mail})
	if e != nil {
		t.Fatal(e)
	}
	defer restarted.Store.Close()
	var status string
	restarted.Store.research.QueryRow("SELECT status FROM notices WHERE kind='vix:watch'").Scan(&status)
	if status != "suppressed_restart" {
		t.Fatal(status)
	}
	vixPush(t, restarted, at.Add(time.Minute), "32")
	if vixCount(t, restarted, "vix_events") != 1 {
		t.Fatal("restart duplicate")
	}
	if e := restarted.Store.maintainResearch(ctx, at.Add(40*24*time.Hour), 30); e != nil {
		t.Fatal(e)
	}
	if vixCount(t, restarted, "notices") != 1 {
		t.Fatal("crypto retention removed 90-day VIX notice")
	}
	if e := restarted.Store.maintainVIX(ctx, at.Add(91*24*time.Hour)); e != nil {
		t.Fatal(e)
	}
	if vixCount(t, restarted, "vix_events") != 0 || vixCount(t, restarted, "vix_minute") != 0 || vixStateForTest(t, restarted).CycleID != state.CycleID {
		t.Fatal("retention erased persistent cycle or retained expired facts")
	}
}
func TestVIXFetchLimitsAndCancellation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/large":
			w.Write([]byte(strings.Repeat("x", vixBodyLimit+1)))
		case "/wait":
			<-r.Context().Done()
		case "/rate":
			w.Header().Set("Retry-After", "600")
			w.WriteHeader(429)
		default:
			http.Redirect(w, r, "/large", 302)
		}
	}))
	defer srv.Close()
	for _, p := range []string{"/large", "/redirect"} {
		if _, e := fetchVIX(context.Background(), srv.URL+p); e == nil {
			t.Fatal("unbounded fetch", p)
		}
	}
	_, e := fetchVIX(context.Background(), srv.URL+"/rate")
	var he vixHTTPError
	if !errors.As(e, &he) || he.delay != 10*time.Minute {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, e := fetchVIX(ctx, srv.URL+"/wait"); e == nil {
		t.Fatal("cancel ignored")
	}
}
func TestVIXLiveContractOptional(t *testing.T) {
	if os.Getenv("TIDAL_VIX_LIVE_CHECK") != "1" {
		t.Skip("explicit read-only public-source check")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	b, e := fetchVIX(ctx, vixMinuteURL)
	if e != nil {
		t.Fatal(e)
	}
	p, e := parseVIXMinute(b, time.Now().UTC())
	if e != nil {
		t.Fatal(e)
	}
	b, e = fetchVIX(ctx, vixDailyURL)
	if e != nil {
		t.Fatal(e)
	}
	d, e := parseVIXDaily(b, time.Now().UTC())
	if e != nil {
		t.Fatal(e)
	}
	t.Logf("Sina %d observations, latest %+v; Cboe %d retained daily rows, last %+v", len(p), p[len(p)-1], len(d), d[len(d)-1])
	// Optional isolated browser fixture contains only these real public feeds.
	// Refuse any existing database and any location outside the ignored tmp tree.
	if root := os.Getenv("TIDAL_VIX_QA_ROOT"); root != "" {
		abs, e := filepath.Abs(root)
		if e != nil {
			t.Fatal(e)
		}
		allowed, e := filepath.Abs("../../tmp")
		if e != nil {
			t.Fatal(e)
		}
		if !strings.HasPrefix(abs, allowed+string(os.PathSeparator)) {
			t.Fatal("QA database must be under repository tmp")
		}
		if _, e = os.Stat(filepath.Join(abs, "research.sqlite")); !os.IsNotExist(e) {
			t.Fatal("QA database already exists")
		}
		h, e := Open(Config{Root: abs, Offline: true})
		if e != nil {
			t.Fatal(e)
		}
		defer h.Store.Close()
		if e = h.ingestVIX(ctx, p, time.Now().UTC()); e != nil {
			t.Fatal(e)
		}
		if e = h.ingestVIXDaily(ctx, d, time.Now().UTC()); e != nil {
			t.Fatal(e)
		}
	}
}
