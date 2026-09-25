package datahub

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"reflect"
	"testing"
	"time"
)

func TestRollingBaselineMatchesReference(t *testing.T) {
	from := testTime("2026-07-01T00:00:00Z")
	bars := map[int64]FlowBar{}
	for i := 0; i < 35*288; i++ {
		if i%113 == 0 {
			continue
		}
		at := from.Add(time.Duration(i) * 5 * time.Minute)
		bars[at.Unix()] = FlowBar{at, int64(100 + i%101), int64(80 + i%131)}
	}
	r := newRollingBaseline(bars, from, from.Add(30*24*time.Hour))
	now := from.Add(35 * 24 * time.Hour)
	for end := r.to; end.Before(now); end = end.Add(55 * time.Minute) {
		r.advance(end)
		want := baselineFromBars(bars, end.Add(-30*24*time.Hour), end, now)
		got := r.result(now)
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("%v mismatch\n%+v\n%+v", end, got, want)
		}
	}
}
func BenchmarkRollingStudyWindows(b *testing.B) {
	end := time.Now().UTC().Truncate(time.Hour)
	from := end.Add(-90 * 24 * time.Hour)
	bars := map[int64]FlowBar{}
	for at := from; at.Before(end); at = at.Add(5 * time.Minute) {
		bars[at.Unix()] = FlowBar{at, 100 + int64(at.Minute()), 90}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := newRollingBaseline(bars, from, from.Add(30*24*time.Hour))
		for at := r.to; at.Before(end); at = at.Add(time.Hour) {
			r.advance(at)
			_ = r.result(end)
		}
	}
}
func BenchmarkObservationCodec(b *testing.B) {
	o := Observation{Dataset: "flow.btc..spot", FetchedAt: time.Now().UTC(), Resolution: 300, Quality: "valid", Payload: Payload{Flow: &Flow{"1000000", "999000"}}}
	b.Run("pack", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, e := pack(o); e != nil {
				b.Fatal(e)
			}
		}
	})
	data, _ := pack(o)
	b.Run("unpack", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, e := unpack(data); e != nil {
				b.Fatal(e)
			}
		}
	})
}
func boardWire(t *testing.T, h *Hub, q url.Values) map[string]any {
	t.Helper()
	v, e := h.LargeBoard(context.Background(), "BTC", q)
	if e != nil {
		t.Fatal(e)
	}
	b, _ := json.Marshal(v)
	var m map[string]any
	json.Unmarshal(b, &m)
	return m
}
func TestLargeBoardAllRowsFrozenAndIsolated(t *testing.T) {
	h, e := Open(Config{Root: t.TempDir(), Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	d, _ := FindDataset("large.btc.coinbase.spot")
	now := time.Now().UTC()
	o := orderObservation(d, now, orderFixture(now))
	o.Payload.Large = nil
	for i := 0; i < 137; i++ {
		r := orderFixture(now)
		r.ID = fmt.Sprintf("%03d", i)
		r.Quantity = "1"
		r.Side = "bid"
		if i%2 == 1 {
			r.Side = "ask"
		}
		r.Start = nil
		if i%3 == 0 {
			start := now.Add(-time.Hour)
			r.Start = &start
		}
		o.Payload.Large = append(o.Payload.Large, r)
	}
	if _, e = h.Store.Ingest(d, o); e != nil {
		t.Fatal(e)
	}
	q := url.Values{}
	first := boardWire(t, h, q)
	q.Set("version", first["version"].(string))
	seen := map[string]bool{}
	for page := 0; page < 3; page++ {
		q.Set("offset", fmt.Sprint(page*50))
		m := boardWire(t, h, q)
		if m["total"] != float64(137) || m["scaleMaxCents"] != first["scaleMaxCents"] {
			t.Fatal("page scale or total changed")
		}
		for _, raw := range m["items"].([]any) {
			r := raw.(map[string]any)
			id := r["id"].(string)
			if seen[id] {
				t.Fatal("duplicate page row")
			}
			seen[id] = true
			if r["durationSeconds"] == nil {
				t.Fatal("missing duration basis")
			}
		}
		if page == 0 {
			o.FetchedAt = now.Add(time.Second)
			o.Payload.Large = o.Payload.Large[:3]
			h.Store.Ingest(d, o)
		}
	}
	if len(seen) != 137 {
		t.Fatal("truncated list")
	}
	fresh := boardWire(t, h, url.Values{})
	if fresh["total"] != float64(3) {
		t.Fatal("fresh version not updated")
	}
	q.Set("side", "bid")
	q.Set("offset", "0")
	q.Set("limit", "100")
	m := boardWire(t, h, q)
	if m["total"] != float64(69) || len(m["items"].([]any)) != 69 {
		t.Fatal("filter before pagination failed")
	}
	activity, e := h.ActivityView(context.Background(), "BTC", 1, 5)
	if e != nil {
		t.Fatal(e)
	}
	if reflect.ValueOf(activity.(map[string]any)["orders"]).Len() != 0 {
		t.Fatal("large orders leaked into activity")
	}
	for i := 0; i < 100; i++ {
		boardWire(t, h, q)
	}
	if h.Scheduler.quota.Calls != 0 {
		t.Fatal("GET requested upstream")
	}
}
func TestBTCScope(t *testing.T) {
	h, e := Open(Config{Root: t.TempDir(), Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	if _, e = h.CreateStudy(StudyRequest{Asset: "ETH"}); e == nil {
		t.Fatal("ETH research accepted")
	}
	for _, d := range Registry() {
		if d.Asset != "ETH" {
			continue
		}
		j := h.Scheduler.jobs[d.ID]
		if j != nil && j.Mode == "live" && j.Disabled != (d.Kind == "oi-history") {
			t.Fatalf("ETH daily collection scope changed: %s", d.ID)
		}
	}
}
func TestHourlyCaseDoesNotBecomeFiveMinuteStrategy(t *testing.T) {
	h, e := Open(Config{Root: t.TempDir(), Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	at := testTime("2026-08-16T16:00:00Z")
	now := testTime("2026-09-25T00:00:00Z")
	d, _ := FindDataset("flow.btc..spot")
	o := Observation{Dataset: d.ID, Source: d.Source, ObservedAt: &at, FetchedAt: now, Resolution: 3600, Quality: "valid", Payload: Payload{Flow: &Flow{"100", "10"}}}
	h.Store.Ingest(d, o)
	s := Study{Asset: "BTC", From: at, To: now}
	r, e := h.evaluateStudy(context.Background(), s, now)
	if e != nil {
		t.Fatal(e)
	}
	if r.CoreCalculated || r.FlowCoverage != 0 {
		t.Fatal("hourly passed 5m test")
	}
	c := r.Cases[0]
	p := c["hourly"].([]map[string]any)[0]
	if p["netCents"].(*int64) == nil || *p["netCents"].(*int64) != 9000 || p["flowResolutionSeconds"] != 3600 {
		t.Fatal("hourly fallback missing")
	}
}

func TestHistoryCapabilityIsPerWindowAndRestores(t *testing.T) {
	w := testStore(t)
	now := time.Now().UTC().Truncate(time.Hour)
	s := NewScheduler(w, Registry(), nil, true, now)
	from, to := now.Add(-40*24*time.Hour), now.Add(-39*24*time.Hour)
	req := DataRequest{Dataset: "flow.btc..spot", From: &from, To: &to, Resolution: 300, Purpose: "case"}
	j, e := s.Request(req, now, false)
	if e != nil {
		t.Fatal(e)
	}
	run := func(id string) {
		s.mu.Lock()
		cp := *s.jobs[id]
		s.jobs[id].InFlight = true
		s.inflight++
		s.mu.Unlock()
		s.run(context.Background(), cp)
	}
	s.fetch = func(context.Context, Dataset) ([]byte, error) { return nil, &FetchError{Code: "400", Status: 200} }
	run(j.ID)
	failed := s.jobs[j.ID]
	if !failed.Disabled || failed.ErrorKind != "request_rejected" || len(failed.Gaps) != 1 {
		t.Fatal("unclassified failed range")
	}
	again, _ := s.Request(req, now, false)
	if !again.Disabled {
		t.Fatal("click revived rejected range")
	}
	recent, end := now.Add(-time.Hour), now
	req.From = &recent
	req.To = &end
	req.Resolution = 3600
	recentJob, e := s.Request(req, now, false)
	if e != nil {
		t.Fatal(e)
	}
	s.fetch = func(_ context.Context, d Dataset) ([]byte, error) {
		if d.Params["interval"] != "1h" {
			t.Fatal("wrong interval")
		}
		return []byte(fmt.Sprintf(`{"code":"0","data":[{"time":%d,"aggregated_buy_volume_usd":100,"aggregated_sell_volume_usd":20}]}`, recent.UnixMilli())), nil
	}
	run(recentJob.ID)
	if !s.jobs[recentJob.ID].Completed || s.jobs[recentJob.ID].Disabled || len(s.jobs[recentJob.ID].Covered) != 1 {
		t.Fatal("recent/hourly window poisoned by old failure")
	}
	restored := NewScheduler(w, Registry(), nil, false, now)
	if !restored.jobs[j.ID].Disabled || !restored.jobs[recentJob.ID].Completed {
		t.Fatal("range ledger lost on restart")
	}
}
func TestLargeFXMissingAndFreshness(t *testing.T) {
	h, e := Open(Config{Root: t.TempDir(), Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	now := time.Now().UTC()
	d, _ := FindDataset("large.btc.binance.spot")
	r := orderFixture(now)
	h.Store.Ingest(d, orderObservation(d, now, r))
	m := boardWire(t, h, url.Values{})
	row := m["items"].([]any)[0].(map[string]any)
	if row["usdCents"] != nil || m["validCount"] != float64(0) {
		t.Fatal("FX missing invented dollar")
	}
	fx, _ := FindDataset("fx.usd.kraken")
	at := now.Add(-time.Minute)
	h.Store.Ingest(fx, Observation{Dataset: fx.ID, Source: fx.Source, ObservedAt: &at, FetchedAt: now, Quality: "valid", Payload: Payload{Rates: []Rate{{"USDT", "1"}}}})
	m = boardWire(t, h, url.Values{})
	if m["items"].([]any)[0].(map[string]any)["usdCents"] != nil {
		t.Fatal("old FX accepted")
	}
}
func TestActivityPlainLanguage(t *testing.T) {
	for _, v := range []struct {
		b    string
		p    float64
		want string
	}{{"相对均衡", .16, "买卖接近，价格另有方向"}, {"买方较主动", .3, "成交与价格方向一致"}, {"卖方较主动", .2, "成交与价格方向不同"}, {"买方较主动", .04, "价格变化小"}, {"证据不足", .4, "数据不足，暂不判断配合"}} {
		if activityInterpretation(v.b, &v.p)["price"] != v.want {
			t.Fatal(v)
		}
	}
}
