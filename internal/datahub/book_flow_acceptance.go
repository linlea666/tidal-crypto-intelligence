package datahub

import (
	"context"
	"database/sql"
	"time"
)

// Collection acceptance has its own origin and denominator. It cannot create
// events or advance a research origin, even after a successful 60 minute window.
type bookCollection struct {
	Origin  time.Time       `json:"origin"`
	Last    time.Time       `json:"lastProcessedAt"`
	Through time.Time       `json:"through"`
	Pending map[int64]uint8 `json:"pending"`
	Closed  map[int64]uint8 `json:"closed"`
}

func (h *Hub) collectBookFlow(ctx context.Context, now time.Time) error {
	var s bookCollection
	err := h.Store.bookFlowLoad(ctx, "collection", BookFlowCollection, &s)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	if s.Origin.IsZero() {
		s.Origin = now
	}
	if s.Pending == nil {
		s.Pending = map[int64]uint8{}
	}
	if s.Closed == nil {
		s.Closed = map[int64]uint8{}
	}
	record := func(o Observation, bit uint8, foot bool) {
		at := o.Time().Truncate(time.Minute)
		end := at.Add(time.Minute)
		if !at.After(s.Through) || at.Before(s.Origin) || o.Quality != "valid" || o.FetchedAt.After(now) || o.FetchedAt.Before(h.boot) || o.FetchedAt.Before(s.Origin) || o.Time().After(now) || now.After(end.Add(3*time.Minute)) {
			return
		}
		if foot && (o.Resolution != 60 || end.After(now) || !validMinuteFoot(o)) {
			return
		}
		if !foot && (h.bookRate(now) == nil || buildBookPoint(o, h.bookRate(now), now).Reference == nil) {
			return
		}
		s.Pending[at.Unix()] |= bit
	}
	for i, venue := range []string{"Binance", "OKX"} {
		if o, ok := h.Store.Latest(ID("book", "BTC", venue, "spot")); ok {
			record(o, uint8(1<<i), false)
		}
		d, _ := h.Dataset(minuteFootID(venue))
		if err = h.Store.Visit(ctx, d, 60, now.Truncate(time.Minute).Add(-4*time.Minute), now.Truncate(time.Minute), func(o Observation) error { record(o, uint8(4<<i), true); return nil }); err != nil {
			return err
		}
	}
	start := s.Through.Add(time.Minute)
	if s.Through.IsZero() {
		start = s.Origin.Truncate(time.Minute).Add(time.Minute)
	}
	end := now.Truncate(time.Minute).Add(-4 * time.Minute)
	// After a long restart, omitted minutes are zero bits in the natural clock;
	// retaining only the last hour bounds both storage and catch-up work.
	if start.Before(end.Add(-59 * time.Minute)) {
		start = end.Add(-59 * time.Minute)
	}
	for at := start; !at.After(end); at = at.Add(time.Minute) {
		s.Closed[at.Unix()] = s.Pending[at.Unix()]
		delete(s.Pending, at.Unix())
		s.Through = at
	}
	for at := range s.Closed {
		if at < end.Add(-59*time.Minute).Unix() {
			delete(s.Closed, at)
		}
	}
	for at := range s.Pending {
		if at <= s.Through.Unix() {
			delete(s.Pending, at)
		}
	}
	s.Last = now
	return h.Store.bookFlowSave(ctx, "collection", BookFlowCollection, now, s, false)
}
func (h *Hub) bookInputsHealthy(ctx context.Context, now time.Time) ([]string, error) {
	blocked := []string{}
	if h.bookRate(now) == nil {
		blocked = append(blocked, "美元汇率无效或过期")
	}
	for _, venue := range []string{"Binance", "OKX"} {
		var c minuteContract
		err := h.Store.bookFlowLoad(ctx, "contract", minuteFootID(venue), &c)
		if err != nil && err != sql.ErrNoRows {
			return nil, err
		}
		c = currentMinuteContract(c, now)
		if c.VerifiedAt == nil || c.ActivatedAt == nil {
			blocked = append(blocked, venue+" 分钟契约或五分钟兼容聚合未通过")
		}
		if c.Healthy == nil || !*c.Healthy {
			blocked = append(blocked, venue+" 分钟足迹当前健康未确认")
		}
		for _, id := range []string{ID("book", "BTC", venue, "spot"), minuteFootID(venue)} {
			o, ok := h.Store.Latest(id)
			at := o.Time()
			if id == minuteFootID(venue) {
				at = at.Add(time.Minute)
			}
			if !ok || o.Quality != "valid" || at.After(now) || now.Sub(at) > 3*time.Minute || o.FetchedAt.After(now) || now.Sub(o.FetchedAt) > 3*time.Minute {
				input := "盘口"
				if id == minuteFootID(venue) {
					input = "分钟成交"
				}
				blocked = append(blocked, venue+" "+input+"当前无效或过期")
			}
		}
	}
	return blocked, nil
}
func (h *Hub) bookFlowAcceptance(ctx context.Context, now time.Time) (map[string]any, error) {
	var s bookCollection
	err := h.Store.bookFlowLoad(ctx, "collection", BookFlowCollection, &s)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	blocked, err := h.bookInputsHealthy(ctx, now)
	if err != nil {
		return nil, err
	}
	timely := 0
	inputs := []int{0, 0, 0, 0}
	for _, bits := range s.Closed {
		if bits == 15 {
			timely++
		}
		for i := range inputs {
			if bits&(1<<i) != 0 {
				inputs[i]++
			}
		}
	}
	var ratio *float64
	if len(s.Closed) > 0 {
		v := float64(timely) / float64(len(s.Closed))
		ratio = &v
	}
	if len(s.Closed) < 60 {
		blocked = append(blocked, "最近完整60分钟采集验收尚未完成")
	} else if timely < 57 {
		blocked = append(blocked, "四项联合及时覆盖低于95%")
	}
	if s.Last.IsZero() || now.Sub(s.Last) > time.Minute {
		blocked = append(blocked, "采集登记已过期或尚未成功")
	}
	if h.Store.bookFlowError != "" || h.Store.Status().ResearchPaused || h.Store.Status().Paused {
		blocked = append(blocked, "研究存储不可写或容量保护")
	}
	h.mu.RLock()
	runtime := h.bookFlowRuntime
	h.mu.RUnlock()
	if runtime.FailureAt != nil && (runtime.Success == nil || runtime.FailureAt.After(*runtime.Success)) {
		blocked = append(blocked, "最近研究写入未恢复")
	}
	return map[string]any{"ready": len(blocked) == 0, "blockedReasons": blocked, "origin": nullableBookTime(s.Origin), "through": nullableBookTime(s.Through), "lastProcessedAt": nullableBookTime(s.Last), "observedMinutes": len(s.Closed), "timelyMinutes": timely, "coverage": ratio, "inputTimelyMinutes": inputs, "inputOrder": []string{"Binance盘口", "OKX盘口", "Binance分钟足迹", "OKX分钟足迹"}, "requiredMinutes": 60, "minimumCoverage": .95, "deadlineSeconds": 180}, nil
}
func nullableBookTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}
func (h *Hub) bookFlowCycle(ctx context.Context, now time.Time) error {
	if err := h.collectBookFlow(ctx, now); err != nil {
		return err
	}
	if h.bookFlowMode != "run" {
		return nil
	}
	var state bookFlowState
	err := h.Store.bookFlowLoad(ctx, "state", BookFlowRules, &state)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	if state.Origin.IsZero() {
		acceptance, err := h.bookFlowAcceptance(ctx, now)
		if err != nil {
			return err
		}
		if !acceptance["ready"].(bool) {
			return nil
		}
	}
	return h.bookFlowStep(ctx, now)
}
func (h *Hub) bookFlowRunStatus(ctx context.Context, s bookFlowState, now time.Time) (map[string]any, error) {
	a, err := h.bookFlowAcceptance(ctx, now)
	if err != nil {
		return nil, err
	}
	state, label := "collecting", "采集验收中，研究尚未开始"
	warmup := 0.0
	if h.bookFlowMode == "off" {
		state, label = "off", "新观察已关闭，保留历史"
	} else if s.Origin.IsZero() {
		if a["ready"].(bool) {
			state, label = "ready", "可启动，研究尚未开始"
		}
	} else {
		warmup = min(1, max(0, now.Sub(s.Origin).Minutes()/30))
		state, label = "running", "前向观察运行中"
		if warmup < 1 {
			state, label = "warming", "前向观察预热中"
		}
		if h.bookFlowMode != "run" {
			state, label = "paused", "研究已暂停，保留前向起点"
		} else if s.Last.IsZero() || now.Sub(s.Last) > 3*time.Minute || len(a["blockedReasons"].([]string)) > 0 {
			state, label = "degraded", "当前输入或登记异常，查看阻塞原因"
		}
	}
	return map[string]any{"state": state, "label": label, "blockedReasons": a["blockedReasons"], "collectionAcceptance": a, "warmupProgress": warmup, "warmupMinutes": 30, "warmupEndsAt": func() *time.Time {
		if s.Origin.IsZero() {
			return nil
		}
		t := s.Origin.Add(30 * time.Minute)
		return &t
	}()}, nil
}
