package datahub

import (
	"context"
	"errors"
	"net/http"
	"time"
)

func (h *Hub) costFeed(ctx context.Context) (CostFeed, error) {
	var f CostFeed
	if e := h.Store.onchain.available(); e != nil {
		return f, e
	}
	e := costLoad(ctx, h.Store.onchain.db, "feed", &f)
	return f, e
}
func costFeedStatus(f CostFeed, now time.Time) (string, string) {
	if f.Disabled {
		return "disabled", "来源鉴权、方法或接口契约变化，已停止自动请求"
	}
	if f.CostError != "" {
		return "contract", f.CostError
	}
	if f.LastDate == "" {
		return "missing", "等待首次有效链上快照"
	}
	if f.LastCheck == nil || now.Sub(*f.LastCheck) > 3*time.Hour || f.LastCheck.After(now.Add(time.Minute)) {
		return "unreachable", "超过3小时未成功检查来源，暂停建立新结构；已有冻结区按独立价格继续判断"
	}
	today := now.UTC().Truncate(24 * time.Hour)
	expected := costDate(today.AddDate(0, 0, -1))
	if now.Before(today.Add(6 * time.Hour)) {
		expected = costDate(today.AddDate(0, 0, -2))
	}
	if f.LastDate < expected {
		return "delayed", "最新完成UTC日超过6小时宽限仍未更新"
	}
	if f.LastError != "" {
		return "error", "最近采集失败，保留最后有效快照；结构判断暂停，价格条件按各自输入核验"
	}
	return "fresh", "日线快照有效；不是实时持仓或成交数据"
}
func (h *Hub) onchainHealth() any {
	s := h.Store.onchain
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	f, e := h.costFeed(ctx)
	status, reason := costFeedStatus(f, time.Now().UTC())
	if e != nil {
		status, reason = "unavailable", "链上存储不可用或查询繁忙"
	}
	if h.onchainDisabled {
		status, reason = "disabled", "链上采集已由配置关闭"
	}
	n := s.bytes()
	if n >= onchainBudget*95/100 {
		status, reason = "capacity", "链上容量保护，暂停新增记录和判断"
	}
	var evaluation map[string]any
	if s.available() == nil {
		_ = costLoad(ctx, s.db, "evaluation-error", &evaluation)
	}
	return map[string]any{"evaluation": evaluation, "status": status, "reason": reason, "source": onchainSource, "feed": f, "usedBytes": n, "budgetBytes": onchainBudget, "capacityWarning": n >= onchainBudget*80/100, "rulesVersion": OnchainRules, "eventsEnabled": !h.onchainEventsDisabled && !h.onchainDisabled, "offline": h.offline, "storage": s.storageDetail(ctx, time.Now().UTC()), "capabilities": h.costCapabilities(ctx, time.Now().UTC())}
}
func (h *Hub) onchainCollector(ctx context.Context) {
	client := &http.Client{Timeout: 30 * time.Second}
	// A single source worker owns upstream requests; local evaluation has its own bounded worker.
	for ctx.Err() == nil {
		now := time.Now().UTC()
		_, _ = h.pollOnchain(ctx, client, onchainURL, now)
		delay := time.Hour
		if f, e := h.costFeed(ctx); e == nil && f.NextAttempt != nil {
			delay = time.Until(*f.NextAttempt)
			if delay < time.Minute {
				delay = time.Minute
			}
		}
		if !sleep(ctx, delay) {
			return
		}
	}
}
func (h *Hub) pollOnchain(ctx context.Context, client *http.Client, base string, now time.Time) (bool, error) {
	s := h.Store.onchain
	if e := s.available(); e != nil {
		return false, e
	}
	s.fetchMu.Lock()
	defer s.fetchMu.Unlock()
	defer s.epoch.Add(1)
	f, e := h.costFeed(ctx)
	if e != nil {
		return false, e
	}
	if h.onchainDisabled || f.Disabled {
		return false, nil
	}
	if f.NextAttempt != nil && now.Before(*f.NextAttempt) {
		return false, nil
	}
	f.LastAttempt = &now
	initial := f.LastDate == "" && f.LastPriceDate == ""
	err := func() error {
		if h.Store.Status().Paused {
			return errors.New("项目磁盘容量保护，链上采集暂停")
		}
		if e := s.writable(); e != nil {
			return e
		}
		requestCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		version, e := costVersion(requestCtx, client, base)
		cancel()
		if e != nil {
			return e
		}
		full := initial
		if !initial && now.UTC().Hour() >= 6 && (f.LastFull == nil || costDate(*f.LastFull) != costDate(now)) {
			full = true
		}
		if !initial && (f.LastFull == nil || costDate(*f.LastFull) != costDate(now)) {
			prices, e := s.prices(ctx, now)
			if e != nil {
				return e
			}
			end := now.UTC().Truncate(24*time.Hour).AddDate(0, 0, -1)
			for i := 0; i < 22; i++ {
				if _, ok := prices[costDate(end.AddDate(0, 0, -i))]; !ok {
					full = true
					break
				}
			}
		}
		if version == f.Version && !full {
			f.LastCheck = &now
			f.LastError = ""
			return nil
		}
		etag := f.ETag
		if full {
			etag = f.FullETag
			f.LastFull = &now
		}
		requestCtx, cancel = context.WithTimeout(ctx, 30*time.Second)
		bundle, unchanged, e := fetchCostBundle(requestCtx, client, base, etag, full, now)
		cancel()
		if e != nil {
			return e
		}
		if unchanged {
			if initial {
				return &costProtocolError{"首次采集返回304但本地无数据"}
			}
		} else {
			s.mu.Lock()
			e = s.ingest(ctx, bundle, now, initial)
			s.mu.Unlock()
			if e != nil {
				return e
			}
			f.CostError, f.PriceError = bundle.CostError, bundle.PriceError
			for _, p := range bundle.Prices {
				if p.Date > f.LastPriceDate {
					f.LastPriceDate = p.Date
				}
			}
			if full {
				f.FullETag = bundle.ETag
			} else {
				f.ETag = bundle.ETag
			}
			for _, frame := range bundle.Frames {
				if frame.Date > f.LastDate {
					f.LastDate = frame.Date
				}
			}
		}
		f.Version = version
		f.LastCheck = &now
		f.LastError = ""
		return nil
	}()
	delay := time.Hour
	if err != nil {
		f.LastError = err.Error()
		f.Failures++
		delay = time.Duration(1<<min(f.Failures-1, 6)) * time.Minute
		var protocol *costProtocolError
		var he *costHTTPError
		if errors.As(err, &protocol) {
			f.Disabled = true
		}
		if errors.As(err, &he) {
			if he.Status == 400 || he.Status == 401 || he.Status == 403 || he.Status == 404 || he.Status == 405 || he.Status == 406 || he.Status == 410 || he.Status == 422 {
				f.Disabled = true
			}
			if he.Retry > delay {
				delay = he.Retry
			}
		}
	} else {
		f.Failures = 0
	}
	next := now.Add(delay)
	f.NextAttempt = &next
	if e = s.save(ctx, "feed", f); e != nil {
		return false, e
	}
	if err != nil {
		return false, err
	}
	if !h.onchainEventsDisabled {
		if e = h.evaluateCostDay(ctx, now, initial); e != nil {
			return false, e
		}
	}
	var baseline struct {
		At    time.Time `json:"at"`
		Bytes int64     `json:"bytes"`
	}
	if costLoad(ctx, s.db, "growth-baseline", &baseline) == nil && baseline.At.IsZero() {
		baseline.At = now
		baseline.Bytes = s.bytes()
		_ = s.save(ctx, "growth-baseline", baseline)
	}

	return true, nil
}
