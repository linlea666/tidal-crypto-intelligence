package datahub

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Job struct {
	ID          string     `json:"id"`
	Dataset     Dataset    `json:"dataset"`
	Next        time.Time  `json:"next"`
	LastAttempt *time.Time `json:"lastAttempt"`
	LastSuccess *time.Time `json:"lastSuccess"`
	Error       string     `json:"error,omitempty"`
	Failures    int        `json:"failures"`
	InFlight    bool       `json:"inFlight"`
	Mode        string     `json:"mode"`
	From        *time.Time `json:"from,omitempty"`
	To          *time.Time `json:"to,omitempty"`
	Disabled    bool       `json:"disabled"`
	Completed   bool       `json:"completed"`
	Calls       int64      `json:"calls"`
}
type Quota struct {
	Starts      []time.Time `json:"starts"`
	Cooldown    time.Time   `json:"cooldownUntil"`
	Calls       int64       `json:"calls"`
	RateLimited int         `json:"rateLimited"`
	AuthFailed  bool        `json:"authFailed"`
}

func (q *Quota) prune(now time.Time) {
	i := 0
	for i < len(q.Starts) && !q.Starts[i].After(now.Add(-time.Minute)) {
		i++
	}
	q.Starts = append([]time.Time(nil), q.Starts[i:]...)
}
func (q *Quota) available(now time.Time) bool {
	q.prune(now)
	if q.AuthFailed || now.Before(q.Cooldown) || len(q.Starts) >= 12 {
		return false
	}
	return len(q.Starts) == 0 || now.Sub(q.Starts[len(q.Starts)-1]) >= 5100*time.Millisecond
}

type FetchError struct {
	Status     int
	RetryAfter time.Duration
	Code       string
}

func (e *FetchError) Error() string {
	return fmt.Sprintf("上游 HTTP %d / 业务码 %s", e.Status, e.Code)
}

type Fetcher func(context.Context, Dataset) ([]byte, error)
type savedJob struct {
	Job    Job               `json:"job"`
	Path   string            `json:"path"`
	Params map[string]string `json:"params"`
}
type Scheduler struct {
	mu       sync.Mutex
	store    *Warehouse
	jobs     map[string]*Job
	quota    Quota
	fetch    Fetcher
	inflight int
	wg       sync.WaitGroup
	enabled  bool
}

func NewFetcher(base, key string) Fetcher {
	client := &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(req *http.Request, via []*http.Request) error { return http.ErrUseLastResponse }}
	return func(ctx context.Context, d Dataset) ([]byte, error) {
		q := url.Values{}
		for k, v := range d.Params {
			q.Set(k, v)
		}
		req, e := http.NewRequestWithContext(ctx, "GET", strings.TrimRight(base, "/")+d.Path+"?"+q.Encode(), nil)
		if e != nil {
			return nil, errors.New("invalid upstream URL")
		}
		req.Header.Set("X-Api-Key", key)
		req.Header.Set("Accept", "application/json")
		r, e := client.Do(req)
		if e != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, errors.New("上游连接失败或超时")
		}
		defer r.Body.Close()
		if r.StatusCode != 200 {
			retry := time.Duration(0)
			if sec, e := strconv.Atoi(r.Header.Get("Retry-After")); e == nil {
				retry = time.Duration(sec) * time.Second
			} else if t, e := http.ParseTime(r.Header.Get("Retry-After")); e == nil {
				retry = time.Until(t)
			}
			return nil, &FetchError{Status: r.StatusCode, RetryAfter: retry}
		}
		b, e := io.ReadAll(io.LimitReader(r.Body, (16<<20)+1))
		if e != nil {
			return nil, errors.New("upstream body read failed")
		}
		if len(b) > 16<<20 {
			return nil, errors.New("上游响应超过16MiB限制")
		}
		var envelope struct {
			Code json.RawMessage `json:"code"`
		}
		if e = json.Unmarshal(b, &envelope); e != nil {
			return nil, errors.New("invalid JSON envelope")
		}
		code := strings.Trim(string(envelope.Code), "\"")
		if code != "0" {
			status := 200
			if code == "429" {
				status = 429
			}
			if code == "401" || code == "403" {
				status = 401
			}
			return nil, &FetchError{Status: status, Code: code}
		}
		return b, nil
	}
}
func NewScheduler(store *Warehouse, registry []Dataset, fetch Fetcher, enabled bool, now time.Time) *Scheduler {
	s := &Scheduler{store: store, jobs: map[string]*Job{}, fetch: fetch, enabled: enabled}
	store.LoadState("quota", &s.quota)
	var persisted []savedJob
	store.LoadState("jobs", &persisted)
	var saved []Job
	old := map[string]Job{}
	for _, v := range persisted {
		j := v.Job
		j.Dataset.Path = v.Path
		j.Dataset.Params = v.Params
		saved = append(saved, j)
		old[j.ID] = j
	}
	for _, d := range registry {
		if d.Source != "coinglass" {
			continue
		}
		h := fnv.New32a()
		h.Write([]byte(d.ID))
		phase := time.Duration(h.Sum32()%uint32(min(d.Refresh, 60))) * time.Second
		if d.Priority == 0 {
			phase = time.Duration(h.Sum32()%30) * time.Second
		}
		j := Job{ID: d.ID, Dataset: d, Next: now.Add(phase), Mode: "live"}
		if prev, ok := old[d.ID]; ok {
			j = prev
			j.Dataset = d
			j.InFlight = false
			if j.Next.Before(now) {
				j.Next = now.Add(phase)
			}
		}
		s.jobs[j.ID] = &j
	}
	for _, j := range saved {
		if j.Mode != "live" {
			j.InFlight = false
			s.jobs[j.ID] = &j
		}
	}
	return s
}
func (s *Scheduler) persistLocked() error {
	jobs := make([]savedJob, 0, len(s.jobs))
	for _, j := range s.jobs {
		jobs = append(jobs, savedJob{*j, j.Dataset.Path, j.Dataset.Params})
	}
	if e := s.store.SaveState("quota", s.quota); e != nil {
		return e
	}
	return s.store.SaveState("jobs", jobs)
}
func (s *Scheduler) State() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.quota.prune(time.Now())
	jobs := []Job{}
	for _, j := range s.jobs {
		cp := *j
		cp.Dataset.Params = nil
		jobs = append(jobs, cp)
	}
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].ID < jobs[j].ID })
	return map[string]any{"enabled": s.enabled, "limitPerMinute": 12, "usedLastMinute": len(s.quota.Starts), "calls": s.quota.Calls, "cooldownUntil": s.quota.Cooldown, "rateLimited": s.quota.RateLimited, "authFailed": s.quota.AuthFailed, "inFlight": s.inflight, "jobs": jobs}
}
func (s *Scheduler) priority(j *Job, now time.Time) float64 {
	wait := now.Sub(j.Next).Seconds()
	p := float64(j.Dataset.Priority)*1000 - wait/2
	if j.Mode != "live" {
		p = 2500 - wait/2
		if j.Mode == "history" {
			p = 4500 - wait/2
		}
		if j.Mode == "baseline" {
			p = 5500 - wait/2
		}
	}
	if j.Mode == "live" {
		if j.LastSuccess == nil {
			p -= 500
		} else {
			elapsed := now.Sub(*j.LastSuccess).Seconds()
			if elapsed > float64(j.Dataset.SoftDeadline) {
				p -= 2000
			}
			if elapsed > float64(j.Dataset.TTL-30) {
				p -= 5000
			}
		}
	}
	return p
}

// Step is the only gate allowed to initiate a CoinGlass request, including retries
// and on-demand jobs. The rolling ledger is committed before the network call.
func (s *Scheduler) Step(ctx context.Context, now time.Time) bool {
	s.mu.Lock()
	if !s.enabled || s.inflight >= 2 || !s.quota.available(now) {
		s.mu.Unlock()
		return false
	}
	var selected *Job
	for _, j := range s.jobs {
		if j.Disabled || j.Completed || j.InFlight || now.Before(j.Next) {
			continue
		}
		if j.Mode != "live" && s.store.Status().Paused {
			continue
		}
		if selected == nil || s.priority(j, now) < s.priority(selected, now) {
			selected = j
		}
	}
	if selected == nil {
		s.mu.Unlock()
		return false
	}
	selected.InFlight = true
	selected.LastAttempt = &now
	selected.Calls++
	s.inflight++
	s.quota.Starts = append(s.quota.Starts, now)
	s.quota.Calls++
	if e := s.persistLocked(); e != nil {
		selected.InFlight = false
		selected.Error = "无法保存额度账本，暂停请求"
		s.inflight--
		s.mu.Unlock()
		return false
	}
	copyJob := *selected
	s.mu.Unlock()
	s.wg.Add(1)
	go func() { defer s.wg.Done(); s.run(ctx, copyJob) }()
	return true
}
func (s *Scheduler) run(ctx context.Context, j Job) {
	d := j.Dataset
	params := map[string]string{}
	for k, v := range d.Params {
		params[k] = v
	}
	d.Params = params
	if j.From != nil {
		d.Params["start_time"] = strconv.FormatInt(j.From.UnixMilli(), 10)
	}
	if j.To != nil {
		end := *j.To
		if j.From != nil {
			end = minTime(end, j.From.Add(time.Duration(max(60, d.Resolution)*100)*time.Second))
		}
		d.Params["end_time"] = strconv.FormatInt(end.UnixMilli()-1, 10)
	}
	if j.Mode == "baseline" || j.Mode == "history" {
		d.Params["limit"] = "100"
	}
	raw, err := s.fetch(ctx, d)
	fetched := time.Now().UTC()
	var observations []Observation
	if err == nil {
		observations, err = Normalize(d, raw, fetched)
	}
	var last time.Time
	valid := 0
	if err == nil {
		for _, o := range observations {
			if j.From != nil && o.Time().Before(*j.From) {
				continue
			}
			if j.To != nil && !o.Time().Before(*j.To) {
				continue
			}
			if o.Quality != "missing" {
				valid++
			}
			if o.Time().After(last) {
				last = o.Time()
			}
			if _, e := s.store.Ingest(d, o); e != nil {
				err = e
				break
			}
		}
		if valid == 0 {
			err = ErrNoData
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.jobs[j.ID]
	current.InFlight = false
	s.inflight--
	now := time.Now().UTC()
	if err != nil {
		current.Failures++
		current.Error = err.Error()
		backoff := time.Duration(15*(1<<min(current.Failures, 7))) * time.Second
		current.Next = now.Add(backoff)
		var fe *FetchError
		if errors.As(err, &fe) {
			if fe.Status == 429 {
				s.quota.RateLimited++
				cool := max(time.Minute, fe.RetryAfter)
				s.quota.Cooldown = now.Add(min(time.Hour, cool))
			}
			if fe.Status == 401 || fe.Status == 403 {
				s.quota.AuthFailed = true
			}
		}
		// A bad market/unsupported parameter cannot be retried on every tick.
		if current.Failures >= 5 {
			current.Next = now.Add(max(time.Hour, time.Duration(d.Refresh)*time.Second))
			if current.Mode != "live" {
				current.Disabled = true
			}
		}
	} else {
		current.Error = ""
		current.Failures = 0
		current.LastSuccess = &fetched
		if j.Mode == "live" {
			next := current.Next
			for !next.After(now) {
				next = next.Add(time.Duration(d.Refresh) * time.Second)
			}
			current.Next = next
		} else if j.From != nil && j.To != nil {
			next := last.Add(time.Duration(max(60, d.Resolution)) * time.Second)
			if !next.After(*j.From) {
				current.Error = "历史接口未推进，停止重复补采"
				current.Disabled = true
			} else if current.From != nil && current.From.Before(*j.From) {
				current.Next = now.Add(10 * time.Second)
			} else if !next.Before(*current.To) {
				current.Completed = true
			} else {
				current.From = &next
				current.Next = now.Add(10 * time.Second)
			}
		} else {
			current.Completed = true
		}
	}
	_ = s.persistLocked()
}
func (s *Scheduler) Run(ctx context.Context) {
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			s.wg.Wait()
			return
		case now := <-tick.C:
			s.Step(ctx, now)
		}
	}
}

type DataRequest struct {
	Dataset    string     `json:"dataset"`
	From       *time.Time `json:"from"`
	To         *time.Time `json:"to"`
	Resolution int        `json:"resolutionSeconds"`
	Range      string     `json:"range"`
	Address    string     `json:"address"`
}

var publicAddress = regexp.MustCompile(`^0x[0-9a-fA-F]{40}$`)

func (s *Scheduler) Request(req DataRequest, now time.Time, baseline bool) (Job, error) {
	d, e := FindDataset(req.Dataset)
	if e != nil {
		return Job{}, e
	}
	if d.Source != "coinglass" {
		return Job{}, errors.New("该数据不使用CoinGlass按需队列")
	}
	mode := "on_demand"
	id := d.ID
	if req.Address != "" {
		if !publicAddress.MatchString(req.Address) {
			return Job{}, errors.New("无效公开地址")
		}
		hash := sha256.Sum256([]byte(strings.ToLower(req.Address)))
		d.ID = fmt.Sprintf("wallet.%x", hash[:8])
		d.Kind = "wallet"
		d.Path = "/v4/api/hyperliquid/user-position"
		d.Params = map[string]string{"user_address": strings.ToLower(req.Address)}
		d.Refresh = 0
		d.TTL = 180
		id = d.ID
	} else if req.Range != "" {
		if d.Kind != "map" && d.Kind != "heatmap" {
			return Job{}, errors.New("该数据不支持模型范围")
		}
		r := req.Range
		if r != "24h" && r != "7d" && r != "30d" {
			return Job{}, errors.New("仅支持24h、7d、30d")
		}
		if d.Kind == "map" && r == "24h" {
			r = "1d"
		}
		d.Params = map[string]string{"symbol": d.Asset, "range": r}
		if r != d.Params["range"] {
			return Job{}, errors.New("invalid range")
		}
		d.ID = d.ID + "@" + req.Range
		id = d.ID
	} else if req.From != nil || req.To != nil {
		if req.From == nil || req.To == nil || !req.From.Before(*req.To) || req.To.After(now) || req.From.Before(now.Add(-90*24*time.Hour)) {
			return Job{}, errors.New("历史范围必须在过去90天内")
		}
		if d.Kind != "book" && d.Kind != "footprint" && d.Kind != "flow" && d.Kind != "liquidations" {
			return Job{}, errors.New("该数据无已验证的历史补采接口")
		}
		interval := map[int]string{60: "1m", 300: "5m", 900: "15m", 3600: "1h"}[req.Resolution]
		if interval == "" {
			return Job{}, errors.New("无效历史粒度")
		}
		if d.Kind == "footprint" && req.Resolution < 300 {
			return Job{}, errors.New("1分钟足迹未验证可用")
		}
		if d.Kind == "book" && ((req.Resolution == 60 && req.From.Before(now.Add(-72*time.Hour))) || (req.Resolution == 300 && req.From.Before(now.Add(-15*24*time.Hour)))) {
			return Job{}, errors.New("超过上游盘口历史范围")
		}
		p := map[string]string{}
		for k, v := range d.Params {
			p[k] = v
		}
		p["interval"] = interval
		d.Params = p
		d.Resolution = req.Resolution
		mode = "history"
		if baseline {
			mode = "baseline"
		}
		id = fmt.Sprintf("%s@history%d", d.ID, req.Resolution)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.jobs) >= 128 {
		for key, j := range s.jobs {
			if j.Mode != "live" && !j.InFlight && (j.Completed || j.Disabled) && j.LastSuccess != nil && now.Sub(*j.LastSuccess) > 24*time.Hour {
				delete(s.jobs, key)
			}
		}
		if len(s.jobs) >= 128 {
			return Job{}, errors.New("任务记录达到上限，请等待过期清理")
		}
	}
	if old, ok := s.jobs[id]; ok {
		if old.Mode == "live" {
			return *old, nil
		}
		if req.From != nil && req.To != nil {
			if old.From != nil && req.From.Before(*old.From) {
				old.From = req.From
			}
			if old.To == nil || req.To.After(*old.To) {
				old.To = req.To
			}
			old.Completed = false
			old.Disabled = false
		} else if old.LastSuccess != nil && now.Sub(*old.LastSuccess) < time.Duration(d.TTL)*time.Second {
			return *old, nil
		} else {
			old.Completed = false
			old.Disabled = false
		}
		if old.Next.After(now) {
			old.Next = now
		}
		_ = s.persistLocked()
		return *old, nil
	}
	active := 0
	for _, j := range s.jobs {
		if j.Mode != "live" && !j.Completed {
			active++
		}
	}
	if active >= 32 {
		return Job{}, errors.New("后台队列已满，请稍后再试")
	}
	j := Job{ID: id, Dataset: d, Mode: mode, Next: now, From: req.From, To: req.To}
	s.jobs[id] = &j
	if e = s.persistLocked(); e != nil {
		delete(s.jobs, id)
		return Job{}, e
	}
	return j, nil
}
