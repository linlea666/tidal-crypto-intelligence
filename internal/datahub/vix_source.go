package datahub

// VIX is an index of implied volatility, never a crypto asset or USD price.
import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/shopspring/decimal"
)

const (
	vixMinuteURL = "https://gi.finance.sina.com.cn/hq/min?symbol=VIX"
	vixDailyURL  = "https://cdn-api.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv"
	vixSourceTTL = 25 * time.Minute
	vixFetchTTL  = 2 * time.Minute
	vixBodyLimit = 2 << 20
)

var vixShanghai = mustVIXLocation("Asia/Shanghai")
var vixNewYork = mustVIXLocation("America/New_York")

func mustVIXLocation(name string) *time.Location {
	z, err := time.LoadLocation(name)
	if err != nil {
		panic(err)
	} // tzdata is embedded by observers.go.
	return z
}

type VIXPoint struct {
	At          time.Time `json:"at"`
	Value       string    `json:"value"`
	TradingDate string    `json:"tradingDate"`
}
type VIXDaily struct {
	Date  string `json:"date"`
	Open  string `json:"open"`
	High  string `json:"high"`
	Low   string `json:"low"`
	Close string `json:"close"`
}
type VIXFeed struct {
	Source      string     `json:"source"`
	Latest      *VIXPoint  `json:"latest"`
	FetchedAt   *time.Time `json:"fetchedAt"`
	AttemptedAt *time.Time `json:"attemptedAt"`
	Error       string     `json:"error"`
	LastDate    string     `json:"lastDate,omitempty"`
}

func vixNumber(s string) (string, error) {
	if len(s) == 0 || len(s) > 24 || strings.ContainsAny(s, "eE+ \t\r\n") {
		return "", errors.New("VIX数值格式错误")
	}
	x, err := decimal.NewFromString(s)
	if err != nil || !x.IsPositive() {
		return "", errors.New("VIX必须为有效正数")
	}
	return x.String(), nil
}

// Sina explicitly documents Beijing times. Only the first row has the session
// date; a genuine 23:xx -> 00:xx rollover advances the calendar date once.
// The two zero columns and first-row sixth column have no verified contract.
func parseVIXMinute(b []byte, now time.Time) ([]VIXPoint, error) {
	var root struct {
		Code   *int `json:"code"`
		Result struct {
			Data   [][]string `json:"data"`
			Status struct {
				Code *int `json:"code"`
			} `json:"status"`
		} `json:"result"`
	}
	if len(b) > vixBodyLimit || json.Unmarshal(b, &root) != nil || root.Code == nil || *root.Code != 0 || root.Result.Status.Code == nil || *root.Result.Status.Code != 0 {
		return nil, errors.New("新浪VIX响应格式或状态码异常")
	}
	rows := root.Result.Data
	if len(rows) == 0 || len(rows) > 1500 || len(rows[0]) != 6 {
		return nil, errors.New("新浪VIX缺少有效分时/首条日期")
	}
	day, err := time.ParseInLocation("2006-01-02", rows[0][4], vixShanghai)
	if err != nil {
		return nil, errors.New("新浪VIX交易日格式错误")
	}
	date := rows[0][4]
	out := make([]VIXPoint, 0, len(rows))
	previous, rolled := -1, false
	for i, r := range rows {
		if i > 0 && len(r) != 4 {
			return nil, errors.New("新浪VIX分时字段数变化")
		}
		t, err := time.Parse("15:04", r[0])
		if err != nil {
			return nil, errors.New("新浪VIX分钟时间格式错误")
		}
		minute := t.Hour()*60 + t.Minute()
		if minute < previous {
			if rolled || previous < 23*60 || minute >= 60 {
				return nil, errors.New("新浪VIX时间乱序")
			}
			day = day.AddDate(0, 0, 1)
			rolled = true
		}
		value, err := vixNumber(r[1])
		if err != nil {
			return nil, err
		}
		at := day.Add(time.Duration(minute) * time.Minute).UTC()
		if at.After(now.Add(30*time.Second)) || !vixSession(at) || at.In(vixNewYork).Format("2006-01-02") != date {
			return nil, errors.New("新浪VIX时间超前或不在已核实发布时段")
		}
		p := VIXPoint{at, value, date}
		if len(out) > 0 && at.Equal(out[len(out)-1].At) {
			if value != out[len(out)-1].Value {
				return nil, errors.New("新浪VIX同分钟数值冲突")
			}
			continue
		}
		out = append(out, p)
		previous = minute
	}
	return out, nil
}

func parseVIXDaily(b []byte, now time.Time) ([]VIXDaily, error) {
	if len(b) > vixBodyLimit {
		return nil, errors.New("Cboe日线超过响应上限")
	}
	r := csv.NewReader(bytes.NewReader(bytes.TrimPrefix(b, []byte{0xef, 0xbb, 0xbf})))
	header, err := r.Read()
	if err != nil || strings.Join(header, ",") != "DATE,OPEN,HIGH,LOW,CLOSE" {
		return nil, errors.New("Cboe日线列定义变化")
	}
	out := []VIXDaily{}
	last := ""
	cutoff := now.In(vixNewYork).AddDate(-1, 0, -1).Format("2006-01-02")
	for n := 0; ; n++ {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil || len(row) != 5 || n >= 20000 {
			return nil, errors.New("Cboe日线格式或行数异常")
		}
		d, err := time.ParseInLocation("01/02/2006", row[0], vixNewYork)
		if err != nil {
			return nil, errors.New("Cboe日期格式错误")
		}
		date := d.Format("2006-01-02")
		if date <= last || d.Add(16*time.Hour+15*time.Minute).After(now) {
			return nil, errors.New("Cboe日期重复、乱序或尚未收盘")
		}
		last = date
		// The official archive contains a few inconsistent legacy OHLC rows.
		// They are outside this product's one-year import; never repair them or
		// let unconsumed historical prices invalidate the current window.
		if date < cutoff {
			continue
		}
		values := [4]string{}
		for j := range values {
			values[j], err = vixNumber(row[j+1])
			if err != nil {
				return nil, err
			}
		}
		if dec(values[1]).LessThan(dec(values[0])) || dec(values[1]).LessThan(dec(values[3])) || dec(values[2]).GreaterThan(dec(values[0])) || dec(values[2]).GreaterThan(dec(values[3])) {
			return nil, errors.New("Cboe日线高低范围冲突")
		}
		if date >= cutoff {
			out = append(out, VIXDaily{date, values[0], values[1], values[2], values[3]})
		}
	}
	if len(out) == 0 {
		return nil, errors.New("Cboe近一年日线缺失")
	}
	return out, nil
}

func vixSession(at time.Time) bool {
	t := at.In(vixNewYork)
	if t.Weekday() == time.Saturday || t.Weekday() == time.Sunday {
		return false
	}
	m := t.Hour()*60 + t.Minute()
	return m >= 195 && m <= 565 || m >= 570 && m <= 975
}
func vixFastPoll(at time.Time) bool {
	// Poll through the delayed final publication; begin at each session opening
	// even if a previous slow poll would otherwise sleep across it.
	return vixSession(at) || vixSession(at.Add(-25*time.Minute))
}
func vixNextPoll(now time.Time) time.Duration {
	if vixFastPoll(now) {
		return time.Minute
	}
	for d := time.Minute; d < 15*time.Minute; d += time.Minute {
		if vixFastPoll(now.Add(d)) {
			return d
		}
	}
	return 15 * time.Minute
}
func vixLevel(value string) string {
	v := dec(value)
	if v.GreaterThan(dec("40")) {
		return "priority"
	}
	if v.GreaterThanOrEqual(dec("30")) {
		return "watch"
	}
	if v.GreaterThanOrEqual(dec("20")) {
		return "elevated"
	}
	return "low"
}
func vixQuality(f VIXFeed, now time.Time) (string, string) {
	if f.Error != "" {
		return "error", f.Error
	}
	if f.Latest == nil || f.FetchedAt == nil {
		return "missing", "尚未取得新浪分时数据"
	}
	if f.Latest.At.After(now.Add(30*time.Second)) || f.FetchedAt.After(now.Add(30*time.Second)) || !vixSession(f.Latest.At) {
		return "error", "行情时间异常，提醒暂停"
	}
	if now.Sub(f.Latest.At) > vixSourceTTL {
		if !vixFastPoll(now) {
			return "closed", "非发布时段，显示最后有效行情；不触发提醒"
		}
		return "stale", "来源时间超过25分钟；节假日或源停更待核实，提醒暂停"
	}
	if now.Sub(*f.FetchedAt) > vixFetchTTL {
		return "stale", "超过2分钟未成功获取，提醒暂停"
	}
	return "delayed", "新浪延时行情（来源声明至少15分钟）；按检测时刻提醒"
}

type vixHTTPError struct {
	delay  time.Duration
	status int
}

func (e vixHTTPError) Error() string { return fmt.Sprintf("VIX数据源HTTP %d", e.status) }

func fetchVIX(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Timeout: 12 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	r, err := client.Do(req)
	if err != nil {
		return nil, errors.New("VIX数据源连接失败或超时")
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		d := time.Duration(0)
		if s, e := strconv.Atoi(r.Header.Get("Retry-After")); e == nil && s > 0 {
			d = time.Duration(min(s, 86400)) * time.Second
		} else if t, e := http.ParseTime(r.Header.Get("Retry-After")); e == nil {
			d = min(24*time.Hour, max(time.Duration(0), time.Until(t)))
		}
		return nil, vixHTTPError{d, r.StatusCode}
	}
	b, err := io.ReadAll(io.LimitReader(r.Body, vixBodyLimit+1))
	if err != nil || len(b) > vixBodyLimit {
		return nil, errors.New("VIX数据响应不完整或超过2MiB")
	}
	return b, nil
}

func (h *Hub) vixCollector(ctx context.Context) {
	backoff := time.Minute
	for ctx.Err() == nil {
		b, err := h.vixFetch(ctx, vixMinuteURL)
		now := time.Now().UTC()
		if err == nil {
			var points []VIXPoint
			points, err = parseVIXMinute(b, now)
			if err == nil {
				err = h.ingestVIX(ctx, points, now)
			}
		}
		delay := vixNextPoll(now)
		if err != nil {
			_ = h.vixFailure(ctx, "sina", err.Error(), now)
			delay = max(delay, backoff)
			backoff = min(15*time.Minute, backoff*2)
			var httpErr vixHTTPError
			if errors.As(err, &httpErr) {
				delay = max(delay, httpErr.delay)
			}
		} else {
			backoff = time.Minute
		}
		if !sleep(ctx, delay) {
			return
		}
	}
}
func (h *Hub) vixDailyCollector(ctx context.Context) {
	for ctx.Err() == nil {
		now := time.Now().UTC()
		f, err := h.vixFeed(ctx, "cboe")
		if err == nil && f.FetchedAt != nil && f.Error == "" && now.Sub(*f.FetchedAt) < 6*time.Hour {
			if !sleep(ctx, min(time.Minute, 6*time.Hour-now.Sub(*f.FetchedAt))) {
				return
			}
			continue
		}
		b, err := h.vixFetch(ctx, vixDailyURL)
		now = time.Now().UTC()
		if err == nil {
			var rows []VIXDaily
			rows, err = parseVIXDaily(b, now)
			if err == nil {
				err = h.ingestVIXDaily(ctx, rows, now)
			}
		}
		delay := 6 * time.Hour
		if err != nil {
			_ = h.vixFailure(ctx, "cboe", err.Error(), now)
			delay = 15 * time.Minute
			var httpErr vixHTTPError
			if errors.As(err, &httpErr) {
				delay = max(delay, httpErr.delay)
			}
		}
		if !sleep(ctx, delay) {
			return
		}
	}
}
