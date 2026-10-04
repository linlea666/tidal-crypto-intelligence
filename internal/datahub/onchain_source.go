package datahub

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const costWireLimit int64 = 12 << 20
const costBodyLimit int64 = 32 << 20

type costPacked struct {
	T0 int64                      `json:"t0"`
	D  []int                      `json:"d"`
	N  int                        `json:"n"`
	V  []json.Number              `json:"v"`
	X  map[string]json.RawMessage `json:"x"`
	XD json.RawMessage            `json:"xd"`
}
type costSourceSeries struct {
	Key string      `json:"series_key"`
	PD  *costPacked `json:"pd"`
	PF  *costPacked `json:"pf"`
}
type costSourceChart struct {
	ID          int                `json:"id"`
	Slug        string             `json:"slug"`
	Description json.RawMessage    `json:"description"`
	Series      []costSourceSeries `json:"series"`
}
type costBundle struct {
	Frames     []CostFrame
	Prices     []CostPrice
	ETag       string
	BuiltAt    *time.Time
	CostError  string
	PriceError string
}
type costProtocolError struct{ message string }

func (e *costProtocolError) Error() string { return e.message }

type costHTTPError struct {
	Status int
	Retry  time.Duration
}

func (e *costHTTPError) Error() string { return fmt.Sprintf("链上来源 HTTP %d", e.Status) }

func costDates(p *costPacked, now time.Time) ([]string, error) {
	if p == nil || p.N != len(p.V) || p.N < 1 || p.N > 20000 || len(p.D) != 0 && len(p.D) != p.N {
		return nil, errors.New("时间序列长度变化")
	}
	t := time.UnixMilli(p.T0).UTC()
	if t.Hour() != 0 || t.Minute() != 0 || t.Second() != 0 || t.Nanosecond() != 0 {
		return nil, errors.New("日期不是 UTC 零点")
	}
	out := make([]string, p.N)
	for i := range p.V {
		if i > 0 {
			delta := 1
			if len(p.D) > 0 {
				delta = p.D[i]
			}
			if delta <= 0 || delta > 366 {
				return nil, errors.New("日期重复、乱序或间隔异常")
			}
			t = t.AddDate(0, 0, delta)
		}
		if _, e := costDay(costDate(t)); e != nil || !t.Before(now.UTC().Truncate(24*time.Hour)) {
			return nil, errors.New("日期未完成或无效")
		}
		out[i] = costDate(t)
	}
	return out, nil
}
func costDecodeCohorts(p *costPacked, now time.Time) (map[string]CostCohort, error) {
	dates, e := costDates(p, now)
	if e != nil {
		return nil, e
	}
	if len(dates) > 5000 {
		return nil, errors.New("成本快照数量超限")
	}
	out := map[string]CostCohort{}
	for i, date := range dates {
		raw, ok := p.X[strconv.Itoa(i)]
		if !ok {
			raw = p.XD
		}
		var x struct {
			Start  json.Number   `json:"start"`
			Step   json.Number   `json:"step"`
			Values []json.Number `json:"values"`
		}
		if len(raw) == 0 || json.Unmarshal(raw, &x) != nil {
			return nil, errors.New("缺少原始分桶")
		}
		c := CostCohort{Start: x.Start.String(), Step: x.Step.String(), Total: p.V[i].String(), Values: make([]string, len(x.Values))}
		for j, n := range x.Values {
			c.Values[j] = n.String()
		}
		if e := validateCostCohort(c); e != nil {
			return nil, e
		}
		out[date] = c
	}
	return out, nil
}

// Decode one chart at a time. Unrelated chart data never enters our warehouse.
func parseCostBundle(reader io.Reader, full bool, now time.Time) (costBundle, error) {
	out := costBundle{}
	limit := &io.LimitedReader{R: reader, N: costBodyLimit + 1}
	d := json.NewDecoder(limit)
	t, e := d.Token()
	if e != nil || t != json.Delim('[') {
		return out, &costProtocolError{"链上包根结构变化"}
	}
	var selected []costSourceChart
	for n := 0; d.More(); n++ {
		if n >= 512 {
			return out, &costProtocolError{"图表数量超限"}
		}
		var raw json.RawMessage
		if e = d.Decode(&raw); e != nil {
			return out, &costProtocolError{"链上包无法解析"}
		}
		var header struct {
			ID int `json:"id"`
		}
		if json.Unmarshal(raw, &header) != nil {
			return out, &costProtocolError{"图表标识无效"}
		}
		if header.ID == 1056 || header.ID == 118 {
			var c costSourceChart
			if json.Unmarshal(raw, &c) != nil {
				if header.ID == 1056 {
					out.CostError = "成本图结构变化"
				} else {
					out.PriceError = "价格图结构变化"
				}
				continue
			}
			selected = append(selected, c)
		}
	}
	if _, e = d.Token(); e != nil {
		return out, &costProtocolError{"链上包未结束"}
	}
	var extra any
	if e = d.Decode(&extra); e != io.EOF || limit.N <= 0 {
		return out, &costProtocolError{"链上包超限或存在尾随数据"}
	}
	prices := map[string]string{}
	cohorts := map[string]map[string]CostCohort{}
	seen := map[int]bool{}
	for _, c := range selected {
		if seen[c.ID] {
			return out, &costProtocolError{"重复图表"}
		}
		seen[c.ID] = true
		if c.ID == 1056 {
			var description string
			if json.Unmarshal(c.Description, &description) != nil || strings.Join(strings.Fields(description), " ") != onchainSourceDescription {
				out.CostError = "来源成本方法变化，需核对后更新适配器"
				continue
			}
		}
		priceSeen := false
		for _, s := range c.Series {
			p := s.PD
			if full && s.PF != nil {
				p = s.PF
			}
			if c.ID == 118 && s.Key == "price" {
				if priceSeen {
					out.PriceError = "重复价格序列"
					continue
				}
				priceSeen = true
				dates, err := costDates(p, now)
				if err != nil {
					out.PriceError = err.Error()
					continue
				}
				for i, date := range dates {
					v, err := costNumber(p.V[i].String(), true)
					if err != nil {
						out.PriceError = err.Error()
						break
					}
					prices[date] = v.String()
					out.Prices = append(out.Prices, CostPrice{Date: date, Value: v.String()})
				}
			}
			if c.ID == 1056 && (s.Key == "sth_supply" || s.Key == "lth_supply") {
				if cohorts[s.Key] != nil {
					out.CostError = "重复成本序列"
					continue
				}
				v, err := costDecodeCohorts(p, now)
				if err != nil {
					out.CostError = err.Error()
					continue
				}
				cohorts[s.Key] = v
			}
		}
	}
	if len(prices) == 0 {
		out.PriceError = "缺少完成日价格"
	}
	if out.PriceError != "" {
		out.Prices = nil
		prices = nil
	}
	if len(cohorts["sth_supply"]) == 0 || len(cohorts["sth_supply"]) != len(cohorts["lth_supply"]) {
		out.CostError = "缺少两类成本供给或日期未对齐"
	}
	if out.CostError == "" && out.PriceError == "" {
		for date, sth := range cohorts["sth_supply"] {
			lth, ok := cohorts["lth_supply"][date]
			price, pok := prices[date]
			if !ok || !pok {
				out.CostError = "两类供给与同日价格未对齐"
				break
			}
			f := CostFrame{Date: date, STH: sth, LTH: lth, Price: price, Method: onchainMethod}
			if err := validateCostFrame(f); err != nil {
				out.CostError = err.Error()
				break
			}
			out.Frames = append(out.Frames, f)
		}
	}
	if out.CostError != "" || out.PriceError != "" {
		out.Frames = nil
	}
	if len(out.Frames) == 0 && len(out.Prices) == 0 {
		return out, &costProtocolError{"目标数据均不可用：" + out.CostError + "; " + out.PriceError}
	}

	return out, nil
}
func costRequest(ctx context.Context, client *http.Client, url, etag string) (*http.Response, error) {
	req, e := http.NewRequestWithContext(ctx, "GET", url, nil)
	if e != nil {
		return nil, e
	}
	req.Header.Set("Accept-Encoding", "gzip")
	req.Header.Set("User-Agent", "Tidal/OnchainCost (public data; hourly)")
	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}
	r, e := client.Do(req)
	if e != nil {
		return nil, e
	}
	if r.StatusCode != 200 && r.StatusCode != 304 {
		r.Body.Close()
		retry := time.Duration(0)
		if n, e := strconv.Atoi(r.Header.Get("Retry-After")); e == nil && n > 0 {
			retry = time.Duration(n) * time.Second
		} else if at, e := http.ParseTime(r.Header.Get("Retry-After")); e == nil {
			retry = time.Until(at)
		}
		return nil, &costHTTPError{r.StatusCode, retry}
	}
	return r, nil
}
func fetchCostBundle(ctx context.Context, client *http.Client, base, etag string, full bool, now time.Time) (costBundle, bool, error) {
	resolution := "downsampled_v2"
	if full {
		resolution = "full_v2"
	}
	r, e := costRequest(ctx, client, base+"/serve?resolution="+resolution, etag)
	if e != nil {
		return costBundle{}, false, e
	}
	defer r.Body.Close()
	if r.StatusCode == 304 {
		return costBundle{}, true, nil
	}
	wire := &io.LimitedReader{R: r.Body, N: costWireLimit + 1}
	var reader io.Reader = wire
	switch r.Header.Get("Content-Encoding") {
	case "gzip":
		z, e := gzip.NewReader(wire)
		if e != nil {
			return costBundle{}, false, e
		}
		defer z.Close()
		reader = z
	case "", "identity":
	default:
		return costBundle{}, false, &costProtocolError{"未知压缩编码"}
	}
	b, e := parseCostBundle(reader, full, now)
	if wire.N <= 0 {
		return b, false, &costProtocolError{"压缩包超过12MiB"}
	}
	if e != nil {
		return b, false, e
	}
	b.ETag = r.Header.Get("ETag")
	for _, key := range []string{"X-Bundle-Built-At", "X-Bundle-BuiltAt"} {
		if at, e := time.Parse(time.RFC3339Nano, r.Header.Get(key)); e == nil {
			b.BuiltAt = &at
			break
		}
	}
	return b, false, nil
}
func costVersion(ctx context.Context, client *http.Client, base string) (string, error) {
	r, e := costRequest(ctx, client, base+"/version", "")
	if e != nil {
		return "", e
	}
	defer r.Body.Close()
	wire := &io.LimitedReader{R: r.Body, N: 65537}
	var reader io.Reader = wire
	switch r.Header.Get("Content-Encoding") {
	case "gzip":
		z, err := gzip.NewReader(wire)
		if err != nil {
			return "", err
		}
		defer z.Close()
		reader = z
	case "", "identity":
	default:
		return "", &costProtocolError{"版本接口压缩编码变化"}
	}
	b, e := io.ReadAll(io.LimitReader(reader, 65537))
	if e != nil {
		return "", e
	}
	var v struct {
		Version string `json:"version"`
	}
	if wire.N <= 0 || len(b) > 65536 || json.Unmarshal(b, &v) != nil || v.Version == "" || len(v.Version) > 128 {
		return "", &costProtocolError{"版本接口契约变化"}
	}
	return v.Version, nil
}
