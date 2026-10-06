package datahub

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

type ShortOperation struct {
	Count  int     `json:"count"`
	LastMS float64 `json:"lastMs"`
	MaxMS  float64 `json:"maxMs"`
}
type ShortStage struct {
	LastFailureAt        *time.Time                `json:"lastFailureAt,omitempty"`
	LastFailure          string                    `json:"lastFailure,omitempty"`
	LastFailureClass     string                    `json:"lastFailureClass,omitempty"`
	LastFailureOperation string                    `json:"lastFailureOperation,omitempty"`
	State                string                    `json:"state"`
	At                   time.Time                 `json:"at"`
	Success              *time.Time                `json:"lastSuccessAt"`
	Errors               int                       `json:"errors"`
	Error                string                    `json:"error,omitempty"`
	Class                string                    `json:"errorClass,omitempty"`
	DurationMS           float64                   `json:"durationMs"`
	Operations           map[string]ShortOperation `json:"operations"`
}
type ShortRuntime struct {
	Version          string                `json:"pipelineVersion"`
	Origin           time.Time             `json:"origin"`
	At               time.Time             `json:"at"`
	Stages           map[string]ShortStage `json:"stages"`
	PersistenceError string                `json:"persistenceError,omitempty"`
}
type shortTraceKey struct{}
type shortTrace struct {
	Operations    map[string]time.Duration
	LastOperation string
	Yielded       bool
}

func shortMeasure(ctx context.Context, name string, start time.Time) {
	if v, ok := ctx.Value(shortTraceKey{}).(*shortTrace); ok {
		v.Operations[name] += time.Since(start)
		v.LastOperation = name
	}
}
func shortYield(ctx context.Context) {
	if v, ok := ctx.Value(shortTraceKey{}).(*shortTrace); ok {
		v.Yielded = true
	}
}

type shortWriteFailure struct{ error }

func (e shortWriteFailure) Unwrap() error { return e.error }
func shortWriteError(e error) error {
	if e == nil {
		return nil
	}
	return shortWriteFailure{e}
}
func shortErrorClass(e error) string {
	if e == nil {
		return ""
	}
	if strings.Contains(e.Error(), "容量") || strings.Contains(e.Error(), "sub-budget") || strings.Contains(e.Error(), "SQLITE_FULL") {
		return "capacity"
	}
	var write shortWriteFailure
	if errors.As(e, &write) {
		return "write"
	}
	if errors.Is(e, context.DeadlineExceeded) {
		return "timeout"
	}
	if strings.Contains(e.Error(), "SQLITE_BUSY") || strings.Contains(e.Error(), "locked") {
		return "database_busy"
	}
	return "read_or_compute"
}
func (w *Warehouse) shortRuntimeView() *ShortRuntime {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.shortRuntime == nil {
		return nil
	}
	// A bounded deep copy lets JSON encoders run outside the state mutex.
	b, _ := json.Marshal(w.shortRuntime)
	var out ShortRuntime
	_ = json.Unmarshal(b, &out)
	return &out
}
func (w *Warehouse) recordShortRuntime(name string, now time.Time, elapsed time.Duration, trace *shortTrace, e error) {
	r := w.shortRuntimeView()
	if r == nil {
		r = &ShortRuntime{Version: ShortPipeline, Origin: now, Stages: map[string]ShortStage{}}
	}
	if r.Stages == nil {
		r.Stages = map[string]ShortStage{}
	}
	r.At = now
	s := r.Stages[name]
	s.At = now
	s.DurationMS = float64(elapsed) / float64(time.Millisecond)
	if s.Operations == nil {
		s.Operations = map[string]ShortOperation{}
	}
	for key, d := range trace.Operations {
		op := s.Operations[key]
		op.Count++
		op.LastMS = float64(d) / float64(time.Millisecond)
		op.MaxMS = max(op.MaxMS, op.LastMS)
		s.Operations[key] = op
	}
	s.State = "ready"
	s.Error = ""
	s.Class = ""
	if e != nil {
		s.State = "error"
		s.Errors++
		s.Error = e.Error()
		s.Class = shortErrorClass(e)
		s.LastFailureAt, s.LastFailure, s.LastFailureClass = flowPtr(now), e.Error(), s.Class
		s.LastFailureOperation = trace.LastOperation
	} else if trace.Yielded {
		s.State = "yielded"
	} else {
		s.Success = flowPtr(now)
	}
	r.Stages[name] = s
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	r.PersistenceError = ""
	if e := w.shortSaveState(ctx, "short-flow/runtime-v2", r); e != nil {
		r.PersistenceError = "诊断持久化失败；当前运行数据仅在内存中"
	}
	w.mu.Lock()
	w.shortRuntime = r
	w.mu.Unlock()
}

type shortPublication struct {
	Input     string     `json:"inputVersion"`
	First     *time.Time `json:"firstGeneratedAt"`
	Available *time.Time `json:"inputAvailableAt"`
}

// Price/background/threshold refreshes do not restart a flow publication clock.
func (h *Hub) shortPublication(ctx context.Context, s *ShortObservation, now time.Time) (*shortPublication, error) {
	values := map[string]FlowWindow{}
	for key, w := range s.Windows {
		w.VolumeRatio = nil
		values[key] = w
	}
	b, _ := json.Marshal(struct {
		Windows   map[string]FlowWindow
		Available *time.Time
	}{values, s.InputAvailable})
	hash := sha256.Sum256(b)
	s.InputVersion = fmt.Sprintf("%x", hash[:])
	var p shortPublication
	e := h.Store.shortLoad(ctx, "publication", s.InputVersion, &p)
	if e != nil && e != sql.ErrNoRows {
		return nil, e
	}
	fresh := e == sql.ErrNoRows
	if fresh {
		p = shortPublication{Input: s.InputVersion, Available: s.InputAvailable}
		var origin time.Time
		if err := h.Store.shortLoad(ctx, "pipeline-origin", ShortPipeline, &origin); err != nil {
			return nil, err
		}
		if s.InputAvailable != nil && !s.InputAvailable.Before(origin) {
			p.First = flowPtr(now)
			var old ShortObservation
			if h.Store.shortState(ctx, "short-flow/current", &old) && old.InputVersion == s.InputVersion {
				p.First = old.FirstGenerated
			}
		}
	}
	s.FirstGenerated = p.First
	s.InputAvailable = p.Available
	if p.First != nil && p.Available != nil {
		s.FirstDelay = flowPtr(max(0, p.First.Sub(*p.Available).Seconds()))
	}
	if s.Available != nil {
		s.ArrivalDelay = flowPtr(max(0, s.Available.Sub(s.Through).Seconds()))
	}
	if !fresh {
		return nil, nil
	}
	return &p, nil
}
