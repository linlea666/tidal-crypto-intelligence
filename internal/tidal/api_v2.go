package tidal

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"strings"
	"time"

	"github.com/coder/websocket"
	"github.com/linlea666/tidal-crypto-intelligence/internal/datahub"
)

func (s *Server) apiV2(w http.ResponseWriter, r *http.Request, a string) bool {
	path := strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, "/api/v2/"), "/api/v1/")
	if path == "settings" || path == "annotations" || path == "logout" {
		if strings.HasPrefix(r.URL.Path, "/api/v2/") {
			copyReq := r.Clone(r.Context())
			copyReq.URL.Path = "/api/v1/" + path
			s.api(w, copyReq)
			return true
		}
		return false
	}
	if path == "health" {
		if r.Method != "GET" {
			problem(w, 405, "GET required")
			return true
		}
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		jsonOut(w, map[string]any{"runtime": map[string]any{"heapBytes": mem.HeapAlloc, "heapSysBytes": mem.HeapSys, "goroutines": runtime.NumGoroutine()}, "data": s.Hub.Status(), "storage": s.Hub.Store.Status(), "version": s.Version, "now": time.Now().UTC()})
		return true
	}
	if path == "watchlist" {
		problem(w, 410, "旧地址轮询已下线，请通过V2钱包详情队列查询")
		return true
	}
	if path == "data-requests" {
		if r.Method != "POST" {
			problem(w, 405, "POST required")
			return true
		}
		var req datahub.DataRequest
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
		dec.DisallowUnknownFields()
		if dec.Decode(&req) != nil {
			problem(w, 400, "无效任务请求")
			return true
		}
		j, e := s.Hub.Request(req)
		if e != nil {
			problem(w, 400, e.Error())
			return true
		}
		w.WriteHeader(http.StatusAccepted)
		jsonOut(w, j)
		return true
	}
	if r.Method != "GET" {
		problem(w, 405, "GET required")
		return true
	}
	if path == "stream" {
		s.streamV2(w, r)
		return true
	}
	select {
	case s.queries <- struct{}{}:
		defer func() { <-s.queries }()
	default:
		problem(w, 429, "本地查询繁忙，请稍后重试")
		return true
	}
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()
	b, e := s.Hub.Read(ctx, path, r.URL.Query())
	if e != nil {
		problem(w, 400, e.Error())
		return true
	}
	// Authentication was performed before both local-cache reads and 304 responses.
	hash := sha256.Sum256(b)
	etag := fmt.Sprintf("\"%x\"", hash[:12])
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "private, no-cache")
	w.Header().Set("Vary", "Cookie")
	w.Header().Set("ETag", etag)
	if r.Header.Get("If-None-Match") == etag {
		w.WriteHeader(304)
		return true
	}
	_, _ = w.Write(b)
	return true
}
func (s *Server) streamV2(w http.ResponseWriter, r *http.Request) {
	conn, e := websocket.Accept(w, r, nil)
	if e != nil {
		return
	}
	defer conn.CloseNow()
	conn.SetReadLimit(1024)
	ctx := conn.CloseRead(r.Context())
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		if !s.authenticated(r) {
			_ = conn.Close(websocket.StatusPolicyViolation, "会话过期")
			return
		}
		b, e := s.Hub.Read(ctx, "overview", r.URL.Query())
		if e != nil {
			return
		}
		writeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		e = conn.Write(writeCtx, websocket.MessageText, b)
		cancel()
		if e != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}
