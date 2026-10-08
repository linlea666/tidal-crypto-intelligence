package datahub

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/url"
	"sort"
	"time"
)

type auditEvent struct {
	Kind     string    `json:"kind"`
	At       time.Time `json:"at"`
	ParentID string    `json:"parentId"`
	Text     string    `json:"text"`
	Price    *string   `json:"priceUsdt"`
	Group    string    `json:"group,omitempty"`
}

func auditSignal(s Signal) map[string]any {
	price := s.DetectionPrice
	if price == nil && s.ReferencePrice > 0 {
		price = &s.ReferencePrice
	}
	var input any
	if s.Multifactor != nil {
		v := s.Multifactor
		input = map[string]any{"dataThrough": v.DataThrough, "net1hUsd": netDecimal(v.Spot["60"].Net), "net4hUsd": netDecimal(v.Spot["240"].Net), "priceUsdt": finiteDecimal(v.Price.Close), "priorAtrUsdt": finiteDecimal(v.Price.PriorATR), "displacementAtr": finiteDecimal(v.Price.DisplacementATR)}
	}
	return map[string]any{"id": s.ID, "direction": s.Direction, "rulesVersion": s.Rules, "at": s.At, "dataThrough": s.DataThrough, "expiresAt": s.Expires, "priceUsdt": finiteDecimal(price), "confirmedAt": s.ConfirmedAt, "confirmedDataThrough": s.ConfirmedThrough, "computedAt": s.ComputedAt, "confirmationComputedAt": s.ConfirmationComputedAt, "coreInputFirstSeenAt": s.CoreInputFirstSeenAt, "coreInputAvailableAt": s.CoreInputAvailableAt, "initialInput": input, "meaning": alertMeaning(s, false)}
}
func auditTrial(t ShortTrial) map[string]any {
	outcomes := []any{}
	for _, o := range t.Outcomes {
		if o.Minutes == 60 || o.Minutes == 240 {
			outcomes = append(outcomes, map[string]any{"minutes": o.Minutes, "state": o.State, "coverage": o.Coverage, "returnPercent": finiteDecimal(o.Return), "mfePercent": finiteDecimal(o.MFE), "maePercent": finiteDecimal(o.MAE), "mfeBarAt": o.MFEAt, "maeBarAt": o.MAEAt})
		}
	}
	return map[string]any{"at": t.At, "start": t.Start, "referenceUsdt": finiteDecimal(t.Reference), "done": t.Done, "outcomes": outcomes, "note": "登记时点后的下一根完整五分钟现货K线开盘价；无费用，不是合约成交收益"}
}

func auditRange(q url.Values, now time.Time) (time.Time, time.Time, error) {
	to, from := now, now.Add(-48*time.Hour)
	var e error
	if q.Get("to") != "" {
		to, e = time.Parse(time.RFC3339, q.Get("to"))
		if e != nil {
			return from, to, errors.New("结束时间必须为RFC3339")
		}
	}
	if q.Get("from") != "" {
		from, e = time.Parse(time.RFC3339, q.Get("from"))
		if e != nil {
			return from, to, errors.New("开始时间必须为RFC3339")
		}
	} else {
		from = to.Add(-48 * time.Hour)
	}
	if !from.Before(to) || to.Sub(from) > 7*24*time.Hour || to.After(now.Add(time.Minute)) {
		return from, to, errors.New("审查范围须为已发生的最多7天")
	}
	return from.UTC(), to.UTC(), nil
}
func (h *Hub) alertAudit(ctx context.Context, q url.Values, now time.Time) (any, error) {
	if h.Store.alertAuditError != "" {
		return nil, errors.New("候选存储初始化失败：" + h.Store.alertAuditError)
	}
	if q.Get("asset") != "" && q.Get("asset") != "BTC" {
		return nil, errors.New("分层审查仅支持BTC")
	}
	from, to, e := auditRange(q, now)
	if e != nil {
		return nil, e
	}
	layer, side, id := q.Get("layer"), q.Get("direction"), q.Get("id")
	if layer != "" && layer != "formal" && layer != "confirmation" && layer != "candidate" && layer != "rejected" && layer != "risk" {
		return nil, errors.New("无效预警层级")
	}
	if side != "" && side != "buy" && side != "sell" {
		return nil, errors.New("无效方向")
	}
	if len(id) > 240 {
		return nil, errors.New("无效事件ID")
	}
	limit, offset := parseInt(q, "limit", 20, 1, 50), parseInt(q, "offset", 0, 0, 10000)
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	db := h.Store.shortDB()
	var origin *time.Time
	var raw []byte
	e = db.QueryRowContext(ctx, "SELECT payload FROM alert_audit WHERE kind='origin' AND id=?", LayeredRules).Scan(&raw)
	if e == nil {
		var t time.Time
		if e = json.Unmarshal(raw, &t); e != nil {
			return nil, e
		}
		origin = &t
	} else if e != sql.ErrNoRows {
		return nil, e
	}
	var used int64
	if e = db.QueryRowContext(ctx, "SELECT used FROM alert_audit_budget WHERE id=1").Scan(&used); e != nil {
		return nil, e
	}
	h.mu.RLock()
	runtime := map[string]any{"lastAttemptAt": h.layeredLastAttempt, "lastSuccessAt": h.layeredLastSuccess, "lastFailureAt": h.layeredLastFailure, "lastError": h.layeredFailure}
	h.mu.RUnlock()
	result := map[string]any{"at": now, "from": from, "to": to, "metricDefinitions": alertMetricDefinitions(), "rulesVersion": LayeredRules, "origin": origin, "runtime": runtime, "storageBytes": used, "budgetBytes": layeredBudget, "limit": limit, "offset": offset, "note": "风险观察→正式异动及独立突破确认→影子候选。候选不发邮件、不改变A/B模拟。历史无记录保持未知。", "items": []any{}, "events": []auditEvent{}, "prices": []any{}, "more": false}
	if layer == "risk" {
		return h.auditRisk(ctx, q, from, to, result)
	}
	// Filter parent events, with confirmation/candidate/rejection time in range.
	// id detail intentionally ignores the list interval, but cannot fetch upstream.
	query := `SELECT d.payload FROM documents d WHERE d.kind='signal' AND d.asset='BTC' AND json_extract(d.payload,'$.rulesVersion')=? AND (?='' OR d.id=?) AND (?='' OR json_extract(d.payload,'$.direction')=?) AND (?<>'' OR (d.at>=? AND d.at<?))`
	args := []any{MultifactorRules, id, id, side, side, id, from.Add(-4 * time.Hour).Unix(), to.Unix()}
	if layer == "confirmation" {
		query += ` AND json_extract(d.payload,'$.confirmedAt') IS NOT NULL`
	}
	if layer == "candidate" || layer == "rejected" {
		kind := "candidate"
		if layer == "rejected" {
			kind = "decision"
		}
		query += ` AND EXISTS(SELECT 1 FROM alert_audit a WHERE a.parent=d.id AND a.kind=? AND a.at>=? AND a.at<?`
		args = append(args, kind, from.Unix(), to.Unix())
		if layer == "rejected" {
			query += ` AND json_extract(a.payload,'$.accepted')=0`
		}
		query += `)`
	}
	if id == "" {
		if layer == "confirmation" {
			query += ` AND CAST(strftime('%s',json_extract(d.payload,'$.confirmedAt')) AS INTEGER)>=? AND CAST(strftime('%s',json_extract(d.payload,'$.confirmedAt')) AS INTEGER)<?`
			args = append(args, from.Unix(), to.Unix())
		} else if layer == "formal" {
			query += ` AND d.at>=?`
			args = append(args, from.Unix())
		} else if layer == "" {
			query += ` AND (d.at>=? OR CAST(strftime('%s',json_extract(d.payload,'$.confirmedAt')) AS INTEGER)>=?)`
			args = append(args, from.Unix(), from.Unix())
		}
	}
	query += ` ORDER BY d.at DESC,d.id DESC LIMIT ? OFFSET ?`
	args = append(args, limit+1, offset)
	signals, e := paperRows[Signal](ctx, db, query, args...)
	if e != nil {
		return nil, e
	}
	more := len(signals) > limit
	if more {
		signals = signals[:limit]
	}
	ids := []string{}
	for _, s := range signals {
		ids = append(ids, s.ID)
	}
	mails, e := h.signalMailResults(ctx, ids)
	if e != nil {
		return nil, e
	}
	items := []any{}
	events := []auditEvent{}
	for _, s := range signals {
		item := auditSignal(s)
		// Descriptive event returns are separate from candidate enrollment and
		// paper fills. GET may calculate from local retained facts, never writes.
		observation := newShortTrial("audit/"+s.ID, "audit-descriptive", s.Direction, s.At, s.DataThrough, nil)
		if e = h.advanceShortTrial(ctx, observation, now); e != nil {
			return nil, e
		}
		observed := auditTrial(*observation)
		observed["note"] = "按当前仍保存的历史现货K线复算；首次异动后下一根完整5分钟开盘为起点，无费用，不补成候选或模拟前向成绩"
		item["observation"] = observed
		var clock any
		e = db.QueryRowContext(ctx, "SELECT payload FROM documents WHERE kind='signal-clock' AND id=?", s.ID).Scan(&raw)
		if e == nil {
			if e = json.Unmarshal(raw, &clock); e != nil {
				return nil, e
			}
		} else if e != sql.ErrNoRows {
			return nil, e
		}
		item["publicationClock"] = clock
		decisions, e := paperRows[LayeredDecision](ctx, db, "SELECT payload FROM alert_audit WHERE kind='decision' AND parent=? ORDER BY at,id LIMIT 100", s.ID)
		if e != nil {
			return nil, e
		}
		item["decisions"] = decisions
		candidates, e := paperRows[LayeredDecision](ctx, db, "SELECT payload FROM alert_audit WHERE kind='candidate' AND parent=? ORDER BY at LIMIT 1", s.ID)
		if e != nil {
			return nil, e
		}
		item["candidates"] = candidates
		outcomes, e := paperRows[ShortTrial](ctx, db, "SELECT payload FROM alert_audit WHERE kind='outcome' AND parent=? LIMIT 1", s.ID)
		if e != nil {
			return nil, e
		}
		item["candidateOutcome"] = nil
		for _, t := range outcomes {
			item["candidateOutcome"] = auditTrial(t)
		}
		itemMails := []SignalMailResult{}
		price, _ := item["priceUsdt"].(*string)
		events = append(events, auditEvent{Kind: "formal", At: s.At, ParentID: s.ID, Text: "首次正式异动", Price: price})
		if s.ConfirmedAt != nil {
			events = append(events, auditEvent{Kind: "confirmation", At: *s.ConfirmedAt, ParentID: s.ID, Text: "已发生突破确认"})
		}
		for _, n := range mails {
			if n.SignalID != s.ID {
				continue
			}
			itemMails = append(itemMails, n)
			if n.Completed != nil {
				events = append(events, auditEvent{Kind: "notification", At: *n.Completed, ParentID: s.ID, Text: n.Status + " · " + n.Kind})
			}
		}
		item["notifications"] = itemMails
		for _, d := range candidates {
			events = append(events, auditEvent{Kind: "candidate", At: d.At, ParentID: s.ID, Text: "影子候选产生", Price: d.Input.Price})
		}
		trades, e := h.auditPaper(ctx, s.ID, &events)
		if e != nil {
			return nil, e
		}
		item["paper"] = trades
		items = append(items, item)
	}
	prices := []any{}
	byTime := map[int64]*string{}
	e = h.Store.FactsAsOf(ctx, ID("candles", "BTC", "Binance", "spot"), from.Truncate(5*time.Minute), to, now, func(o Observation) error {
		at := recordTime(o)
		if o.Quality == "valid" && o.Resolution == 300 && at.Unix()%300 == 0 && o.Payload.Candle != nil && !at.Add(5*time.Minute).After(to) {
			byTime[at.Add(5*time.Minute).Unix()] = finiteDecimal(&o.Payload.Candle.Close)
		}
		return nil
	})
	if e != nil {
		return nil, e
	}
	for at := from.Truncate(5 * time.Minute).Add(5 * time.Minute); !at.After(to); at = at.Add(5 * time.Minute) {
		prices = append(prices, map[string]any{"at": at, "closeUsdt": byTime[at.Unix()]})
	}
	quality := []paperEvent{}
	qualityMore := false
	if p := h.Store.paper; p != nil {
		quality, e = paperRows[paperEvent](ctx, p.readDB, "SELECT payload FROM paper_events WHERE at>=? AND at<? ORDER BY at LIMIT 201", from.UnixNano(), to.UnixNano())
		if e != nil {
			return nil, e
		}
		qualityMore = len(quality) > 200
		if qualityMore {
			quality = quality[:200]
		}
		for _, v := range quality {
			events = append(events, auditEvent{Kind: "quality", At: v.At, Text: v.Kind + " · " + v.Reason})
		}
	}
	result["qualityMore"] = qualityMore
	visible := []auditEvent{}
	for _, v := range events {
		if !v.At.Before(from) && v.At.Before(to) {
			visible = append(visible, v)
		}
	}
	sort.SliceStable(visible, func(i, j int) bool { return visible[i].At.Before(visible[j].At) })
	result["items"], result["events"], result["prices"], result["more"] = items, visible, prices, more
	result["timelineNote"] = "时间轴显示当前页关联事件及区间内系统质量记录；价格是Binance现货完整五分钟收盘，模拟成交是独立永续报价，不混算收益。"
	return result, nil
}
func (h *Hub) auditPaper(ctx context.Context, id string, events *[]auditEvent) ([]any, error) {
	out := []any{}
	p := h.Store.paper
	if p == nil {
		return out, nil
	}
	tx, e := p.readDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if e != nil {
		return nil, e
	}
	defer tx.Rollback()
	var s paperState
	var raw []byte
	if e = tx.QueryRowContext(ctx, "SELECT payload FROM paper_state WHERE id=1").Scan(&raw); e != nil {
		return nil, e
	}
	if e = json.Unmarshal(raw, &s); e != nil {
		return nil, e
	}
	for _, group := range []string{"opposite", "risk"} {
		prefix := s.Generation + "/" + group + "/" + id + "/"
		intakes, e := paperRows[paperIntake](ctx, tx, "SELECT payload FROM paper_intakes WHERE id IN (?,?,?,?,?) ORDER BY at", prefix+"seen", prefix+"skipped", prefix+"associated", prefix+"opened", prefix+"closed")
		if e != nil {
			return nil, e
		}
		for _, v := range intakes {
			*events = append(*events, auditEvent{Kind: "intake", At: v.At, ParentID: id, Text: v.State + " · " + v.Reason, Group: group})
		}
	}
	// IDs contain generation/group/parent. Read only the two deterministic keys.
	trades, e := paperRows[paperTrade](ctx, tx, "SELECT payload FROM paper_trades WHERE id IN (?,?)", s.Generation+"/opposite/"+id, s.Generation+"/risk/"+id)
	if e != nil {
		return nil, e
	}
	for _, t := range trades {
		fills, e := paperRows[paperFill](ctx, tx, "SELECT payload FROM paper_fills WHERE trade_id=? ORDER BY at,id LIMIT 1000", t.ID)
		if e != nil {
			return nil, e
		}
		view := paperTradeView(t, s)
		delete(view, "trade")
		view["id"], view["group"], view["actionableAt"], view["enteredAt"], view["exitedAt"] = t.ID, t.Group, t.Seen, t.Entered, t.Exited
		view["entryPrice"], view["quantity"], view["fees"], view["exitReason"], view["quality"], view["fills"] = t.Entry, t.Quantity, t.Fees, t.ExitReason, t.Quality, fills
		out = append(out, view)
		for _, f := range fills {
			*events = append(*events, auditEvent{Kind: "fill", At: f.At, ParentID: id, Text: f.Kind + " · " + t.ExitReason, Price: flowPtr(f.Price.String()), Group: t.Group})
		}
	}
	return out, nil
}
func (h *Hub) auditRisk(ctx context.Context, q url.Values, from, to time.Time, r map[string]any) (any, error) {
	limit, offset := r["limit"].(int), r["offset"].(int)
	side := q.Get("direction")
	rows, e := paperRows[ShortEpisode](ctx, h.Store.shortDB(), `SELECT payload FROM sf_records WHERE kind='episode' AND at>=? AND at<? AND (?='' OR json_extract(payload,'$.direction')=?) ORDER BY at DESC,id DESC LIMIT ? OFFSET ?`, from.Unix(), to.Unix(), side, side, limit+1, offset)
	if e != nil {
		return nil, e
	}
	more := len(rows) > limit
	if more {
		rows = rows[:limit]
	}
	items := []any{}
	for _, v := range rows {
		trials := []any{}
		for _, key := range []string{"5", "10"} {
			t := v.Trials[key]
			if t == nil {
				continue
			}
			x := auditTrial(*t)
			x["minutes"] = key
			x["rule"] = t.Rule
			trials = append(trials, x)
		}
		items = append(items, map[string]any{"id": v.ID, "at": v.At, "direction": v.Direction, "trials": trials})
	}
	r["items"], r["more"] = items, more
	r["note"] = "5/10分钟风险观察；独立短周期研究，不发送邮件，不开仓。未登记窗口不补成有效覆盖。"
	return r, nil
}
