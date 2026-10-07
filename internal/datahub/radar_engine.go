package datahub

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/shopspring/decimal"
	"sort"
	"strings"
	"time"
)

func radarAge(w *RadarWallet, now time.Time) {
	w.Age = "unknown"
	if !w.FirstSeen.IsZero() && w.FirstSeen.Before(now.Add(-7*24*time.Hour)) {
		w.Age = "established"
		return
	}
	if w.Earliest != nil && w.Earliest.Before(now.Add(-7*24*time.Hour)) {
		w.Age = "established"
		return
	}
	if w.HistoryComplete && w.Earliest != nil {
		w.Age = "recent"
	}
}
func radarFillID(f radarFill) string { return fmt.Sprintf("%s/%d/%d", f.Coin, f.Time, f.TID) }
func radarEventID(address string, f radarFill) string {
	s := sha256.Sum256([]byte(address + "/" + radarFillID(f)))
	return fmt.Sprintf("hl-%x", s[:16])
}

// All quantity arithmetic uses the execution's signed before/after position.
// Selling an existing long cannot create a short unless it actually crosses zero.
func radarTransition(f radarFill) (before, after, opened decimal.Decimal, err error) {
	var ok bool
	before, ok = radarNumber(f.Start)
	if !ok {
		return before, after, opened, errors.New("无效成交前仓位")
	}
	size, ok := radarNumber(f.Sz)
	if !ok || !size.IsPositive() {
		return before, after, opened, errors.New("无效成交数量")
	}
	p, ok := radarNumber(f.Px)
	if !ok || !p.IsPositive() || !ValidAsset(f.Coin) || f.Time <= 0 || f.TID < 0 {
		return before, after, opened, errors.New("无效成交事实")
	}
	if size.Mul(p).GreaterThan(decimal.NewFromInt(1000000000000)) {
		return before, after, opened, errors.New("成交名义金额超过校验上限")
	}
	if f.Side == "A" {
		size = size.Neg()
	} else if f.Side != "B" {
		return before, after, opened, errors.New("未知成交方向")
	}
	after = before.Add(size)
	if before.IsZero() || (!after.IsZero() && before.Sign() != after.Sign()) {
		opened = after.Abs()
	} else if after.Abs().GreaterThan(before.Abs()) {
		opened = after.Abs().Sub(before.Abs())
	}
	return
}

// snapshot and wallet evidence are frozen with the event; later facts do not
// rewrite already queued mail payloads. The outbox is committed with the event.
func (h *Hub) radarApply(ctx context.Context, w RadarWallet, fills []radarFill, a radarAccount, now time.Time) error {
	r := h.Store.radar
	if r == nil {
		return errors.New("雷达存储不可用")
	}
	if r.size() >= radarBudget || h.Store.Status().Paused {
		return errors.New("雷达容量保护")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var prior RadarWallet
	if e := radarLoad(ctx, tx, "wallet", w.Address, &prior); e == nil {
		w.FirstSeen = prior.FirstSeen
		if prior.Earliest != nil && (w.Earliest == nil || prior.Earliest.Before(*w.Earliest)) {
			w.Earliest = prior.Earliest
		}
	} else if e != sql.ErrNoRows {
		return e
	}
	w.Updated = now
	radarAge(&w, now)
	if err = radarPut(ctx, tx, "wallet", w.Address, now, w, false); err != nil {
		return err
	}
	rate, fxAt, fxOK := h.Rate("USDC", now)
	fx, validFX := radarNumber(rate)
	fxOK = fxOK && validFX && fx.IsPositive()
	// Trade IDs are identities, not sequence numbers. Preserve source order within
	// a millisecond; any position discontinuity below suppresses the episode.
	sort.SliceStable(fills, func(i, j int) bool { return fills[i].Time < fills[j].Time })
	positions := map[string]radarPosition{}
	events := map[string]RadarEvent{}
	chainOK := map[string]bool{"BTC": true, "ETH": true}
	for _, asset := range Assets() {
		var p radarPosition
		err = radarLoad(ctx, tx, "position", w.Address+"/"+asset, &p)
		if err != nil && err != sql.ErrNoRows {
			return err
		}
		positions[asset] = p
		if p.Event != "" {
			v, e := radarReadEvent(ctx, tx, p.Event)
			if e == nil {
				events[v.ID] = v
			} else if e != sql.ErrNoRows {
				return e
			}
		}
	}
	for _, f := range fills {
		if !ValidAsset(f.Coin) || radarTime(f.Time).Before(now.Add(-30*time.Minute)) {
			continue
		}
		before, after, opened, e := radarTransition(f)
		if e != nil {
			return e
		}
		if radarTime(f.Time).After(now.Add(5 * time.Second)) {
			return errors.New("成交时间位于未来")
		}
		raw, _ := json.Marshal(f)
		var old []byte
		e = tx.QueryRowContext(ctx, "SELECT payload FROM fills WHERE address=? AND coin=? AND ts=? AND tid=?", w.Address, f.Coin, f.Time, f.TID).Scan(&old)
		if e == nil {
			if string(old) != string(raw) {
				return errors.New("重复成交发生冲突修订")
			}
			continue
		}
		if e != sql.ErrNoRows {
			return e
		}
		if _, e = tx.ExecContext(ctx, "INSERT INTO fills VALUES(?,?,?,?,?,?)", w.Address, f.Coin, f.Time, f.TID, f.Hash, raw); e != nil {
			return e
		}
		p := positions[f.Coin]
		if p.Last > f.Time {
			chainOK[f.Coin] = false
			continue
		}
		if p.Last > 0 && !dec(p.Size).Equal(before) {
			chainOK[f.Coin] = false
		}
		// Closing or flipping ends the prior episode. A list disappearance never does.
		if p.Event != "" && (after.IsZero() || (!before.IsZero() && before.Sign() != after.Sign())) {
			v := events[p.Event]
			t := radarTime(f.Time)
			v.Closed = &t
			v.Size = "0"
			v.Updated = now
			events[v.ID] = v
			p.Event = ""
		}
		newOpening := before.IsZero() || (!after.IsZero() && before.Sign() != after.Sign())
		if newOpening && !after.IsZero() {
			t := radarTime(f.Time)
			v := RadarEvent{ID: radarEventID(w.Address, f), Rules: RadarRules, Address: w.Address, Asset: f.Coin, Side: radarSide(after), Opened: t, Detected: now, Updated: now, Size: after.String(), OpenedQuantity: "0", OpenedNative: "0", Age: w.Age, Level: "candidate", Members: []string{}, Reasons: []string{}, Context: []string{}, LiveOpening: !t.Before(h.boot)}
			if existing, e := radarReadEvent(ctx, tx, v.ID); e == nil {
				v = existing
			} else if e != sql.ErrNoRows {
				return e
			}
			events[v.ID] = v
			p.Event = v.ID
		}
		if p.Event != "" {
			v := events[p.Event]
			v.Size = after.String()
			v.Through = radarTime(f.Time)
			v.Updated = now
			v.OpenedQuantity = dec(v.OpenedQuantity).Add(opened).String()
			v.OpenedNative = dec(v.OpenedNative).Add(opened.Mul(dec(f.Px))).String()
			if opened.IsPositive() && fxOK && v.Threshold == nil && after.Abs().Mul(dec(f.Px)).Mul(fx).GreaterThanOrEqual(decimal.NewFromInt(1000000)) {
				t := radarTime(f.Time)
				v.Threshold = &t
			}
			if !chainOK[f.Coin] {
				v.LiveOpening = false
				v.Reasons = append(v.Reasons, "仓位连续性缺口，不追认建仓")
			}
			events[v.ID] = v
		}
		p.Size = after.String()
		p.Last = f.Time
		positions[f.Coin] = p
	}
	accountFresh := a.Time > 0 && now.Sub(radarTime(a.Time)) <= 90*time.Second && !radarTime(a.Time).After(now.Add(5*time.Second))
	for id, v := range events {
		p := positions[v.Asset]
		v.Updated = now
		v.Age = w.Age
		v.HistoryComplete = w.HistoryComplete
		v.Verified = false
		if v.Closed == nil && p.Event == id && chainOK[v.Asset] && accountFresh {
			for _, row := range a.Positions {
				pos := row.Position
				if pos.Coin != v.Asset {
					continue
				}
				sz, ok := radarNumber(pos.Size)
				value, vok := radarNumber(pos.Value)
				entry, eok := radarNumber(pos.Entry)
				if !ok || !vok || !eok || !value.IsPositive() || !entry.IsPositive() || !sz.Equal(dec(p.Size)) {
					continue
				}
				v.Verified = true
				v.Through = radarTime(a.Time)
				v.Native = value.String()
				v.Entry = entry.String()
				v.Liquidation = pos.Liquidation
				if pos.Liquidation != nil {
					liquidation, valid := radarNumber(*pos.Liquidation)
					if !valid || !liquidation.IsPositive() {
						return errors.New("无效清算参考价")
					}
				}
				v.ConfiguredLeverage = pos.Leverage.Value
				if fxOK {
					c, valid := radarMoney(value.Mul(fx))
					if !valid {
						return errors.New("仓位美元金额超过校验上限")
					}
					v.USDCents = &c
					v.FX = rate
					v.FXAt = fxAt
					if v.ReferenceUSD == "" {
						v.ReferenceUSD = value.Div(sz.Abs()).Mul(fx).String()
					}
				} else {
					v.USDCents = nil
					v.FX = ""
					v.FXAt = nil
				}
				eq, eqOK := radarNumber(a.Margin.Equity)
				total, tOK := radarNumber(a.Margin.Total)
				v.EffectiveLeverage = nil
				v.Concentration = nil
				v.Concentrated = false
				if eqOK && tOK && eq.IsPositive() && total.IsPositive() && total.GreaterThanOrEqual(value) {
					lev := total.Div(eq).String()
					share := value.Div(total).String()
					v.EffectiveLeverage = &lev
					v.Concentration = &share
					v.Concentrated = dec(lev).GreaterThanOrEqual(decimal.NewFromInt(5)) && dec(share).GreaterThanOrEqual(decimal.RequireFromString("0.8"))
				}
			}
		}
		v.Rapid = false
		v.DepositCents = nil
		if w.LedgerComplete && fxOK {
			sum := decimal.Zero
			for _, l := range w.Ledger {
				t := radarTime(l.Time)
				n, ok := radarNumber(l.Delta.USDC)
				if l.Delta.Type == "deposit" && ok && n.IsPositive() && !t.Before(v.Opened.Add(-30*time.Minute)) && !t.After(v.Opened) {
					sum = sum.Add(n)
				}
			}
			c, valid := radarMoney(sum.Mul(fx))
			if !valid {
				return errors.New("入金美元金额超过校验上限")
			}
			v.DepositCents = &c
			v.Rapid = c >= 10000000
		}
		v.Context = h.radarContext(v.Asset, v.Side, now)
		eligible := v.LiveOpening && v.Verified && v.Closed == nil && v.Age != "established" && v.Threshold != nil && v.USDCents != nil && *v.USDCents >= 100000000
		if eligible && !v.InitialRecorded {
			v.InitialRecorded = true
			v.Level = "opening"
			v.Reasons = append(v.Reasons, "核验新建仓达到100万美元")
			if err = radarQueueNotice(ctx, tx, v, "opening", now); err != nil {
				return err
			}
		}
		if eligible && now.Sub(*v.Threshold) <= radarNoticeTTL && v.Rapid && v.Concentrated && !v.UpgradeRecorded {
			v.UpgradeRecorded = true
			v.Level = "priority"
			v.Reasons = append(v.Reasons, "快速外部入金＋该永续账户敞口集中")
			if err = radarQueueNotice(ctx, tx, v, "priority", now); err != nil {
				return err
			}
		}
		if err = radarSaveEvent(ctx, tx, v); err != nil {
			return err
		}
		if err = h.radarFreezeTrial(ctx, tx, v, now); err != nil {
			return err
		}
	}
	for asset, p := range positions {
		if p.Last > 0 {
			if err = radarPut(ctx, tx, "position", w.Address+"/"+asset, now, p, false); err != nil {
				return err
			}
		}
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return nil
}
func (h *Hub) radarContext(asset, side string, now time.Time) []string {
	out := []string{"其他交易所对冲敞口未知；同步操作不证明同一控制人"}
	for _, kind := range []string{"flow", "oi", "funding"} {
		found := false
		for _, d := range Registry() {
			if d.Kind != kind || (d.Asset != asset && d.Asset != "ALL") {
				continue
			}
			o, ok := h.Store.Latest(d.ID)
			if !ok || !o.Fresh(d, now) {
				continue
			}
			found = true
			if kind == "flow" && d.Market == "spot" && o.Payload.Flow != nil {
				net := dec(o.Payload.Flow.Buy).Sub(dec(o.Payload.Flow.Sell))
				label := "冲突"
				if net.IsZero() {
					label = "中性"
				} else if net.IsPositive() == (side == "long") {
					label = "同向"
				}
				out = append(out, "现货成交快照："+label+"（不是充值提现）")
				break
			}
		}
		if !found {
			out = append(out, kind+"：缺失或过期")
		} else if kind != "flow" {
			out = append(out, kind+"：可读取当前背景，未作方向推断")
		}
	}
	return out
}

// A synchronized group is behavioural evidence, never an ownership assertion.
func (h *Hub) radarGroups(ctx context.Context, now time.Time) error {
	r := h.Store.radar
	r.mu.Lock()
	defer r.mu.Unlock()
	rows, err := r.db.QueryContext(ctx, "SELECT payload FROM events WHERE opened>=? ORDER BY opened,id LIMIT 501", now.Add(-15*time.Minute).UnixMilli())
	if err != nil {
		return err
	}
	groups := map[string][]RadarEvent{}
	count := 0
	for rows.Next() {
		count++
		var b []byte
		if err = rows.Scan(&b); err != nil {
			break
		}
		var v RadarEvent
		if err = json.Unmarshal(b, &v); err != nil {
			break
		}
		if v.LiveOpening && v.Verified && v.Closed == nil && v.Age != "established" && now.Sub(v.Through) <= 90*time.Second && v.USDCents != nil && *v.USDCents >= 25000000 {
			groups[v.Asset+"/"+v.Side] = append(groups[v.Asset+"/"+v.Side], v)
		}
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return err
	}
	if count > 500 {
		return errors.New("同步建仓窗口超过处理上限")
	}
	for _, list := range groups {
		seen := map[string]bool{}
		sum := int64(0)
		members := []string{}
		unique := []RadarEvent{}
		for _, v := range list {
			if !seen[v.Address] {
				seen[v.Address] = true
				sum += *v.USDCents
				members = append(members, v.Address)
				unique = append(unique, v)
			}
		}
		if len(unique) < 3 || sum < 300000000 {
			continue
		}
		sort.Strings(members)
		gid := "group/" + unique[0].ID
		for _, v := range unique {
			if v.Group != "" {
				gid = v.Group
				break
			}
		}
		tx, e := r.db.BeginTx(ctx, nil)
		if e != nil {
			return e
		}
		var group map[string]any
		loadErr := radarLoad(ctx, tx, "group", gid, &group)
		if loadErr != nil && loadErr != sql.ErrNoRows {
			tx.Rollback()
			return loadErr
		}
		already := loadErr == nil
		var notice *RadarEvent
		for _, v := range unique {
			v.Group = gid
			v.Members = members
			if v.InitialRecorded && !v.UpgradeRecorded {
				v.UpgradeRecorded = true
				v.Level = "priority"
				v.Reasons = append(v.Reasons, "15分钟内多地址同币同向建仓；控制关系未知")
				if notice == nil {
					x := v
					notice = &x
				}
			}
			if e = radarSaveEvent(ctx, tx, v); e != nil {
				break
			}
			// Only the analytical grouping may evolve; discovery evidence stays frozen.
			var trial radarTrial
			if te := radarLoad(ctx, tx, "study", v.ID, &trial); te == nil {
				trial.Group = gid
				e = radarPut(ctx, tx, "study", v.ID, trial.At, trial, false)
			} else if te != sql.ErrNoRows {
				e = te
			}
			if e != nil {
				break
			}
		}
		if e == nil {
			e = radarPut(ctx, tx, "group", gid, now, map[string]any{"id": gid, "members": members, "usdCents": sum, "at": now, "ownership": "unknown"}, false)
		}
		if e == nil && !already && notice != nil {
			e = radarQueueNotice(ctx, tx, *notice, "priority", now)
		}
		if e == nil {
			e = tx.Commit()
		} else {
			tx.Rollback()
		}
		if e != nil {
			return e
		}
	}
	return nil
}
func radarAddress(s string) string { return strings.ToLower(s) }
