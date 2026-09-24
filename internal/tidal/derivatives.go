package tidal

import (
	"context"
	"fmt"
	"strings"
	"time"
)

func (c *Collector) derivativePoll(ctx context.Context) {
	bybitHours := map[string]float64{}
	binanceHours := map[string]float64{}
	lastMeta := time.Time{}
	for ctx.Err() == nil {
		if time.Since(lastMeta) > 30*time.Minute {
			d, err := c.get(ctx, "https://api.bybit.com/v5/market/instruments-info?category=linear", nil)
			if err == nil {
				for _, x := range arr(obj(obj(d)["result"])["list"]) {
					m := obj(x)
					bybitHours[str(m["symbol"])] = number(m["fundingInterval"]) / 60
				}
			}
			d, err = c.get(ctx, "https://fapi.binance.com/fapi/v1/fundingInfo", nil)
			if err == nil {
				for _, x := range arr(d) {
					m := obj(x)
					binanceHours[str(m["symbol"])] = number(m["fundingIntervalHours"])
				}
			}
			lastMeta = time.Now()
		}
		for _, a := range []string{"BTC", "ETH"} {
			for _, v := range []string{"okx", "binance", "bybit"} {
				fx, valid := c.E.Rate("USDT")
				if !valid {
					continue
				}
				d := Derivative{Venue: v, Asset: a, Quote: "USDT", At: time.Now().UTC(), IntervalHours: 8, Source: "public REST"}
				var err error
				switch v {
				case "okx":
					symbol := a + "-USDT-SWAP"
					var raw any
					raw, err = c.get(ctx, "https://www.okx.com/api/v5/public/open-interest?instType=SWAP&instId="+symbol, nil)
					if err != nil {
						break
					}
					data := arr(obj(raw)["data"])
					if len(data) == 0 {
						err = fmt.Errorf("empty OKX OI")
						break
					}
					m := obj(data[0])
					d.OIBase = number(m["oiCcy"])
					d.OIUSDCents = cents(number(m["oiUsd"]))
					raw, err = c.get(ctx, "https://www.okx.com/api/v5/public/funding-rate?instId="+symbol, nil)
					if err != nil {
						break
					}
					data = arr(obj(raw)["data"])
					if len(data) == 0 {
						err = fmt.Errorf("empty OKX funding")
						break
					}
					m = obj(data[0])
					if str(m["fundingRate"]) == "" || !finite(number(m["fundingRate"])) {
						err = fmt.Errorf("funding rate unavailable")
						break
					}
					d.Funding = number(m["fundingRate"])
					d.NextFunding = num(m["fundingTime"])
					interval := num(m["nextFundingTime"]) - num(m["fundingTime"])
					if interval > 0 {
						d.IntervalHours = float64(interval) / 3600000
					}
					raw, err = c.get(ctx, "https://www.okx.com/api/v5/public/mark-price?instType=SWAP&instId="+symbol, nil)
					if err == nil {
						data = arr(obj(raw)["data"])
						if len(data) > 0 {
							d.Mark = number(obj(data[0])["markPx"]) * fx.Value
						}
					}
					raw, err = c.get(ctx, "https://www.okx.com/api/v5/market/index-tickers?instId="+a+"-USDT", nil)
					if err == nil {
						data = arr(obj(raw)["data"])
						if len(data) > 0 {
							d.Index = number(obj(data[0])["idxPx"]) * fx.Value
						}
					}
				case "binance":
					symbol := a + "USDT"
					var raw any
					raw, err = c.get(ctx, "https://fapi.binance.com/fapi/v1/openInterest?symbol="+symbol, nil)
					if err != nil {
						break
					}
					d.OIBase = number(obj(raw)["openInterest"])
					raw, err = c.get(ctx, "https://fapi.binance.com/fapi/v1/premiumIndex?symbol="+symbol, nil)
					if err != nil {
						break
					}
					m := obj(raw)
					d.Mark = number(m["markPrice"]) * fx.Value
					d.Index = number(m["indexPrice"]) * fx.Value
					if str(m["lastFundingRate"]) == "" || !finite(number(m["lastFundingRate"])) {
						err = fmt.Errorf("funding rate unavailable")
						break
					}
					d.Funding = number(m["lastFundingRate"])
					d.NextFunding = num(m["nextFundingTime"])
					if h := binanceHours[symbol]; h > 0 {
						d.IntervalHours = h
					}
					d.OIUSDCents = cents(d.Mark * d.OIBase)
				case "bybit":
					symbol := a + "USDT"
					var raw any
					raw, err = c.get(ctx, "https://api.bybit.com/v5/market/tickers?category=linear&symbol="+symbol, nil)
					if err != nil {
						break
					}
					data := arr(obj(obj(raw)["result"])["list"])
					if len(data) == 0 {
						err = fmt.Errorf("empty Bybit ticker")
						break
					}
					m := obj(data[0])
					if m["singleOpenInterest"] == nil {
						err = fmt.Errorf("single-sided OI missing; refusing double count")
						break
					}
					d.OIBase = number(m["singleOpenInterest"])
					d.Mark = number(m["markPrice"]) * fx.Value
					d.Index = number(m["indexPrice"]) * fx.Value
					d.OIUSDCents = cents(d.Mark * d.OIBase)
					if str(m["fundingRate"]) == "" || !finite(number(m["fundingRate"])) {
						err = fmt.Errorf("funding rate unavailable")
						break
					}
					d.Funding = number(m["fundingRate"])
					d.NextFunding = num(m["nextFundingTime"])
					if h := bybitHours[symbol]; h > 0 {
						d.IntervalHours = h
					}
					if h := number(m["fundingIntervalHour"]); h > 0 {
						d.IntervalHours = h
					}
				}
				if err != nil {
					c.E.SetHealth("derivatives:"+v+":"+a, false, err.Error())
					continue
				}
				d.Valid = d.Mark > 0 && d.Index > 0 && d.OIBase > 0
				d.At = time.Now().UTC()
				c.E.SetDerivative(d)
				c.E.SetHealth("derivatives:"+v+":"+a, d.Valid, "单边 OI；资金费率按实际结算间隔展示")
			}
		}
		if !wait(ctx, 30*time.Second) {
			return
		}
	}
}
func (c *Collector) perp(ctx context.Context, v string) error {
	wsURL := ""
	subs := []any{}
	var ping any
	mult := map[string]float64{"BTC": 1, "ETH": 1}
	switch v {
	case "okx":
		raw, err := c.get(ctx, "https://www.okx.com/api/v5/public/instruments?instType=SWAP", nil)
		if err != nil {
			return err
		}
		mult = map[string]float64{}
		for _, x := range arr(obj(raw)["data"]) {
			m := obj(x)
			for _, a := range []string{"BTC", "ETH"} {
				if str(m["instId"]) == a+"-USDT-SWAP" && str(m["ctValCcy"]) == a {
					mult[a] = number(m["ctVal"])
				}
			}
		}
		if mult["BTC"] == 0 || mult["ETH"] == 0 {
			return fmt.Errorf("OKX contract units unavailable")
		}
		wsURL = "wss://ws.okx.com:8443/ws/v5/public"
		args := []any{}
		for _, a := range []string{"BTC", "ETH"} {
			args = append(args, map[string]any{"channel": "trades", "instId": a + "-USDT-SWAP"})
		}
		args = append(args, map[string]any{"channel": "liquidation-orders", "instType": "SWAP"})
		subs = append(subs, map[string]any{"op": "subscribe", "args": args})
		ping = "ping"
	case "binance":
		wsURL = "wss://fstream.binance.com/market/stream?streams=btcusdt@aggTrade/ethusdt@aggTrade/btcusdt@forceOrder/ethusdt@forceOrder"
	case "bybit":
		wsURL = "wss://stream.bybit.com/v5/public/linear"
		subs = append(subs, map[string]any{"op": "subscribe", "args": []string{"publicTrade.BTCUSDT", "publicTrade.ETHUSDT", "allLiquidation.BTCUSDT", "allLiquidation.ETHUSDT"}})
		ping = map[string]any{"op": "ping"}
	}
	conn, child, cancel, err := c.connect(ctx, wsURL, subs, ping)
	if err != nil {
		return err
	}
	defer cancel()
	defer conn.CloseNow()
	c.E.SetHealth("perp:"+v, true, "成交与已发生清算订阅已连接；无消息不等于无清算")
	for child.Err() == nil {
		m, err := read(child, conn)
		if err != nil {
			return err
		}
		if m["event"] == "error" || m["success"] == false {
			return fmt.Errorf("perp subscription error: %v", m)
		}
		switch v {
		case "okx":
			arg := obj(m["arg"])
			if arg["channel"] == "trades" {
				a := assetOf(str(arg["instId"]))
				if a == "" {
					continue
				}
				i := Instrument{v, a, "USDT", str(arg["instId"]), "perp"}
				for _, r := range arr(m["data"]) {
					d := obj(r)
					c.E.Trade(i, str(d["tradeId"]), str(d["side"]), str(d["px"]), str(d["sz"]), time.UnixMilli(num(d["ts"])), mult[a])
				}
			} else if arg["channel"] == "liquidation-orders" {
				for _, r := range arr(m["data"]) {
					d := obj(r)
					symbol := str(d["instId"])
					a := assetOf(symbol)
					if a == "" || symbol != a+"-USDT-SWAP" {
						continue
					}
					for _, raw := range arr(d["details"]) {
						x := obj(raw)
						side := str(x["posSide"])
						if side != "long" && side != "short" {
							side = "long"
							if x["side"] == "buy" {
								side = "short"
							}
						}
						c.recordLiquidation(v, a, side, number(x["bkPx"]), number(x["sz"])*mult[a], time.UnixMilli(num(x["ts"])), "bankruptcy", "sampled")
					}
				}
			}
		case "binance":
			d := obj(m["data"])
			if d["e"] == "aggTrade" {
				a := assetOf(str(d["s"]))
				side := "buy"
				if d["m"] == true {
					side = "sell"
				}
				c.E.Trade(Instrument{v, a, "USDT", str(d["s"]), "perp"}, str(d["a"]), side, str(d["p"]), str(d["q"]), time.UnixMilli(num(d["T"])), 1)
			} else if d["e"] == "forceOrder" {
				o := obj(d["o"])
				side := "long"
				if o["S"] == "BUY" {
					side = "short"
				}
				q := number(o["z"])
				p := number(o["ap"])
				if q > 0 && p > 0 {
					c.recordLiquidation(v, assetOf(str(o["s"])), side, p, q, time.UnixMilli(num(o["T"])), "average_execution", "latest_per_1000ms")
				}
			}
		case "bybit":
			topic := str(m["topic"])
			for _, raw := range arr(m["data"]) {
				d := obj(raw)
				a := assetOf(str(d["s"]))
				if strings.HasPrefix(topic, "publicTrade.") {
					c.E.Trade(Instrument{v, a, "USDT", str(d["s"]), "perp"}, str(d["i"]), strings.ToLower(str(d["S"])), str(d["p"]), str(d["v"]), time.UnixMilli(num(d["T"])), 1)
				} else if strings.HasPrefix(topic, "allLiquidation.") {
					side := "short"
					if d["S"] == "Buy" {
						side = "long"
					}
					c.recordLiquidation(v, a, side, number(d["p"]), number(d["v"]), time.UnixMilli(num(d["T"])), "bankruptcy", "all_published")
				}
			}
		}
	}
	return child.Err()
}
func (c *Collector) recordLiquidation(v, a, side string, p, q float64, t time.Time, kind, coverage string) {
	c.E.mu.RLock()
	fx, ok := c.E.historicalRateLocked("USDT", t)
	c.E.mu.RUnlock()
	if !ok || a == "" || !finite(p*q) || p <= 0 || q <= 0 {
		return
	}
	c.E.Liquidation(Liquidation{v, a, side, p * fx, cents(p * q * fx), t, kind, coverage})
}
