package datahub

import (
	"math"
	"time"
)

func hourlyATR(c map[int64]Candle, to time.Time) *float64 {
	// Fourteen completed hourly true ranges, requiring the prior close.
	to = to.Truncate(time.Hour)
	sum := 0.0
	prev, ok := c[to.Add(-14*time.Hour-5*time.Minute).Unix()]
	if !ok {
		return nil
	}
	last := prev.Close
	for i := 14; i > 0; i-- {
		end := to.Add(-time.Duration(i-1) * time.Hour)
		hi, lo, cl := 0.0, math.Inf(1), 0.0
		for j := 12; j > 0; j-- {
			v, ok := c[end.Add(-time.Duration(j)*5*time.Minute).Unix()]
			if !ok {
				return nil
			}
			hi = max(hi, v.High)
			lo = min(lo, v.Low)
			cl = v.Close
		}
		sum += max(hi-lo, max(math.Abs(hi-last), math.Abs(lo-last)))
		last = cl
	}
	v := sum / 14
	return &v
}

func completeCandles(c map[int64]Candle, from, to time.Time) bool {
	for t := from; t.Before(to); t = t.Add(5 * time.Minute) {
		if v, ok := c[t.Unix()]; !ok || v.Close <= 0 {
			return false
		}
	}
	return true
}
