package tidal

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/linlea666/tidal-crypto-intelligence/internal/datahub"
	"golang.org/x/crypto/bcrypt"
)

// Opt-in, localhost-only acceptance fixture. No generated observations can enter
// production: this server exists only in the test binary and uses a temp store.
func TestOrderZoneBrowserServer(t *testing.T) {
	if os.Getenv("TIDAL_ORDER_ZONE_BROWSER") != "1" {
		t.Skip("manual offline browser acceptance")
	}
	root := t.TempDir()
	h, e := datahub.Open(datahub.Config{Root: filepath.Join(root, "v2"), Offline: true})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Store.Close()
	st, e := OpenStore(root)
	if e != nil {
		t.Fatal(e)
	}
	defer st.Close()
	hash, _ := bcrypt.GenerateFromPassword([]byte("offline-zone-browser-only"), bcrypt.MinCost)
	web, e := filepath.Abs("../../web/dist/client")
	if e != nil {
		t.Fatal(e)
	}
	srv := NewServer(NewEngine(), st, nil, string(hash), web, "offline-zone-acceptance", false)
	srv.Hub = h
	venues := []string{"Binance", "OKX", "Coinbase", "Kraken", "Bitfinex"}
	ingest := func(d datahub.Dataset, o datahub.Observation) {
		if _, err := h.Store.Ingest(d, o); err != nil {
			t.Fatal(err)
		}
	}
	seed := func(at time.Time, history bool) {
		fx, _ := h.Dataset("fx.usd.kraken")
		ingest(fx, datahub.Observation{Dataset: fx.ID, ObservedAt: &at, FetchedAt: at, Quality: "valid", Resolution: 60, Payload: datahub.Payload{Rates: []datahub.Rate{{Quote: "USDT", USD: "1.0002"}}}})
		for _, a := range []string{"BTC", "ETH"} {
			p, step := 84700., 250.
			if a == "ETH" {
				p, step = 3350, 10
			}
			pd, _ := h.Dataset(datahub.ID("price", a, "Binance", "spot"))
			ingest(pd, datahub.Observation{Dataset: pd.ID, ObservedAt: &at, FetchedAt: at, Quality: "valid", Payload: datahub.Payload{Price: &datahub.Price{Value: fmt.Sprint(p), Quote: "USDT"}}})
			for v, venue := range venues {
				ld, _ := h.Dataset(datahub.ID("large", a, venue, "spot"))
				orders := []datahub.LargeOrder{}
				bids, asks := []datahub.Level{}, []datahub.Level{}
				for i := 1; i <= 10; i++ {
					buy, sell := p-float64(i)*step, p+float64(i)*step
					buy = float64(int(buy/step)) * step
					sell = float64(int(sell/step)) * step
					quantity := fmt.Sprint(8 + (i%4)*7 + v*3)
					started := at.Add(-time.Duration(i) * 24 * time.Hour)
					initial := quantity
					executed := "2.5"
					for _, side := range []string{"bid", "ask"} {
						price := buy
						if side == "ask" {
							price = sell
						}
						orders = append(orders, datahub.LargeOrder{ID: fmt.Sprintf("qa-%s-%d", side, i), Side: side, Price: fmt.Sprint(price), Quantity: quantity, RawState: 1, Start: &started, InitialQuantity: &initial, ExecutedQuantity: &executed})
					}
					bids = append(bids, datahub.Level{Price: fmt.Sprint(buy), Quantity: quantity})
					asks = append(asks, datahub.Level{Price: fmt.Sprint(sell), Quantity: quantity})
				}
				ingest(ld, datahub.Observation{Dataset: ld.ID, FetchedAt: at, Quality: "valid", Payload: datahub.Payload{Large: orders}})
				bd, _ := h.Dataset(datahub.ID("book", a, venue, "spot"))
				ingest(bd, datahub.Observation{Dataset: bd.ID, ObservedAt: &at, FetchedAt: at, Resolution: 300, Quality: "valid", Payload: datahub.Payload{Book: &datahub.Book{Low: p - 11*step, High: p + 11*step, Bids: bids, Asks: asks}}})
			}
			if history {
				cd, _ := h.Dataset(datahub.ID("candles", a, "Binance", "spot"))
				ingest(cd, datahub.Observation{Dataset: cd.ID, ObservedAt: &at, FetchedAt: at, Resolution: 300, Quality: "valid", Payload: datahub.Payload{Candle: &datahub.Candle{Open: p - 2*step, High: p + step, Low: p - 3*step, Close: p}}})
				for _, v := range []string{"Binance", "OKX", "Bybit"} {
					fd, _ := h.Dataset(datahub.ID("footprint", a, v, "spot"))
					ingest(fd, datahub.Observation{Dataset: fd.ID, ObservedAt: &at, FetchedAt: at, Resolution: 300, Quality: "valid", Payload: datahub.Payload{Foot: []datahub.Foot{{Low: fmt.Sprint(p - 4*step), High: fmt.Sprint(p - 3*step), BuyBase: "3.125", SellBase: "4.25", BuyQuote: fmt.Sprint(3.125 * (p - 4*step)), SellQuote: fmt.Sprint(4.25 * (p - 4*step))}, {Low: fmt.Sprint(p + 4*step), High: fmt.Sprint(p + 5*step), BuyBase: "5", SellBase: "2", BuyQuote: fmt.Sprint(5 * (p + 4*step)), SellQuote: fmt.Sprint(2 * (p + 4*step))}}}})
				}
			}
		}
	}
	now := time.Now().UTC().Truncate(5 * time.Minute)
	for i := 60; i >= 1; i-- {
		if i == 7 || i == 8 || i == 9 {
			continue
		}
		seed(now.Add(-time.Duration(i)*5*time.Minute), true)
	}
	seed(time.Now().UTC(), false)
	server := &http.Server{Addr: "127.0.0.1:18084", Handler: srv.Handler()}
	go server.ListenAndServe()
	defer server.Close()
	t.Log("Offline acceptance server http://127.0.0.1:18084")
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	until := time.NewTimer(45 * time.Minute)
	defer until.Stop()
	for {
		select {
		case at := <-tick.C:
			seed(at.UTC(), false)
		case <-until.C:
			return
		}
	}
}
