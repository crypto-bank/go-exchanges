package poloniex

import (
	"context"
	"time"

	"github.com/golang/glog"
	"github.com/k0kubun/pp"

	"github.com/crypto-bank/proto/currency"
	"github.com/crypto-bank/proto/order"
)

func ExampleHistory() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()

	req := HistoryRequest{
		Pair:  currency.NewPair("BTC", "XRP"),
		Start: time.Now().Add(-time.Hour * 24),
		End:   time.Now(),
	}

	// Create results channel
	results := make(chan []*order.Trade, 10)

	// Get all trades in goroutine
	go func() {
		if err := History(ctx, req, results); err != nil {
			glog.Fatal(err)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			if err := ctx.Err(); err != nil {
				glog.Fatal(err)
			}
			break
		case trades := <-results:
			pp.Println(trades[:10])
			return
		}
	}
}
