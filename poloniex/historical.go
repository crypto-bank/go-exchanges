package poloniex

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"time"

	"github.com/golang/glog"
	"github.com/k0kubun/pp"
	"golang.org/x/net/context/ctxhttp"

	"github.com/crypto-bank/proto/currency"
)

func main() {
	flag.CommandLine.Parse([]string{"-logtostderr"})

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*15)
	defer cancel()

	req := &tradesRequest{
		Pair:  currency.NewPair("BTC", "XRP"),
		Start: time.Now().Add(-time.Hour * 24),
		End:   time.Now(),
	}

	// Create response channel
	results := make(chan []*historicalTrade, 1)

	// Get all trades in goroutine
	go func() {
		if err := getAllTrades(ctx, req, results); err != nil {
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

type historicalTrade struct {
	GlobalTradeID int    `json:"globalTradeID"`
	TradeID       int    `json:"tradeID"`
	Date          string `json:"date"`
	Type          string `json:"type"`
	Rate          string `json:"rate"`
	Amount        string `json:"amount"`
	Total         string `json:"total"`
}

type tradesRequest struct {
	Pair  *currency.Pair
	Start time.Time
	End   time.Time
}

func getAllTrades(ctx context.Context, req *tradesRequest, results chan<- []*historicalTrade) (err error) {
	for {
		// Pass in case context is done
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Get trades
		trades, err := getTrades(ctx, req)
		if err != nil {
			return err
		}

		// Stop the looop if no trades were found
		if len(trades) == 0 {
			return nil
		}

		// Send trades to results channel
		results <- trades

		// If reached max amount in one request
		// We will start from last trade in list
		if len(trades) >= 50000 {
			// Get last trade
			last := trades[len(trades)-1]
			// Parse last trade time
			// and set start to last trade time
			start, err := time.Parse("2006-01-02 15:04:05", last.Date)
			if err != nil {
				return err
			}
			// Set next request
			req = &tradesRequest{
				Pair:  req.Pair,
				Start: start,
				End:   req.End,
			}
		}
	}
}

func getTrades(ctx context.Context, req *tradesRequest) (trades []*historicalTrade, err error) {
	// Construct historical data URL
	dataURL := fmt.Sprintf(
		"https://poloniex.com/public?command=returnTradeHistory&currencyPair=%s&start=%d&end=%d",
		req.Pair.Concat("_"),
		req.Start.Unix(),
		req.End.Unix(),
	)

	// Request HTTP URL to get data
	resp, err := ctxhttp.Get(ctx, nil, dataURL)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	// Unmarshal response
	err = json.NewDecoder(resp.Body).Decode(&trades)
	return
}
