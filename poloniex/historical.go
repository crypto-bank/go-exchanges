package poloniex

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gogo/protobuf/types"
	"github.com/golang/glog"
	"golang.org/x/net/context/ctxhttp"

	"github.com/crypto-bank/go-exchanges/common"
	"github.com/crypto-bank/proto/currency"
	"github.com/crypto-bank/proto/exchange"
	"github.com/crypto-bank/proto/order"
)

// HistoryRequest - Trades history request.
type HistoryRequest struct {
	Pair  *currency.Pair
	Start time.Time
	End   time.Time
}

// History - Reads trades history from start to end.
// Results channel will be closed by this function.
func History(ctx context.Context, req HistoryRequest, results chan<- []*order.Trade) (err error) {
	// Begin a year ago, not before
	if lastYear := time.Now().Add(-time.Hour * 24 * 365); req.Start.Before(lastYear) {
		req.Start = lastYear
	}

	// Set `End` to now if it is zero
	if req.End.Unix() == 0 {
		req.End = time.Now()
	}

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
		if len(trades) >= 1 {
			// Get last trade
			last := trades[len(trades)-1]
			// Set last trade time as request end
			req.End, err = types.TimestampFromProto(last.Time)
			if err != nil {
				return err
			}
		}

		// Break if less than 50k results were returned
		if len(trades) < 50000 {
			return nil
		}
	}
}

func getTrades(ctx context.Context, req HistoryRequest) (trades []*order.Trade, err error) {
	// Construct historical data URL
	dataURL := fmt.Sprintf(
		"https://poloniex.com/public?command=returnTradeHistory&currencyPair=%s&start=%d&end=%d",
		req.Pair.Concat("_"),
		req.Start.Unix(),
		req.End.Unix(),
	)

	glog.V(2).Infof("Requesting poloniex history %s to %s", req.Start, req.End)

	// Request HTTP URL to get data
	resp, err := ctxhttp.Get(ctx, nil, dataURL)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	// Unmarshal response
	var data []*historicalTrade
	err = json.NewDecoder(resp.Body).Decode(&data)
	if err != nil {
		return
	}

	return tradesFromHistory(req.Pair, data)
}

func tradesFromHistory(pair *currency.Pair, history []*historicalTrade) (res []*order.Trade, err error) {
	res = make([]*order.Trade, len(history))
	for i, item := range history {
		res[i], err = tradeFromHistory(pair, item)
		if err != nil {
			return
		}
	}
	return
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

func tradeFromHistory(pair *currency.Pair, trade *historicalTrade) (res *order.Trade, err error) {
	res = &order.Trade{
		ID: int64(trade.TradeID),
		Order: &order.Order{
			Exchange: exchange.Poloniex,
		},
	}
	res.Order.Type, err = order.TypeFromString(trade.Type)
	if err != nil {
		return
	}
	res.Order.Rate, err = common.ParseIVolume(pair.First, trade.Rate)
	if err != nil {
		return
	}
	res.Order.Volume, err = common.ParseIVolume(pair.Second, trade.Amount)
	if err != nil {
		return
	}
	res.Time, err = common.ParseTime("2006-01-02 15:04:05", trade.Date)
	if err != nil {
		return
	}
	return
}
