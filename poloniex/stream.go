package poloniex

import (
	"fmt"
	"time"

	"github.com/beatgammit/turnpike"
	"github.com/gogo/protobuf/types"
	"github.com/golang/glog"

	"github.com/crypto-bank/go-exchanges/common"
	"github.com/crypto-bank/proto/currency"
	"github.com/crypto-bank/proto/exchange"
	"github.com/crypto-bank/proto/order"
	"github.com/crypto-bank/proto/orderbook"
)

// Stream - Poloniex stream.
type Stream struct {
	client *turnpike.Client
	events chan *orderbook.Event
	errors chan error
}

const (
	// WebsocketAddress - Poloniex Websocket address.
	WebsocketAddress = "wss://api.poloniex.com"

	// WebsocketRealm - Poloniex Websocket realm name.
	WebsocketRealm = "realm1"
)

// NewStream - Creates a new connected poloniex stream.
func NewStream() (stream *Stream, err error) {
	client, err := turnpike.NewWebsocketClient(turnpike.JSON, WebsocketAddress, nil)
	if err != nil {
		return
	}

	_, err = client.JoinRealm(WebsocketRealm, nil)
	if err != nil {
		return
	}

	return &Stream{
		client: client,
		events: make(chan *orderbook.Event, 1000),
		errors: make(chan error, 10),
	}, nil
}

// Subscribe - Subscribes to currency pair order book.
func (stream *Stream) Subscribe(pair *currency.Pair) error {
	return stream.client.Subscribe(pair.Concat("_"), stream.eventHandler(pair))
}

// Events - Channel of orderbook.
func (stream *Stream) Events() <-chan *orderbook.Event {
	return stream.events
}

// Errors - Channel of errors.
func (stream *Stream) Errors() <-chan error {
	return stream.errors
}

// Close - Closes a stream.
func (stream *Stream) Close() (err error) {
	// We have to first close the stream
	err = stream.client.Close()
	if err != nil {
		return
	}

	// It should be safe to close channels now
	close(stream.errors)
	close(stream.events)
	return
}

// eventHandler - Creates stream event handler for currency pair.
func (stream *Stream) eventHandler(pair *currency.Pair) turnpike.EventHandler {
	return func(args []interface{}, kwargs map[string]interface{}) {
		if len(args) == 0 {
			glog.Info("HEARTBEAT")
		}
		for _, v := range args {
			// Cast to underlying value type
			value := v.(map[string]interface{})

			// Message type
			typ := value["type"].(string)

			// Message data
			data := value["data"].(map[string]interface{})

			// Handle message
			res, err := parseEvent(typ, pair, data)
			if err != nil {
				stream.errors <- fmt.Errorf("unhandled poloniex message: %v", err)
			} else {
				stream.events <- res
			}
		}
	}
}

var handlers = map[string]func(*currency.Pair, map[string]interface{}) (*orderbook.Event, error){
	"orderBookRemove": parseRemove,
	"orderBookModify": parseModify,
	"newTrade":        parseTrade,
}

func parseEvent(typ string, pair *currency.Pair, data map[string]interface{}) (_ *orderbook.Event, err error) {
	// Get handler by message type
	handler, ok := handlers[typ]
	if !ok {
		return nil, fmt.Errorf("unknown type: %q", typ)
	}

	// Handle message
	return handler(pair, data)
}

func parseRemove(pair *currency.Pair, data map[string]interface{}) (_ *orderbook.Event, err error) {
	res := &order.Order{
		Exchange: exchange.Poloniex,
		Volume:   currency.NewVolume(pair.Second, 0.0),
	}
	res.Type, err = order.TypeFromString(data["type"].(string))
	if err != nil {
		return
	}
	res.Rate, err = common.ParseIVolume(pair.First, data["rate"])
	if err != nil {
		return
	}
	return orderbook.NewEvent(res), nil
}

func parseModify(pair *currency.Pair, data map[string]interface{}) (_ *orderbook.Event, err error) {
	res := &order.Order{
		Exchange: exchange.Poloniex,
	}
	res.Type, err = order.TypeFromString(data["type"].(string))
	if err != nil {
		return
	}
	res.Rate, err = common.ParseIVolume(pair.First, data["rate"])
	if err != nil {
		return
	}
	res.Volume, err = common.ParseIVolume(pair.Second, data["amount"])
	if err != nil {
		return
	}
	return orderbook.NewEvent(res), nil
}

func parseTrade(pair *currency.Pair, data map[string]interface{}) (_ *orderbook.Event, err error) {
	res := new(order.Trade)
	res.ID, err = common.ParseIInt64(data["tradeID"])
	if err != nil {
		return
	}
	t, err := time.Parse("2006-01-02 15:04:05", data["date"].(string))
	if err != nil {
		return
	}
	res.Time, err = types.TimestampProto(t)
	if err != nil {
		return
	}
	res.Order = &order.Order{
		Exchange: exchange.Poloniex,
	}
	res.Order.Type, err = order.TypeFromString(data["type"].(string))
	if err != nil {
		return
	}
	res.Order.Rate, err = common.ParseIVolume(pair.First, data["rate"])
	if err != nil {
		return
	}
	res.Order.Volume, err = common.ParseIVolume(pair.Second, data["amount"])
	if err != nil {
		return
	}
	return orderbook.NewEvent(res), nil
}
