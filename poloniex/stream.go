package poloniex

import (
	"context"
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

	// hbchan - heartbeat channel
	hbchan chan interface{}

	// subs - active subscriptions
	subs []*currency.Pair

	// timestamp - time of last event
	timestamp time.Time
}

const (
	// WebsocketAddress - Poloniex Websocket address.
	WebsocketAddress = "wss://api.poloniex.com"

	// WebsocketRealm - Poloniex Websocket realm name.
	WebsocketRealm = "realm1"
)

// NewStream - Creates a new connected poloniex stream.
func NewStream() (stream *Stream, err error) {
	stream = &Stream{
		events: make(chan *orderbook.Event, 1000),
		errors: make(chan error, 10),
		hbchan: make(chan interface{}, 1),
	}
	err = stream.connect()
	if err != nil {
		return
	}
	go stream.heartbeatPoll()
	return
}

// connect - Connects to poloniex websocket server
func (stream *Stream) connect() (err error) {
	// Create WS client and connect
	stream.client, err = turnpike.NewWebsocketClient(turnpike.JSON, WebsocketAddress, nil)
	if err != nil {
		return
	}
	// Join to poloniex realm
	_, err = stream.client.JoinRealm(WebsocketRealm, nil)
	return
}

// Subscribe - Sends subscription to the server.
// Stores subscription pair in local memory,
// in case of disconnection it will re-subscribe.
func (stream *Stream) Subscribe(pair *currency.Pair) error {
	// Add pair to local subscriptions map
	// if disconnect happens we might have to reuse it
	stream.subs = append(stream.subs, pair)
	// Send subscription to the server
	return stream.subscribe(pair)
}

// subscribe - Sends subscription to the server.
func (stream *Stream) subscribe(pair *currency.Pair) error {
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
	close(stream.hbchan)
	return
}

// eventHandler - Creates stream event handler for currency pair.
func (stream *Stream) eventHandler(pair *currency.Pair) turnpike.EventHandler {
	return func(args []interface{}, kwargs map[string]interface{}) {
		// Send a heartbeat to a goroutine,
		// which polls for it and re-connects in case of heartbeat stop.
		stream.hbchan <- true

		// If more than one argument is present
		// it means we received an event.
		// We have to store timestamp of last event,
		// so in case no events in latest 60 seconds will be sent
		// heartbeats will increase to 8 seconds (sent by Poloniex)
		// and we won't reconnect.
		if len(args) >= 1 {
			stream.timestamp = time.Now()
		}

		// Parse and emit all events
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

// heartbeatPoll - Polls for heartbeats and re-connects in case of stop.
// Should be executed in separate goroutine.
func (stream *Stream) heartbeatPoll() {
	for {
		// Calculate heartbeat interval,
		// adding one second in case of connection problems.
		timeout := stream.heartbeatInterval() + time.Second

		// Create a context with timeout.
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		// Poll on either a heartbeat or a timeout channel.
		select {
		case hb := <-stream.hbchan:
			// If heartbeat is nil, it means heartbeat channel was closed
			// and this goroutine should be garbage collected
			if hb == nil {
				return
			}
		case <-ctx.Done():
			// Timeout passed for a heartbeat to arrive.
			// Warn about reconnecting and start the process.
			glog.Warningf("No heartbeat received for %s, reconnecting.", timeout)
			// Start reconnecting to websocket
			stream.startReconnecting()
			// Re-subscribe after re-connection
			stream.resubscribe()
		}
	}
}

func (stream *Stream) startReconnecting() {
	for range time.Tick(time.Millisecond * 250) {
		// Connect to websocket
		err := stream.connect()
		if err != nil {
			glog.Warningf("Re-connect error: %v", err)
			continue
		}
		// Stop re-connecting
		break
	}
}

func (stream *Stream) heartbeatInterval() time.Duration {
	if time.Since(stream.timestamp) > time.Minute {
		return time.Second * 8
	}
	return time.Second

}

// resubscribe - Re-subscribes client after re-connection.
func (stream *Stream) resubscribe() (err error) {
	for _, pair := range stream.subs {
		err = stream.subscribe(pair)
		if err != nil {
			return
		}
	}
	return
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
