package bitfinex

import (
	"fmt"
	"math"

	"github.com/gogo/protobuf/types"
	"github.com/gorilla/websocket"
	"github.com/tidwall/gjson"

	"encoding/json"

	"github.com/crypto-bank/proto/currency"
	"github.com/crypto-bank/proto/order"
	"github.com/crypto-bank/proto/orderbook"
)

// Stream - Bitfinex stream.
type Stream struct {
	client *websocket.Conn
	events chan *orderbook.Event
	errors chan error

	pairs    map[string]*currency.Pair
	channels map[int64]*currency.Pair
}

const (
	// WebsocketAddress - Bitfinex Websocket address.
	WebsocketAddress = "wss://api2.bitfinex.com:3000/ws"
)

// NewStream - Creates a new connected Bitfinex stream.
func NewStream() (stream *Stream, err error) {
	client, _, err := websocket.DefaultDialer.Dial(WebsocketAddress, nil)
	if err != nil {
		return
	}

	// Read handshake but ignore it
	_, _, err = client.ReadMessage()
	if err != nil {
		return
	}

	stream = &Stream{
		client:   client,
		events:   make(chan *orderbook.Event, 1000),
		errors:   make(chan error, 10),
		pairs:    make(map[string]*currency.Pair),
		channels: make(map[int64]*currency.Pair),
	}

	go stream.readPump()

	return
}

var pairNameMap = map[string]string{
	"BTCLTC": "ETHBTC",
	"BTCETC": "ETCBTC",
	"BTCETH": "ETHBTC",
	"BTCZEC": "ZECBTC",
}

// Subscribe - Subscribes to currency pair order book.
func (stream *Stream) Subscribe(pair *currency.Pair) error {
	// Concatenate without separator
	name := pair.Concat("")
	// Check if it's mapped to other pair
	if n, ok := pairNameMap[name]; ok {
		name = n
	} else {
		return fmt.Errorf("currency pair %q was not mapped", pair)
	}
	// Add pair name to local map
	stream.pairs[name] = pair
	// Send JSON message
	return stream.send(map[string]string{
		"event":   "subscribe",
		"channel": "trades",
		"prec":    "P0",
		"freq":    "F0",
		"pair":    name,
	})
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

func (stream *Stream) send(v map[string]string) (err error) {
	body, err := json.Marshal(v)
	if err != nil {
		return
	}
	return stream.client.WriteMessage(websocket.TextMessage, body)
}

func (stream *Stream) readPump() {
	for {
		typ, resp, err := stream.client.ReadMessage()
		if err != nil {
			stream.errors <- err
			continue
		}
		if typ != websocket.TextMessage || len(resp) == 0 {
			continue
		}
		if resp[0] == '[' {
			err = stream.parseEventArray(resp)
		} else {
			err = stream.parseEvent(resp)
		}
		if err != nil {
			stream.errors <- err
			continue
		}
	}
}

func (stream *Stream) parseEventArray(body []byte) (err error) {
	var event []interface{}
	if err = json.Unmarshal(body, &event); err != nil {
		return
	}

	// Pass if it's a heartbeat
	// or if it's a snapshot
	if len(event) == 2 {
		return
	}

	// Second element is type of event
	evType := event[1].(string)

	// Order update is not required
	if evType == "te" {
		return
	}

	// Fourth element is an ID
	tradeID := event[3].(float64)

	// Fifth element is a timestamp
	timestamp := int64(event[4].(float64))

	// Sixth element is a price
	price := event[5].(float64)

	// Seventh element is amount
	amount := event[6].(float64)

	// Channel ID is a first element
	chanID := int64(event[0].(float64))

	// Currency pair by channel ID
	pair := stream.channels[chanID]

	// Construct event
	trade := &order.Trade{
		ID: int64(tradeID),
		Order: &order.Order{
			Rate:   currency.NewVolume(pair.First, price),
			Volume: currency.NewVolume(pair.Second, math.Abs(amount)),
		},
		Time: &types.Timestamp{Seconds: timestamp},
	}

	// Set order type based on amount
	// "Positive values mean bid, negative values mean ask."
	if amount < 0 {
		trade.Order.Type = order.Ask
	} else {
		trade.Order.Type = order.Bid
	}

	// Send event to channel
	stream.events <- orderbook.NewEvent(trade)
	return
}

func (stream *Stream) parseEvent(body []byte) (err error) {
	event := gjson.ParseBytes(body)
	name := event.Get("event").String()
	if name == "info" {
		return
	}
	if name == "subscribed" {
		return stream.handleSubscription(event)
	}
	if name == "auth" {
		status := event.Get("status").String()
		if status == "OK" {
			return
		}
		return fmt.Errorf("auth error %q", event.Get("code").String())
	}
	return fmt.Errorf("unknown event %s", body)
}

func (stream *Stream) handleSubscription(event gjson.Result) (err error) {
	pair := event.Get("pair").String()
	channel := event.Get("chanId").Int()
	stream.channels[channel] = stream.pairs[pair]
	return
}
