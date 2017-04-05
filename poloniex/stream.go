package poloniex

import (
	"fmt"
	"time"

	"github.com/beatgammit/turnpike"
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
	hbchan chan struct{}

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
	client, err := connectWS()
	if err != nil {
		return
	}
	stream = &Stream{
		client: client,
		events: make(chan *orderbook.Event, 1000),
		errors: make(chan error, 1000),
		hbchan: make(chan struct{}, 1000),
	}
	go stream.heartbeatPoll()
	return
}

// connect - Connects to poloniex websocket server
func connectWS() (client *turnpike.Client, err error) {
	// Create WS client and connect
	client, err = turnpike.NewWebsocketClient(turnpike.JSON, WebsocketAddress, nil)
	if err != nil {
		return
	}
	// Join to poloniex realm
	_, err = client.JoinRealm(WebsocketRealm, nil)
	if err != nil {
		return nil, err
	}
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
		stream.hbchan <- struct{}{}

		// If more than one argument is present
		// it means we received an event.
		// We have to store timestamp of last event,
		// so in case no events in latest 60 seconds will be sent
		// heartbeats will increase to 8 seconds (sent by Poloniex)
		// and we won't reconnect.
		if len(args) >= 1 {
			stream.timestamp = time.Now()
		}

		// If no sequence ID we are not having the right thing
		fseq, ok := kwargs["seq"].(float64)
		if !ok {
			glog.Warningf("No sequence number in kwargs")
			return
		}

		// Convert `float64` sequence ID to `int64`
		seq := int64(fseq)

		// Parse and emit all events
		for _, v := range args {
			// Cast to underlying value type
			value := v.(map[string]interface{})

			// Message type
			typ := value["type"].(string)

			// Message data
			data := value["data"].(map[string]interface{})

			// Handle message
			res, err := parseEvent(typ, seq, pair, data)
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
		// adding two seconds in case of connection problems.
		timeout := stream.heartbeatInterval() + (2 * time.Second)

		// Poll on either a heartbeat or a timeout channel.
		select {
		case _, ok := <-stream.hbchan:
			// If ok is false, it means heartbeat channel was closed
			// and this goroutine should be garbage collected
			if !ok {
				return
			}
		case <-time.After(timeout):
			// Timeout passed for a heartbeat to arrive.
			// Warn about reconnecting and start the process.
			glog.Warningf("No heartbeat received for %s, reconnecting.", timeout)
			// Start reconnecting to websocket
			stream.reconnect()
		}
	}
}

func (stream *Stream) reconnect() {
	// Channel on which event will be sent if we are done connecting
	done := make(chan struct{}, 2)
	result := make(chan *turnpike.Client, 1)

	// Defer closing of the channels
	defer func() {
		close(done)
		close(result)
	}()

	// Start re-connecting in goroutine
	go func() {
		for {
			glog.V(1).Info("Re-connecting to poloniex")

			// Connect to websocket
			client, err := connectWS()
			if err != nil {
				glog.Warningf("Re-connect error: %v", err)
				stream.errors <- err
			} else {
				glog.V(1).Info("Re-connected successfuly")
				result <- client
				return
			}

			select {
			case <-time.After(time.Millisecond * 250):
				continue
			case <-done:
				// If we receive on `done` channel
				// it means we have received heartbeat before
				// if we've got a client we have to close it
				if client != nil {
					glog.V(1).Info("Closing re-connected client")
					client.Close()
				}
			}
		}
	}()

	select {
	case hb, ok := <-stream.hbchan:
		glog.V(1).Infof("Heartbeat restored before re-connect (state: %v)", ok)
		// Send event telling we are done connecting
		done <- struct{}{}
		// If ok is false, it means heartbeat channel was closed
		// and this goroutine should be garbage collected
		if !ok {
			glog.V(1).Info("Closing re-connected client")
			return
		}
		// Re-send heartbeat on the channel
		stream.hbchan <- hb
		// Try to read from result channel
		if client, ok := <-result; ok {
			glog.V(1).Info("Closing re-connected client")
			// Close client, it won't be used anymore
			client.Close()
		} else {
			glog.V(1).Info("Re-connected client was not recovered")
		}
		return
	case client := <-result:
		glog.V(1).Info("Replacing old client")
		// First we have to close old client
		if err := stream.client.Close(); err != nil {
			glog.Warningf("Client close error: %v", err)
			stream.errors <- err
		}

		// now we can replace it with new client
		stream.client = client
	}

	// Re-subscribe after re-connection
	glog.V(1).Infof("Re-subscribing to %d channels", len(stream.subs))
	stream.resubscribe()
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

var handlers = map[string]func(int64, *currency.Pair, map[string]interface{}) (*orderbook.Event, error){
	"orderBookRemove": parseRemove,
	"orderBookModify": parseModify,
	"newTrade":        parseTrade,
}

func parseEvent(typ string, seq int64, pair *currency.Pair, data map[string]interface{}) (_ *orderbook.Event, err error) {
	// Get handler by message type
	handler, ok := handlers[typ]
	if !ok {
		return nil, fmt.Errorf("unknown type: %q", typ)
	}

	// Handle message
	return handler(seq, pair, data)
}

func parseRemove(seq int64, pair *currency.Pair, data map[string]interface{}) (_ *orderbook.Event, err error) {
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
	return orderbook.NewEvent(seq, res), nil
}

func parseModify(seq int64, pair *currency.Pair, data map[string]interface{}) (_ *orderbook.Event, err error) {
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
	return orderbook.NewEvent(seq, res), nil
}

func parseTrade(seq int64, pair *currency.Pair, data map[string]interface{}) (_ *orderbook.Event, err error) {
	res := new(order.Trade)
	res.ID, err = common.ParseIInt64(data["tradeID"])
	if err != nil {
		return
	}
	res.Time, err = common.ParseTime("2006-01-02 15:04:05", data["date"].(string))
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
	return orderbook.NewEvent(seq, res), nil
}
