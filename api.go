package exchanges

import (
	"github.com/crypto-bank/proto/currency"
	"github.com/crypto-bank/proto/orderbook"
)

// Stream - Exchange stream.
type Stream interface {
	// Subscribe - Subscribe to currency pair exchanges.
	Subscribe(*currency.Pair) error

	// Events - Channel of orderbook.
	Events() <-chan *orderbook.Event

	// Errors - Channel of errors.
	Errors() <-chan error

	// Close - Closes a stream.
	Close() error
}
