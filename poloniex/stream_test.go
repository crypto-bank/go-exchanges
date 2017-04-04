package poloniex

import (
	"testing"

	"github.com/crypto-bank/proto/currency"
	"github.com/crypto-bank/proto/order"

	. "gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type ParseSuite struct{}

var _ = Suite(&ParseSuite{})

func (s *ParseSuite) TestParseRemove(c *C) {
	e, err := parseEvent("orderBookRemove", currency.NewPair("BTC", "DCR"), map[string]interface{}{
		"type": "ask",
		"rate": "0.01227622",
	})
	c.Assert(err, Equals, nil)
	c.Assert(e, NotNil)

	ev := e.Inner().(*order.Order)
	c.Assert(ev.Type, Equals, order.Ask)
	// Check if rate matches
	c.Assert(ev.Rate.Amount, Equals, 0.01227622)
	c.Assert(ev.Rate.Currency.Symbol, Equals, "BTC")
	// Check if volume matches
	c.Assert(ev.Volume.Amount, Equals, 0.0)
	c.Assert(ev.Volume.Currency.Symbol, Equals, "DCR")
}

func (s *ParseSuite) TestParseModify(c *C) {
	e, err := parseEvent("orderBookModify", currency.NewPair("BTC", "DCR"), map[string]interface{}{
		"type":   "ask",
		"rate":   "0.01300000",
		"amount": "1147.68249538",
	})
	c.Assert(err, Equals, nil)
	c.Assert(e, NotNil)

	ev := e.Inner().(*order.Order)
	c.Assert(ev.Type, Equals, order.Ask)
	// Check if rate matches
	c.Assert(ev.Rate.Amount, Equals, 0.01300000)
	c.Assert(ev.Rate.Currency.Symbol, Equals, "BTC")
	// Check if volume matches
	c.Assert(ev.Volume.Amount, Equals, 1147.68249538)
	c.Assert(ev.Volume.Currency.Symbol, Equals, "DCR")
}

func (s *ParseSuite) TestParseTrade(c *C) {
	e, err := parseEvent("newTrade", currency.NewPair("BTC", "DCR"), map[string]interface{}{
		"amount":  "4.15417989",
		"date":    "2017-03-29 05:19:25",
		"rate":    "0.01203607",
		"total":   "0.04999999",
		"tradeID": "587591",
		"type":    "sell",
	})
	c.Assert(err, Equals, nil)

	ev := e.Inner().(*order.Trade)
	c.Assert(ev.ID, Equals, int64(587591))
	c.Assert(ev.Order.Type, Equals, order.Ask)
	// Check if rate matches
	c.Assert(ev.Order.Rate.Amount, Equals, 0.01203607)
	c.Assert(ev.Order.Rate.Currency.Symbol, Equals, "BTC")
	// Check if volume matches
	c.Assert(ev.Order.Volume.Amount, Equals, 4.15417989)
	c.Assert(ev.Order.Volume.Currency.Symbol, Equals, "DCR")
	c.Assert(ev.Order.TotalPrice().Amount, Equals, 0.049999999948632294)
	c.Assert(ev.Order.TotalPrice().Currency.Symbol, Equals, "BTC")
	c.Assert(ev.Time.String(), Equals, "2017-03-29T05:19:25Z")
}
