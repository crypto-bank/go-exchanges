# go-exchanges

[![GoDoc](https://godoc.org/github.com/crypto-bank/go-exchanges?status.svg)](https://godoc.org/github.com/crypto-bank/go-exchanges)

Golang client for cryptocurrency exchanges.

## Support

Currently only [Poloniex](https://poloniex.com/) exchange is supported but contributions are welcome.

## Exchanges

| Exchange | Stream | REST | Planned |
| -------- | ------ | ---- | ------- |
| Poloniex | ✓      | ✗    |         |
| GDAX     | ✗      | ✗    | ✓       |
| Bitfinex | trades | ✗    | ✓       |

## Streaming

Streaming clients should be always self-healing and rely on heartbeats to indicate state of connection.

## License

                                 Apache License
                           Version 2.0, January 2004
                        http://www.apache.org/licenses/
