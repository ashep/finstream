package monobank

import (
	"github.com/ashep/go-monobank-client"
)

type Monobank struct {
	cli *monobank.Client
}

func New(apiKey string) *Monobank {
	return &Monobank{
		cli: monobank.New(apiKey),
	}
}
