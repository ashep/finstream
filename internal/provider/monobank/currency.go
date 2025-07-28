package monobank

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ashep/go-banking"
	"github.com/shopspring/decimal"
)

func (m *Monobank) GetCurrencyRates(ctx context.Context) ([]banking.CurrencyRate, error) {
	monoRates, err := m.cli.GetCurrencyRates(ctx)
	if err != nil {
		return nil, fmt.Errorf("get currency rates: %w", err)
	}

	rates := make([]banking.CurrencyRate, 0, len(monoRates))
	for _, rate := range monoRates {
		// In Monobank API, CurrencyCodeA is the target currency (e.g., USD, EUR)
		target, err := banking.NewCurrencyByNum(rate.CurrencyCodeA)
		if errors.Is(err, banking.ErrCurrencyNotFound) {
			continue // Silently skip, because `banking` does not support this currency yet
		} else if err != nil {
			return nil, fmt.Errorf("new currency by num %d: %w", rate.CurrencyCodeA, err)
		}

		// In Monobank API, CurrencyCodeB is the base currency
		base, err := banking.NewCurrencyByNum(rate.CurrencyCodeB)
		if err != nil {
			return nil, fmt.Errorf("new currency by num %d: %w", rate.CurrencyCodeA, err)
		}

		var val decimal.Decimal
		if rate.RateSell != 0 { // the highest rate
			val = decimal.NewFromFloat(rate.RateSell)
		} else if rate.RateCross != 0 {
			val = decimal.NewFromFloat(rate.RateCross)
		} else {
			return nil, fmt.Errorf("no valid rate found for currency %s", target.Code)
		}

		if rate.Date == 0 {
			return nil, fmt.Errorf("rzero ate date for currency %s", target.Code)
		}

		rates = append(rates, banking.CurrencyRate{
			Base:   base,
			Target: target,
			Rate:   val.Round(int32(target.Digits)),
			Date:   time.Unix(rate.Date, 0),
		})
	}

	return rates, nil
}
