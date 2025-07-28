package streamer

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/ashep/finstream/internal/apperr"
	"github.com/ashep/go-banking"
)

type currencyRateChangeEvent struct {
	Before banking.CurrencyRate `json:"before"`
	After  banking.CurrencyRate `json:"after"`
}

func (s *Streamer) fetchCurrencyRates(ctx context.Context) error {
	for prvName, prv := range s.pr {
		rates, err := prv.GetCurrencyRates(ctx)
		if err != nil {
			return fmt.Errorf("%s: get currency rates: %w", prvName, err)
		}

		for _, rate := range rates {
			if !slices.Contains(s.cfg.Currency.List, rate.Base.Code) {
				continue
			}
			if !slices.Contains(s.cfg.Currency.List, rate.Target.Code) {
				continue
			}

			exRate, err := s.st.GetCurrencyRate(ctx, rate.Base.Num, rate.Target.Num)
			if err != nil && !errors.Is(err, apperr.ErrCurrencyRateNotFound) {
				return fmt.Errorf("%s: get existing rate: %w", prvName, err)
			}

			updated, err := s.st.SetCurrencyRate(ctx, rate)
			if err != nil {
				return fmt.Errorf("%s: store rate: %w", prvName, err)
			}

			if updated {
				ll := s.l.Info().
					Str("provider", prvName).
					Str("base", rate.Base.Code).
					Str("target", rate.Target.Code).
					Str("rate", rate.Rate.String()).
					Str("date", rate.Date.Format(time.DateTime))
				if exRate != nil {
					ll = ll.Str("ex_rate", exRate.Rate.String()).Str("ex_date", exRate.Date.Format(time.DateTime))
				}
				ll.Msg("currency rate updated")
			} else {
				s.l.Info().
					Str("provider", prvName).
					Str("base", rate.Base.Code).
					Str("target", rate.Target.Code).
					Str("rate", rate.Rate.String()).
					Str("date", rate.Date.Format(time.DateTime)).
					Msg("currency rate isn't changed")
			}
		}
	}

	return nil
}
