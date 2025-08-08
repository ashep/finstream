package streamer

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/ashep/finstream/internal/apperr"
	"github.com/ashep/go-banking"
	"github.com/shopspring/decimal"
)

func (s *Streamer) fetchCurrencyRates(ctx context.Context) error {
	for prvName, prv := range s.providers {
		rates, err := prv.GetCurrencyRates(ctx)
		if err != nil {
			return fmt.Errorf("%s: get currency rates: %w", prvName, err)
		}

		for _, rateNew := range rates {
			if !slices.Contains(s.cfg.Currency.List, rateNew.Base.Code) {
				continue
			}
			if !slices.Contains(s.cfg.Currency.List, rateNew.Target.Code) {
				continue
			}

			rateBefore, err := s.storage.GetCurrencyRate(ctx, rateNew.Provider, rateNew.Base.Code, rateNew.Target.Code)
			if err != nil && !errors.Is(err, apperr.ErrCurrencyRateNotFound) {
				return fmt.Errorf("%s: get existing rate: %w", prvName, err)
			}

			updated, err := s.storage.SetCurrencyRate(ctx, rateNew)
			if err != nil {
				return fmt.Errorf("%s: store rate: %w", prvName, err)
			}

			if !updated {
				return nil
			}

			for snkName, snk := range s.sinks {
				if snk.Currency == nil {
					continue
				}

				k := fmt.Sprintf("%s:%s:%s", rateNew.Provider, rateNew.Base.Code, rateNew.Target.Code)
				ev := banking.CurrencyRateChange{
					After: rateNew,
				}
				if rateBefore != nil {
					ev.Before = *rateBefore
				} else {
					ev.Before = banking.CurrencyRate{
						Provider: rateNew.Provider,
						Base:     rateNew.Base,
						Target:   rateNew.Target,
						Rate:     decimal.Zero,
						Date:     time.Unix(0, 0),
					}
				}

				if err := snk.Currency.Write(ctx, k, ev); err != nil {
					return fmt.Errorf("sink write failed: %s: %w", snkName, err)
				}
			}

			ll := s.l.Info().
				Str("provider", prvName).
				Str("base", rateNew.Base.Code).
				Str("target", rateNew.Target.Code).
				Str("rate", rateNew.Rate.String()).
				Str("date", rateNew.Date.Format(time.DateTime))
			if rateBefore != nil {
				ll = ll.Str("ex_rate", rateBefore.Rate.String()).Str("ex_date", rateBefore.Date.Format(time.DateTime))
			}
			ll.Msg("currency rate updated")
		}
	}

	return nil
}
