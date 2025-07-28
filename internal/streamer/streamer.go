package streamer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ashep/finstream/internal/config"
	"github.com/ashep/finstream/internal/storage/ddb"
	"github.com/ashep/go-banking"
	"github.com/rs/zerolog"
)

type provider interface {
	GetCurrencyRates(ctx context.Context) ([]banking.CurrencyRate, error)
}

type storage interface {
	GetCurrencyRate(ctx context.Context, baseNum, targetNum int) (*banking.CurrencyRate, error)
	SetCurrencyRate(ctx context.Context, rate banking.CurrencyRate) (bool, error)
}

type Streamer struct {
	cfg *config.Config
	pr  map[string]provider
	st  storage
	l   zerolog.Logger
}

func New(cfg *config.Config, l zerolog.Logger) (*Streamer, error) {
	var st storage
	switch cfg.Storage.Driver {
	case config.StorageDDB:
		st = ddb.New(
			cfg.Storage.DDB.Region,
			cfg.Storage.DDB.TableName,
			cfg.Storage.DDB.AccessKeyID,
			cfg.Storage.DDB.AccessKeySecret,
		)
	}

	return &Streamer{
		cfg: cfg,
		pr:  make(map[string]provider),
		st:  st,
		l:   l,
	}, nil
}

func (s *Streamer) RegisterProvider(name string, p provider) error {
	if _, exists := s.pr[name]; exists {
		return fmt.Errorf("provider already registered: %s", name)
	}

	s.pr[name] = p

	return nil
}

func (s *Streamer) Run(ctx context.Context) error {
	if len(s.pr) == 0 {
		return errors.New("no providers registered")
	}

	if err := s.fetchCurrencyRates(ctx); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			if !errors.Is(ctx.Err(), context.Canceled) {
				return ctx.Err()
			}
			return nil
		case <-time.After(time.Second * time.Duration(s.cfg.Currency.RefreshPeriod)):
			if err := s.fetchCurrencyRates(ctx); err != nil {
				return err
			}
		}
	}
}
