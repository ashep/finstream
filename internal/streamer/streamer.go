package streamer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ashep/finstream/internal/config"
	"github.com/ashep/finstream/internal/sink/kafka"
	"github.com/ashep/finstream/internal/storage/ddb"
	"github.com/ashep/go-banking"
	"github.com/rs/zerolog"
)

type provider interface {
	GetCurrencyRates(ctx context.Context) ([]banking.CurrencyRate, error)
}

type storage interface {
	GetCurrencyRate(ctx context.Context, provider, baseCode, targetCode string) (*banking.CurrencyRate, error)
	SetCurrencyRate(ctx context.Context, rate banking.CurrencyRate) (bool, error)
}

type sinkWriter interface {
	Write(ctx context.Context, key string, val any) error
}

type sink struct {
	Currency sinkWriter
}

type Streamer struct {
	cfg       *config.Config
	storage   storage
	providers map[string]provider
	sinks     map[string]sink
	l         zerolog.Logger
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

	sinks := make(map[string]sink)
	if cfg.Streaming.Kafka.Enabled {
		sinks["kafka"] = sink{
			Currency: kafka.New(cfg.Streaming.Kafka.BootstrapServers, cfg.Streaming.Kafka.Topics.Currency, l),
		}
	}

	return &Streamer{
		cfg:       cfg,
		providers: make(map[string]provider),
		storage:   st,
		sinks:     sinks,
		l:         l,
	}, nil
}

func (s *Streamer) RegisterProvider(name string, p provider) error {
	if _, exists := s.providers[name]; exists {
		return fmt.Errorf("provider already registered: %s", name)
	}

	s.providers[name] = p

	return nil
}

func (s *Streamer) Run(ctx context.Context) error {
	if len(s.providers) == 0 {
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
