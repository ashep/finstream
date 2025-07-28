package app

import (
	"context"
	"fmt"

	"github.com/ashep/finstream/internal/config"
	"github.com/ashep/finstream/internal/provider/monobank"
	"github.com/ashep/finstream/internal/streamer"
	"github.com/ashep/go-app/runner"
	"github.com/rs/zerolog"
)

type App struct {
	cfg *config.Config
	l   zerolog.Logger
}

func New(cfg *config.Config, rt *runner.Runtime) (*App, error) {
	return &App{
		cfg: cfg,
		l:   rt.Logger,
	}, nil
}

func (a *App) Run(ctx context.Context) error {
	st, err := streamer.New(a.cfg, a.l)
	if err != nil {
		return fmt.Errorf("create streamer: %w", err)
	}

	if a.cfg.Monobank.Enabled {
		if err := st.RegisterProvider("monobank", monobank.New(a.cfg.Monobank.APIKey)); err != nil {
			return fmt.Errorf("register monobank provider: %w", err)
		}
	}

	return st.Run(ctx)
}
