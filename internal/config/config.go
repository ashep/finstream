package config

import (
	"fmt"

	"github.com/ashep/go-banking"
)

type StorageDriver string

const (
	StorageDDB StorageDriver = "ddb"
)

type Monobank struct {
	Enabled bool   `yaml:"enabled" envconfig:"APP_MONOBANK_ENABLED"`
	APIKey  string `yaml:"api_key" envconfig:"APP_MONOBANK_API_KEY"`
}

type Currency struct {
	List          []string `yaml:"list" envconfig:"APP_CURRENCY_LIST"`
	RefreshPeriod int      `yaml:"refresh_period" envconfig:"APP_CURRENCY_REFRESH_PERIOD"` // in seconds
}

type Storage struct {
	Driver StorageDriver    `yaml:"driver" envconfig:"APP_STORAGE_DRIVER"`
	DDB    StorageDriverDDB `yaml:"ddb" envconfig:"APP_STORAGE_DDB"`
}

type StorageDriverDDB struct {
	Region          string `yaml:"region" envconfig:"APP_STORAGE_DDB_REGION"`
	TableName       string `yaml:"table_name" envconfig:"APP_STORAGE_DDB_TABLE_NAME"`
	AccessKeyID     string `yaml:"access_key_id" envconfig:"APP_STORAGE_DDB_ACCESS_KEY_ID"`
	AccessKeySecret string `yaml:"access_key_secret" envconfig:"APP_STORAGE_DDB_ACCESS_KEY_SECRET"`
}

type Config struct {
	Monobank Monobank `yaml:"monobank"`
	Currency Currency `yaml:"currency"`
	Storage  Storage  `yaml:"storage"`
}

func (c *Config) Validate() error {
	if err := c.validateMonobank(); err != nil {
		return err
	}

	if err := c.validateStorage(); err != nil {
		return err
	}

	if c.Currency.RefreshPeriod == 0 {
		c.Currency.RefreshPeriod = 60
	}

	if c.Currency.RefreshPeriod < 60 {
		return fmt.Errorf("currency.refresh_period must not be less than 60 seconds")
	}

	if len(c.Currency.List) == 0 {
		c.Currency.List = []string{"EUR", "UAH", "USD"}
	}

	for _, code := range c.Currency.List {
		if _, err := banking.NewCurrencyByCode(code); err != nil {
			return fmt.Errorf("invalid currency code %s: %w", code, err)
		}
	}

	return nil
}

func (c *Config) validateMonobank() error {
	if !c.Monobank.Enabled {
		return nil
	}

	if c.Monobank.APIKey == "" {
		return fmt.Errorf("monobank.api_key is required when monobank is enabled")
	}

	return nil
}

func (c *Config) validateStorage() error {
	if c.Storage.Driver == "" {
		return fmt.Errorf("storage.driver is required")
	}

	switch c.Storage.Driver {
	case StorageDDB:
		if c.Storage.DDB.Region == "" {
			return fmt.Errorf("storage.ddb.region is required")
		}
		if c.Storage.DDB.TableName == "" {
			return fmt.Errorf("storage.ddb.table_name is required")
		}
		if c.Storage.DDB.AccessKeyID == "" {
			return fmt.Errorf("storage.ddb.access_key_id is required")
		}
		if c.Storage.DDB.AccessKeySecret == "" {
			return fmt.Errorf("storage.ddb.access_key_secret is required")
		}
	default:
		return fmt.Errorf("unsupported storage driver: %s", c.Storage.Driver)
	}

	return nil
}
