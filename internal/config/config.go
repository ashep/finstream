package config

import (
	"fmt"

	"github.com/ashep/go-banking"
)

type StorageDriver string

type StreamingDriver string

const (
	StorageDDB StorageDriver = "ddb"
)

type Monobank struct {
	Enabled bool   `yaml:"enabled" envconfig:"MONOBANK_ENABLED"`
	APIKey  string `yaml:"api_key" envconfig:"MONOBANK_API_KEY"`
}

type Currency struct {
	List          []string `yaml:"list" envconfig:"CURRENCY_LIST"`
	RefreshPeriod int      `yaml:"refresh_period" envconfig:"CURRENCY_REFRESH_PERIOD"` // in seconds
}

type Storage struct {
	Driver StorageDriver `yaml:"driver" envconfig:"STORAGE_DRIVER"`
	DDB    StorageDriverOptionsDDB
}

type StorageDriverOptionsDDB struct {
	Region          string `yaml:"region" envconfig:"STORAGE_DDB_REGION"`
	TableName       string `yaml:"table_name" envconfig:"STORAGE_DDB_TABLE_NAME"`
	AccessKeyID     string `yaml:"access_key_id" envconfig:"STORAGE_DDB_ACCESS_KEY_ID"`
	AccessKeySecret string `yaml:"access_key_secret" envconfig:"STORAGE_DDB_ACCESS_KEY_SECRET"`
}

type StreamingDriverOptionsKafkaTopics struct {
	Currency string `yaml:"currency" envconfig:"STREAMING_KAFKA_TOPICS_CURRENCY"`
}

type StreamingOptionsKafka struct {
	Enabled          bool                              `yaml:"enabled" envconfig:"STREAMING_KAFKA_ENABLED"`
	BootstrapServers []string                          `yaml:"bootstrap_servers" envconfig:"STREAMING_KAFKA_BOOTSTRAP_SERVERS"`
	Topics           StreamingDriverOptionsKafkaTopics `yaml:"topics"`
}

type Streaming struct {
	Kafka StreamingOptionsKafka `yaml:"kafka"`
}

type Config struct {
	Storage   Storage   `yaml:"storage"`
	Streaming Streaming `yaml:"streaming"`
	Currency  Currency  `yaml:"currency"`
	Monobank  Monobank  `yaml:"monobank"`
}

func (c *Config) Validate() error {
	if err := c.validateStorage(); err != nil {
		return err
	}

	if err := c.ValidateStreaming(); err != nil {
		return err
	}

	if err := c.validateMonobank(); err != nil {
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

func (c *Config) ValidateStreaming() error {
	if c.Streaming.Kafka.Enabled {
		if len(c.Streaming.Kafka.BootstrapServers) == 0 {
			c.Streaming.Kafka.BootstrapServers = []string{"localhost:9092"}
		}
		if c.Streaming.Kafka.Topics.Currency == "" {
			c.Streaming.Kafka.Topics.Currency = "currency"
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
