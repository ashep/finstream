package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/rs/zerolog"
	kafkago "github.com/segmentio/kafka-go"
)

type Kafka struct {
	cli *kafkago.Writer
	l   zerolog.Logger
}

func New(addr []string, topic string, l zerolog.Logger) *Kafka {
	w := kafkago.Writer{
		Addr:                   kafkago.TCP(addr...),
		Topic:                  topic,
		AllowAutoTopicCreation: true,
	}

	return &Kafka{
		cli: &w,
		l:   l,
	}
}

func (k *Kafka) Write(ctx context.Context, key string, val any) error {
	b, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	msg := kafkago.Message{
		Key:   []byte(key),
		Value: b,
	}
	if err := k.cli.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("write: %w", err)
	}

	return nil
}
