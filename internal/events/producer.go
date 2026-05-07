// go - kafka level  producer implementation
// using kafka client - franz go
// here we will make the client model (producer and consumer) and then close it

package events

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

// kafka producer , producer using franz go
type kafkaProducer struct {
	client *kgo.Client
	topic  string
	log    *zap.Logger
}

// new kafka producer , built an franz go kafka producer connected to brokers

// it comes with idempotency , so retries does not cause duplication

func NewKafkaProducer(brokers []string, topic string, log *zap.Logger) (Producer, error) {

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.LeaderAck()),
		kgo.ProducerBatchMaxBytes(1<<20),
		kgo.RecordDeliveryTimeout(5e9),
	)

	if err != nil {
		return nil, fmt.Errorf("kafka producer: connect failed: %w", err)
	}

	return &kafkaProducer{client: client, topic: topic, log: log}, nil
}

// record click will serialize the event to json, their is go routine exist after the produce call returns

func (p *kafkaProducer) RecordClick(e ClickEvent) {

	// go routine

	go func() {

		b, err := json.Marshal(e)

		if err != nil {
			p.log.Error("kafka: marshal click event", zap.Error(err))
			return
		}

		results := p.client.ProduceSync(context.Background(), &kgo.Record{Topic: p.topic, Value: b})

		if err := results.FirstErr(); err != nil {
			p.log.Error("kafka: produce click event",
				zap.String("short_code", e.ShortCode),
				zap.Error(err),
			)
		}
	}()

}

// close the kafka client

func (p *kafkaProducer) Close() error {
	p.client.Close()
	return nil
}
