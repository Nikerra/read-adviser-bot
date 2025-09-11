package producer

import (
	"encoding/json"
	"errors"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"read-adviser-bot/lib/e"
	"read-adviser-bot/storage"
	"strings"
	"time"
)

const (
	flushTimeout = time.Millisecond * 5000
)

type Producer struct {
	producer *kafka.Producer
}

func NewProducer(address []string) (*Producer, error) {

	conf := &kafka.ConfigMap{
		"bootstrap.servers": strings.Join(address, ","),
	}

	p, err := kafka.NewProducer(conf)
	if err != nil {
		return nil, e.Wrap("error new producer", err)
	}

	return &Producer{producer: p}, nil
}

func (p *Producer) Produce(message *storage.Page, topic string) error {

	data, err := json.Marshal(message)
	if err != nil {
		return e.Wrap("error marshalling message", err)
	}
	kafkaMsg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: data,
		Key:   []byte(uuid.NewString()),
	}
	kafkaChan := make(chan kafka.Event)
	if err := p.producer.Produce(kafkaMsg, kafkaChan); err != nil {
		return e.Wrap("error producing message", err)
	}

	event := <-kafkaChan
	switch ev := event.(type) {
	case *kafka.Message:
		return nil
	case kafka.Error:
		return ev
	default:
		return errors.New("unknown event type")
	}
}

func (p *Producer) Close() {
	p.producer.Flush(int(flushTimeout))
	p.producer.Close()
}
