package kafka

import (
	"encoding/json"
	"errors"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
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

func NewProducer(adress []string) (*Producer, error) {

	conf := &kafka.ConfigMap{
		"bootstrap.servers": strings.Join(adress, ","),
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
		Key:   nil,
	}
	kafcaChan := make(chan kafka.Event)
	if err := p.producer.Produce(kafkaMsg, kafcaChan); err != nil {
		return e.Wrap("error producing message", err)
	}

	e := <-kafcaChan
	switch ev := e.(type) {
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
