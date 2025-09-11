package consumer

import (
	"context"
	"encoding/json"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/emicklei/go-restful/v3/log"
	"read-adviser-bot/events/telegram"
	"read-adviser-bot/lib/e"
	"read-adviser-bot/storage"
	"strings"
)

const (
	sessionTimeout = 6000
	noTimeout      = -1
)

type Consumer struct {
	consumer  *kafka.Consumer
	processor *telegram.Processor
	stop      bool
}

func NewConsumer(address []string, topic, consumerGroup string, processor *telegram.Processor) (*Consumer, error) {

	cfg := &kafka.ConfigMap{
		"bootstrap.servers":        strings.Join(address, ","),
		"group.id":                 consumerGroup,
		"session.timeout.ms":       sessionTimeout,
		"enable.auto.commit":       true,
		"enable.auto.offset.store": true,
		"auto.offset.reset":        "earliest",
	}

	consumer, err := kafka.NewConsumer(cfg)
	if err != nil {
		return nil, e.Wrap("Failed to create consumer", err)
	}

	if err := consumer.Subscribe(topic, nil); err != nil {
		return nil, e.Wrap("Failed to subscribe to topic", err)
	}

	return &Consumer{consumer: consumer, processor: processor}, nil
}

func (c *Consumer) Start(ctx context.Context) {
	log.Print("Kafka consumer started")
	for {
		if c.stop {
			break
		}

		kafkaMsg, err := c.consumer.ReadMessage(noTimeout)
		if err != nil {
			log.Printf("[KafkaConsumer] read error: %v", err)
			continue
		}

		if kafkaMsg == nil {
			continue
		}
		log.Printf("[KafkaConsumer] got message: topic=%s partition=%d offset=%d key=%s",
			*kafkaMsg.TopicPartition.Topic,
			kafkaMsg.TopicPartition.Partition,
			kafkaMsg.TopicPartition.Offset,
			string(kafkaMsg.Key),
		)

		var page storage.Page
		if err := json.Unmarshal(kafkaMsg.Value, &page); err != nil {
			log.Printf("[KafkaConsumer] unmarshal error: %v", err)
			continue
		}

		log.Printf("Saving page from Kafka: URL=%s, UserName=%s", page.URL, page.UserName)

		if err := c.processor.DoCmd(ctx, page.URL, page.ChatID, page.UserName); err != nil {
			log.Printf("[KafkaConsumer] can't save page via doCmd: %v", err)
		}
	}
}

func (c *Consumer) Stop() {
	c.stop = true
	c.consumer.Close()
}
