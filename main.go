package main

import (
	"context"
	"flag"
	"log"
	tgClient "read-adviser-bot/clients/telegram"
	"read-adviser-bot/consumer/event-consumer"
	"read-adviser-bot/events/telegram"
	"read-adviser-bot/internal/kafka/consumer"
	kl "read-adviser-bot/internal/kafka/producer"
	"read-adviser-bot/storage/sqllite"
	"strings"
)

const (
	storagePath      = "data/sqlite/bot.db"
	BatchSize        = 100
	BootstrapServers = "localhost:9092"
)

func main() {
	//Инициализая контекста
	ctx := context.TODO()
	//s:=files.New(storagePath)
	// Инициализация БД
	s, err := sqllite.New(storagePath)
	if err != nil {
		log.Fatal(err)
	}
	err = s.Init(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Инициализация Kafka Producer
	producer, err := kl.NewProducer(strings.Split(BootstrapServers, ","))
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	// Создание Processor с продюсером
	eventsProcessor := telegram.New(
		tgClient.New(mustHost(), mustToken()),
		s,
		producer, // <-- передаем продюсер
	)
	log.Print("service started")

	tgConsumer := event_consumer.New(eventsProcessor, eventsProcessor, BatchSize)

	kafkaConsumer, err := consumer.NewConsumer(
		strings.Split(BootstrapServers, ","),
		"pages-topic",
		"bot-group",
		eventsProcessor,
	)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		err := tgConsumer.Start(ctx)
		if err != nil {
			log.Fatal(err)
		}
	}()
	go kafkaConsumer.Start(ctx)

	log.Print("service started")
	select {}
}

func mustToken() string {
	token := flag.String(
		"tg-bot-token",
		"",
		"token for access to telegram bot",
	)

	flag.Parse()
	if *token == "" {
		log.Fatal("token is required")
	}

	return *token
}

func mustHost() string {
	return "api.telegram.org"
}
