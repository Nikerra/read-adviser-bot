package main

import (
	"context"
	"flag"
	"log"
	tgClient "read-adviser-bot/clients/telegram"
	"read-adviser-bot/consumer/event-consumer"
	"read-adviser-bot/events/telegram"
	"read-adviser-bot/storage/sqllite"
)

const (
	storagePath = "data/sqlite/bot.db"
	BatchSize   = 100
)

func main() {
	//s:=files.New(storagePath)
	s, err := sqllite.New(storagePath)
	if err != nil {
		log.Fatal(err)
	}
	err = s.Init(context.TODO())
	if err != nil {
		log.Fatal(err)
	}

	eventsProcessor := telegram.New(
		tgClient.New(mustHost(), mustToken()),
		s,
	)
	log.Print("service started")

	consumer := event_consumer.New(eventsProcessor, eventsProcessor, BatchSize)
	if err := consumer.Start(context.TODO()); err != nil {
		log.Fatal(err)
	}
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
