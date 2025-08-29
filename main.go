package main

import (
	"flag"
	"log"
	tgClient "read-adviser-bot/clients/telegram"
	"read-adviser-bot/consumer/event-consumer"
	"read-adviser-bot/events/telegram"
	"read-adviser-bot/storage/files"
)

const (
	storagePath = "./storage/storage.db"
	BatchSize   = 100
)

func main() {
	eventsProcessor := telegram.New(
		tgClient.New(mustHost(), mustToken()),
		files.New(storagePath),
	)
	log.Print("service started")

	consumer := event_consumer.New(eventsProcessor, eventsProcessor, BatchSize)
	if err := consumer.Start(); err != nil {
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
