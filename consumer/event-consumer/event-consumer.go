package event_consumer

import (
	"context"
	"log"
	"read-adviser-bot/events"
	"time"
)

type Consumer struct {
	events.Fetcher
	events.Processor
	batchSize int
}

func New(fetcher events.Fetcher, processor events.Processor, batchSize int) Consumer {
	return Consumer{
		Fetcher:   fetcher,
		Processor: processor,
		batchSize: batchSize,
	}
}

func (c Consumer) Start(ctx context.Context) error {
	for {
		gotEvents, err := c.Fetch(c.batchSize) //подумать над инструментом ретраев
		if err != nil {
			log.Printf("[WARN] consumer: %s", err.Error())
			continue
		}

		if len(gotEvents) == 0 {
			time.Sleep(1 * time.Second)
			continue
		}

		if err := c.handleEvents(ctx, gotEvents); err != nil {
			log.Printf("[INFO] consumer: %s", err.Error())
			continue
		}
	}
}

/*
1. Потеря событый: ретраи, возвращение в хранилище, фолбэки, подтверждение для фетчера
2. Обработка всей пачки: остановка после первой ошибки, счетчик ошибок
3. Параллельная обработка через sync.WaitGroup{}
*/
func (c Consumer) handleEvents(ctx context.Context, events []events.Event) error {
	for _, event := range events {
		log.Printf("got new event: %s", event.Text)

		if err := c.Process(ctx, event); err != nil {
			log.Printf("can't handle event: %s", err.Error())
			continue
		}
	}
	return nil
}
