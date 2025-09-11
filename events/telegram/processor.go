package telegram

import (
	"context"
	"errors"
	"log"
	"read-adviser-bot/clients/telegram"
	"read-adviser-bot/events"
	kl "read-adviser-bot/internal/kafka/producer"
	"read-adviser-bot/lib/e"
	"read-adviser-bot/storage"
)

const (
	PagesTopic = "pages-topic"
)

type Processor struct {
	tg       *telegram.Client
	offset   int
	storage  storage.Storage
	producer *kl.Producer
}

type Meta struct {
	ChatID   int
	Username string
}

var ErrUnknownEventType = errors.New("unknown event type")
var ErrUnknownMetaType = errors.New("unknown meta type")

func New(client *telegram.Client, storage storage.Storage, producer *kl.Producer) *Processor {
	return &Processor{
		tg:       client,
		storage:  storage,
		producer: producer,
	}
}

func (p *Processor) Fetch(limit int) ([]events.Event, error) {
	const errMsg = "can't get events"
	updates, err := p.tg.Updates(p.offset, limit)
	if err != nil {
		return nil, e.Wrap(errMsg, err)
	}

	if len(updates) == 0 {
		return nil, storage.ErrNoSavedPages
	}
	res := make([]events.Event, 0, len(updates))

	for _, u := range updates {
		res = append(res, event(u))
	}

	p.offset = updates[len(updates)-1].ID + 1

	return res, nil
}

func (p *Processor) Process(ctx context.Context, event events.Event) error {
	const errMsg = "can't process event"
	switch event.Type {
	case events.Message:
		return p.processMessage(ctx, event)
	default:
		return e.Wrap(errMsg, ErrUnknownEventType)
	}
}

// Обработка сообщения с отправкой в Kafka до DoCmd
func (p *Processor) processMessage(ctx context.Context, event events.Event) error {
	const errMsg = "can't process message"
	meta, err := meta(event)
	if err != nil {
		return e.Wrap(errMsg, err)
	}

	text := event.Text

	// Если это URL — отправляем в Kafka, не сохраняем напрямую
	if isURL(text) {
		page := &storage.Page{
			URL:      text,
			UserName: meta.Username,
			ChatID:   meta.ChatID,
		}
		if p.producer != nil {
			if err := p.producer.Produce(page, PagesTopic); err != nil {
				log.Printf("[KafkaProducer] can't produce message: %v", err)
			} else {
				log.Printf("[KafkaProducer] sent message: %+v", *page)
			}
		}
		return p.tg.SendMessages(meta.ChatID, "👌 Ссылка получена, сохраняем...")
	}

	// Иначе это команда — обрабатываем как раньше
	return p.DoCmd(ctx, text, meta.ChatID, meta.Username)
}

func meta(event events.Event) (Meta, error) {
	const errMsg = "can't get metadata"
	res, ok := event.Meta.(Meta)
	if !ok {
		return Meta{}, e.Wrap(errMsg, ErrUnknownMetaType)
	}

	return res, nil
}

func event(upd telegram.Update) events.Event {
	updType := fetchType(upd)

	res := events.Event{
		Type: updType,
		Text: fetchText(upd),
	}

	if updType == events.Message {
		res.Meta = Meta{
			ChatID:   upd.Message.Chat.ID,
			Username: upd.Message.From.Username,
		}
	}

	return res
}

func fetchText(upd telegram.Update) string {
	if upd.Message == nil {
		return ""
	}
	return upd.Message.Text
}

func fetchType(upd telegram.Update) events.Type {
	if upd.Message == nil {
		return events.Unknown
	}

	return events.Message
}
