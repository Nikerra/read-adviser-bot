package telegram

import (
	"context"
	"errors"
	"log"
	"net/url"
	"read-adviser-bot/clients/telegram"
	"read-adviser-bot/lib/e"
	"read-adviser-bot/storage"
	"strings"
)

const (
	RndCmd   = "/rnd"
	HelpCmd  = "/help"
	StartCmd = "/start"
)

func (p *Processor) doCmd(ctx context.Context, text string, chatID int, username string) error {
	text = strings.TrimSpace(text)

	log.Printf("got new command'%s' from '%s'", text, username)

	if isAddCmd(text) {
		return p.savePage(ctx, text, chatID, username)
	}

	switch text {
	case RndCmd:
		return p.sendRandom(ctx, chatID, username)
	case HelpCmd:
		return p.sendHelp(chatID)
	case StartCmd:
		return p.sendHello(chatID)
	default:
		return p.tg.SendMessages(chatID, msgUnknownCommand)
	}
}

func (p *Processor) savePage(ctx context.Context, pageURL string, chatID int, username string) (err error) {
	const errMsg = "can't do command: save page"
	defer func() { err = e.WrapIfErr(errMsg, err) }()

	sendMsg := newMessageSender(chatID, p.tg) //замыкание
	page := &storage.Page{
		URL:      pageURL,
		UserName: username,
	}

	isExists, err := p.storage.IsExists(ctx, page)
	if err != nil {
		return err
	}
	if isExists {
		return sendMsg(msgAlreadyExists) //замыкание
	}

	if err := p.storage.Save(ctx, page); err != nil {
		return err
	}

	if err := p.tg.SendMessages(chatID, msgSaved); err != nil {
		return err
	}

	return nil
}

func (p *Processor) sendRandom(ctx context.Context, chatID int, username string) (err error) {
	const errMsg = "can't do command: send random"
	defer func() { err = e.WrapIfErr(errMsg, err) }()

	page, err := p.storage.PickRandom(ctx, username)
	if err != nil && !errors.Is(err, storage.ErrNoSavedPages) {
		return err
	}
	if errors.Is(err, storage.ErrNoSavedPages) {
		return p.tg.SendMessages(chatID, msgAlreadyExists)
	}

	if err := p.tg.SendMessages(chatID, page.URL); err != nil {
		return err
	}

	return p.storage.Remove(ctx, page)
}

func (p *Processor) sendHelp(chatID int) error {
	return p.tg.SendMessages(chatID, msgHelp)
}
func (p *Processor) sendHello(chatID int) error {
	return p.tg.SendMessages(chatID, msgHello)
}

func newMessageSender(chatID int, tg *telegram.Client) func(string) error {
	return func(msg string) error {
		return tg.SendMessages(chatID, msg)
	}
}

func isAddCmd(text string) bool {
	return isURL(text)
}

func isURL(text string) bool {
	u, err := url.Parse(text)
	return err == nil && u.Host != ""
}
