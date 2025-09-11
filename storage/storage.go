package storage

import (
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"read-adviser-bot/lib/e"
)

type Storage interface {
	Save(ctx context.Context, p *Page) error
	PickRandom(ctx context.Context, userName string) (*Page, error)
	Remove(ctx context.Context, p *Page) error
	IsExists(ctx context.Context, p *Page) (bool, error)
}

var ErrNoSavedPages = errors.New("no saved pages")

type Page struct {
	URL      string
	UserName string
	ChatID   int
}

func (p Page) Hash() (string, error) {
	const errMsg = "can't calculate page"
	h := sha1.New()

	if _, err := io.WriteString(h, p.URL); err != nil {
		return "", e.Wrap(errMsg, err)
	}

	if _, err := io.WriteString(h, p.UserName); err != nil {
		return "", e.Wrap(errMsg, err)
	}

	return fmt.Sprintf("%x", h.Sum(nil)), nil
}
