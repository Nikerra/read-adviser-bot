package sqllite

import (
	"context"
	"database/sql"
	"errors"
	_ "github.com/mattn/go-sqlite3"
	"read-adviser-bot/lib/e"
	"read-adviser-bot/storage"
)

type Storage struct {
	db *sql.DB
}

func New(path string) (*Storage, error) {
	const errMsgOpen = "sqllite: can't open to database"
	const errMsgConn = "sqllite: can't connect to database"
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, e.Wrap(errMsgOpen, err)
	}

	if err := db.Ping(); err != nil {
		return nil, e.Wrap(errMsgConn, err)
	}

	return &Storage{db: db}, nil
}

func (s *Storage) Save(ctx context.Context, p *storage.Page) error {
	const errMsg = "can't save page"
	query := `INSERT INTO pages(url, user_name) VALUES (?, ?)`

	_, err := s.db.ExecContext(ctx, query, p.URL, p.UserName)
	if err != nil {
		return e.Wrap(errMsg, err)
	}

	return nil
}

func (s *Storage) PickRandom(ctx context.Context, userName string) (*storage.Page, error) {
	const errMsg = "can't pick random page"
	query := `SELECT url FROM pages WHERE user_name = ? ORDER BY RANDOM() LIMIT 1`

	var url string
	err := s.db.QueryRowContext(ctx, query, userName).Scan(&url)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, e.Wrap(errMsg, err)
	}

	return &storage.Page{
		URL:      url,
		UserName: userName,
	}, nil
}

func (s *Storage) Remove(ctx context.Context, p *storage.Page) error {
	const errMsg = "can't remove page"
	query := `DELETE FROM pages WHERE url = ? AND user_name = ?`
	_, err := s.db.ExecContext(ctx, query, p.URL, p.UserName)
	if err != nil {
		return e.Wrap(errMsg, err)
	}

	return nil
}

func (s *Storage) IsExists(ctx context.Context, p *storage.Page) (bool, error) {
	const errMsg = "can't check if page"
	query := `SELECT COUNT(*) FROM pages WHERE url = ? AND user_name = ?`

	var count int
	err := s.db.QueryRowContext(ctx, query, p.URL, p.UserName).Scan(&count)
	if err != nil {
		return false, e.Wrap(errMsg, err)
	}

	return count > 0, nil
}

func (s *Storage) Init(ctx context.Context) error {
	const errMsg = "can't init storage"
	query := `CREATE TABLE IF NOT EXISTS pages (url TEXT, user_name TEXT)`

	_, err := s.db.ExecContext(ctx, query)
	if err != nil {
		return e.Wrap(errMsg, err)
	}

	return nil
}
