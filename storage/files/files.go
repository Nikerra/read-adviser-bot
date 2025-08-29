package files

import (
	"encoding/gob"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"read-adviser-bot/lib/e"
	"read-adviser-bot/storage"
	"time"
)

type Storage struct {
	basePath string
}

const defaultPerm = 0774

func New(basePath string) Storage {
	return Storage{basePath: basePath}
}

func (s Storage) Save(page *storage.Page) (err error) {
	const errMsg = "can't save page"
	defer func() { err = e.WrapIfErr(errMsg, err) }()

	fPath := filepath.Join(s.basePath, page.UserName)

	if err := os.MkdirAll(fPath, defaultPerm); err != nil {
		return err
	}

	fName, err := fileName(page)
	if err != nil {
		return err
	}

	fPath = filepath.Join(fPath, fName)

	file, err := os.Create(fPath)
	defer file.Close()

	err = gob.NewEncoder(file).Encode(page)
	if err != nil {
		return err
	}

	return nil
}

/*
1.Проверять наличие папки для записи
2.Создать папку в ходе работы
3.Перейти на полноценную БД через докер
*/
func (s Storage) PickRandom(userName string) (page *storage.Page, err error) {
	const errMsg = "can't pick random page"
	defer func() { err = e.WrapIfErr(errMsg, err) }()

	path := filepath.Join(s.basePath, userName)

	files, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}

	if len(files) == 0 {
		return nil, storage.ErrNoSavedPages
	}

	source := rand.NewSource(time.Now().UnixNano())
	rng := rand.New(source)
	n := rng.Intn(len(files))

	file := files[n]

	return s.decodePage(filepath.Join(path, file.Name()))
}

func (s Storage) Remove(p *storage.Page) error {
	const errMsg = "can't remove file"
	fileName, err := fileName(p)
	if err != nil {
		return e.Wrap(errMsg, err)
	}

	path := filepath.Join(s.basePath, p.UserName, fileName)

	if err := os.Remove(path); err != nil {
		msg := fmt.Sprint(errMsg, "%s")
		return e.Wrap(fmt.Sprintf(msg, path), err)
	}

	return nil
}
func (s Storage) IsExists(p *storage.Page) (bool, error) {
	const errMsg = "can't check if file %s exists"
	fileName, err := fileName(p)
	if err != nil {
		return false, e.Wrap(errMsg, err)
	}

	path := filepath.Join(s.basePath, p.UserName, fileName)
	switch _, err := os.Stat(path); {
	case errors.Is(err, os.ErrNotExist):
		return false, nil
	case err != nil:
		msg := fmt.Sprintf("can't check if file %s exists", path)
		return false, e.Wrap(msg, err)
	}

	return true, nil
}

func (s Storage) decodePage(fPath string) (*storage.Page, error) {
	const errMsg = "can't decode page"

	f, err := os.Open(fPath)
	if err != nil {
		return nil, e.Wrap(errMsg, err)
	}
	defer f.Close()

	var p storage.Page

	if err := gob.NewDecoder(f).Decode(&p); err != nil {
		return nil, e.Wrap(errMsg, err)
	}

	return &p, nil
}
func fileName(p *storage.Page) (string, error) {
	return p.Hash()
}
