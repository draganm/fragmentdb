package store

import (
	"github.com/pkg/errors"
)

type LayeredBackend []Backend

func (ls LayeredBackend) Get(k Key) ([]byte, error) {
	for i := len(ls) - 1; i >= 0; i-- {
		v, err := ls[i].Get(k)
		if err == ErrNotFound {
			continue
		}
		return v, err
	}

	return nil, ErrNotFound
}

func (ls LayeredBackend) Put(k Key, data []byte) error {
	return ls[len(ls)-1].Put(k, data)
}

func (ls LayeredBackend) Delete(k Key) error {
	return errors.New("delete is no supported by layered store backend")
}

func (ls LayeredBackend) DeleteAll() error {
	return errors.New("deleteall is no supported by layered store backend")
}
