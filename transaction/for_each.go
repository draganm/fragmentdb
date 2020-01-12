package transaction

import (
	"github.com/pkg/errors"
	"github.com/draganm/fragmentdb/store"
	"github.com/draganm/fragmentdb/wbbtree"
)

var StopIteration = errors.New("stop iteration")

func (t *ReadTransaction) ForEach(path string, cb func(key string) error) error {
	tk, err := t.GetKey(path)
	if err == ErrNotExists {
		return err
	}
	if err != nil {
		return errors.Wrapf(err, "while navigating nested maps %q", path)
	}

	err = wbbtree.ForEach(t.store, tk, func(key []byte, _ store.Key) error {
		return cb(string(key))
	})

	if err == StopIteration {
		return nil
	}

	if err != nil {
		return errors.Wrapf(err, "while iterating %q", path)
	}

	return nil
}
