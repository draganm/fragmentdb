package transaction

import (
	"github.com/pkg/errors"
	"github.com/draganm/fragmentdb/dbpath"
	"github.com/draganm/fragmentdb/store"
	"github.com/draganm/fragmentdb/trie"
)

type pathElement struct {
	parent store.Key
	key    []byte
}

func (t *Transaction) UpdatePath(path string, value store.Key) error {
	pathElements := []pathElement{}

	parts, err := dbpath.Split(path)
	if err != nil {
		return err
	}

	if len(parts) == 0 {
		return errors.New("modification of the root is not allowed")
	}

	ck := t.newRoot

	for i, p := range parts {
		pathElements = append(pathElements, pathElement{
			parent: ck,
			key:    []byte(p),
		})

		ck, err = trie.Get(t.store, ck, []byte(p))

		if err == trie.ErrNotFound && i == len(parts)-1 {
			ck = store.NilKey
		} else if err != nil {
			return errors.Wrapf(err, "while geting child %q of trie", p)
		}
	}

	for i := len(pathElements) - 1; i >= 0; i-- {
		pe := pathElements[i]

		if value == store.NilKey {
			value, err = trie.Delete(t.store, pe.parent, pe.key)
			if err != nil {
				return errors.Wrapf(err, "while deleting %q from a trie", string(pe.key))
			}
		} else {
			value, err = trie.Insert(t.store, pe.parent, pe.key, value)
			if err != nil {
				return errors.Wrapf(err, "while inserting %q into a trie", string(pe.key))
			}
		}
	}

	t.newRoot = value
	return nil
}
