package wbbtree

import (
	"bytes"
	"github.com/draganm/fragmentdb/fragment"
	"github.com/draganm/fragmentdb/store"
	"github.com/pkg/errors"

	serrors "errors"
)

var ErrNotFound = serrors.New("Not found")

func Search(s fragment.Store, root store.Key, key []byte) (store.Key, error) {
	if root == store.NilKey {
		return store.NilKey, ErrNotFound
	}

	f, err := s.Get(root)
	if err != nil {
		return store.NilKey, errors.Wrap(err, "while getting wbbtree fragment")
	}

	if f.Specific().Which() != fragment.Fragment_specific_Which_wbbtreeNode {
		return store.NilKey, errors.Errorf("Wrong type of fragment: %s", f.Specific().Which())
	}

	n, err := f.Specific().WbbtreeNode()
	if err != nil {
		return store.NilKey, errors.Wrap(err, "while getting wbbtree node data")
	}

	k, err := n.Key()
	if err != nil {
		return store.NilKey, errors.Wrap(err, "while getting wbbtree node key")
	}

	cmp := bytes.Compare(key, k)

	ch, err := f.Children()
	if err != nil {
		return store.NilKey, errors.Wrap(err, "while getting wbbtree fragment children")
	}

	if ch.Len() != 3 {
		return store.NilKey, errors.Wrapf(err, "Expected wbbtree fragment to have 3 children, but got %d", ch.Len())
	}

	if cmp == 0 {
		valueKeyBytes, err := ch.At(2)
		if err != nil {
			return store.NilKey, errors.Wrap(err, "while getting calues key bytes")
		}
		return store.BytesToKey(valueKeyBytes), nil
	}

	return store.NilKey, errors.New("not yet implemented")
}
