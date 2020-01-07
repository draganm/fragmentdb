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

	nr := newNodeReader(s, root)

	cmp := bytes.Compare(key, nr.key())

	if cmp == 0 {
		return nr.valueKey(), nr.err()
	}

	return store.NilKey, errors.New("not yet implemented")
}
