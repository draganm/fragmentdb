package wbbtree

import (
	"bytes"

	"github.com/draganm/fragmentdb/fragment"
	"github.com/draganm/fragmentdb/store"
	"github.com/pkg/errors"
)

func Delete(s fragment.Store, root store.Key, key []byte) (store.Key, error) {
	if root == store.NilKey {
		return store.NilKey, store.ErrNotFound
	}

	nr := newNodeReader(s, root)

	cmp := bytes.Compare(key, nr.key())

	if cmp == 0 {
		if nr.leftNodeKey() == store.NilKey && nr.rightNodeKey() == store.NilKey {
			return store.NilKey, nr.err()
		}
	}

	return store.NilKey, errors.New("not yet implemented")

}
