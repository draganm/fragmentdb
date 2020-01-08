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
		return nr.value(), nr.err()
	}

	switch cmp {
	case 0:
		return nr.value(), nr.err()
	case -1:
		return Search(s, nr.leftChild(), key)
	case 1:
		return Search(s, nr.rightChild(), key)
	default:
		return store.NilKey, errors.New("should never be reached")
	}

}
