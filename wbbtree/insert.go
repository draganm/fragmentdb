package wbbtree

import (
	"bytes"
	"github.com/draganm/fragmentdb/fragment"
	"github.com/draganm/fragmentdb/store"
	"github.com/pkg/errors"
)

func Insert(s fragment.Store, root store.Key, key []byte, value store.Key) (store.Key, error) {
	if root == store.NilKey {
		return s.Create(func(f fragment.Fragment) error {
			nm := newNodeModifier(f)
			nm.setKey(key)
			nm.setValue(value)
			return nm.err()
		})
	}

	nr := newNodeReader(s, root)

	cmp := bytes.Compare(key, nr.key())

	if cmp == 0 {
		return s.Create(func(f fragment.Fragment) error {
			nm := newNodeModifier(f)
			nm.setLeftChild(nr.leftChild())
			nm.setRightChild(nr.rightChild())
			nm.setLeftCount(nr.leftCount())
			nm.setRightCount(nr.rightCount())
			nm.setKey(nr.key())

			nm.setValue(value)

			if nr.err() != nil {
				return nr.err()
			}

			return nm.err()
		})
	}

	return store.NilKey, errors.New("not yet implemented")
}
