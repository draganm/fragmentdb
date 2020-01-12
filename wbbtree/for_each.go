package wbbtree

import (
	"github.com/draganm/fragmentdb/fragment"
	"github.com/draganm/fragmentdb/store"
)

func ForEach(s fragment.Store, root store.Key, f func([]byte, store.Key) error) error {

	if root == store.NilKey {
		return nil
	}

	nr := newNodeReader(s, root)
	if nr.isEmpty() {
		return nr.err()
	}

	lc := nr.leftChild()
	rc := nr.rightChild()
	k := nr.key()
	v := nr.value()

	if nr.err() != nil {
		return nr.err()
	}

	err := ForEach(s, lc, f)
	if err != nil {
		return err
	}

	err = f(k, v)
	if err != nil {
		return err
	}

	return ForEach(s, rc, f)

}
