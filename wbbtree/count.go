package wbbtree

import (
	"github.com/draganm/fragmentdb/fragment"
	"github.com/draganm/fragmentdb/store"
)

func Count(f fragment.Store, root store.Key) (uint64, error) {
	if root == store.NilKey {
		return 0, nil
	}

	nr := newNodeReader(f, root)
	if nr.isEmpty() {
		return 0, nr.err()
	}

	return nr.leftCount() + nr.rightCount() + 1, nr.err()
}
