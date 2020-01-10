package wbbtree

import (
	"github.com/draganm/fragmentdb/fragment"
	"github.com/draganm/fragmentdb/store"
)

const weight = 4

func balance(s fragment.Store, k store.Key) (store.Key, error) {
	nr := newNodeReader(s, k)
	ln := nr.leftCount()
	rn := nr.rightCount()

	if ln+rn <= 2 {
		return k, nil
	}

	if rn > weight*ln { // right is too big
		rnnr := newNodeReader(s, nr.rightChild())
		rln := rnnr.leftCount()
		rrn := rnnr.rightCount()
		if rln < rrn {
			return singleLeft(s, k)
		} else {
			return doubleLeft(s, k)
		}
	}

	if ln > weight*rn { // left is too big
		lnnr := newNodeReader(s, nr.leftChild())
		lln := lnnr.leftCount()
		lrn := lnnr.rightCount()

		if lrn < lln {
			return singleRight(s, k)
		} else {
			return doubleRight(s, k)
		}
	}

	return k, nil

}
