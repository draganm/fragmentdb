package wbbtree

import (
	"github.com/draganm/fragmentdb/fragment"
	"github.com/draganm/fragmentdb/store"
)

const weight = 4

func balance(s fragment.Store, k store.Key) (store.Key, error) {
	if k == store.NilKey {
		return k, nil

	}
	nr := newNodeReader(s, k)
	ln := nr.leftCount()
	rn := nr.rightCount()

	if nr.err() != nil {
		return store.NilKey, nr.err()
	}

	if ln+rn <= 2 {
		return k, nil
	}

	if rn > weight*ln { // right is too big
		rnnr := newNodeReader(s, nr.rightChild())
		rln := rnnr.leftCount()
		rrn := rnnr.rightCount()

		if rnnr.err() != nil {
			return store.NilKey, rnnr.err()
		}

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

		if lnnr.err() != nil {
			return store.NilKey, lnnr.err()
		}

		if lrn < lln {
			return singleRight(s, k)
		} else {
			return doubleRight(s, k)
		}
	}

	return k, nil

}

func IsBalanced(s fragment.Store, root store.Key) (bool, error) {
	if root == store.NilKey {
		return true, nil
	}

	nr := newNodeReader(s, root)

	lcnt := nr.leftCount()
	rcnt := nr.rightCount()

	if nr.err() != nil {
		return false, nr.err()
	}

	if lcnt > weight*rcnt {
		return false, nil
	}

	lc := nr.leftChild()
	if nr.err() != nil {
		return false, nr.err()
	}

	bal, err := IsBalanced(s, lc)
	if err != nil {
		return false, err
	}

	if !bal {
		return false, err
	}

	rc := nr.rightChild()

	if nr.err() != nil {
		return false, nr.err()
	}

	return IsBalanced(s, rc)
}
