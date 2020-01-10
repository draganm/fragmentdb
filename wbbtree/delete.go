package wbbtree

import (
	"bytes"

	"github.com/draganm/fragmentdb/fragment"
	"github.com/draganm/fragmentdb/store"
	"github.com/pkg/errors"
)

func Delete(s fragment.Store, root store.Key, key []byte) (store.Key, error) {
	nr, err := delete(s, root, key)
	if err != nil {
		return store.NilKey, err
	}
	return balance(s, nr)
}

func delete(s fragment.Store, root store.Key, key []byte) (store.Key, error) {
	if root == store.NilKey {
		return store.NilKey, store.ErrNotFound
	}

	nr := newNodeReader(s, root)

	cmp := bytes.Compare(key, nr.key())

	if cmp == 0 {
		if nr.leftChild() == store.NilKey && nr.rightChild() == store.NilKey {
			return store.NilKey, nr.err()
		}
	}

	switch cmp {
	case 0:
		if nr.leftChild() == store.NilKey {
			return nr.rightChild(), nr.err()
		}

		if nr.rightChild() == store.NilKey {
			return nr.leftChild(), nr.err()
		}

		succ, err := findSuccessor(s, nr.rightChild())
		if err != nil {
			return store.NilKey, errors.Wrap(err, "while finding successor")
		}

		succRe := newNodeReader(s, succ)

		newRight, err := Delete(s, nr.rightChild(), succRe.key())

		if err != nil {
			return store.NilKey, errors.Wrap(err, "while deleting successor")
		}

		nc, err := Count(s, newRight)
		if err != nil {
			return store.NilKey, err
		}

		return s.Create(func(f fragment.Fragment) error {
			nm := newNodeModifier(f)
			nm.setLeftChild(nr.leftChild())
			nm.setLeftCount(nr.leftCount())
			nm.setRightChild(newRight)
			nm.setRightCount(nc)
			nm.setKey(succRe.key())
			nm.setValue(succRe.value())

			if nr.err() != nil {
				return nr.err()
			}

			if succRe.err() != nil {
				return succRe.err()
			}
			return nm.err()
		})

	case -1:
		newLeft, err := Delete(s, nr.leftChild(), key)
		if err != nil {
			return store.NilKey, err
		}

		return s.Create(func(f fragment.Fragment) error {
			nm := newNodeModifier(f)
			nm.setRightChild(nr.rightChild())
			nm.setRightCount(nr.rightCount())
			nm.setKey(nr.key())
			nm.setValue(nr.value())

			nm.setLeftChild(newLeft)
			nc, err := Count(s, newLeft)
			if err != nil {
				return err
			}

			nm.setLeftCount(nc)

			if nr.err() != nil {
				return nr.err()
			}

			return nm.err()
		})

	case 1:
		newRight, err := Delete(s, nr.rightChild(), key)
		if err != nil {
			return store.NilKey, err
		}

		return s.Create(func(f fragment.Fragment) error {
			nm := newNodeModifier(f)
			nm.setLeftChild(nr.leftChild())
			nm.setLeftCount(nr.leftCount())
			nm.setKey(nr.key())
			nm.setValue(nr.value())

			nm.setRightChild(newRight)

			nc, err := Count(s, newRight)
			if err != nil {
				return err
			}

			nm.setRightCount(nc)

			if nr.err() != nil {
				return nr.err()
			}

			return nm.err()
		})

	default:
		return store.NilKey, errors.New("should never be reached")
	}

}

func findSuccessor(s fragment.Store, k store.Key) (store.Key, error) {
	nr := newNodeReader(s, k)
	lc := nr.leftChild()
	if lc == store.NilKey {
		return k, nr.err()
	}

	if nr.err() != nil {
		return store.NilKey, nr.err()
	}

	return findSuccessor(s, lc)
}
