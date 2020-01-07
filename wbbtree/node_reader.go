package wbbtree

import (
	"github.com/draganm/fragmentdb/fragment"
	"github.com/draganm/fragmentdb/store"
	"github.com/pkg/errors"
)

type nodeReader struct {
	f fragment.Fragment
	e error
}

func newNodeReader(st fragment.Store, k store.Key) *nodeReader {

	nr := &nodeReader{fragment.Fragment{}, nil}

	f, err := st.Get(k)
	if err != nil {
		nr.setError(errors.Wrapf(err, "while getting fragment with key %s", k.String()))
		return nr
	}

	nr.f = f

	if f.Specific().Which() != fragment.Fragment_specific_Which_wbbtreeNode {
		nr.setError(errors.Errorf("Wrong type of fragment: %s", f.Specific().Which()))
		return nr
	}

	ch, err := f.Children()
	if err != nil {
		nr.setError(errors.Wrap(err, "while getting wbbtree fragment children"))
		return nr
	}

	if ch.Len() != 3 {
		nr.setError(errors.Wrapf(err, "Expected wbbtree fragment to have 3 children, but got %d", ch.Len()))
		return nr
	}

	return nr
}

func (n *nodeReader) err() error {
	return n.e
}

func (n *nodeReader) setError(err error) {
	if n.e == nil {
		n.e = err
	}
}

func (n *nodeReader) leftNodeKey() store.Key {
	if n.e != nil {
		return store.NilKey
	}

	ch, err := n.f.Children()
	if err != nil {
		n.setError(errors.Wrap(err, "while getting wbbtree fragment children"))
		return store.NilKey
	}

	lnkb, err := ch.At(0)
	if err != nil {
		n.setError(errors.Wrap(err, "while getting left child key bytes"))
		return store.NilKey
	}

	return store.BytesToKey(lnkb)
}

func (n *nodeReader) rightNodeKey() store.Key {
	if n.e != nil {
		return store.NilKey
	}

	ch, err := n.f.Children()
	if err != nil {
		n.setError(errors.Wrap(err, "while getting wbbtree fragment children"))
		return store.NilKey
	}

	rnkb, err := ch.At(1)
	if err != nil {
		n.setError(errors.Wrap(err, "while getting right child key bytes"))
		return store.NilKey
	}

	return store.BytesToKey(rnkb)
}

func (n *nodeReader) valueKey() store.Key {
	if n.e != nil {
		return store.NilKey
	}

	ch, err := n.f.Children()
	if err != nil {
		n.setError(errors.Wrap(err, "while getting wbbtree fragment children"))
		return store.NilKey
	}

	vkb, err := ch.At(1)
	if err != nil {
		n.setError(errors.Wrap(err, "while getting value key bytes"))
		return store.NilKey
	}

	return store.BytesToKey(vkb)
}

func (n *nodeReader) key() []byte {
	if n.e != nil {
		return nil
	}

	tn, err := n.f.Specific().WbbtreeNode()
	if err != nil {
		n.setError(errors.Wrap(err, "while getting wbbtree node data"))
		return nil
	}

	k, err := tn.Key()
	if err != nil {
		n.setError(errors.Wrap(err, "while getting wbbtree node key"))
		return nil
	}

	return k
}

func (n *nodeReader) leftCount() uint64 {
	if n.e != nil {
		return 0
	}

	tn, err := n.f.Specific().WbbtreeNode()
	if err != nil {
		n.setError(errors.Wrap(err, "while getting wbbtree node data"))
		return 0
	}

	return tn.CountLeft()
}

func (n *nodeReader) rightCount() uint64 {
	if n.e != nil {
		return 0
	}

	tn, err := n.f.Specific().WbbtreeNode()
	if err != nil {
		n.setError(errors.Wrap(err, "while getting wbbtree node data"))
		return 0
	}

	return tn.CountLeft()
}
