package wbbtree

import (
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

	return store.NilKey, errors.New("not yet implemented")
}
