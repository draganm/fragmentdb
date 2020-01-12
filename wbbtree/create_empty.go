package wbbtree

import (
	"github.com/draganm/fragmentdb/fragment"
	"github.com/draganm/fragmentdb/store"
)

func CreateEmpty(s fragment.Store) (store.Key, error) {
	return s.Create(func(f fragment.Fragment) error {
		nm := newNodeModifier(f)
		return nm.err()
	})
}
