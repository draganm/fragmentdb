package trie

import (
	"bytes"

	"github.com/pkg/errors"
	"github.com/draganm/fragmentdb/fragment"
	"github.com/draganm/fragmentdb/store"
)

var ErrNotFound = errors.New("not found")

func Get(s fragment.Store, root store.Key, key []byte) (store.Key, error) {
	f, err := s.Get(root)
	if err != nil {
		return store.NilKey, errors.Wrap(err, "while getting trie fragment")
	}

	tm := NewTrieModifier(f)
	prefix := tm.GetPrefix()

	if bytes.Equal(prefix, key) {
		v := tm.GetChild(256)

		if tm.Error() != nil {
			return store.NilKey, tm.Error()
		}

		if v == store.NilKey {
			return store.NilKey, ErrNotFound
		}

		return v, nil
	}

	_, kr, pr := commonPrefix(key, prefix)

	if len(pr) != 0 {
		return store.NilKey, ErrNotFound
	}

	idx := int(kr[0])

	chKey := tm.GetChild(idx)

	if tm.Error() != nil {
		return store.NilKey, tm.Error()
	}

	if chKey == store.NilKey {
		return store.NilKey, ErrNotFound
	}

	return Get(s, chKey, kr[1:])

}
