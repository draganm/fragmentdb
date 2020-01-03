package trie

import (
	"bytes"

	"github.com/pkg/errors"
	"github.com/draganm/fragmentdb/fragment"
	"github.com/draganm/fragmentdb/store"
)

func Delete(s fragment.Store, root store.Key, key []byte) (store.Key, error) {
	nr, err := delete(s, root, key)
	if err != nil {
		return store.NilKey, err
	}

	if nr == store.NilKey {
		return CreateEmpty(s)
	}

	return nr, nil
}

func delete(s fragment.Store, root store.Key, key []byte) (store.Key, error) {
	f, err := s.Get(root)
	if err != nil {
		return store.NilKey, errors.Wrap(err, "while getting trie fragment")
	}

	tm := NewTrieModifier(f)
	prefix := tm.GetPrefix()

	if bytes.Equal(prefix, key) {
		noChildren := true
		for i := 0; i < 256; i++ {
			noChildren = noChildren && tm.GetChild(i) == store.NilKey
		}

		if tm.Error() != nil {
			return store.NilKey, tm.Error()
		}

		if noChildren {
			return store.NilKey, nil
		}

		return createTrieNodeCopy(s, f, func(f TrieModifier) error {
			f.SetChild(256, store.NilKey)
			return f.Error()
		})
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

	return createTrieNodeCopy(s, f, func(f TrieModifier) error {
		nck, err := delete(s, chKey, kr[1:])
		if err != nil {
			f.SetError(err)
		}
		f.SetChild(idx, nck)
		return f.Error()
	})

}
