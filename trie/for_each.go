package trie

import (
	"github.com/pkg/errors"
	"github.com/draganm/fragmentdb/fragment"
	"github.com/draganm/fragmentdb/store"
)

var StopIteration = errors.New("stop iteration")

func ForEach(s fragment.Store, root store.Key, f func(key []byte, value store.Key) error) error {
	err := forEach(s, root, nil, f)
	if err == StopIteration {
		return nil
	}
	return err
}

func forEach(s fragment.Store, root store.Key, prefix []byte, f func(key []byte, value store.Key) error) error {
	fr, err := s.Get(root)
	if err != nil {
		return errors.Wrap(err, "while getting trie fragment")
	}

	tm := NewTrieModifier(fr)
	tp := tm.GetPrefix()

	vk := tm.GetChild(256)

	if tm.Error() != nil {
		return tm.Error()
	}

	key := make([]byte, len(prefix))
	copy(key, prefix)
	key = append(key, tp...)

	if vk != store.NilKey {
		err = f(key, vk)
		if err != nil {
			return err
		}
	}

	for i := 0; i < 256; i++ {
		ck := tm.GetChild(i)
		if tm.Error() != nil {
			return tm.Error()
		}

		chPrefix := append(key, byte(i))

		if ck != store.NilKey {
			err = forEach(s, ck, chPrefix, f)
			if err != nil {
				return err
			}
		}
	}

	return nil

}
