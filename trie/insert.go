package trie

import (
	"github.com/pkg/errors"
	"github.com/draganm/fragmentdb/fragment"
	"github.com/draganm/fragmentdb/store"
	capnp "zombiezen.com/go/capnproto2"
)

func CreateEmpty(s fragment.Store) (store.Key, error) {
	return s.Create(func(f fragment.Fragment) error {
		dl, err := capnp.NewDataList(f.Segment(), 257)
		if err != nil {
			return errors.Wrap(err, "while creating new trie children list")
		}

		err = f.SetChildren(dl)
		if err != nil {
			return err
		}

		err = f.Specific().SetTrieNode(nil)
		if err != nil {
			return err
		}

		return nil

	})
}

func isEmpty(f fragment.Fragment) (bool, error) {
	children, err := f.Children()
	if err != nil {
		return false, errors.Wrap(err, "while getting children of trie fragment")
	}

	for i := 0; i < children.Len(); i++ {
		ch, err := children.At(i)
		if err != nil {
			return false, errors.Wrapf(err, "while getting child %d of a trie fragment", i)
		}

		if len(ch) > 0 {
			return false, nil
		}
	}

	prefix, err := f.Specific().TrieNode()
	if err != nil {
		return false, errors.Wrap(err, "while getting trie node prefix")
	}

	return len(prefix) == 0, nil
}

func copyDataList(l capnp.DataList, s *capnp.Segment) (capnp.DataList, error) {
	nl, err := capnp.NewDataList(s, int32(l.Len()))
	if err != nil {
		return capnp.DataList{}, errors.Wrap(err, "while creating new data list")
	}
	for i := 0; i < nl.Len(); i++ {
		d, err := l.At(i)
		if err != nil {
			return capnp.DataList{}, errors.Wrapf(err, "while getting datalist element %d", i)
		}

		err = nl.Set(i, d)

		if err != nil {
			return capnp.DataList{}, errors.Wrapf(err, "while setting datalist element %d", i)
		}

	}

	return nl, nil
}

func createTrieNode(s fragment.Store, fn func(f TrieModifier) error) (store.Key, error) {
	return s.Create(func(f fragment.Fragment) error {

		dl, err := capnp.NewDataList(f.Segment(), 257)
		if err != nil {
			return errors.Wrap(err, "while creating new trie children list")
		}

		err = f.SetChildren(dl)
		if err != nil {
			return err
		}

		err = f.Specific().SetTrieNode(nil)
		if err != nil {
			return err
		}

		err = fn(TrieModifier{fragment.Modifier{Fragment: f}})

		if err != nil {
			return errors.Wrap(err, "while updating trie node")
		}

		return nil
	})
}

func createTrieNodeCopy(s fragment.Store, original fragment.Fragment, fn func(f TrieModifier) error) (store.Key, error) {
	return s.Create(func(f fragment.Fragment) error {
		ch, err := original.Children()
		if err != nil {
			return errors.Wrap(err, "while getting children from the original fragment")
		}

		nch, err := copyDataList(ch, f.Segment())
		if err != nil {
			return errors.Wrap(err, "while copying children list")
		}

		err = f.SetChildren(nch)
		if err != nil {
			return errors.Wrap(err, "setting children list")
		}

		prefix, err := original.Specific().TrieNode()

		if err != nil {
			return errors.Wrap(err, "while getting prefix from the original trie node")
		}

		err = f.Specific().SetTrieNode(prefix)

		if err != nil {
			return errors.Wrap(err, "while setting trie node prefix")
		}

		err = fn(TrieModifier{fragment.Modifier{Fragment: f}})

		if err != nil {
			return errors.Wrap(err, "while updating trie node copy")
		}

		return nil
	})
}

func Insert(s fragment.Store, root store.Key, key []byte, value store.Key) (store.Key, error) {

	rf, err := s.Get(root)
	if err != nil {
		return store.NilKey, errors.Wrap(err, "while getting root fragment of trie")
	}

	em, err := isEmpty(rf)
	if err != nil {
		return store.NilKey, errors.Wrap(err, "while checking for emtpy trie node")
	}

	if em {
		return createTrieNodeCopy(s, rf, func(f TrieModifier) error {
			f.SetPrefix(key)
			f.SetChild(256, value)
			return f.Error()
		})
	}

	triePrefix, err := rf.Specific().TrieNode()
	if err != nil {
		return store.NilKey, errors.Wrap(err, "while getting trie node prefix")
	}

	cp, kp, pp := commonPrefix(key, triePrefix)

	if len(cp) == len(key) && len(pp) == 0 && len(kp) == 0 {
		return createTrieNodeCopy(s, rf, func(f TrieModifier) error {
			f.SetChild(256, value)
			f.SetPrefix(key)
			return f.Error()
		})
	}

	if len(pp) > 0 {
		return createTrieNode(s, func(f TrieModifier) error {

			f.SetPrefix(cp)

			if len(kp) > 0 {
				idx := int(kp[0])
				chKey, err := createTrieNode(s, func(f TrieModifier) error {
					f.SetPrefix(kp[1:])
					f.SetChild(256, value)
					return f.Error()
				})

				f.SetError(err)
				f.SetChild(idx, chKey)
				f.SetChild(256, store.NilKey)
			} else {
				f.SetChild(256, value)
			}

			idx := int(pp[0])
			chKey, err := createTrieNodeCopy(s, rf, func(f TrieModifier) error {
				f.SetPrefix(pp[1:])
				return f.Error()
			})

			f.SetError(err)

			f.SetChild(idx, chKey)

			return f.Error()
		})
	}

	if len(pp) == 0 && len(kp) > 0 {
		return createTrieNodeCopy(s, rf, func(f TrieModifier) error {
			childIndex := int(kp[0])
			chk := f.GetChild(childIndex)
			if chk == store.NilKey {
				chk, err = CreateEmpty(s)
				f.SetError(err)
			}
			nc, err := Insert(s, chk, kp[1:], value)
			f.SetError(err)
			f.SetChild(childIndex, nc)
			return f.Error()
		})
	}

	return store.NilKey, errors.New("this part of trie.Insert should never be reached")
}

func commonPrefix(p1, p2 []byte) ([]byte, []byte, []byte) {

	maxIndex := len(p1)
	if len(p2) < maxIndex {
		maxIndex = len(p2)
	}

	for i := 0; i < maxIndex; i++ {
		if p1[i] != p2[i] {
			return p1[:i], p1[i:], p2[i:]
		}
	}

	return p1[:maxIndex], p1[maxIndex:], p2[maxIndex:]
}
