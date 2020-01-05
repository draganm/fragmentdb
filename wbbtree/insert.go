package wbbtree

import (
	"github.com/draganm/fragmentdb/fragment"
	"github.com/draganm/fragmentdb/store"
	"github.com/pkg/errors"
	capnp "zombiezen.com/go/capnproto2"
)

func Insert(s fragment.Store, root store.Key, key []byte, value store.Key) (store.Key, error) {
	if root == store.NilKey {
		return s.Create(func(f fragment.Fragment) error {
			wbtn, err := fragment.NewWBBTreeNode(f.Segment())
			if err != nil {
				return errors.Wrap(err, "while creating new WBBTreeNode")
			}

			wbtn.SetCountLeft(0)
			wbtn.SetCountRight(0)
			err = wbtn.SetKey(key)
			if err != nil {
				return errors.Wrap(err, "while setting key to WBBTreeNode")
			}

			err = f.Specific().SetWbbtreeNode(wbtn)
			if err != nil {
				return errors.Wrap(err, "while setting WBBTreeNode to Fragment")
			}

			dl, err := capnp.NewDataList(f.Segment(), 3)
			if err != nil {
				return errors.Wrap(err, "while creating new data list")
			}

			err = dl.Set(2, value.Bytes())
			if err != nil {
				return errors.Wrap(err, "while setting value of WBBTreeNode Fragment")
			}

			err = f.SetChildren(dl)
			if err != nil {
				return errors.Wrap(err, "while setting children of a WBBTreeNode Fragment")
			}

			return nil
		})
	}

	return store.NilKey, errors.New("not yet implemented")
}
