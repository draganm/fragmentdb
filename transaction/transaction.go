package transaction

import (
	"io/ioutil"

	"github.com/draganm/fragmentdb/data"
	"github.com/draganm/fragmentdb/dbpath"
	"github.com/draganm/fragmentdb/fragment"
	"github.com/draganm/fragmentdb/store"
	"github.com/draganm/fragmentdb/wbbtree"
	"github.com/pkg/errors"
)

func New(
	storeBackend, txStoreBackend store.Backend,
	root store.Key,
	fragSize, fanout int,
	commitReceiver CommitReceiver,
	discard store.Discard,
) *Transaction {
	return &Transaction{
		ReadTransaction: &ReadTransaction{
			store:   fragment.NewStore(store.LayeredBackend{storeBackend, txStoreBackend}),
			newRoot: root,
			discard: discard,
		},
		storeBackend:   storeBackend,
		txStoreBackend: txStoreBackend,
		root:           root,
		fragSize:       fragSize,
		fanout:         fanout,
		commitReceiver: commitReceiver,
	}
}

type Transaction struct {
	*ReadTransaction

	storeBackend   store.Backend
	txStoreBackend store.Backend
	root           store.Key

	fragSize int
	fanout   int

	commitReceiver CommitReceiver
}

func (t *Transaction) Put(path string, d []byte) error {

	vk, err := data.StoreData(t.store, d, t.fragSize, t.fanout)
	if err != nil {
		return errors.Wrap(err, "while storing data")
	}

	return t.UpdatePath(path, vk)
}

func (t *ReadTransaction) GetKey(path string) (store.Key, error) {

	pathElements, err := dbpath.Split(path)
	if err != nil {
		return store.NilKey, err
	}

	trk := t.newRoot

	for _, pe := range pathElements {
		trk, err = wbbtree.Search(t.store, trk, []byte(pe))

		if err != nil {
			if errors.Cause(err) == wbbtree.ErrNotFound {
				return store.NilKey, ErrNotExists
			}
			return store.NilKey, errors.Wrap(err, "while getting key from wbbtree")
		}
	}

	return trk, nil
}

func (t *ReadTransaction) Get(path string) ([]byte, error) {
	trk, err := t.GetKey(path)
	if err != nil {
		return nil, err
	}

	r, err := data.NewReader(trk, t.store)
	if err != nil {
		return nil, errors.Wrap(err, "while reading data")
	}

	return ioutil.ReadAll(r)

}

func (t *Transaction) CreateMap(path string) error {
	vk, err := wbbtree.CreateEmpty(t.store)
	if err != nil {
		return errors.Wrap(err, "while creating empty wbbtree")
	}

	return t.UpdatePath(path, vk)
}
