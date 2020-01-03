package badger

import (
	badger "github.com/dgraph-io/badger"
	"github.com/pkg/errors"
	"github.com/draganm/fragmentdb/store"
)

type BadgerStore struct {
	db *badger.DB
}

func New(opt badger.Options) (*BadgerStore, error) {
	db, err := badger.Open(opt)
	if err != nil {
		return nil, errors.Wrap(err, "while opening local badger")
	}

	return &BadgerStore{db}, nil

}

func (b *BadgerStore) NewReadWriteBackend() (store.Backend, store.Commit, store.Discard) {
	tx := b.db.NewTransaction(true)
	return txnstore{tx}, tx.Commit, tx.Discard
}

func (b *BadgerStore) NewReadOnlyBackend() (store.Backend, store.Discard) {
	tx := b.db.NewTransaction(false)
	return txnstore{tx}, tx.Discard
}

func (b *BadgerStore) Close() error {
	return b.db.Close()
}

type txnstore struct {
	btxn *badger.Txn
}

func (s txnstore) Get(k store.Key) ([]byte, error) {
	it, err := s.btxn.Get(k.Bytes())
	if err == badger.ErrKeyNotFound {
		return nil, store.ErrNotFound
	}
	return it.ValueCopy(nil)
}

func (s txnstore) Put(k store.Key, data []byte) error {
	return s.btxn.Set(k.Bytes(), data)
}

func (s txnstore) Delete(k store.Key) error {
	err := s.btxn.Delete(k.Bytes())
	if err == badger.ErrKeyNotFound {
		return store.ErrNotFound
	}
	return err
}

func (s txnstore) DeleteAll() error {
	it := s.btxn.NewIterator(badger.IteratorOptions{
		PrefetchSize: 100,
	})

	for ; it.Valid(); it.Next() {
		key := make([]byte, len(it.Item().Key()))
		copy(key, it.Item().Key())
		err := s.btxn.Delete(key)
		if err != nil {
			return errors.Wrap(err, "while deleting all keys")
		}
	}

	it.Close()

	return nil

}
