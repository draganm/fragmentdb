package fragmentdb

import (
	"github.com/pkg/errors"
	"github.com/draganm/fragmentdb/store"
	"github.com/draganm/fragmentdb/transaction"
)

func (db *DB) NewReadTransaction() (*transaction.ReadTransaction, error) {
	be, discard := db.backendFactory.NewReadOnlyBackend()
	rootBytes, err := be.Get(store.NilKey)
	if err != nil {
		return nil, errors.Wrap(err, "while getting root")
	}
	return transaction.NewReadTransaction(be, store.BytesToKey(rootBytes), discard), nil
}
