package transaction

import (
	"github.com/draganm/fragmentdb/store"
)

type CommitReceiver func(
	newFragments store.Backend,
	oldRoot, newRoot store.Key,
) error

func (t *Transaction) Commit() error {
	return t.commitReceiver(t.txStoreBackend, t.root, t.newRoot)
}
