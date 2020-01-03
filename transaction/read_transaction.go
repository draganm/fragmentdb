package transaction

import (
	"github.com/draganm/fragmentdb/fragment"
	"github.com/draganm/fragmentdb/store"
)

func NewReadTransaction(s store.Backend, root store.Key, discard store.Discard) *ReadTransaction {
	return &ReadTransaction{
		newRoot: root,
		store:   fragment.NewStore(s),
		discard: discard,
	}
}

type ReadTransaction struct {
	newRoot store.Key
	store   fragment.Store

	discard store.Discard
}

func (t *ReadTransaction) Discard() {
	t.discard()
}
