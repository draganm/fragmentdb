package transaction

import (
	"github.com/draganm/fragmentdb/store"
)

func (t *Transaction) Delete(path string) error {
	return t.UpdatePath(path, store.NilKey)
}
