package data

import (
	"github.com/pkg/errors"
	"github.com/draganm/fragmentdb/fragment"
	"github.com/draganm/fragmentdb/store"
)

func StoreData(st fragment.Store, data []byte, fragSize, fanout int) (store.Key, error) {
	w := NewDataWriter(st, fragSize, fanout)
	_, err := w.Write(data)
	if err != nil {
		return store.NilKey, errors.Wrap(err, "while writing to data writer")
	}

	return w.Finish()

}
