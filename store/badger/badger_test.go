package badger_test

import (
	"io/ioutil"
	"os"
	"testing"

	bdb "github.com/dgraph-io/badger"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"github.com/draganm/fragmentdb/store"
	"github.com/draganm/fragmentdb/store/badger"
)

func createTestBadgerStore() (*badger.BadgerStore, func() error, error) {
	dir, err := ioutil.TempDir("", "badgerstore")
	if err != nil {
		return nil, nil, errors.Wrap(err, "while creating temp dir")
	}
	bs, err := badger.New(bdb.DefaultOptions(dir))
	if err != nil {
		return nil, nil, errors.Wrap(err, "while opening badger store")
	}

	return bs, func() error {
		err := bs.Close()
		if err != nil {
			return errors.Wrap(err, "while closing badger db")
		}

		return os.RemoveAll(dir)
	}, nil

}

func TestBadgerStore(t *testing.T) {
	bs, cleanup, err := createTestBadgerStore()
	require.NoError(t, err)
	defer cleanup()
	t.Run("NewReadWriteBackend", func(t *testing.T) {
		be, commit, _ := bs.NewReadWriteBackend()
		t.Run("put/get/delete/put", func(t *testing.T) {
			err = be.Put(store.NilKey, []byte{1, 2, 3})
			require.NoError(t, err)

			d, err := be.Get(store.NilKey)
			require.NoError(t, err)
			require.Equal(t, []byte{1, 2, 3}, d)

			err = be.Delete(store.NilKey)
			require.NoError(t, err)

			err = be.Put(store.NilKey, []byte{1, 2, 3, 4})
			require.NoError(t, err)

		})
		err = commit()
		require.NoError(t, err)
	})

	t.Run("NewReadOnlyBackend", func(t *testing.T) {
		be, discard := bs.NewReadOnlyBackend()

		d, err := be.Get(store.NilKey)
		require.NoError(t, err)
		require.Equal(t, []byte{1, 2, 3, 4}, d)

		t.Run("getting not existing key", func(t *testing.T) {
			_, err = be.Get(store.BytesToKey([]byte{1}))
			require.Equal(t, store.ErrNotFound, err)
		})

		discard()

	})

}
