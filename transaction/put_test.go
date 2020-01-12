package transaction_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/draganm/fragmentdb/fragment"
	"github.com/draganm/fragmentdb/store"
	"github.com/draganm/fragmentdb/transaction"
	"github.com/draganm/fragmentdb/wbbtree"
)

func newTestTransaction(t *testing.T) *transaction.Transaction {
	storeBackend := store.NewMemoryBackendFactory()
	txStoreBackend := store.NewMemoryBackendFactory()

	root, err := wbbtree.CreateEmpty(fragment.NewStore(storeBackend))
	require.NoError(t, err)

	return transaction.New(storeBackend, txStoreBackend, root, 1024, 4, nil, nil)
}

func TestPut(t *testing.T) {

	t.Run("when path has one level", func(t *testing.T) {
		path := "a"

		t.Run("and I put one byte", func(t *testing.T) {
			tx := newTestTransaction(t)
			err := tx.Put(path, []byte{1})
			require.NoError(t, err)

			t.Run("I should be able to read that byte", func(t *testing.T) {
				d, err := tx.Get(path)
				require.NoError(t, err)
				require.Equal(t, []byte{1}, d)
			})
		})

	})

	t.Run("when path has two levels", func(t *testing.T) {
		path := "a/b"

		t.Run("and I put one byte", func(t *testing.T) {
			tx := newTestTransaction(t)

			err := tx.CreateMap("a")
			require.NoError(t, err)

			err = tx.Put(path, []byte{1})
			require.NoError(t, err)

			t.Run("I should be able to read that byte", func(t *testing.T) {
				d, err := tx.Get(path)
				require.NoError(t, err)
				require.Equal(t, []byte{1}, d)
			})
		})

	})

}
