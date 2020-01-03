package transaction_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/draganm/fragmentdb/transaction"
)

func TestTypeOf(t *testing.T) {

	t.Run("when path does not exist", func(t *testing.T) {
		tx := newTestTransaction(t)
		_, err := tx.TypeOf("foo")
		t.Run("then the typeof should return ErrNotFound error", func(t *testing.T) {
			require.Equal(t, transaction.ErrNotExists, err)
		})
	})

	t.Run("when path has one level and exists", func(t *testing.T) {
		tx := newTestTransaction(t)
		err := tx.Put("foo", []byte{1, 2, 3})
		require.NoError(t, err)

		t.Run("then the typeof should return Type Data", func(t *testing.T) {
			tp, err := tx.TypeOf("foo")
			require.NoError(t, err)
			require.Equal(t, transaction.TypeData, tp)
		})
	})

	t.Run("when path has two levels and exists", func(t *testing.T) {
		tx := newTestTransaction(t)
		err := tx.CreateMap("foo")
		require.NoError(t, err)
		err = tx.Put("foo/bar", []byte{1, 2, 3})
		require.NoError(t, err)

		t.Run("then the typeof should return Type Data", func(t *testing.T) {
			tp, err := tx.TypeOf("foo")
			require.NoError(t, err)
			require.Equal(t, transaction.TypeMap, tp)
			tp, err = tx.TypeOf("foo/bar")
			require.NoError(t, err)
			require.Equal(t, transaction.TypeData, tp)
		})
	})

}
