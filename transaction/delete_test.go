package transaction_test

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"github.com/draganm/fragmentdb/store"
)

func TestDelete(t *testing.T) {

	t.Run("when there is an element on the first level", func(t *testing.T) {
		tx := newTestTransaction(t)
		err := tx.Put("test", []byte{1, 2, 3})
		require.NoError(t, err)

		t.Run("when I delete that element", func(t *testing.T) {
			err = tx.Delete("test")
			require.NoError(t, err)
			t.Run("it should not exist anymore", func(t *testing.T) {
				err = tx.Delete("test")
				require.Equal(t, store.ErrNotFound, errors.Cause(err))
			})
		})
	})

}
