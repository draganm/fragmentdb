package transaction_test

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExists(t *testing.T) {
	t.Run("when the path does not exist", func(t *testing.T) {
		tx := newTestTransaction(t)

		t.Run("it should return false", func(t *testing.T) {
			ex, err := tx.Exists("foo")
			require.NoError(t, err)
			require.False(t, ex)
		})
	})

	t.Run("when the path does exist", func(t *testing.T) {
		tx := newTestTransaction(t)
		err := tx.Put("foo", []byte{1, 2, 3})
		require.NoError(t, err)

		t.Run("it should return true", func(t *testing.T) {
			ex, err := tx.Exists("foo")
			require.NoError(t, err)
			require.True(t, ex)
		})

	})
}
