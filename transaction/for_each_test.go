package transaction_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/draganm/fragmentdb/transaction"
)

func TestForEach(t *testing.T) {
	t.Run("when map is empty", func(t *testing.T) {
		tx := newTestTransaction(t)
		keys := []string{}
		err := tx.ForEach("", func(key string) error {
			keys = append(keys, key)
			return nil
		})

		require.NoError(t, err)
		require.Empty(t, keys)
	})

	t.Run("when map has one element", func(t *testing.T) {
		tx := newTestTransaction(t)

		err := tx.Put("a", []byte{1, 2, 3})
		require.NoError(t, err)

		keys := []string{}
		err = tx.ForEach("", func(key string) error {
			keys = append(keys, key)
			return nil
		})

		require.NoError(t, err)
		require.Equal(t, []string{"a"}, keys)
	})

	t.Run("when map has two elements", func(t *testing.T) {
		tx := newTestTransaction(t)

		err := tx.Put("a", []byte{1, 2, 3})
		require.NoError(t, err)

		err = tx.Put("b", []byte{3, 4, 5})
		require.NoError(t, err)

		keys := []string{}
		err = tx.ForEach("", func(key string) error {
			keys = append(keys, key)
			return nil
		})

		require.NoError(t, err)
		require.Equal(t, []string{"a", "b"}, keys)
	})

	t.Run("when map has three elements", func(t *testing.T) {
		tx := newTestTransaction(t)

		err := tx.Put("a", []byte{1, 2, 3})
		require.NoError(t, err)

		err = tx.Put("b", []byte{3, 4, 5})
		require.NoError(t, err)

		err = tx.Put("c", []byte{6, 7, 8})
		require.NoError(t, err)

		t.Run("it should list all the keys", func(t *testing.T) {
			keys := []string{}
			err = tx.ForEach("", func(key string) error {
				keys = append(keys, key)
				return nil
			})

			require.NoError(t, err)
			require.Equal(t, []string{"a", "b", "c"}, keys)

		})

		t.Run("when I stop the iteration after the second element", func(t *testing.T) {
			keys := []string{}
			err = tx.ForEach("", func(key string) error {
				keys = append(keys, key)
				if len(keys) ==2 {
					return transaction.StopIteration
				}
				return nil
			})

			require.NoError(t, err)
			require.Equal(t, []string{"a", "b"}, keys)

		})
	})

}
