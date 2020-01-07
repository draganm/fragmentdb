package wbbtree_test

import (
	"testing"

	"github.com/draganm/fragmentdb/data"
	"github.com/draganm/fragmentdb/fragment"
	"github.com/draganm/fragmentdb/store"
	"github.com/draganm/fragmentdb/wbbtree"
	"github.com/stretchr/testify/require"
)

func TestInsert(t *testing.T) {
	t.Run("when inserting element in an empty tree", func(t *testing.T) {
		st := fragment.NewStore(store.NewMemoryBackendFactory())
		valueKey, err := data.StoreData(st, []byte{1}, 8129, 4)
		require.NoError(t, err)

		nr, err := wbbtree.Insert(st, store.NilKey, []byte{1, 2, 3}, valueKey)
		require.NoError(t, err)

		t.Run("it should contain the value", func(t *testing.T) {
			vk, err := wbbtree.Search(st, nr, []byte{1, 2, 3})
			require.NoError(t, err)
			require.Equal(t, valueKey, vk)
		})

		t.Run("it should have count of 1", func(t *testing.T) {
			cnt, err := wbbtree.Count(st, nr)
			require.NoError(t, err)
			require.Equal(t, uint64(1), cnt)
		})

		t.Run("when I delete the element", func(t *testing.T) {
			nr, err = wbbtree.Delete(st, nr, []byte{1, 2, 3})
			require.NoError(t, err)
			require.Equal(t, store.NilKey, nr)
		})

		t.Run("it should have count of 0", func(t *testing.T) {
			cnt, err := wbbtree.Count(st, nr)
			require.NoError(t, err)
			require.Equal(t, uint64(0), cnt)
		})

	})
}
