package wbbtree_test

import (
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/draganm/fragmentdb/data"
	"github.com/draganm/fragmentdb/fragment"
	"github.com/draganm/fragmentdb/store"
	"github.com/draganm/fragmentdb/wbbtree"
	"github.com/stretchr/testify/require"
)

type kv struct {
	key   []byte
	value []byte
}

func newTreeTester() *treeTester {
	return &treeTester{
		st: fragment.NewStore(store.NewMemoryBackendFactory()),
	}
}

type treeTester struct {
	st fragment.Store
	rk store.Key
}

func (tt *treeTester) insert(t *testing.T, k, v []byte) {
	valueKey, err := data.StoreData(tt.st, v, 8129, 4)
	require.NoError(t, err)

	nr, err := wbbtree.Insert(tt.st, tt.rk, k, valueKey)
	require.NoError(t, err)
	tt.rk = nr
}

func (tt *treeTester) count(t *testing.T) uint64 {
	cnt, err := wbbtree.Count(tt.st, tt.rk)
	require.NoError(t, err)
	return cnt
}

func (tt *treeTester) delete(t *testing.T, key []byte) {
	nr, err := wbbtree.Delete(tt.st, tt.rk, key)
	require.NoError(t, err)
	tt.rk = nr
}

func (tt *treeTester) list(t *testing.T) []kv {
	kvs := []kv{}
	err := wbbtree.ForEach(tt.st, tt.rk, func(k []byte, v store.Key) error {
		r, err := data.NewReader(v, tt.st)
		if err != nil {
			return err
		}

		vd, err := ioutil.ReadAll(r)
		if err != nil {
			return err
		}

		kvs = append(kvs, kv{k, vd})
		return nil
	})

	require.NoError(t, err)

	return kvs
}

func (tt *treeTester) dump() {
	wbbtree.Dump(tt.st, tt.rk, "")
}

func (tt *treeTester) ensureBalanced(t *testing.T) {
	bal, err := wbbtree.IsBalanced(tt.st, tt.rk)
	require.NoError(t, err)
	require.True(t, bal)
}

func TestDelete(t *testing.T) {
	t.Run("when deleting from a tree with one element", func(t *testing.T) {
		tt := newTreeTester()
		tt.insert(t, []byte{5}, []byte{1, 2, 3})

		tt.delete(t, []byte{5})
		t.Run("then it should produce a tree with count 0", func(t *testing.T) {
			require.Equal(t, uint64(0), tt.count(t))
		})
	})

	t.Run("when deleting from a tree with two elements", func(t *testing.T) {
		tt := newTreeTester()
		tt.insert(t, []byte{5}, []byte{1, 2, 3})
		tt.insert(t, []byte{6}, []byte{3, 4, 5})

		tt.delete(t, []byte{5})
		t.Run("then it should produce a tree with count 1", func(t *testing.T) {
			require.Equal(t, uint64(1), tt.count(t))
		})

		t.Run("then it should retain the other value in the tree", func(t *testing.T) {
			require.Equal(t, []kv{
				kv{
					key:   []byte{0x6},
					value: []byte{0x3, 0x4, 0x5},
				},
			}, tt.list(t))
		})
	})

	t.Run("when deleting from a tree with three elements - root", func(t *testing.T) {
		tt := newTreeTester()
		tt.insert(t, []byte{5}, []byte{1, 2, 3})
		tt.insert(t, []byte{6}, []byte{3, 4, 5})
		tt.insert(t, []byte{4}, []byte{0, 1, 2})

		fmt.Println()

		tt.delete(t, []byte{5})

		t.Run("then it should produce a tree with count 2", func(t *testing.T) {
			require.Equal(t, uint64(2), tt.count(t))
		})

		t.Run("then it should retain the other value in the tree", func(t *testing.T) {
			require.Equal(t, []kv{
				kv{
					key:   []byte{4},
					value: []byte{0, 1, 2},
				},
				kv{
					key:   []byte{6},
					value: []byte{3, 4, 5},
				},
			}, tt.list(t))
		})
	})

	t.Run("when deleting from a tree with three elements - left child", func(t *testing.T) {
		tt := newTreeTester()
		tt.insert(t, []byte{5}, []byte{1, 2, 3})
		tt.insert(t, []byte{6}, []byte{3, 4, 5})
		tt.insert(t, []byte{4}, []byte{0, 1, 2})

		fmt.Println()

		tt.delete(t, []byte{4})

		t.Run("then it should produce a tree with count 2", func(t *testing.T) {
			require.Equal(t, uint64(2), tt.count(t))
		})

		t.Run("then it should retain the other value in the tree", func(t *testing.T) {
			require.Equal(t, []kv{
				kv{
					key:   []byte{5},
					value: []byte{1, 2, 3},
				},
				kv{
					key:   []byte{6},
					value: []byte{3, 4, 5},
				},
			}, tt.list(t))
		})
	})

	t.Run("when deleting from a tree with three elements - right child", func(t *testing.T) {
		tt := newTreeTester()
		tt.insert(t, []byte{5}, []byte{1, 2, 3})
		tt.insert(t, []byte{6}, []byte{3, 4, 5})
		tt.insert(t, []byte{4}, []byte{0, 1, 2})

		fmt.Println()

		tt.delete(t, []byte{6})

		t.Run("then it should produce a tree with count 2", func(t *testing.T) {
			require.Equal(t, uint64(2), tt.count(t))
		})

		t.Run("then it should retain the other value in the tree", func(t *testing.T) {
			require.Equal(t, []kv{
				kv{
					key:   []byte{4},
					value: []byte{0, 1, 2},
				},
				kv{
					key:   []byte{5},
					value: []byte{1, 2, 3},
				},
			}, tt.list(t))
		})
	})

}
