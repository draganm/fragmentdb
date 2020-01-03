package store_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/draganm/fragmentdb/store"
)

func TestRandomKey(t *testing.T) {
	t.Run("when I create two keys", func(t *testing.T) {
		k1 := store.RandomKey()
		k2 := store.RandomKey()
		t.Run("keys should not be equal", func(t *testing.T) {
			require.NotEqual(t, k1, k2, "keys should be different")
		})
	})

	t.Run("key should never be NIL key", func(t *testing.T) {
		require.NotEqual(t, store.NilKey, store.RandomKey())
	})

}
