package wbbtree_test

import (
	"testing"
)

func TestTreeBalancing(t *testing.T) {

	t.Run("when I insert 12 nodes to the tree", func(t *testing.T) {
		tt := newTreeTester()

		for i := 0; i < 12; i++ {
			tt.insert(t, []byte{byte(i)}, []byte{byte(i)})
		}

		t.Run("then the tree should be balanced", func(t *testing.T) {
			tt.ensureBalanced(t)
		})
	})
}
