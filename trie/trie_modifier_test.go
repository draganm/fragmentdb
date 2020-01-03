package trie_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/draganm/fragmentdb/fragment"
	"github.com/draganm/fragmentdb/trie"
)

func TestTrieModifier(t *testing.T) {
	tm := trie.TrieModifier{
		fragment.Modifier{
			Fragment: fragment.Fragment{},
		},
	}

	tm.SetError(errors.New("my error"))

	require.Error(t, tm.Error())
}
