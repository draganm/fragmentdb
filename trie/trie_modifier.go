package trie

import (
	"github.com/pkg/errors"
	"github.com/draganm/fragmentdb/fragment"
)

func NewTrieModifier(f fragment.Fragment) TrieModifier {
	return TrieModifier{
		fragment.Modifier{
			Fragment: f,
		},
	}
}

type TrieModifier struct {
	fragment.Modifier
}

func (tm *TrieModifier) SetPrefix(prefix []byte) {

	if tm.Error() != nil {
		return
	}

	err := tm.Specific().SetTrieNode(prefix)
	if err != nil {
		tm.SetError(errors.Wrap(err, "while setting trie prefi"))
		return
	}
}

func (tm *TrieModifier) SetError(err error) {
	tm.Modifier.SetError(err)
}

func (tm *TrieModifier) GetPrefix() []byte {
	if tm.Error() != nil {
		return nil
	}

	pr, err := tm.Specific().TrieNode()
	if err != nil {
		tm.SetError(err)
		return nil
	}

	return pr
}
