package trie

import (
	"fmt"

	"github.com/draganm/fragmentdb/fragment"
	"github.com/draganm/fragmentdb/store"
)

func PrintTrie(s fragment.Store, root store.Key, prefix string) error {
	f, err := s.Get(root)
	if err != nil {
		return err
	}

	fmt.Print(prefix)

	fmt.Printf("Key: %s", root)

	tm := NewTrieModifier(f)

	pr := tm.GetPrefix()
	fmt.Printf(" prefix: %x", pr)

	value := tm.GetChild(256)
	if value != store.NilKey {
		fmt.Printf(" value: %s", value)
	} else {
		fmt.Print(" no value")
	}

	fmt.Println()

	for i := 0; i < 256; i++ {
		chk := tm.GetChild(i)
		if chk != store.NilKey {
			PrintTrie(s, chk, fmt.Sprintf("%s  child %02x: ", prefix, i))
		}
	}

	return nil

}
