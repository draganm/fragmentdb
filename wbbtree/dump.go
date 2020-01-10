package wbbtree

import (
	"fmt"

	"github.com/draganm/fragmentdb/fragment"
	"github.com/draganm/fragmentdb/store"
)

func Dump(s fragment.Store, root store.Key, prefix string) {
	if root == store.NilKey {
		fmt.Println(prefix, "NIL")
		return
	}
	nr := newNodeReader(s, root)
	fmt.Printf("%sKey: %x  LC: %d RC: %d Value %s\n", prefix, nr.key(), nr.leftCount(), nr.rightCount(), nr.value())
	Dump(s, nr.leftChild(), prefix+"L:  ")
	Dump(s, nr.rightChild(), prefix+"R:  ")
}
