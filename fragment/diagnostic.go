package fragment

import (
	"fmt"

	"github.com/draganm/fragmentdb/store"
)

func PrintTree(s Store, k store.Key, level int) error {

	for i := 0; i < level; i++ {
		fmt.Print("  ")
	}

	f, err := s.Get(k)
	if err != nil {
		return err
	}

	fmt.Print(k.String() + " " + f.Specific().Which().String())

	fmt.Print()

	switch f.Specific().Which() {
	case Fragment_specific_Which_dataLeaf:
		data, err := f.Specific().DataLeaf()
		if err != nil {
			return err
		}
		fmt.Printf(": 0x%x", data)
	}

	fmt.Println()

	ch, err := f.Children()
	if err != nil {
		return err
	}

	for i := 0; i < ch.Len(); i++ {
		chkd, err := ch.At(i)
		if err != nil {
			return err
		}

		chk := store.BytesToKey(chkd)

		if chk == store.NilKey {
			continue
		}

		err = PrintTree(s, chk, level+1)
		if err != nil {
			return err
		}
	}

	return nil
}
