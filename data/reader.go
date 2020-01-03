package data

import (
	"fmt"
	"io"

	"github.com/pkg/errors"
	"github.com/draganm/fragmentdb/fragment"
	"github.com/draganm/fragmentdb/store"
)

type reader struct {
	store        fragment.Store
	path         []int
	root         store.Key
	currentBlock []byte
}

func NewReader(root store.Key, store fragment.Store) (io.Reader, error) {
	r := &reader{
		store: store,
		root:  root,
	}

	err := r.firstBlock()
	if err != nil {
		return nil, err
	}

	return r, nil
}

func (r *reader) Read(p []byte) (n int, err error) {
	if len(r.currentBlock) == 0 {
		err := r.nextBlock()
		if err != nil {
			return 0, err
		}

		fmt.Printf("block: 0x%x\n", r.currentBlock)
	}

	n = len(p)
	if n > len(r.currentBlock) {
		n = len(r.currentBlock)
	}

	copy(p, r.currentBlock[:n])
	r.currentBlock = r.currentBlock[n:]
	return n, nil

}

func (r *reader) nextBlock() error {

	if len(r.path) == 0 {
		return io.EOF
	}

	r.path[len(r.path)-1]++

	keys := make([]store.Key, len(r.path)+1, len(r.path)+1)
	keys[0] = r.root

	for i := 0; ; i++ {
		df, err := r.store.Get(keys[i])
		if err != nil {
			return errors.Wrap(err, "while reading data fragment")
		}

		switch df.Specific().Which() {
		case fragment.Fragment_specific_Which_dataNode:
			ch, err := df.Children()

			if err != nil {
				return errors.Wrap(err, "while getting children of data node fragment")
			}

			if ch.Len() == 0 {
				return errors.Errorf("found data node with 0 children")
			}

			idx := r.path[i]

			if idx >= ch.Len() {
				// oops, drop last, increase second but last

				if i == 0 {
					return io.EOF
				}

				r.path[i] = 0

				i--
				r.path[i]++
				i--
				continue
			}

			kb, err := ch.At(idx)
			if err != nil {
				return errors.Wrap(err, "while getting first child of a data node")
			}
			keys[i+1] = store.BytesToKey(kb)

		case fragment.Fragment_specific_Which_dataLeaf:
			data, err := df.Specific().DataLeaf()
			if err != nil {
				return errors.Wrap(err, "while getting first data leaf data")
			}

			r.currentBlock = data

			return nil

		default:
			return errors.Errorf("Unexpected segment while reading data %s", df.Specific().Which())
		}
	}

}

func (r *reader) firstBlock() error {

	k := r.root

	for {
		df, err := r.store.Get(k)
		if err != nil {
			return errors.Wrap(err, "while reading data fragment")
		}

		switch df.Specific().Which() {
		case fragment.Fragment_specific_Which_dataNode:
			r.path = append(r.path, 0)
			ch, err := df.Children()

			if err != nil {
				return errors.Wrap(err, "while getting children of data node fragment")
			}

			if ch.Len() == 0 {
				return errors.Errorf("found data node with 0 children")
			}

			kb, err := ch.At(0)
			if err != nil {
				return errors.Wrap(err, "while getting first child of a data node")
			}
			k = store.BytesToKey(kb)

		case fragment.Fragment_specific_Which_dataLeaf:
			data, err := df.Specific().DataLeaf()
			if err != nil {
				return errors.Wrap(err, "while getting first data leaf data")
			}

			r.currentBlock = data

			return nil

		default:
			return errors.Errorf("Unexpected segment while reading data %q", df.Specific().Which())
		}
	}

}
