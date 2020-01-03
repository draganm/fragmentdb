package fragment

import (
	"github.com/pkg/errors"
	"github.com/draganm/fragmentdb/store"
)

type Modifier struct {
	Fragment
	err error
}

func (m *Modifier) SetChild(i int, k store.Key) {
	if m.err != nil {
		return
	}

	ch, err := m.Fragment.Children()
	if err != nil {
		m.err = errors.Wrap(err, "while getting children")
		return
	}
	data := k.Bytes()
	if k == store.NilKey {
		data = nil
	}
	err = ch.Set(i, data)

	if err != nil {
		m.err = errors.Wrapf(err, "while setting child %d", i)
		return
	}
}

func (m *Modifier) GetChild(i int) store.Key {
	if m.err != nil {
		return store.NilKey
	}

	ch, err := m.Fragment.Children()
	if err != nil {
		m.err = errors.Wrap(err, "while getting children of the fragment")
		return store.NilKey
	}

	if i >= ch.Len() {
		m.err = errors.Wrapf(err, "trying to get child #%d which is more than %d", i, ch.Len())
		return store.NilKey
	}

	kd, err := ch.At(i)
	if err != nil {
		m.err = errors.Wrapf(err, "while getting key of the child %d", i)
		return store.NilKey
	}

	return store.BytesToKey(kd)
}

func (m *Modifier) NumberOfChildren() int {
	if m.err != nil {
		return 0
	}

	ch, err := m.Fragment.Children()
	if err != nil {
		m.err = errors.Wrap(err, "while getting children of the fragment")
		return 0
	}

	return ch.Len()
}

func (m Modifier) Error() error {
	return m.err
}

func (m *Modifier) SetError(err error) {
	if m.err == nil {
		m.err = err
	}
}
