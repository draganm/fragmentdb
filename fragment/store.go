package fragment

import (
	"github.com/pkg/errors"
	"github.com/draganm/fragmentdb/store"
	capnp "zombiezen.com/go/capnproto2"
)

type Store struct {
	b store.Backend
}

func NewStore(b store.Backend) Store {
	return Store{b: b}
}

func (s Store) CreateWithKey(k store.Key, f func(f Fragment) error) error {
	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	if err != nil {
		return errors.Wrap(err, "while creating new capnp message")
	}
	frag, err := NewRootFragment(seg)
	if err != nil {
		return errors.Wrap(err, "while creating root capnp fragment")
	}

	err = f(frag)

	if err != nil {
		return errors.Wrap(err, "while creating fragment")
	}

	data, err := msg.Marshal()
	if err != nil {
		return errors.Wrap(err, "while marshalling fragment")
	}

	err = s.b.Put(k, data)

	if err != nil {
		return errors.Wrap(err, "while storing fragment in the backend")
	}

	return nil
}

func (s Store) Create(f func(f Fragment) error) (store.Key, error) {
	k := store.RandomKey()
	err := s.CreateWithKey(k, f)
	if err != nil {
		return store.NilKey, err
	}

	return k, nil
}

func (s Store) Get(k store.Key) (Fragment, error) {
	d, err := s.b.Get(k)
	if err != nil {
		return Fragment{}, errors.Wrapf(err, "while getting key %s from store", k.String())
	}

	msg, err := capnp.Unmarshal(d)
	if err != nil {
		return Fragment{}, errors.Wrap(err, "while unmarshalling message")
	}

	f, err := ReadRootFragment(msg)
	if err != nil {
		return Fragment{}, errors.Wrap(err, "while reading capnp Fragment")
	}

	return f, nil
}
