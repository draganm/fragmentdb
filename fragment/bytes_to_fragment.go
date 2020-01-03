package fragment

import (
	"github.com/pkg/errors"
	capnp "zombiezen.com/go/capnproto2"
)

func BytesToFragment(d []byte) (Fragment, error) {
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
