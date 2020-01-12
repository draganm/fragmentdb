package transaction

import (
	"github.com/draganm/fragmentdb/fragment"
	"github.com/pkg/errors"
)

type Type int

const TypeUnknown Type = 0
const TypeMap Type = 2
const TypeData Type = 3

func (t *Transaction) TypeOf(path string) (Type, error) {
	k, err := t.GetKey(path)

	if err == ErrNotExists {
		return TypeUnknown, err
	}

	if err != nil {
		return TypeUnknown, errors.Wrap(err, "while navigating path")
	}

	f, err := t.store.Get(k)
	if err != nil {
		return TypeUnknown, errors.Wrapf(err, "while getting fagment %q", k)
	}

	switch f.Specific().Which() {
	case fragment.Fragment_specific_Which_dataLeaf:
		return TypeData, nil
	case fragment.Fragment_specific_Which_dataNode:
		return TypeData, nil
	case fragment.Fragment_specific_Which_wbbtreeNode:
		return TypeMap, nil
	default:
		return TypeUnknown, nil
	}
}
