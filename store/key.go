package store

import (
	"crypto/rand"
	"encoding/hex"

	"github.com/pkg/errors"
)

type Key [16]byte

var NilKey = Key{}

func (k Key) String() string {
	return hex.EncodeToString(k[:])
}

func (k Key) Bytes() []byte {
	b := make([]byte, len(k[:]))
	copy(b, k[:])
	return b
}

func RandomKey() Key {
	k := Key{}

	for k == NilKey {
		_, err := rand.Read(k[:])
		if err != nil {
			panic(errors.Wrap(err, "unexpected error while creating random key"))
		}
	}
	return k
}

func BytesToKey(b []byte) Key {
	k := Key{}
	copy(k[:], b)
	return k
}
