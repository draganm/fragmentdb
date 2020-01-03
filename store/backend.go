package store

import (
	"errors"
)

type Backend interface {
	Get(k Key) ([]byte, error)
	Put(k Key, data []byte) error
	Delete(k Key) error
	DeleteAll() error
}

var ErrNotFound = errors.New("key not found")

type Commit func() error
type Discard func()

type BackendFactory interface {
	NewReadOnlyBackend() (Backend, Discard)
	NewReadWriteBackend() (Backend, Commit, Discard)
}
