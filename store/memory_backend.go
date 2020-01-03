package store

type MemoryBackend map[Key][]byte

func NewMemoryBackendFactory() MemoryBackend {
	return MemoryBackend{}
}

func (m MemoryBackend) Get(k Key) ([]byte, error) {
	d, ok := m[k]
	if !ok {
		return nil, ErrNotFound
	}
	return d, nil
}

func (m MemoryBackend) Put(k Key, data []byte) error {
	m[k] = data
	return nil
}

func (m MemoryBackend) Delete(k Key) error {
	delete(m, k)
	return nil
}

func (m MemoryBackend) DeleteAll() error {
	for k := range m {
		delete(m, k)
	}
	return nil
}

func (m MemoryBackend) NewReadOnlyBackend() (Backend, Discard) {
	return m, func() {}
}
func (m MemoryBackend) NewReadWriteBackend() (Backend, Commit, Discard) {
	return m, func() error { return nil }, func() {}
}
