package s3

import (
	"context"

	"github.com/pkg/errors"
	"github.com/draganm/fragmentdb/store"
)

type teeBackend struct {
	tw   TransactionWriter
	orig store.Backend
}

func (t teeBackend) Get(k store.Key) ([]byte, error) {
	return t.orig.Get(k)
}
func (t teeBackend) Put(k store.Key, data []byte) error {
	if k != store.NilKey {
		err := t.tw.WriteFragment(k, data)
		if err != nil {
			return err
		}
	}

	return t.orig.Put(k, data)
}

func (t teeBackend) Delete(k store.Key) error {
	if k != store.NilKey {
		err := t.tw.WriteFragment(k, nil)
		if err != nil {
			return err
		}
	}
	return t.orig.Delete(k)
}

func (t teeBackend) DeleteAll() error {
	return t.orig.DeleteAll()
}

type noopWriterCloser struct{}

func (noopWriterCloser) Write(p []byte) (n int, err error) {
	return len(p), nil
}

func (noopWriterCloser) Close() error {
	return nil
}

func (s *S3) TeeTransactionBackend(
	ctx context.Context,
	orig store.Backend,
	oldRoot, newRoot store.Key,
) (tee store.Backend, closeFn, delFn func() error, err error) {
	tw := TransactionWriter{
		wc: noopWriterCloser{},
	}

	delFn = func() error {
		return nil
	}

	closeFn = func() error {
		return tw.Close()
	}

	if s != nil {
		t, id, err := s.WriteTransaction(ctx, oldRoot, newRoot)
		if err != nil {
			return nil, nil, nil, errors.Wrap(err, "while creating s3 transaction writer")
		}
		tw = t
		delFn = func() error {
			return s.DeleteTransaction(ctx, id)
		}

	}
	return teeBackend{
		tw:   tw,
		orig: orig,
	}, closeFn, delFn, nil
}
