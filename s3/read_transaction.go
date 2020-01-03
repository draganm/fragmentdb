package s3

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/minio/minio-go"
	"github.com/pkg/errors"
	"github.com/draganm/fragmentdb/store"
)

type TransactionReader struct {
	r io.ReadCloser
}

func (s *S3) ReadTransaction(ctx context.Context, id string) (from store.Key, to store.Key, tr TransactionReader, err error) {
	obj, err := s.client.GetObjectWithContext(ctx, s.bucketName, fmt.Sprintf("%s/%s/%s", s.prefix, transactionsPrefix, id), minio.GetObjectOptions{})
	if err != nil {
		return store.NilKey, store.NilKey, TransactionReader{}, errors.Wrap(err, "while getting transaction object")
	}

	fromAndTo := make([]byte, len(store.NilKey.Bytes())*2)

	_, err = io.ReadFull(obj, fromAndTo)
	if err != nil {
		return store.NilKey, store.NilKey, TransactionReader{}, errors.Wrap(err, "while reading from and to key")
	}

	from = store.BytesToKey(fromAndTo[:16])
	to = store.BytesToKey(fromAndTo[16:])

	return from, to, TransactionReader{obj}, nil
}

func (r TransactionReader) ReadFragment() (store.Key, []byte, error) {
	d := make([]byte, 4)
	_, err := io.ReadFull(r.r, d)
	if err == io.ErrUnexpectedEOF {
		return store.NilKey, nil, io.EOF
	}

	if err == io.EOF {
		return store.NilKey, nil, io.EOF
	}

	if err != nil {
		return store.NilKey, nil, errors.Wrap(err, "while reading fragment header")
	}

	length := binary.BigEndian.Uint32(d)
	if length < 16 {
		return store.NilKey, nil, errors.Errorf("Too short fragment: %d", length)
	}
	keyAndData := make([]byte, length)

	_, err = io.ReadFull(r.r, keyAndData)
	if err != nil {
		return store.NilKey, nil, errors.Wrap(err, "while reading fragment")
	}

	return store.BytesToKey(keyAndData[:16]), keyAndData[16:], nil

}

func (r TransactionReader) Close() error {
	return r.r.Close()
}
