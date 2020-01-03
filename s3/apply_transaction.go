package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	"github.com/minio/minio-go"
	"github.com/pkg/errors"
	"github.com/draganm/fragmentdb/store"
)

const fragmentsPrefix = "fragments"

func IsNotExists(err error) bool {
	return err != nil && strings.Contains(err.Error(), "The specified key does not exist.")
}

func (s *S3) GetFragment(ctx context.Context, k store.Key) ([]byte, error) {
	obj, err := s.client.GetObjectWithContext(
		ctx,
		s.bucketName,
		fmt.Sprintf("%s/%s/%s", s.prefix, fragmentsPrefix, k.String()),
		minio.GetObjectOptions{},
	)
	if err != nil {
		return nil, errors.Wrap(err, "while getting fragment")
	}
	defer obj.Close()

	return ioutil.ReadAll(obj)
}

func (s *S3) PutFragment(ctx context.Context, k store.Key, d []byte) error {
	_, err := s.client.PutObjectWithContext(
		ctx,
		s.bucketName,
		fmt.Sprintf("%s/%s/%s", s.prefix, fragmentsPrefix, k.String()),
		bytes.NewReader(d),
		int64(len(d)),
		minio.PutObjectOptions{
			ContentType: "application/octet-stream",
		},
	)
	if err != nil {
		return errors.Wrapf(err, "while putting fragment %s", k.String())
	}

	return nil
}

func (s *S3) DeleteFragment(ctx context.Context, k store.Key) error {
	err := s.client.RemoveObject(
		s.bucketName,
		fmt.Sprintf("%s/%s/%s", s.prefix, fragmentsPrefix, k.String()),
	)
	if err != nil {
		return errors.Wrapf(err, "while deleting fragment %s", k.String())
	}

	return nil
}

func (s *S3) ApplyTransaction(ctx context.Context, id string) error {

	oldRootBytes, err := s.GetFragment(ctx, store.NilKey)
	if IsNotExists(err) {
		oldRootBytes = nil
	} else if err != nil {
		return errors.Wrap(err, "while reading old root")
	}

	oldRoot := store.BytesToKey(oldRootBytes)

	from, to, reader, err := s.ReadTransaction(ctx, id)
	if err != nil {
		return errors.Wrap(err, "while appying transaction")
	}

	defer reader.Close()

	if oldRoot != from {
		return errors.Errorf("cannot apply transaction %s: from (%s) does not match old root (%s)", id, from.String(), oldRoot.String())
	}

	for {
		k, d, err := reader.ReadFragment()
		if err == io.EOF {
			break
		}

		if err != nil {
			return errors.Wrap(err, "while reading fragment")
		}

		if len(d) > 0 {
			err = s.PutFragment(ctx, k, d)
		} else {
			err = s.DeleteFragment(ctx, k)
		}

		if err != nil {
			return errors.Wrap(err, "while applying transaction")
		}
	}

	err = s.PutFragment(ctx, store.NilKey, to.Bytes())
	if err != nil {
		return errors.Wrap(err, "while applying transaction")
	}

	return s.DeleteTransaction(ctx, id)
}
