package s3

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/minio/minio-go"
	"github.com/pkg/errors"
	"github.com/segmentio/ksuid"
	"github.com/draganm/fragmentdb/store"
)

type TransactionWriter struct {
	wc                  io.WriteCloser
	applierNotification chan struct{}
}

const transactionsPrefix = "transactions"

func (s *S3) WriteTransaction(ctx context.Context, from, to store.Key) (TransactionWriter, string, error) {
	id := ksuid.New()
	r, w := io.Pipe()

	go s.client.PutObjectWithContext(ctx, s.bucketName, fmt.Sprintf("%s/%s/%s", s.prefix, transactionsPrefix, id.String()), r, -1, minio.PutObjectOptions{
		ContentType: "application/octet-stream",
	})

	_, err := w.Write(from.Bytes())
	if err != nil {
		return TransactionWriter{}, "", errors.Wrap(err, "while writing transaction from header")
	}

	_, err = w.Write(to.Bytes())
	if err != nil {
		return TransactionWriter{}, "", errors.Wrap(err, "while writing transaction to header")
	}

	return TransactionWriter{
		wc:                  w,
		applierNotification: s.txnAddedChan,
	}, id.String(), nil
}

func (t TransactionWriter) WriteFragment(k store.Key, d []byte) error {

	data := make([]byte, len(k.Bytes())+len(d)+4)
	binary.BigEndian.PutUint32(data, uint32(len(d)+len(k.Bytes())))
	copy(data[4:len(k.Bytes())+4], k.Bytes())
	copy(data[4+len(k.Bytes()):], d)

	_, err := t.wc.Write(data)
	if err != nil {
		return err
	}

	return nil
}

func (t TransactionWriter) Close() error {
	err := t.wc.Close()
	if err != nil {
		return err
	}

	if t.applierNotification == nil {
		return nil
	}

	select {
	case t.applierNotification <- struct{}{}:
	default:
	}

	return nil

}
