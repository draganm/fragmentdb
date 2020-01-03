package s3

import (
	"context"
	"github.com/minio/minio-go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func New(
	endpoint, accessKeyID, secretAccessKey string,
	secure bool,
	bucketName, prefix string,
	log logrus.FieldLogger,
) (*S3, error) {

	mc, err := minio.New(
		endpoint,
		accessKeyID,
		secretAccessKey,
		secure,
	)

	if err != nil {
		return nil, errors.Wrap(err, "while creating minio client")
	}

	s := &S3{
		client:       mc,
		prefix:       prefix,
		bucketName:   bucketName,
		txnAddedChan: make(chan struct{}, 1),
		log:          log,
	}
	go s.transactonApplier()
	return s, nil
}

type S3 struct {
	client       *minio.Client
	bucketName   string
	prefix       string
	log          logrus.FieldLogger
	txnAddedChan chan struct{}
}

func (s S3) transactonApplier() {
	for range s.txnAddedChan {
		err := s.ApplyPendingTransactions(context.Background())
		if err != nil {
			s.log.WithError(err).Error("while applying pending transactions")
		}
	}
}
