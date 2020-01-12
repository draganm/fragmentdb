package fragmentdb

import (
	"github.com/draganm/fragmentdb/s3"
	"github.com/pkg/errors"
)

func WithS3(endpoint, accessKeyID, secretAccessKey string, secure bool, bucketName, prefix string) func(d *DB) error {
	return func(d *DB) error {
		s, err := s3.New(
			endpoint,
			accessKeyID,
			secretAccessKey,
			secure,
			bucketName,
			prefix,
			logger.WithField("ns", "S3"),
		)

		if err != nil {
			return errors.Wrap(err, "while creating S3 client")
		}

		d.s3 = s
		return nil
	}
}
