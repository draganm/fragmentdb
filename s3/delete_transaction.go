package s3

import (
	"context"
	"fmt"
)

func (s *S3) DeleteTransaction(ctx context.Context, id string) error {
	return s.client.RemoveObject(s.bucketName, fmt.Sprintf("%s/%s/%s", s.prefix, transactionsPrefix, id))
}
