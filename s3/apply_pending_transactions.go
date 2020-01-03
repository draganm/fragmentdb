package s3

import (
	"context"

	"github.com/pkg/errors"
)

func (s *S3) ApplyPendingTransactions(ctx context.Context) error {
	txns, err := s.ListTransactions()
	if err != nil {
		return errors.Wrap(err, "while listing transactions")
	}

	for _, txid := range txns {
		s.log.WithField("txid", txid).Info("applying transaction")
		err = s.ApplyTransaction(ctx, txid)
		if err != nil {
			return errors.Wrapf(err, "while applying transaction %s", txid)
		}
	}
	return nil
}
