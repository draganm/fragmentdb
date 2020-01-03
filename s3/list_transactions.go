package s3

import (
	"fmt"
	"sort"
	"strings"

	"github.com/pkg/errors"
)

func (s *S3) ListTransactions() ([]string, error) {
	names := []string{}
	prefix := fmt.Sprintf("%s/%s", s.prefix, transactionsPrefix)
	for objInfo := range s.client.ListObjects(s.bucketName, prefix, true, make(chan struct{})) {
		if objInfo.Err != nil {
			return nil, errors.Wrap(objInfo.Err, "while listing transactions")
		}
		names = append(names, strings.TrimPrefix(objInfo.Key, prefix+"/"))
	}

	sort.Strings(names)

	return names, nil

}
