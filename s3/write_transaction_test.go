package s3_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/sirupsen/logrus"
	"github.com/draganm/fragmentdb/s3"
	"github.com/draganm/fragmentdb/store"
)

func TestWriteTransaction(t *testing.T) {
	if !hasDocker() {
		return
	}

	client, shutdownMinio, err := startMinio()
	require.NoError(t, err)
	defer shutdownMinio()

	bucketName := "transaction-test"

	err = client.MakeBucket(bucketName, "")
	require.NoError(t, err)

	s3, err := s3.New("localhost:9000", "minio", "miniostorage", false, bucketName, "tx1", logrus.New())
	require.NoError(t, err)

	t.Run("When I write a transaction", func(t *testing.T) {
		from := store.RandomKey()
		to := store.RandomKey()
		w, id, err := s3.WriteTransaction(context.Background(), from, to)
		require.NoError(t, err)

		deletedKey := store.RandomKey()

		err = w.WriteFragment(deletedKey, nil)
		require.NoError(t, err)

		newKey := store.RandomKey()
		err = w.WriteFragment(newKey, []byte{1, 2, 3})
		require.NoError(t, err)

		err = w.Close()
		require.NoError(t, err)

		time.Sleep(100 * time.Millisecond)

		t.Run("It should create a new transaction object", func(t *testing.T) {

			names, err := s3.ListTransactions()
			require.NoError(t, err)

			require.Equal(t, []string{id}, names)
		})

		t.Run("I should be able to read content of the transaction object", func(t *testing.T) {
			readFrom, readTo, reader, err := s3.ReadTransaction(context.Background(), id)
			require.NoError(t, err)

			require.Equal(t, from, readFrom)
			require.Equal(t, to, readTo)

			k, d, err := reader.ReadFragment()
			require.NoError(t, err)

			require.Equal(t, deletedKey, k)
			require.Equal(t, []byte{}, d)

			k, d, err = reader.ReadFragment()
			require.NoError(t, err)

			require.Equal(t, newKey, k)
			require.Equal(t, []byte{1, 2, 3}, d)

			err = reader.Close()
			require.NoError(t, err)
		})

		t.Run("when I delete the transaction", func(t *testing.T) {
			err = s3.DeleteTransaction(context.Background(), id)
			require.NoError(t, err)
			t.Run("It remve the transaction object", func(t *testing.T) {

				names, err := s3.ListTransactions()
				require.NoError(t, err)

				require.Equal(t, []string{}, names)
			})
		})
	})

}
