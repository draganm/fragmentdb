package fragmentdb_test

import (
	"context"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/minio/minio-go"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"github.com/draganm/fragmentdb"
)

func hasDocker() bool {
	cmd := exec.Command("docker", "-v")
	err := cmd.Run()
	if err != nil {
		return false
	}
	return true
}

func startMinio() (*minio.Client, func() error, error) {

	cmd := exec.Command(
		"docker", "run",
		"--name", "minio-test",
		"-e", "MINIO_ACCESS_KEY=minio",
		"-e", "MINIO_SECRET_KEY=miniostorage",
		"-p", "9000:9000",
		"minio/minio", "server", "/data",
	)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Start()
	if err != nil {
		return nil, nil, errors.Wrap(err, "while starting minio")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	for {
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}
		c, err := (&net.Dialer{}).DialContext(ctx, "tcp", "localhost:9000")
		if err != nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		c.Close()
		break
	}

	cl, err := minio.New("localhost:9000", "minio", "miniostorage", false)

	if err != nil {
		return nil, nil, errors.Wrap(err, "while creationg minio client")
	}

	return cl, func() error {

		stopCommand := exec.Command("docker", "rm", "-fv", "minio-test")
		err = stopCommand.Run()
		if err != nil {
			return err
		}

		_, err = cmd.Process.Wait()
		if err != nil {
			return errors.Wrap(err, "while waiting for minio container to shut down")
		}

		return nil

	}, nil

}

func createTestDBInstance(opts ...fragmentdb.Option) (*fragmentdb.DB, func() error, error) {
	dir, err := ioutil.TempDir("", "immersa-test")
	if err != nil {
		return nil, nil, errors.Wrap(err, "while creating temp dir")
	}

	db, err := fragmentdb.New(dir, opts...)
	if err != nil {
		return nil, nil, errors.Wrap(err, "while opening new fragmentdb")
	}

	return db, func() error {
		err := db.Close()
		if err != nil {
			return errors.Wrap(err, "while closing badger db")
		}

		return os.RemoveAll(dir)
	}, nil

}

func TestTransaction(t *testing.T) {
	t.Run("creating data in the root", func(t *testing.T) {

		db, cleanup, err := createTestDBInstance()
		require.NoError(t, err)
		defer cleanup()

		tx, err := db.NewTransaction()
		require.NoError(t, err)

		err = tx.Put("test", []byte{1, 2, 3})
		require.NoError(t, err)

		err = tx.Commit()
		require.NoError(t, err)

		t.Run("reading created data", func(t *testing.T) {

			tx, err := db.NewTransaction()
			require.NoError(t, err)

			d, err := tx.Get("test")
			require.NoError(t, err)
			require.Equal(t, []byte{1, 2, 3}, d)

		})
	})

	t.Run("detecting conflicting transactions", func(t *testing.T) {
		db, cleanup, err := createTestDBInstance()
		require.NoError(t, err)
		defer cleanup()

		tx1, err := db.NewTransaction()
		require.NoError(t, err)

		err = tx1.Put("test", []byte{1, 2, 3})
		require.NoError(t, err)

		tx2, err := db.NewTransaction()
		require.NoError(t, err)

		err = tx2.Put("test", []byte{1, 2, 3, 4})
		require.NoError(t, err)

		err = tx1.Commit()
		require.NoError(t, err)

		err = tx2.Commit()
		require.Equal(t, fragmentdb.ErrConflict, err)

		tx, err := db.NewTransaction()
		require.NoError(t, err)

		d, err := tx.Get("test")
		require.NoError(t, err)
		require.Equal(t, []byte{1, 2, 3}, d)

	})
}

func TestS3DownloadAndUpload(t *testing.T) {
	mc, shutdownMinio, err := startMinio()
	require.NoError(t, err)
	defer shutdownMinio()

	err = mc.MakeBucket("dbtest", "")
	require.NoError(t, err)

	t.Run("when S3 is empty", func(t *testing.T) {
		_, shutdownDB, err := createTestDBInstance(fragmentdb.WithS3(
			"localhost:9000",
			"minio",
			"miniostorage",
			false,
			"dbtest",
			"empty",
		))
		require.NoError(t, err)
		defer shutdownDB()

		time.Sleep(100 * time.Millisecond)

		names := []string{}
		for n := range mc.ListObjects("dbtest", "empty/fragments", true, nil) {
			names = append(names, n.Key)
		}

		require.Contains(t, names, "empty/fragments/00000000000000000000000000000000")

	})

	t.Run("when S3 not is empty", func(t *testing.T) {
		_, shutdownDB, err := createTestDBInstance(fragmentdb.WithS3(
			"localhost:9000",
			"minio",
			"miniostorage",
			false,
			"dbtest",
			"empty",
		))
		require.NoError(t, err)
		defer shutdownDB()
	})

	t.Run("when I execute a transaction", func(t *testing.T) {
		db, shutdownDB, err := createTestDBInstance(fragmentdb.WithS3(
			"localhost:9000",
			"minio",
			"miniostorage",
			false,
			"dbtest",
			"firstTx",
		))
		require.NoError(t, err)
		defer shutdownDB()

		tx, err := db.NewTransaction()
		require.NoError(t, err)

		err = tx.Put("abc", []byte{1, 2, 3})
		require.NoError(t, err)

		err = tx.Commit()
		require.NoError(t, err)

		time.Sleep(100 * time.Millisecond)

		t.Run("it should upload a new transaction to S3", func(t *testing.T) {
			names := []string{}
			for n := range mc.ListObjects("dbtest", "firstTx/transactions", true, nil) {
				names = append(names, n.Key)
			}
			require.NotEmpty(t, names)
		})
	})

	t.Run("when restart the database", func(t *testing.T) {
		db, shutdownDB, err := createTestDBInstance(fragmentdb.WithS3(
			"localhost:9000",
			"minio",
			"miniostorage",
			false,
			"dbtest",
			"firstTx",
		))

		require.NoError(t, err)
		defer shutdownDB()

		t.Run("it should have the latest state", func(t *testing.T) {
			tx, err := db.NewTransaction()
			require.NoError(t, err)

			d, err := tx.Get("abc")
			require.NoError(t, err)

			require.Equal(t, []byte{1, 2, 3}, d)
		})

	})

}
