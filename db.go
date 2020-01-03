package fragmentdb

import (
	"context"
	"time"

	bdb "github.com/dgraph-io/badger"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/draganm/fragmentdb/fragment"
	"github.com/draganm/fragmentdb/s3"
	"github.com/draganm/fragmentdb/store"
	"github.com/draganm/fragmentdb/store/badger"
	"github.com/draganm/fragmentdb/trie"
)

const defaultFragSize = 128 * 1024
const defaultFanout = 128

func (db *DB) ensureNotEmpty() error {
	rwStore, commit, discard := db.backendFactory.NewReadWriteBackend()

	fs := fragment.NewStore(rwStore)
	_, err := rwStore.Get(store.NilKey)
	if errors.Cause(err) == store.ErrNotFound {

		rootTrieKey, err := trie.CreateEmpty(fs)
		if err != nil {
			return errors.Wrap(err, "while creating empty root trie")
		}

		err = rwStore.Put(store.NilKey, rootTrieKey.Bytes())
		if err != nil {
			discard()
			return errors.Wrap(err, "while storing root")
		}

		err = commit()
		if err != nil {
			return errors.Wrap(err, "while commiting new root")
		}

	} else if err != nil {
		discard()
		return errors.Wrap(err, "while reading root")
	} else {
		discard()
	}

	return nil
}

func (db *DB) syncWithS3() (err error) {
	if db.s3 == nil {
		return nil
	}

	ctx := context.Background()

	err = db.s3.ApplyPendingTransactions(ctx)
	if err != nil {
		return errors.Wrap(err, "while applying pending transactions")
	}

	rwStore, commit, discard := db.backendFactory.NewReadWriteBackend()

	defer discard()

	s := db.s3
	_, err = s.GetFragment(ctx, store.NilKey)

	if s3.IsNotExists(err) {
		err = s.UploadDatabase(ctx, rwStore)
		if err != nil {
			return errors.Wrap(err, "while uploading database")
		}
		return commit()
	} else if err != nil {
		return errors.Wrap(err, "while getting root fragment from S3")
	}

	err = rwStore.DeleteAll()
	if err != nil {
		return errors.Wrap(err, "while clearing local store")
	}
	err = db.s3.DownloadDatabase(ctx, rwStore)
	if err != nil {
		return errors.Wrap(err, "while downloading database from S3")
	}

	return commit()

}

var logger *logrus.Logger

func init() {
	logger = logrus.New()
	logger.SetLevel(logrus.InfoLevel)
	logger.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: time.RFC3339,
	})

}

func New(dir string, opts ...Option) (*DB, error) {

	bs, err := badger.New(bdb.DefaultOptions(dir).WithLogger(logger.WithFields(logrus.Fields{"ns": "badger"})))
	if err != nil {
		return nil, errors.Wrapf(err, "while opening badger db: %s", dir)
	}

	db := &DB{
		backendFactory: bs,
		fragSize:       defaultFragSize,
		fanout:         defaultFanout,
	}

	for _, o := range opts {
		err := o(db)
		if err != nil {
			return nil, errors.Wrap(err, "while applyng option")
		}
	}

	err = db.ensureNotEmpty()
	if err != nil {
		return nil, errors.Wrap(err, "while ensuring not empty")
	}

	err = db.syncWithS3()
	if err != nil {
		return nil, errors.Wrap(err, "while syncing with S3")
	}

	return db, nil

}

type DB struct {
	backendFactory *badger.BadgerStore
	fragSize       int
	fanout         int
	s3             *s3.S3
}

type Option func(d *DB) error

func (db *DB) PrintStorageStructure() {
	rob, discard := db.backendFactory.NewReadOnlyBackend()
	defer discard()
	fragment.PrintTree(fragment.NewStore(rob), store.NilKey, 0)
}

func (db *DB) Close() error {
	return db.backendFactory.Close()
}
