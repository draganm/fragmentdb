package fragmentdb

import (
	"context"
	"github.com/dgraph-io/badger"
	"github.com/pkg/errors"
	"github.com/draganm/fragmentdb/fragment"
	"github.com/draganm/fragmentdb/store"
	"github.com/draganm/fragmentdb/transaction"
)

func (db *DB) Transaction(f func(tx *transaction.Transaction) error) error {
	tx, err := db.NewTransaction()
	if err != nil {
		return err
	}

	err = f(tx)

	if err != nil {
		tx.Discard()
		return err
	}

	return tx.Commit()
}

func (db *DB) ReadTransaction(f func(tx *transaction.ReadTransaction) error) error {
	tx, err := db.NewReadTransaction()
	if err != nil {
		return err
	}

	defer tx.Discard()

	return f(tx)

}

func (db *DB) NewTransaction() (*transaction.Transaction, error) {
	st, commit, discard := db.backendFactory.NewReadWriteBackend()

	currentRoot, err := st.Get(store.NilKey)
	if err != nil {
		discard()
		return nil, errors.Wrap(err, "while getting current root")
	}

	return transaction.New(
		st,
		store.NewMemoryBackendFactory(),
		store.BytesToKey(currentRoot),
		db.fragSize,
		db.fanout,
		db.applyCommit(st, commit, discard),
		discard,
	), nil
}

func (db *DB) copyFragmentAndChildren(txb store.Backend, fk store.Key, s store.Backend, hookFragments map[store.Key]struct{}) error {

	fd, err := s.Get(fk)
	if errors.Cause(err) == store.ErrNotFound {
		hookFragments[fk] = struct{}{}
		return nil
	}

	if err != nil {
		return errors.Wrap(err, "copy fragment: while reading fragment data")
	}

	err = txb.Put(fk, fd)
	if err != nil {
		return errors.Wrap(err, "copy fragment: while storing fragment data")
	}

	fs := fragment.NewStore(s)
	fr, err := fs.Get(fk)
	if err != nil {
		return errors.Wrap(err, "copy fragment: while getting fragment")
	}

	ch, err := fr.Children()
	if err != nil {
		return errors.Wrap(err, "copy fragment: while getting fragment children")
	}

	for i := 0; i < ch.Len(); i++ {
		chkb, err := ch.At(i)
		if err != nil {
			return errors.Wrapf(err, "copy fragment: while getting child #%d", i)
		}
		chk := store.BytesToKey(chkb)
		if chk == store.NilKey {
			continue
		}
		err = db.copyFragmentAndChildren(txb, chk, s, hookFragments)
		if err != nil {
			return errors.Wrap(err, "while copying children of fragment")
		}
	}

	return nil

}

var ErrConflict = errors.New("transaction is changing updated data")

func (db *DB) applyCommit(txb store.Backend, cmmt store.Commit, dis store.Discard) func(st store.Backend, oldRoot, newRoot store.Key) error {
	return func(st store.Backend, oldRoot, newRoot store.Key) (er error) {
		defer dis()

		tbe, closeTx, removeTx, err := db.s3.TeeTransactionBackend(context.Background(), txb, oldRoot, newRoot)
		if err != nil {
			return errors.Wrap(err, "while creating S3 tee backend")
		}

		defer func() {
			if er != nil {
				removeTx()
			}
		}()

		hookFragments := map[store.Key]struct{}{}

		err = db.copyFragmentAndChildren(tbe, newRoot, st, hookFragments)

		if err != nil {
			return errors.Wrap(err, "while copying new fragments")
		}

		err = txb.Put(store.NilKey, newRoot.Bytes())
		if err != nil {
			return errors.Wrap(err, "while setting new root")
		}
		err = db.collectGarbage(tbe, oldRoot, hookFragments)

		if err != nil {
			return errors.Wrap(err, "while performing garbage collection")
		}

		err = closeTx()
		if err != nil {
			return errors.Wrap(err, "while closing S3 tx")
		}

		err = cmmt()

		if err == badger.ErrConflict {
			return ErrConflict
		}

		return err

	}
}

func (db *DB) collectGarbage(txb store.Backend, fk store.Key, hookFragments map[store.Key]struct{}) error {

	if fk == store.NilKey {
		return nil
	}

	if _, isHook := hookFragments[fk]; isHook {
		return nil
	}

	fs := fragment.NewStore(txb)

	fr, err := fs.Get(fk)
	if err != nil {
		return errors.Wrapf(err, "while getting fragment %s", fk)
	}

	fm := fragment.Modifier{
		Fragment: fr,
	}

	for i := 0; i < fm.NumberOfChildren(); i++ {
		if fm.Error() != nil {
			return fm.Error()
		}

		chk := fm.GetChild(i)
		if fm.Error() != nil {
			return fm.Error()
		}

		if chk != store.NilKey {
			err = db.collectGarbage(txb, chk, hookFragments)
			if err != nil {
				return errors.Wrap(err, "while collecting garbage of a child")
			}
		}
	}

	return txb.Delete(fk)

}
