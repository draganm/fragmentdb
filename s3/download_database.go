package s3

import (
	"context"
	serrors "errors"

	"github.com/pkg/errors"
	"github.com/draganm/fragmentdb/fragment"
	"github.com/draganm/fragmentdb/store"
)

var ErrEmptyDatabase = serrors.New("empty database")

func (s *S3) DownloadDatabase(ctx context.Context, be store.Backend) error {
	rootKeyBytes, err := s.GetFragment(ctx, store.NilKey)
	if IsNotExists(err) {
		return ErrEmptyDatabase
	}
	if err != nil {
		return err
	}

	rootKey := store.BytesToKey(rootKeyBytes)

	toDo := []store.Key{rootKey}

	for len(toDo) > 0 {
		top := toDo[0]
		toDo = toDo[1:]
		d, err := s.GetFragment(ctx, top)
		if err != nil {
			return errors.Wrap(err, "while copying database")
		}

		err = be.Put(top, d)
		if err != nil {
			return errors.Wrapf(err, "while storing %s in backend", top.String())
		}

		f, err := fragment.BytesToFragment(d)

		if err != nil {
			return errors.Wrapf(err, "while unmarshalling fragment %s", top.String())
		}

		fm := fragment.Modifier{
			Fragment: f,
		}

		for i := 0; i < fm.NumberOfChildren(); i++ {
			ch := fm.GetChild(i)
			if ch != store.NilKey {
				toDo = append(toDo, ch)
			}

		}

		if fm.Error() != nil {
			return errors.Wrapf(err, "while getting children of %s", top.String())
		}

	}

	return be.Put(store.NilKey, rootKey.Bytes())

}
