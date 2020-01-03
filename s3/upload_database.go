package s3

import (
	"context"

	"github.com/pkg/errors"
	"github.com/draganm/fragmentdb/fragment"
	"github.com/draganm/fragmentdb/store"
)

func (s *S3) UploadDatabase(ctx context.Context, be store.Backend) error {
	rootKeyBytes, err := be.Get(store.NilKey)
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
		d, err := be.Get(top)
		if err != nil {
			return errors.Wrap(err, "while uploading database")
		}

		err = s.PutFragment(ctx, top, d)
		if err != nil {
			return errors.Wrapf(err, "while storing %s in s3", top.String())
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

	err = s.PutFragment(ctx, store.NilKey, rootKey.Bytes())
	if err != nil {
		return errors.Wrapf(err, "while storing %s in s3", store.NilKey)
	}

	return nil

}
